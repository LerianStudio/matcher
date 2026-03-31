//go:build e2e

package journeys

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/mock"
)

// TestDiscovery_Journey exercises the full Discovery API lifecycle: status, refresh,
// connection listing/detail, schema retrieval, connection testing, extraction
// submission, polling, and fetcher unavailability scenarios.
//
// Subtests are ordered and share state through closure variables. The mock Fetcher
// server is reset and seeded at the start so every subtest operates on deterministic
// data.
func TestDiscovery_Journey(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 3*time.Minute, func(t *testing.T, _ *e2e.TestContext, apiClient *e2e.Client) {
		mockServer := getMockFetcher()
		if mockServer == nil {
			if os.Getenv(requireFetcherMockEnv) != "" {
				t.Fatal("mock Fetcher server is required for this discovery run")
			}

			t.Skip("Mock Fetcher server not available")
		}

		ctx := context.Background()
		discovery := apiClient.Discovery
		tenantID := apiClient.TenantID()

		// -----------------------------------------------------------------
		// Seed the mock with two connections, schemas, and test results.
		// -----------------------------------------------------------------
		mockServer.Reset()
		mockServer.SetHealthy(true)

		mockServer.AddConnection(mock.MockConnection{
			OrgID:        tenantID,
			ID:           "conn-postgres-001",
			ConfigName:   "postgres-orders",
			DatabaseType: "POSTGRESQL",
			Host:         "db.example.com",
			Port:         5432,
			DatabaseName: "orders_db",
			ProductName:  "PostgreSQL 15",
			Status:       "AVAILABLE",
		})
		mockServer.AddConnection(mock.MockConnection{
			OrgID:        tenantID,
			ID:           "conn-mysql-002",
			ConfigName:   "mysql-payments",
			DatabaseType: "MYSQL",
			Host:         "mysql.example.com",
			Port:         3306,
			DatabaseName: "payments_db",
			ProductName:  "MySQL 8.0",
			Status:       "AVAILABLE",
		})

		mockServer.SetSchema("conn-postgres-001", &mock.MockSchema{
			ConnectionID: "conn-postgres-001",
			Tables: []mock.MockTable{
				{TableName: "orders", Columns: []mock.MockColumn{
					{Name: "id", Type: "uuid", Nullable: false},
					{Name: "amount", Type: "decimal", Nullable: false},
					{Name: "currency", Type: "varchar", Nullable: false},
					{Name: "created_at", Type: "timestamp", Nullable: false},
				}},
				{TableName: "payments", Columns: []mock.MockColumn{
					{Name: "id", Type: "uuid", Nullable: false},
					{Name: "order_id", Type: "uuid", Nullable: false},
					{Name: "amount", Type: "decimal", Nullable: false},
				}},
			},
		})
		mockServer.SetSchema("conn-mysql-002", &mock.MockSchema{
			ConnectionID: "conn-mysql-002",
			Tables: []mock.MockTable{
				{TableName: "transactions", Columns: []mock.MockColumn{
					{Name: "id", Type: "int", Nullable: false},
					{Name: "total", Type: "decimal", Nullable: false},
					{Name: "date", Type: "datetime", Nullable: false},
				}},
			},
		})

		mockServer.SetTestResult("conn-postgres-001", &mock.MockTestResult{
			Healthy:   true,
			LatencyMs: 42,
		})
		mockServer.SetTestResult("conn-mysql-002", &mock.MockTestResult{
			Healthy:   true,
			LatencyMs: 85,
		})

		// Shared state across ordered subtests.
		var (
			postgresConnID string // Matcher's UUID for the postgres connection
			mysqlConnID    string // Matcher's UUID for the mysql connection
			extractionID   string // Matcher's UUID for the extraction request
		)

		// =================================================================
		// 01 — Status before any refresh
		// =================================================================
		t.Run("01_status_before_refresh", func(t *testing.T) {
			status, err := discovery.GetStatus(ctx)
			require.NoError(t, err)
			require.True(t, status.FetcherHealthy, "fetcher should be healthy")
			// Connection count may be 0 or > 0 depending on prior test runs;
			// the important assertion is that the endpoint works without error.
		})

		// =================================================================
		// 02 — Refresh to sync connections from the mock
		// =================================================================
		t.Run("02_refresh_populates_connections", func(t *testing.T) {
			resp, err := discovery.RefreshDiscovery(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(t, resp.ConnectionsSynced, 2, "should sync at least 2 connections")
		})

		// =================================================================
		// 03 — Status after refresh
		// =================================================================
		t.Run("03_status_after_refresh", func(t *testing.T) {
			status, err := discovery.GetStatus(ctx)
			require.NoError(t, err)
			require.True(t, status.FetcherHealthy)
			require.GreaterOrEqual(t, status.ConnectionCount, 2, "should have at least 2 connections")
			require.NotNil(t, status.LastSyncAt, "lastSyncAt should be populated after refresh")
		})

		// =================================================================
		// 04 — List connections and capture their Matcher IDs
		// =================================================================
		t.Run("04_list_connections", func(t *testing.T) {
			list, err := discovery.ListConnections(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(list.Connections), 2, "should list at least 2 connections")

			// Map by configName so we can reliably associate mock IDs with Matcher IDs.
			for i := range list.Connections {
				conn := list.Connections[i]
				switch conn.ConfigName {
				case "postgres-orders":
					postgresConnID = conn.ID
				case "mysql-payments":
					mysqlConnID = conn.ID
				}
			}

			require.NotEmpty(t, postgresConnID, "postgres connection should exist in list")
			require.NotEmpty(t, mysqlConnID, "mysql connection should exist in list")
		})

		// =================================================================
		// 05 — Get individual connections by Matcher ID
		// =================================================================
		t.Run("05_get_connection_postgres", func(t *testing.T) {
			if postgresConnID == "" {
				t.Skip("skipping: depends on 04_list_connections which did not populate postgresConnID")
			}

			conn, err := discovery.GetConnection(ctx, postgresConnID)
			require.NoError(t, err)
			require.Equal(t, postgresConnID, conn.ID)
			require.Equal(t, "postgres-orders", conn.ConfigName)
			require.Equal(t, "POSTGRESQL", conn.DatabaseType)
			require.Equal(t, "AVAILABLE", conn.Status)
		})

		t.Run("05_get_connection_mysql", func(t *testing.T) {
			if mysqlConnID == "" {
				t.Skip("skipping: depends on 04_list_connections which did not populate mysqlConnID")
			}

			conn, err := discovery.GetConnection(ctx, mysqlConnID)
			require.NoError(t, err)
			require.Equal(t, mysqlConnID, conn.ID)
			require.Equal(t, "mysql-payments", conn.ConfigName)
			require.Equal(t, "MYSQL", conn.DatabaseType)
		})

		// =================================================================
		// 06 — Get connection with non-existent ID returns 404
		// =================================================================
		t.Run("06_get_connection_not_found", func(t *testing.T) {
			_, err := discovery.GetConnection(ctx, "00000000-0000-0000-0000-000000000099")
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "error should be APIError")
			require.Equal(t, http.StatusNotFound, apiErr.StatusCode, "expected 404")
		})

		// =================================================================
		// 07 — Get connection schema
		// =================================================================
		t.Run("07_get_connection_schema", func(t *testing.T) {
			if postgresConnID == "" {
				t.Skip("skipping: depends on 04_list_connections which did not populate postgresConnID")
			}

			schema, err := discovery.GetConnectionSchema(ctx, postgresConnID)
			require.NoError(t, err)
			require.NotEmpty(t, schema.ConnectionID, "connectionId should be present")
			require.GreaterOrEqual(t, len(schema.Tables), 2, "postgres schema should have at least 2 tables")

			// Verify the seeded table structure.
			tableNames := make(map[string]bool, len(schema.Tables))
			for _, tbl := range schema.Tables {
				tableNames[tbl.TableName] = true
			}

			require.True(t, tableNames["orders"], "schema should include orders table")
			require.True(t, tableNames["payments"], "schema should include payments table")

			// Verify column detail on the orders table.
			for _, tbl := range schema.Tables {
				if tbl.TableName == "orders" {
					require.GreaterOrEqual(t, len(tbl.Columns), 4, "orders table should have at least 4 columns")

					colNames := make(map[string]bool, len(tbl.Columns))
					for _, col := range tbl.Columns {
						colNames[col.Name] = true
					}

					require.True(t, colNames["id"], "orders should have id column")
					require.True(t, colNames["amount"], "orders should have amount column")
					require.True(t, colNames["currency"], "orders should have currency column")
					require.True(t, colNames["created_at"], "orders should have created_at column")
				}
			}
		})

		// =================================================================
		// 08 — Test connection (healthy)
		// =================================================================
		t.Run("08_test_connection_healthy", func(t *testing.T) {
			if postgresConnID == "" {
				t.Skip("skipping: depends on 04_list_connections which did not populate postgresConnID")
			}

			result, err := discovery.TestConnection(ctx, postgresConnID)
			require.NoError(t, err)
			require.True(t, result.Healthy, "connection should be healthy")
			require.Greater(t, result.LatencyMs, int64(0), "latency should be positive")
			require.Empty(t, result.ErrorMessage, "no error message for healthy connection")
		})

		// =================================================================
		// 09 — Test connection (unhealthy): mutate mock, then verify
		// =================================================================
		t.Run("09_test_connection_unhealthy", func(t *testing.T) {
			if mysqlConnID == "" {
				t.Skip("skipping: depends on 04_list_connections which did not populate mysqlConnID")
			}

			// Make the mysql connection unhealthy in the mock.
			mockServer.SetTestResult("conn-mysql-002", &mock.MockTestResult{
				Healthy:      false,
				LatencyMs:    0,
				ErrorMessage: "connection timed out",
			})

			result, err := discovery.TestConnection(ctx, mysqlConnID)
			require.NoError(t, err)
			require.False(t, result.Healthy, "connection should be unhealthy")

			// Restore the mysql connection to healthy for subsequent tests.
			mockServer.SetTestResult("conn-mysql-002", &mock.MockTestResult{
				Healthy:   true,
				LatencyMs: 85,
			})
		})

		// =================================================================
		// 10 — Start an extraction job
		// =================================================================
		t.Run("10_start_extraction", func(t *testing.T) {
			if postgresConnID == "" {
				t.Skip("skipping: depends on 04_list_connections which did not populate postgresConnID")
			}

			// The mock auto-creates a job when SubmitExtraction is called.
			// The jobID will be "job-conn-postgres-001" because the Fetcher
			// client sends FetcherConnID as the connectionId in the request body.
			extraction, err := discovery.StartExtraction(ctx, postgresConnID, client.DiscoveryStartExtractionRequest{
				Tables: map[string]client.DiscoveryExtractionTableRequest{
					"orders": {Columns: []string{"id", "amount", "currency", "created_at"}},
				},
				StartDate: "2025-01-01",
				EndDate:   "2025-12-31",
			})
			require.NoError(t, err)
			require.NotEmpty(t, extraction.ID, "extraction should have an ID")
			require.NotEmpty(t, extraction.Status, "extraction should have a status")

			extractionID = extraction.ID
		})

		// =================================================================
		// 11 — Get the extraction request by ID
		// =================================================================
		t.Run("11_get_extraction", func(t *testing.T) {
			if extractionID == "" {
				t.Skip("skipping: depends on 10_start_extraction which did not populate extractionID")
			}

			extraction, err := discovery.GetExtraction(ctx, extractionID)
			require.NoError(t, err)
			require.Equal(t, extractionID, extraction.ID)
			require.Equal(t, postgresConnID, extraction.ConnectionID)
			require.NotEmpty(t, extraction.Tables, "extraction should have tables")
			require.Contains(t, extraction.Tables, "orders", "extraction should include orders table")
		})

		// =================================================================
		// 12 — Poll extraction: simulate RUNNING status
		// =================================================================
		t.Run("12_poll_extraction_running", func(t *testing.T) {
			if extractionID == "" {
				t.Skip("skipping: depends on 10_start_extraction which did not populate extractionID")
			}

			// Set the mock job to RUNNING with 50% progress.
			mockServer.SetJobStatus("job-conn-postgres-001", "RUNNING", 50)

			extraction, err := discovery.PollExtraction(ctx, extractionID)
			require.NoError(t, err)

			// The Fetcher client normalizes RUNNING -> EXTRACTING in certain paths,
			// but the actual value depends on the exact transition logic. Accept
			// both RUNNING and EXTRACTING as valid in-progress states.
			require.Contains(t,
				[]string{"RUNNING", "EXTRACTING", "SUBMITTED"},
				extraction.Status,
				"extraction should be in an active state, got: %s", extraction.Status,
			)
		})

		// =================================================================
		// 13 — Poll extraction: simulate COMPLETE status
		// =================================================================
		t.Run("13_poll_extraction_complete", func(t *testing.T) {
			if extractionID == "" {
				t.Skip("skipping: depends on 10_start_extraction which did not populate extractionID")
			}

			// Set the mock job to COMPLETE with a valid result path.
			mockServer.AddJob(mock.MockExtractionJob{
				JobID:      "job-conn-postgres-001",
				Status:     "COMPLETE",
				Progress:   100,
				ResultPath: "/data/extractions/orders-2025.csv",
			})

			extraction, err := discovery.PollExtraction(ctx, extractionID)
			require.NoError(t, err)
			require.Equal(t, "COMPLETE", extraction.Status, "extraction should be complete")
		})

		// =================================================================
		// 14 — Get extraction not found
		// =================================================================
		t.Run("14_get_extraction_not_found", func(t *testing.T) {
			_, err := discovery.GetExtraction(ctx, "00000000-0000-0000-0000-000000000099")
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "error should be APIError")
			require.Equal(t, http.StatusNotFound, apiErr.StatusCode, "expected 404 for missing extraction")
		})

		// =================================================================
		// 15 — Fetcher unavailable: test, refresh, and extraction should fail
		// =================================================================
		t.Run("15_fetcher_unavailable", func(t *testing.T) {
			if postgresConnID == "" {
				t.Skip("skipping: depends on 04_list_connections which did not populate postgresConnID")
			}

			// Make the mock unhealthy.
			mockServer.SetHealthy(false)
			defer mockServer.SetHealthy(true)

			// TestConnection should fail.
			_, testErr := discovery.TestConnection(ctx, postgresConnID)
			require.Error(t, testErr, "test connection should fail when fetcher is unhealthy")

			var testAPIErr *client.APIError
			if errors.As(testErr, &testAPIErr) {
				require.Equal(t, http.StatusServiceUnavailable, testAPIErr.StatusCode,
					"test connection should return 503 when fetcher is unavailable")
			}

			// Refresh should fail.
			_, refreshErr := discovery.RefreshDiscovery(ctx)
			require.Error(t, refreshErr, "refresh should fail when fetcher is unhealthy")

			var refreshAPIErr *client.APIError
			if errors.As(refreshErr, &refreshAPIErr) {
				require.Equal(t, http.StatusServiceUnavailable, refreshAPIErr.StatusCode,
					"refresh should return 503 when fetcher is unavailable")
			}

			// Starting a new extraction should also fail.
			_, extractionErr := discovery.StartExtraction(ctx, postgresConnID, client.DiscoveryStartExtractionRequest{
				Tables: map[string]client.DiscoveryExtractionTableRequest{
					"orders": {Columns: []string{"id", "amount"}},
				},
			})
			require.Error(t, extractionErr, "start extraction should fail when fetcher is unhealthy")

			var extractionAPIErr *client.APIError
			if errors.As(extractionErr, &extractionAPIErr) {
				require.Equal(t, http.StatusServiceUnavailable, extractionAPIErr.StatusCode,
					"start extraction should return 503 when fetcher is unavailable")
			}

			// Existing extraction requests may already be terminal locally, so polling
			// them is not a reliable Fetcher-unavailable assertion here. Starting a new
			// extraction exercises the upstream dependency deterministically.
		})

		// =================================================================
		// 16 — Second refresh is idempotent (connections already exist)
		// =================================================================
		t.Run("16_refresh_idempotent", func(t *testing.T) {
			resp, err := discovery.RefreshDiscovery(ctx)
			require.NoError(t, err)
			// The syncer should still report synced connections (upserts).
			require.GreaterOrEqual(t, resp.ConnectionsSynced, 2)
		})

		// =================================================================
		// 17 — Bad UUID in path returns 400
		// =================================================================
		t.Run("17_get_connection_bad_uuid", func(t *testing.T) {
			_, err := discovery.GetConnection(ctx, "not-a-valid-uuid")
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "error should be APIError")
			require.Equal(t, http.StatusBadRequest, apiErr.StatusCode, "expected 400 for invalid UUID")
		})

		// =================================================================
		// 18 — Schema for non-existent connection returns 404
		// =================================================================
		t.Run("18_get_schema_not_found", func(t *testing.T) {
			_, err := discovery.GetConnectionSchema(ctx, "00000000-0000-0000-0000-000000000099")
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "error should be APIError")
			require.Equal(t, http.StatusNotFound, apiErr.StatusCode, "expected 404 for missing connection schema")
		})

		// =================================================================
		// 19 — Test connection for non-existent ID returns 404
		// =================================================================
		t.Run("19_test_connection_not_found", func(t *testing.T) {
			_, err := discovery.TestConnection(ctx, "00000000-0000-0000-0000-000000000099")
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "error should be APIError")
			require.Equal(t, http.StatusNotFound, apiErr.StatusCode, "expected 404 for missing connection")
		})

		// =================================================================
		// 20 — MySQL connection schema verification
		// =================================================================
		t.Run("20_get_mysql_connection_schema", func(t *testing.T) {
			if mysqlConnID == "" {
				t.Skip("skipping: depends on 04_list_connections which did not populate mysqlConnID")
			}

			schema, err := discovery.GetConnectionSchema(ctx, mysqlConnID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(schema.Tables), 1, "mysql schema should have at least 1 table")

			tableNames := make(map[string]bool, len(schema.Tables))
			for _, tbl := range schema.Tables {
				tableNames[tbl.TableName] = true
			}

			require.True(t, tableNames["transactions"], "schema should include transactions table")
		})
	})
}
