//go:build unit

package ports_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface compliance check.
// Ensures that any future implementation satisfies the FetcherClient contract.
var _ ports.FetcherClient = (*mockFetcherClient)(nil)

type mockFetcherClient struct {
	healthy     bool
	connections []*ports.FetcherConnection
	schema      *ports.FetcherSchema
	testResult  *ports.FetcherTestResult
	jobID       string
	jobStatus   *ports.ExtractionJobStatus
	err         error
}

func (m *mockFetcherClient) IsHealthy(_ context.Context) bool {
	return m.healthy
}

func (m *mockFetcherClient) ListConnections(_ context.Context, _ string) ([]*ports.FetcherConnection, error) {
	return m.connections, m.err
}

func (m *mockFetcherClient) GetSchema(_ context.Context, _ string) (*ports.FetcherSchema, error) {
	return m.schema, m.err
}

func (m *mockFetcherClient) TestConnection(_ context.Context, _ string) (*ports.FetcherTestResult, error) {
	return m.testResult, m.err
}

func (m *mockFetcherClient) SubmitExtractionJob(_ context.Context, _ ports.ExtractionJobInput) (string, error) {
	return m.jobID, m.err
}

func (m *mockFetcherClient) GetExtractionJobStatus(_ context.Context, _ string) (*ports.ExtractionJobStatus, error) {
	return m.jobStatus, m.err
}

func TestFetcherClientInterfaceSatisfaction(t *testing.T) {
	t.Parallel()

	client := &mockFetcherClient{healthy: true}

	var fc ports.FetcherClient = client
	healthy := fc.IsHealthy(context.Background())

	assert.True(t, healthy)
}

func TestFetcherClientInterfaceSatisfaction_ListConnections(t *testing.T) {
	t.Parallel()

	expected := []*ports.FetcherConnection{
		{
			ID:           "conn-1",
			ConfigName:   "prod-db",
			DatabaseType: "POSTGRESQL",
			Host:         "db.example.com",
			Port:         5432,
			DatabaseName: "production",
			ProductName:  "PostgreSQL 16",
			Status:       "AVAILABLE",
		},
	}

	client := &mockFetcherClient{connections: expected}

	var fc ports.FetcherClient = client
	conns, err := fc.ListConnections(context.Background(), "org-1")

	require.NoError(t, err)
	require.Len(t, conns, 1)
	assert.Equal(t, "conn-1", conns[0].ID)
	assert.Equal(t, "POSTGRESQL", conns[0].DatabaseType)
	assert.Equal(t, 5432, conns[0].Port)
}

func TestFetcherClientInterfaceSatisfaction_ErrorPath(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("fetcher unavailable")
	client := &mockFetcherClient{err: expectedErr}

	var fc ports.FetcherClient = client
	conns, err := fc.ListConnections(context.Background(), "org-1")

	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, conns)
}

func TestFetcherConnectionFields(t *testing.T) {
	t.Parallel()

	conn := &ports.FetcherConnection{
		ID:           "test-id",
		ConfigName:   "test-config",
		DatabaseType: "POSTGRESQL",
		Host:         "localhost",
		Port:         5432,
		DatabaseName: "testdb",
		ProductName:  "PostgreSQL 16",
		Status:       "AVAILABLE",
	}

	assert.Equal(t, "test-id", conn.ID)
	assert.Equal(t, "test-config", conn.ConfigName)
	assert.Equal(t, "POSTGRESQL", conn.DatabaseType)
	assert.Equal(t, "localhost", conn.Host)
	assert.Equal(t, 5432, conn.Port)
	assert.Equal(t, "testdb", conn.DatabaseName)
	assert.Equal(t, "PostgreSQL 16", conn.ProductName)
	assert.Equal(t, "AVAILABLE", conn.Status)
}

func TestFetcherSchemaFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	schema := &ports.FetcherSchema{
		ConnectionID: "conn-1",
		Tables: []ports.FetcherTableSchema{
			{
				TableName: "transactions",
				Columns: []ports.FetcherColumnInfo{
					{Name: "id", Type: "uuid", Nullable: false},
					{Name: "amount", Type: "decimal", Nullable: false},
					{Name: "description", Type: "text", Nullable: true},
				},
			},
		},
		DiscoveredAt: now,
	}

	assert.Equal(t, "conn-1", schema.ConnectionID)
	assert.Equal(t, now, schema.DiscoveredAt)
	require.Len(t, schema.Tables, 1)
	assert.Equal(t, "transactions", schema.Tables[0].TableName)
	require.Len(t, schema.Tables[0].Columns, 3)
	assert.Equal(t, "id", schema.Tables[0].Columns[0].Name)
	assert.False(t, schema.Tables[0].Columns[0].Nullable)
	assert.True(t, schema.Tables[0].Columns[2].Nullable)
}

func TestFetcherTestResultFields(t *testing.T) {
	t.Parallel()

	result := &ports.FetcherTestResult{
		ConnectionID: "conn-1",
		Healthy:      true,
		LatencyMs:    42,
		ErrorMessage: "",
	}

	assert.Equal(t, "conn-1", result.ConnectionID)
	assert.True(t, result.Healthy)
	assert.Equal(t, int64(42), result.LatencyMs)
	assert.Empty(t, result.ErrorMessage)
}

func TestFetcherTestResultFields_Unhealthy(t *testing.T) {
	t.Parallel()

	result := &ports.FetcherTestResult{
		ConnectionID: "conn-2",
		Healthy:      false,
		LatencyMs:    0,
		ErrorMessage: "connection refused",
	}

	assert.False(t, result.Healthy)
	assert.Equal(t, "connection refused", result.ErrorMessage)
}

func TestExtractionJobInputFields(t *testing.T) {
	t.Parallel()

	input := ports.ExtractionJobInput{
		ConnectionID: "conn-1",
		Tables: map[string]ports.ExtractionTableConfig{
			"transactions": {
				Columns:   []string{"id", "amount"},
				StartDate: "2026-03-01",
				EndDate:   "2026-03-08",
			},
		},
		Filters: map[string]interface{}{
			"currency": "USD",
		},
	}

	assert.Equal(t, "conn-1", input.ConnectionID)
	require.Len(t, input.Tables, 1)

	txConfig, ok := input.Tables["transactions"]
	require.True(t, ok)
	assert.Equal(t, []string{"id", "amount"}, txConfig.Columns)
	assert.Equal(t, "2026-03-01", txConfig.StartDate)
	assert.Equal(t, "2026-03-08", txConfig.EndDate)
	assert.Equal(t, "USD", input.Filters["currency"])
}

func TestExtractionJobStatusFields(t *testing.T) {
	t.Parallel()

	status := &ports.ExtractionJobStatus{
		JobID:        "job-1",
		Status:       "COMPLETE",
		Progress:     100,
		ResultPath:   "/data/results/job-1.json",
		ErrorMessage: "",
	}

	assert.Equal(t, "job-1", status.JobID)
	assert.Equal(t, "COMPLETE", status.Status)
	assert.Equal(t, 100, status.Progress)
	assert.Equal(t, "/data/results/job-1.json", status.ResultPath)
	assert.Empty(t, status.ErrorMessage)
}

func TestExtractionJobStatusFields_Failed(t *testing.T) {
	t.Parallel()

	status := &ports.ExtractionJobStatus{
		JobID:        "job-2",
		Status:       "FAILED",
		Progress:     45,
		ResultPath:   "",
		ErrorMessage: "connection timeout during extraction",
	}

	assert.Equal(t, "FAILED", status.Status)
	assert.Equal(t, 45, status.Progress)
	assert.Empty(t, status.ResultPath)
	assert.Equal(t, "connection timeout during extraction", status.ErrorMessage)
}
