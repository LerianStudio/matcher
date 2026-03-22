//go:build unit

package ports_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

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
		Filters: &ports.ExtractionFilters{Equals: map[string]string{"currency": "USD"}},
	}

	assert.Equal(t, "conn-1", input.ConnectionID)
	require.Len(t, input.Tables, 1)

	txConfig, ok := input.Tables["transactions"]
	require.True(t, ok)
	assert.Equal(t, []string{"id", "amount"}, txConfig.Columns)
	assert.Equal(t, "2026-03-01", txConfig.StartDate)
	assert.Equal(t, "2026-03-08", txConfig.EndDate)
	require.NotNil(t, input.Filters)
	assert.Equal(t, "USD", input.Filters.Equals["currency"])
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
