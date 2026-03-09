//go:build unit

package fetcher

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetcherHealthResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{"status":"ok"}`

	var resp fetcherHealthResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "ok", resp.Status)
}

func TestFetcherHealthResponse_Marshal(t *testing.T) {
	t.Parallel()

	resp := fetcherHealthResponse{Status: "healthy"}

	data, err := json.Marshal(resp)

	require.NoError(t, err)
	assert.JSONEq(t, `{"status":"healthy"}`, string(data))
}

func TestFetcherConnectionResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"id": "conn-abc",
		"configName": "prod-pg",
		"databaseType": "POSTGRESQL",
		"host": "db.example.com",
		"port": 5432,
		"databaseName": "production",
		"productName": "PostgreSQL 16.2",
		"status": "AVAILABLE"
	}`

	var resp fetcherConnectionResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-abc", resp.ID)
	assert.Equal(t, "prod-pg", resp.ConfigName)
	assert.Equal(t, "POSTGRESQL", resp.DatabaseType)
	assert.Equal(t, "db.example.com", resp.Host)
	assert.Equal(t, 5432, resp.Port)
	assert.Equal(t, "production", resp.DatabaseName)
	assert.Equal(t, "PostgreSQL 16.2", resp.ProductName)
	assert.Equal(t, "AVAILABLE", resp.Status)
}

func TestFetcherConnectionListResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"connections": [
			{"id": "conn-1", "configName": "db1", "databaseType": "POSTGRESQL", "host": "h1", "port": 5432, "databaseName": "d1", "productName": "pg", "status": "AVAILABLE"},
			{"id": "conn-2", "configName": "db2", "databaseType": "MYSQL", "host": "h2", "port": 3306, "databaseName": "d2", "productName": "mysql", "status": "UNREACHABLE"}
		]
	}`

	var resp fetcherConnectionListResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	require.Len(t, resp.Connections, 2)
	assert.Equal(t, "conn-1", resp.Connections[0].ID)
	assert.Equal(t, "conn-2", resp.Connections[1].ID)
	assert.Equal(t, "MYSQL", resp.Connections[1].DatabaseType)
}

func TestFetcherConnectionListResponse_EmptyConnections(t *testing.T) {
	t.Parallel()

	raw := `{"connections": []}`

	var resp fetcherConnectionListResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Empty(t, resp.Connections)
}

func TestFetcherColumnResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{"name": "amount", "type": "decimal", "nullable": false}`

	var resp fetcherColumnResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "amount", resp.Name)
	assert.Equal(t, "decimal", resp.Type)
	assert.False(t, resp.Nullable)
}

func TestFetcherColumnResponse_Nullable(t *testing.T) {
	t.Parallel()

	raw := `{"name": "description", "type": "text", "nullable": true}`

	var resp fetcherColumnResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.True(t, resp.Nullable)
}

func TestFetcherTableResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"tableName": "transactions",
		"columns": [
			{"name": "id", "type": "uuid", "nullable": false},
			{"name": "amount", "type": "decimal", "nullable": false},
			{"name": "note", "type": "text", "nullable": true}
		]
	}`

	var resp fetcherTableResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "transactions", resp.TableName)
	require.Len(t, resp.Columns, 3)
	assert.Equal(t, "id", resp.Columns[0].Name)
	assert.Equal(t, "uuid", resp.Columns[0].Type)
	assert.True(t, resp.Columns[2].Nullable)
}

func TestFetcherSchemaResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"connectionId": "conn-abc",
		"tables": [
			{
				"tableName": "accounts",
				"columns": [
					{"name": "id", "type": "uuid", "nullable": false}
				]
			}
		]
	}`

	var resp fetcherSchemaResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-abc", resp.ConnectionID)
	require.Len(t, resp.Tables, 1)
	assert.Equal(t, "accounts", resp.Tables[0].TableName)
	require.Len(t, resp.Tables[0].Columns, 1)
}

func TestFetcherSchemaResponse_EmptyTables(t *testing.T) {
	t.Parallel()

	raw := `{"connectionId": "conn-xyz", "tables": []}`

	var resp fetcherSchemaResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-xyz", resp.ConnectionID)
	assert.Empty(t, resp.Tables)
}

func TestFetcherTestResponse_Unmarshal_Healthy(t *testing.T) {
	t.Parallel()

	raw := `{"connectionId": "conn-1", "healthy": true, "latencyMs": 42}`

	var resp fetcherTestResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-1", resp.ConnectionID)
	assert.True(t, resp.Healthy)
	assert.Equal(t, int64(42), resp.LatencyMs)
	assert.Empty(t, resp.ErrorMessage)
}

func TestFetcherTestResponse_Unmarshal_Unhealthy(t *testing.T) {
	t.Parallel()

	raw := `{"connectionId": "conn-2", "healthy": false, "latencyMs": 0, "errorMessage": "connection refused"}`

	var resp fetcherTestResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.False(t, resp.Healthy)
	assert.Equal(t, "connection refused", resp.ErrorMessage)
}

func TestFetcherTestResponse_OmitsEmptyError(t *testing.T) {
	t.Parallel()

	resp := fetcherTestResponse{ConnectionID: "conn-1", Healthy: true, LatencyMs: 5}

	data, err := json.Marshal(resp)

	require.NoError(t, err)
	assert.NotContains(t, string(data), "errorMessage")
}

func TestFetcherExtractionSubmitRequest_Marshal(t *testing.T) {
	t.Parallel()

	req := fetcherExtractionSubmitRequest{
		ConnectionID: "conn-abc",
		Tables: map[string]fetcherExtractionTable{
			"transactions": {
				Columns:   []string{"id", "amount"},
				StartDate: "2026-01-01",
				EndDate:   "2026-01-31",
			},
		},
		Filters: map[string]interface{}{
			"currency": "USD",
		},
	}

	data, err := json.Marshal(req)

	require.NoError(t, err)

	var roundTrip fetcherExtractionSubmitRequest

	err = json.Unmarshal(data, &roundTrip)

	require.NoError(t, err)
	assert.Equal(t, "conn-abc", roundTrip.ConnectionID)
	require.Contains(t, roundTrip.Tables, "transactions")
	assert.Equal(t, []string{"id", "amount"}, roundTrip.Tables["transactions"].Columns)
	assert.Equal(t, "USD", roundTrip.Filters["currency"])
}

func TestFetcherExtractionSubmitRequest_OmitsEmptyFilters(t *testing.T) {
	t.Parallel()

	req := fetcherExtractionSubmitRequest{
		ConnectionID: "conn-1",
		Tables:       map[string]fetcherExtractionTable{},
	}

	data, err := json.Marshal(req)

	require.NoError(t, err)
	assert.NotContains(t, string(data), "filters")
}

func TestFetcherExtractionTable_OmitsEmptyFields(t *testing.T) {
	t.Parallel()

	tbl := fetcherExtractionTable{}

	data, err := json.Marshal(tbl)

	require.NoError(t, err)
	assert.NotContains(t, string(data), "columns")
	assert.NotContains(t, string(data), "startDate")
	assert.NotContains(t, string(data), "endDate")
}

func TestFetcherExtractionSubmitResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{"jobId": "job-12345"}`

	var resp fetcherExtractionSubmitResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "job-12345", resp.JobID)
}

func TestFetcherExtractionStatusResponse_Unmarshal_Running(t *testing.T) {
	t.Parallel()

	raw := `{"jobId": "job-1", "status": "RUNNING", "progress": 50}`

	var resp fetcherExtractionStatusResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "job-1", resp.JobID)
	assert.Equal(t, "RUNNING", resp.Status)
	assert.Equal(t, 50, resp.Progress)
	assert.Empty(t, resp.ResultPath)
	assert.Empty(t, resp.ErrorMessage)
}

func TestFetcherExtractionStatusResponse_Unmarshal_Complete(t *testing.T) {
	t.Parallel()

	raw := `{"jobId": "job-2", "status": "COMPLETE", "progress": 100, "resultPath": "/data/job-2.json"}`

	var resp fetcherExtractionStatusResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", resp.Status)
	assert.Equal(t, 100, resp.Progress)
	assert.Equal(t, "/data/job-2.json", resp.ResultPath)
}

func TestFetcherExtractionStatusResponse_Unmarshal_Failed(t *testing.T) {
	t.Parallel()

	raw := `{"jobId": "job-3", "status": "FAILED", "progress": 30, "errorMessage": "timeout"}`

	var resp fetcherExtractionStatusResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "FAILED", resp.Status)
	assert.Equal(t, 30, resp.Progress)
	assert.Equal(t, "timeout", resp.ErrorMessage)
	assert.Empty(t, resp.ResultPath)
}

func TestFetcherExtractionStatusResponse_OmitsEmptyOptionalFields(t *testing.T) {
	t.Parallel()

	resp := fetcherExtractionStatusResponse{
		JobID:    "job-1",
		Status:   "RUNNING",
		Progress: 10,
	}

	data, err := json.Marshal(resp)

	require.NoError(t, err)
	assert.NotContains(t, string(data), "resultPath")
	assert.NotContains(t, string(data), "errorMessage")
}
