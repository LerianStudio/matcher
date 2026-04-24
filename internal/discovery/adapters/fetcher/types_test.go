// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
		"type": "POSTGRESQL",
		"host": "db.example.com",
		"port": 5432,
		"schema": "public",
		"databaseName": "production",
		"userName": "admin",
		"productName": "PostgreSQL 16.2",
		"createdAt": "2026-01-15T10:00:00Z",
		"updatedAt": "2026-01-16T12:00:00Z"
	}`

	var resp fetcherConnectionResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-abc", resp.ID)
	assert.Equal(t, "prod-pg", resp.ConfigName)
	assert.Equal(t, "POSTGRESQL", resp.Type)
	assert.Equal(t, "db.example.com", resp.Host)
	assert.Equal(t, 5432, resp.Port)
	assert.Equal(t, "public", resp.Schema)
	assert.Equal(t, "production", resp.DatabaseName)
	assert.Equal(t, "admin", resp.UserName)
	assert.Equal(t, "PostgreSQL 16.2", resp.ProductName)
	assert.Equal(t, "2026-01-15T10:00:00Z", resp.CreatedAt)
	assert.Equal(t, "2026-01-16T12:00:00Z", resp.UpdatedAt)
}

func TestFetcherConnectionResponse_Unmarshal_IgnoresSSLObject(t *testing.T) {
	t.Parallel()

	raw := `{
		"id": "conn-ssl",
		"configName": "prod-pg",
		"type": "POSTGRESQL",
		"host": "db.example.com",
		"port": 5432,
		"databaseName": "production",
		"userName": "admin",
		"productName": "PostgreSQL 16.2",
		"ssl": {"mode": "require"}
	}`

	var resp fetcherConnectionResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-ssl", resp.ID)
}

func TestFetcherConnectionResponse_Unmarshal_IgnoresLegacySSLBoolean(t *testing.T) {
	t.Parallel()

	raw := `{
		"id": "conn-nossl",
		"configName": "dev-pg",
		"type": "POSTGRESQL",
		"host": "localhost",
		"port": 5432,
		"databaseName": "dev",
		"userName": "dev",
		"ssl": false
	}`

	var resp fetcherConnectionResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-nossl", resp.ID)
}

func TestFetcherConnectionListResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"items": [
			{"id": "conn-1", "configName": "db1", "type": "POSTGRESQL", "host": "h1", "port": 5432, "databaseName": "d1", "productName": "pg"},
			{"id": "conn-2", "configName": "db2", "type": "MYSQL", "host": "h2", "port": 3306, "databaseName": "d2", "productName": "mysql"}
		],
		"page": 1,
		"limit": 10,
		"total": 2
	}`

	var resp fetcherConnectionListResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	require.Len(t, resp.Items, 2)
	assert.Equal(t, "conn-1", resp.Items[0].ID)
	assert.Equal(t, "conn-2", resp.Items[1].ID)
	assert.Equal(t, "MYSQL", resp.Items[1].Type)
	assert.Equal(t, 1, resp.Page)
	assert.Equal(t, 10, resp.Limit)
	assert.Equal(t, 2, resp.Total)
}

func TestFetcherConnectionListResponse_EmptyItems(t *testing.T) {
	t.Parallel()

	raw := `{"items": []}`

	var resp fetcherConnectionListResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Empty(t, resp.Items)
}

func TestFetcherTableResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"name": "transactions",
		"fields": ["id", "amount", "note"]
	}`

	var resp fetcherTableResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "transactions", resp.Name)
	require.Len(t, resp.Fields, 3)
	assert.Equal(t, "id", resp.Fields[0])
	assert.Equal(t, "amount", resp.Fields[1])
	assert.Equal(t, "note", resp.Fields[2])
}

func TestFetcherSchemaResponse_Unmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"id": "conn-abc",
		"configName": "prod-db",
		"databaseName": "production",
		"type": "POSTGRESQL",
		"tables": [
			{
				"name": "accounts",
				"fields": ["id", "name", "balance"]
			}
		]
	}`

	var resp fetcherSchemaResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-abc", resp.ID)
	assert.Equal(t, "prod-db", resp.ConfigName)
	assert.Equal(t, "production", resp.DatabaseName)
	assert.Equal(t, "POSTGRESQL", resp.Type)
	require.Len(t, resp.Tables, 1)
	assert.Equal(t, "accounts", resp.Tables[0].Name)
	require.Len(t, resp.Tables[0].Fields, 3)
}

func TestFetcherSchemaResponse_EmptyTables(t *testing.T) {
	t.Parallel()

	raw := `{"id": "conn-xyz", "tables": []}`

	var resp fetcherSchemaResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "conn-xyz", resp.ID)
	assert.Empty(t, resp.Tables)
}

func TestFetcherTestResponse_Unmarshal_Success(t *testing.T) {
	t.Parallel()

	raw := `{"status": "success", "latencyMs": 42}`

	var resp fetcherTestResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "success", resp.Status)
	assert.Equal(t, int64(42), resp.LatencyMs)
	assert.Empty(t, resp.Message)
}

func TestFetcherTestResponse_Unmarshal_Error(t *testing.T) {
	t.Parallel()

	raw := `{"status": "error", "message": "connection refused", "latencyMs": 0}`

	var resp fetcherTestResponse

	err := json.Unmarshal([]byte(raw), &resp)

	require.NoError(t, err)
	assert.Equal(t, "error", resp.Status)
	assert.Equal(t, "connection refused", resp.Message)
}

func TestFetcherTestResponse_OmitsEmptyMessage(t *testing.T) {
	t.Parallel()

	resp := fetcherTestResponse{Status: "success", LatencyMs: 5}

	data, err := json.Marshal(resp)

	require.NoError(t, err)
	assert.NotContains(t, string(data), "message")
}
