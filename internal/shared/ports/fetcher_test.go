// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestFetcherConnection_JSONRoundtrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)

	original := &ports.FetcherConnection{
		ID:           "conn-001",
		ConfigName:   "pg-primary",
		DatabaseType: "POSTGRESQL",
		Host:         "db.example.com",
		Port:         5432,
		Schema:       "public",
		DatabaseName: "testdb",
		UserName:     "matcher",
		ProductName:  "PostgreSQL 16",
		Metadata:     map[string]any{"region": "us-east-1"},
		CreatedAt:    now,
		UpdatedAt:    now.Add(time.Hour),
	}

	data, err := json.Marshal(original)
	require.NoError(t, err, "marshal FetcherConnection")

	// FetcherConnection has no JSON tags, so encoding/json uses field names directly.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Contains(t, raw, "ID", "expected key 'ID' in JSON output")
	assert.Contains(t, raw, "ConfigName", "expected key 'ConfigName' in JSON output")
	assert.Contains(t, raw, "DatabaseType", "expected key 'DatabaseType' in JSON output")
	assert.Contains(t, raw, "Host", "expected key 'Host' in JSON output")
	assert.Contains(t, raw, "Port", "expected key 'Port' in JSON output")
	assert.Contains(t, raw, "Schema", "expected key 'Schema' in JSON output")
	assert.Contains(t, raw, "DatabaseName", "expected key 'DatabaseName' in JSON output")
	assert.Contains(t, raw, "UserName", "expected key 'UserName' in JSON output")
	assert.Contains(t, raw, "ProductName", "expected key 'ProductName' in JSON output")
	assert.Contains(t, raw, "Metadata", "expected key 'Metadata' in JSON output")
	assert.Contains(t, raw, "CreatedAt", "expected key 'CreatedAt' in JSON output")
	assert.Contains(t, raw, "UpdatedAt", "expected key 'UpdatedAt' in JSON output")

	var roundtripped ports.FetcherConnection
	require.NoError(t, json.Unmarshal(data, &roundtripped))
	assert.Equal(t, original.ID, roundtripped.ID)
	assert.Equal(t, original.ConfigName, roundtripped.ConfigName)
	assert.Equal(t, original.DatabaseType, roundtripped.DatabaseType)
	assert.Equal(t, original.Host, roundtripped.Host)
	assert.Equal(t, original.Port, roundtripped.Port)
	assert.Equal(t, original.Schema, roundtripped.Schema)
	assert.Equal(t, original.DatabaseName, roundtripped.DatabaseName)
	assert.Equal(t, original.UserName, roundtripped.UserName)
	assert.Equal(t, original.ProductName, roundtripped.ProductName)
	assert.Equal(t, original.Metadata["region"], roundtripped.Metadata["region"])
	assert.True(t, original.CreatedAt.Equal(roundtripped.CreatedAt), "CreatedAt roundtrip mismatch")
	assert.True(t, original.UpdatedAt.Equal(roundtripped.UpdatedAt), "UpdatedAt roundtrip mismatch")
}

func TestFetcherTableSchema_JSONRoundtrip(t *testing.T) {
	t.Parallel()

	original := ports.FetcherTableSchema{
		Name:   "transactions",
		Fields: []string{"id", "amount", "description", "created_at"},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err, "marshal FetcherTableSchema")

	// FetcherTableSchema has explicit JSON tags: "Name" and "Fields".
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Contains(t, raw, "Name", "expected JSON tag 'Name'")
	assert.Contains(t, raw, "Fields", "expected JSON tag 'Fields'")
	assert.NotContains(t, raw, "name", "should use tag 'Name', not 'name'")
	assert.NotContains(t, raw, "fields", "should use tag 'Fields', not 'fields'")

	var roundtripped ports.FetcherTableSchema
	require.NoError(t, json.Unmarshal(data, &roundtripped))
	assert.Equal(t, original.Name, roundtripped.Name)
	assert.Equal(t, original.Fields, roundtripped.Fields)
}

func TestFetcherSchema_JSONRoundtrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 15, 10, 30, 0, 0, time.UTC)

	original := &ports.FetcherSchema{
		ID:           "conn-1",
		ConfigName:   "pg-primary",
		DatabaseName: "testdb",
		Type:         "POSTGRESQL",
		Tables: []ports.FetcherTableSchema{
			{Name: "transactions", Fields: []string{"id", "amount", "description"}},
			{Name: "payments", Fields: []string{"id", "order_id", "status"}},
		},
		DiscoveredAt: now,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err, "marshal FetcherSchema")

	// FetcherSchema has explicit JSON tags with PascalCase keys.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Contains(t, raw, "ID", "expected JSON tag 'ID'")
	assert.Contains(t, raw, "ConfigName", "expected JSON tag 'ConfigName'")
	assert.Contains(t, raw, "DatabaseName", "expected JSON tag 'DatabaseName'")
	assert.Contains(t, raw, "Type", "expected JSON tag 'Type'")
	assert.Contains(t, raw, "Tables", "expected JSON tag 'Tables'")
	assert.Contains(t, raw, "DiscoveredAt", "expected JSON tag 'DiscoveredAt'")

	var roundtripped ports.FetcherSchema
	require.NoError(t, json.Unmarshal(data, &roundtripped))
	assert.Equal(t, original.ID, roundtripped.ID)
	assert.Equal(t, original.ConfigName, roundtripped.ConfigName)
	assert.Equal(t, original.DatabaseName, roundtripped.DatabaseName)
	assert.Equal(t, original.Type, roundtripped.Type)
	assert.True(t, original.DiscoveredAt.Equal(roundtripped.DiscoveredAt), "DiscoveredAt roundtrip mismatch")
	require.Len(t, roundtripped.Tables, 2)
	assert.Equal(t, "transactions", roundtripped.Tables[0].Name)
	assert.Equal(t, []string{"id", "amount", "description"}, roundtripped.Tables[0].Fields)
	assert.Equal(t, "payments", roundtripped.Tables[1].Name)
	assert.Equal(t, []string{"id", "order_id", "status"}, roundtripped.Tables[1].Fields)
}

func TestFetcherTestResult_JSONRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		original ports.FetcherTestResult
	}{
		{
			name: "healthy",
			original: ports.FetcherTestResult{
				Status:    "success",
				Message:   "",
				LatencyMs: 42,
			},
		},
		{
			name: "unhealthy",
			original: ports.FetcherTestResult{
				Status:    "error",
				Message:   "connection refused",
				LatencyMs: 0,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(tc.original)
			require.NoError(t, err, "marshal FetcherTestResult")

			var roundtripped ports.FetcherTestResult
			require.NoError(t, json.Unmarshal(data, &roundtripped))
			assert.Equal(t, tc.original.Status, roundtripped.Status)
			assert.Equal(t, tc.original.Message, roundtripped.Message)
			assert.Equal(t, tc.original.LatencyMs, roundtripped.LatencyMs)
		})
	}
}

func TestExtractionJobInput_JSONRoundtrip(t *testing.T) {
	t.Parallel()

	original := ports.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{
			"pg-primary": {
				"transactions": {"id", "amount"},
				"payments":     {"id", "order_id"},
			},
		},
		Filters: map[string]map[string]map[string]any{
			"pg-primary": {
				"transactions": {"currency": "USD"},
			},
		},
		Metadata: map[string]any{"source": "matcher"},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err, "marshal ExtractionJobInput")

	var roundtripped ports.ExtractionJobInput
	require.NoError(t, json.Unmarshal(data, &roundtripped))

	require.Len(t, roundtripped.MappedFields, 1)
	txCols, ok := roundtripped.MappedFields["pg-primary"]["transactions"]
	require.True(t, ok, "expected pg-primary.transactions in roundtripped MappedFields")
	assert.Equal(t, []string{"id", "amount"}, txCols)

	payCols, ok := roundtripped.MappedFields["pg-primary"]["payments"]
	require.True(t, ok, "expected pg-primary.payments in roundtripped MappedFields")
	assert.Equal(t, []string{"id", "order_id"}, payCols)

	assert.Equal(t, "USD", roundtripped.Filters["pg-primary"]["transactions"]["currency"])
	assert.Equal(t, "matcher", roundtripped.Metadata["source"])
}

func TestExtractionJobStatus_JSONRoundtrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 15, 14, 0, 0, 0, time.UTC)
	completed := now.Add(5 * time.Minute)

	tests := []struct {
		name     string
		original ports.ExtractionJobStatus
	}{
		{
			name: "completed_with_result",
			original: ports.ExtractionJobStatus{
				ID:          "job-1",
				Status:      "completed",
				ResultPath:  "/data/results/job-1.json",
				ResultHmac:  "abc123",
				RequestHash: "def456",
				Metadata:    map[string]any{"source": "conn-pg"},
				CreatedAt:   now,
				CompletedAt: &completed,
			},
		},
		{
			name: "pending_no_result",
			original: ports.ExtractionJobStatus{
				ID:        "job-2",
				Status:    "pending",
				CreatedAt: now,
			},
		},
		{
			name: "failed_no_completion",
			original: ports.ExtractionJobStatus{
				ID:        "job-3",
				Status:    "failed",
				CreatedAt: now,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(tc.original)
			require.NoError(t, err, "marshal ExtractionJobStatus")

			var roundtripped ports.ExtractionJobStatus
			require.NoError(t, json.Unmarshal(data, &roundtripped))
			assert.Equal(t, tc.original.ID, roundtripped.ID)
			assert.Equal(t, tc.original.Status, roundtripped.Status)
			assert.Equal(t, tc.original.ResultPath, roundtripped.ResultPath)
			assert.Equal(t, tc.original.ResultHmac, roundtripped.ResultHmac)
			assert.Equal(t, tc.original.RequestHash, roundtripped.RequestHash)
			assert.True(t, tc.original.CreatedAt.Equal(roundtripped.CreatedAt), "CreatedAt roundtrip mismatch")

			if tc.original.CompletedAt != nil {
				require.NotNil(t, roundtripped.CompletedAt)
				assert.True(t, tc.original.CompletedAt.Equal(*roundtripped.CompletedAt), "CompletedAt roundtrip mismatch")
			} else {
				assert.Nil(t, roundtripped.CompletedAt, "expected nil CompletedAt")
			}

			if tc.original.Metadata != nil {
				assert.Equal(t, tc.original.Metadata["source"], roundtripped.Metadata["source"])
			}
		})
	}
}
