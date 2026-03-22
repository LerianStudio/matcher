//go:build unit

package dto

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func TestConnectionFromEntity_ValidEntity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)
	id := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

	entity := &entities.FetcherConnection{
		ID:               id,
		FetcherConnID:    "fetcher-conn-001",
		ConfigName:       "prod-config",
		DatabaseType:     "POSTGRESQL",
		Host:             "db.example.com",
		Port:             5432,
		DatabaseName:     "ledger",
		ProductName:      "PostgreSQL 17.2",
		Status:           vo.ConnectionStatusAvailable,
		SchemaDiscovered: true,
		LastSeenAt:       now,
		CreatedAt:        now.Add(-24 * time.Hour),
		UpdatedAt:        now,
	}

	resp := ConnectionFromEntity(entity)

	assert.Equal(t, id, resp.ID)
	assert.Equal(t, "prod-config", resp.ConfigName)
	assert.Equal(t, "POSTGRESQL", resp.DatabaseType)
	assert.Equal(t, "AVAILABLE", resp.Status)
	assert.True(t, resp.SchemaDiscovered)
	assert.Equal(t, now, resp.LastSeenAt)
}

func TestConnectionFromEntity_NilEntity(t *testing.T) {
	t.Parallel()

	resp := ConnectionFromEntity(nil)

	assert.Equal(t, ConnectionResponse{}, resp)
}

func TestConnectionFromEntity_AllStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   vo.ConnectionStatus
		expected string
	}{
		{name: "available", status: vo.ConnectionStatusAvailable, expected: "AVAILABLE"},
		{name: "unreachable", status: vo.ConnectionStatusUnreachable, expected: "UNREACHABLE"},
		{name: "unknown", status: vo.ConnectionStatusUnknown, expected: "UNKNOWN"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			entity := &entities.FetcherConnection{
				ID:     uuid.New(),
				Status: tc.status,
			}

			resp := ConnectionFromEntity(entity)

			assert.Equal(t, tc.expected, resp.Status)
		})
	}
}

func TestConnectionFromEntity_ZeroValues(t *testing.T) {
	t.Parallel()

	entity := &entities.FetcherConnection{
		ID:     uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		Status: vo.ConnectionStatusUnknown,
	}

	resp := ConnectionFromEntity(entity)

	require.NotNil(t, resp)
	assert.Equal(t, uuid.MustParse("11111111-2222-3333-4444-555555555555"), resp.ID)
	assert.Empty(t, resp.ConfigName)
	assert.Empty(t, resp.DatabaseType)
	assert.Equal(t, "UNKNOWN", resp.Status)
	assert.False(t, resp.SchemaDiscovered)
	assert.True(t, resp.LastSeenAt.IsZero())
}

func TestResponseDTOs_CompileCheck(t *testing.T) {
	t.Parallel()

	// Verify all response DTO types are instantiable (compile-time coverage).
	_ = DiscoveryStatusResponse{}
	_ = ConnectionResponse{}
	_ = ConnectionListResponse{}
	_ = SchemaTableResponse{}
	_ = SchemaColumnResponse{}
	_ = ConnectionSchemaResponse{}
	_ = RefreshDiscoveryResponse{}
	_ = TestConnectionResponse{}
	_ = ExtractionRequestResponse{}
}

func TestExtractionRequestFromEntity_HidesInternalResultPath(t *testing.T) {
	t.Parallel()

	entity := &entities.ExtractionRequest{
		ID:           uuid.New(),
		ConnectionID: uuid.New(),
		Tables:       map[string]any{"transactions": map[string]any{"columns": []string{"id", "amount"}}},
		StartDate:    "2026-03-01",
		EndDate:      "2026-03-08",
		Status:       vo.ExtractionStatusComplete,
		ResultPath:   "/tmp/internal/result.csv",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	resp := ExtractionRequestFromEntity(entity)

	assert.Equal(t, vo.ExtractionStatusComplete.String(), resp.Status)
	assert.Equal(t, "2026-03-01", resp.StartDate)
	assert.Equal(t, "2026-03-08", resp.EndDate)
	assert.Equal(t, []string{"id", "amount"}, resp.Tables["transactions"].Columns)
	assert.Empty(t, resp.ErrorMessage)

	encoded, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "resultPath")
}

func TestDiscoveryStatusResponse_OmitsZeroLastSyncAt(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(DiscoveryStatusResponse{
		FetcherHealthy:  true,
		ConnectionCount: 0,
	})
	require.NoError(t, err)

	var body map[string]any
	require.NoError(t, json.Unmarshal(encoded, &body))
	assert.NotContains(t, body, "lastSyncAt")
}
