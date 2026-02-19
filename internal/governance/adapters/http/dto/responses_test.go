//go:build unit

package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLogResponse_JSON(t *testing.T) {
	t.Parallel()

	actorID := "user@example.com"
	resp := AuditLogResponse{
		ID:         "550e8400-e29b-41d4-a716-446655440000",
		TenantID:   "550e8400-e29b-41d4-a716-446655440001",
		EntityType: "reconciliation_context",
		EntityID:   "550e8400-e29b-41d4-a716-446655440002",
		Action:     "CREATE",
		ActorID:    &actorID,
		Changes:    json.RawMessage(`{"name": "new value"}`),
		CreatedAt:  "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded AuditLogResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.EntityType, decoded.EntityType)
	assert.Equal(t, resp.Action, decoded.Action)
}

func TestListAuditLogsResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := ListAuditLogsResponse{
		Items: []AuditLogResponse{
			{ID: "log-1", Action: "CREATE"},
			{ID: "log-2", Action: "UPDATE"},
		},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ListAuditLogsResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded.Items, 2)
}
