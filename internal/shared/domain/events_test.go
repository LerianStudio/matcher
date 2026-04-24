// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Package shared provides shared domain entities for cross-context communication.
package shared

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventTypeMatchConfirmed(t *testing.T) {
	t.Parallel()

	t.Run("constant value is correct", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "matching.match_confirmed", EventTypeMatchConfirmed)
	})

	t.Run("constant is non-empty", func(t *testing.T) {
		t.Parallel()

		assert.NotEmpty(t, EventTypeMatchConfirmed)
	})
}

func TestMatchConfirmedEvent_ID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		matchID uuid.UUID
	}{
		{
			name:    "returns match ID for valid UUID",
			matchID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		},
		{
			name:    "returns nil UUID when match ID is nil",
			matchID: uuid.Nil,
		},
		{
			name:    "returns different UUIDs correctly",
			matchID: uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := MatchConfirmedEvent{
				MatchID: tt.matchID,
			}

			assert.Equal(t, tt.matchID, event.ID())
		})
	}
}

func TestMatchConfirmedEvent_ID_IsIdempotencyKey(t *testing.T) {
	t.Parallel()

	matchID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	event := MatchConfirmedEvent{
		EventType:  EventTypeMatchConfirmed,
		TenantID:   uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		TenantSlug: "test-tenant",
		ContextID:  uuid.MustParse("770e8400-e29b-41d4-a716-446655440002"),
		RunID:      uuid.MustParse("880e8400-e29b-41d4-a716-446655440003"),
		MatchID:    matchID,
		RuleID:     uuid.MustParse("990e8400-e29b-41d4-a716-446655440004"),
		TransactionIDs: []uuid.UUID{
			uuid.MustParse("aa0e8400-e29b-41d4-a716-446655440005"),
			uuid.MustParse("bb0e8400-e29b-41d4-a716-446655440006"),
		},
		Confidence:  95,
		ConfirmedAt: time.Now(),
		Timestamp:   time.Now(),
	}

	id1 := event.ID()
	id2 := event.ID()

	assert.Equal(t, id1, id2, "ID() should be idempotent")
	assert.Equal(t, matchID, id1, "ID() should return the MatchID")
}

func TestMatchConfirmedEvent_JSONMarshal(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	event := MatchConfirmedEvent{
		EventType:  EventTypeMatchConfirmed,
		TenantID:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		TenantSlug: "acme-corp",
		ContextID:  uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		RunID:      uuid.MustParse("770e8400-e29b-41d4-a716-446655440002"),
		MatchID:    uuid.MustParse("880e8400-e29b-41d4-a716-446655440003"),
		RuleID:     uuid.MustParse("990e8400-e29b-41d4-a716-446655440004"),
		TransactionIDs: []uuid.UUID{
			uuid.MustParse("aa0e8400-e29b-41d4-a716-446655440005"),
		},
		Confidence:  85,
		ConfirmedAt: fixedTime,
		Timestamp:   fixedTime,
	}

	data, err := json.Marshal(event)
	require.NoError(t, err)

	var result map[string]any

	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Equal(t, "matching.match_confirmed", result["eventType"])
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", result["tenantId"])
	assert.Equal(t, "acme-corp", result["tenantSlug"])
	assert.Equal(t, "660e8400-e29b-41d4-a716-446655440001", result["contextId"])
	assert.Equal(t, "770e8400-e29b-41d4-a716-446655440002", result["runId"])
	assert.Equal(t, "880e8400-e29b-41d4-a716-446655440003", result["matchId"])
	assert.Equal(t, "990e8400-e29b-41d4-a716-446655440004", result["ruleId"])
	assert.InDelta(t, float64(85), result["confidence"], 0.01)

	transactionIDs, ok := result["transactionIds"].([]any)
	require.True(t, ok)
	assert.Len(t, transactionIDs, 1)
	assert.Equal(t, "aa0e8400-e29b-41d4-a716-446655440005", transactionIDs[0])
}

func TestMatchConfirmedEvent_JSONUnmarshal(t *testing.T) {
	t.Parallel()

	jsonData := `{
		"eventType": "matching.match_confirmed",
		"tenantId": "550e8400-e29b-41d4-a716-446655440000",
		"tenantSlug": "test-tenant",
		"contextId": "660e8400-e29b-41d4-a716-446655440001",
		"runId": "770e8400-e29b-41d4-a716-446655440002",
		"matchId": "880e8400-e29b-41d4-a716-446655440003",
		"ruleId": "990e8400-e29b-41d4-a716-446655440004",
		"transactionIds": [
			"aa0e8400-e29b-41d4-a716-446655440005",
			"bb0e8400-e29b-41d4-a716-446655440006"
		],
		"confidence": 90,
		"confirmedAt": "2024-01-15T10:30:00Z",
		"timestamp": "2024-01-15T10:30:00Z"
	}`

	var event MatchConfirmedEvent

	err := json.Unmarshal([]byte(jsonData), &event)
	require.NoError(t, err)

	assert.Equal(t, EventTypeMatchConfirmed, event.EventType)
	assert.Equal(t, uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), event.TenantID)
	assert.Equal(t, "test-tenant", event.TenantSlug)
	assert.Equal(t, uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"), event.ContextID)
	assert.Equal(t, uuid.MustParse("770e8400-e29b-41d4-a716-446655440002"), event.RunID)
	assert.Equal(t, uuid.MustParse("880e8400-e29b-41d4-a716-446655440003"), event.MatchID)
	assert.Equal(t, uuid.MustParse("990e8400-e29b-41d4-a716-446655440004"), event.RuleID)
	assert.Len(t, event.TransactionIDs, 2)
	assert.Equal(t, uuid.MustParse("aa0e8400-e29b-41d4-a716-446655440005"), event.TransactionIDs[0])
	assert.Equal(t, uuid.MustParse("bb0e8400-e29b-41d4-a716-446655440006"), event.TransactionIDs[1])
	assert.Equal(t, 90, event.Confidence)
	assert.Equal(t, 2024, event.ConfirmedAt.Year())
	assert.Equal(t, time.January, event.ConfirmedAt.Month())
	assert.Equal(t, 15, event.ConfirmedAt.Day())
}

func TestMatchConfirmedEvent_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 6, 20, 14, 45, 30, 0, time.UTC)

	original := MatchConfirmedEvent{
		EventType:  EventTypeMatchConfirmed,
		TenantID:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		TenantSlug: "round-trip-tenant",
		ContextID:  uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		RunID:      uuid.MustParse("770e8400-e29b-41d4-a716-446655440002"),
		MatchID:    uuid.MustParse("880e8400-e29b-41d4-a716-446655440003"),
		RuleID:     uuid.MustParse("990e8400-e29b-41d4-a716-446655440004"),
		TransactionIDs: []uuid.UUID{
			uuid.MustParse("aa0e8400-e29b-41d4-a716-446655440005"),
			uuid.MustParse("bb0e8400-e29b-41d4-a716-446655440006"),
			uuid.MustParse("cc0e8400-e29b-41d4-a716-446655440007"),
		},
		Confidence:  100,
		ConfirmedAt: fixedTime,
		Timestamp:   fixedTime,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored MatchConfirmedEvent

	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	assert.Equal(t, original.EventType, restored.EventType)
	assert.Equal(t, original.TenantID, restored.TenantID)
	assert.Equal(t, original.TenantSlug, restored.TenantSlug)
	assert.Equal(t, original.ContextID, restored.ContextID)
	assert.Equal(t, original.RunID, restored.RunID)
	assert.Equal(t, original.MatchID, restored.MatchID)
	assert.Equal(t, original.RuleID, restored.RuleID)
	assert.Equal(t, original.TransactionIDs, restored.TransactionIDs)
	assert.Equal(t, original.Confidence, restored.Confidence)
	assert.True(t, original.ConfirmedAt.Equal(restored.ConfirmedAt))
	assert.True(t, original.Timestamp.Equal(restored.Timestamp))
}

func TestMatchConfirmedEvent_EmptyTransactionIDs(t *testing.T) {
	t.Parallel()

	event := MatchConfirmedEvent{
		EventType:      EventTypeMatchConfirmed,
		TenantID:       uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		MatchID:        uuid.MustParse("880e8400-e29b-41d4-a716-446655440003"),
		TransactionIDs: []uuid.UUID{},
		Confidence:     0,
	}

	data, err := json.Marshal(event)
	require.NoError(t, err)

	var restored MatchConfirmedEvent

	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	assert.Empty(t, restored.TransactionIDs)
	assert.Equal(t, 0, restored.Confidence)
}

func TestMatchConfirmedEvent_EmptyTenantSlug(t *testing.T) {
	t.Parallel()

	event := MatchConfirmedEvent{
		EventType:  EventTypeMatchConfirmed,
		TenantID:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		TenantSlug: "",
		MatchID:    uuid.MustParse("880e8400-e29b-41d4-a716-446655440003"),
	}

	data, err := json.Marshal(event)
	require.NoError(t, err)

	var result map[string]any

	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Empty(t, result["tenantSlug"])
}

func TestEventTypeMatchUnmatched(t *testing.T) {
	t.Parallel()

	t.Run("constant value is correct", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "matching.match_unmatched", EventTypeMatchUnmatched)
	})

	t.Run("constant is non-empty", func(t *testing.T) {
		t.Parallel()

		assert.NotEmpty(t, EventTypeMatchUnmatched)
	})
}

func TestMatchUnmatchedEvent_ID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		matchID uuid.UUID
	}{
		{
			name:    "returns match ID for valid UUID",
			matchID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		},
		{
			name:    "returns nil UUID when match ID is nil",
			matchID: uuid.Nil,
		},
		{
			name:    "returns different UUIDs correctly",
			matchID: uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := MatchUnmatchedEvent{
				MatchID: tt.matchID,
			}

			assert.Equal(t, tt.matchID, event.ID())
		})
	}
}

func TestMatchEventPublisherInterface(t *testing.T) {
	t.Parallel()

	var _ MatchEventPublisher = (*mockMatchEventPublisher)(nil)
}

type mockMatchEventPublisher struct{}

func (m *mockMatchEventPublisher) PublishMatchConfirmed(
	_ context.Context,
	_ *MatchConfirmedEvent,
) error {
	return nil
}

func (m *mockMatchEventPublisher) PublishMatchUnmatched(
	_ context.Context,
	_ *MatchUnmatchedEvent,
) error {
	return nil
}

func TestEventTypeAuditLogCreated(t *testing.T) {
	t.Parallel()

	t.Run("constant value is correct", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "governance.audit_log_created", EventTypeAuditLogCreated)
	})

	t.Run("constant is non-empty", func(t *testing.T) {
		t.Parallel()

		assert.NotEmpty(t, EventTypeAuditLogCreated)
	})
}

func TestAuditLogCreatedEvent_ID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		uniqueID uuid.UUID
		entityID uuid.UUID
	}{
		{
			name:     "returns unique ID for valid UUID",
			uniqueID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
			entityID: uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		},
		{
			name:     "returns nil UUID when unique ID is nil",
			uniqueID: uuid.Nil,
			entityID: uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		},
		{
			name:     "returns unique ID not entity ID",
			uniqueID: uuid.MustParse("770e8400-e29b-41d4-a716-446655440002"),
			entityID: uuid.MustParse("880e8400-e29b-41d4-a716-446655440003"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := AuditLogCreatedEvent{
				UniqueID: tt.uniqueID,
				EntityID: tt.entityID,
			}

			assert.Equal(t, tt.uniqueID, event.ID())
			assert.NotEqual(t, tt.entityID, event.ID(), "ID() should return UniqueID, not EntityID")
		})
	}
}

func TestAuditLogCreatedEvent_JSONMarshal(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	actor := "user-123"

	event := AuditLogCreatedEvent{
		UniqueID:   uuid.MustParse("aa0e8400-e29b-41d4-a716-446655440099"),
		EventType:  EventTypeAuditLogCreated,
		TenantID:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		EntityType: "context",
		EntityID:   uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		Action:     "create",
		Actor:      &actor,
		Changes:    map[string]any{"name": "test-context"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	data, err := json.Marshal(event)
	require.NoError(t, err)

	var result map[string]any

	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Equal(t, "aa0e8400-e29b-41d4-a716-446655440099", result["id"])
	assert.Equal(t, "governance.audit_log_created", result["eventType"])
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", result["tenantId"])
	assert.Equal(t, "context", result["entityType"])
	assert.Equal(t, "660e8400-e29b-41d4-a716-446655440001", result["entityId"])
	assert.Equal(t, "create", result["action"])
	assert.Equal(t, "user-123", result["actor"])

	changes, ok := result["changes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-context", changes["name"])
}

func TestAuditLogCreatedEvent_JSONMarshal_NilActor(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	event := AuditLogCreatedEvent{
		UniqueID:   uuid.MustParse("bb0e8400-e29b-41d4-a716-446655440098"),
		EventType:  EventTypeAuditLogCreated,
		TenantID:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		EntityType: "source",
		EntityID:   uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		Action:     "delete",
		Actor:      nil,
		Changes:    nil,
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	data, err := json.Marshal(event)
	require.NoError(t, err)

	var result map[string]any

	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	_, hasActor := result["actor"]
	assert.False(t, hasActor, "actor should be omitted when nil")

	_, hasChanges := result["changes"]
	assert.False(t, hasChanges, "changes should be omitted when nil")
}

func TestAuditLogCreatedEvent_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 6, 20, 14, 45, 30, 0, time.UTC)
	actor := "admin"

	original := AuditLogCreatedEvent{
		UniqueID:   uuid.MustParse("cc0e8400-e29b-41d4-a716-446655440097"),
		EventType:  EventTypeAuditLogCreated,
		TenantID:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		EntityType: "field_map",
		EntityID:   uuid.MustParse("660e8400-e29b-41d4-a716-446655440001"),
		Action:     "update",
		Actor:      &actor,
		Changes:    map[string]any{"mapping": map[string]any{"field1": "value1"}},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored AuditLogCreatedEvent

	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	assert.Equal(t, original.UniqueID, restored.UniqueID)
	assert.Equal(t, original.EventType, restored.EventType)
	assert.Equal(t, original.TenantID, restored.TenantID)
	assert.Equal(t, original.EntityType, restored.EntityType)
	assert.Equal(t, original.EntityID, restored.EntityID)
	assert.Equal(t, original.Action, restored.Action)
	require.NotNil(t, restored.Actor)
	assert.Equal(t, *original.Actor, *restored.Actor)
	assert.True(t, original.OccurredAt.Equal(restored.OccurredAt))
	assert.True(t, original.Timestamp.Equal(restored.Timestamp))
}

func TestAuditEventPublisherInterface(t *testing.T) {
	t.Parallel()

	var _ AuditEventPublisher = (*mockAuditEventPublisher)(nil)
}

type mockAuditEventPublisher struct{}

func (m *mockAuditEventPublisher) PublishAuditLogCreated(
	_ context.Context,
	_ *AuditLogCreatedEvent,
) error {
	return nil
}
