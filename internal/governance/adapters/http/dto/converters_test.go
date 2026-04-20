//go:build unit

package dto

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

func TestAuditLogToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.AuditLog
		expected AuditLogResponse
	}{
		{
			name:  "nil input returns empty struct with empty changes",
			input: nil,
			expected: AuditLogResponse{
				Changes: json.RawMessage("{}"),
			},
		},
		{
			name: "full entity conversion",
			input: &entities.AuditLog{
				ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				TenantID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				EntityType: "reconciliation_context",
				EntityID:   uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Action:     "CREATE",
				ActorID:    ptrString("user@example.com"),
				Changes:    json.RawMessage(`{"name": "new-context"}`),
				CreatedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: AuditLogResponse{
				ID:         "11111111-1111-1111-1111-111111111111",
				TenantID:   "22222222-2222-2222-2222-222222222222",
				EntityType: "reconciliation_context",
				EntityID:   "33333333-3333-3333-3333-333333333333",
				Action:     "CREATE",
				ActorID:    ptrString("user@example.com"),
				Changes:    json.RawMessage(`{"name": "new-context"}`),
				CreatedAt:  "2025-01-15T10:30:00Z",
			},
		},
		{
			name: "without actor id",
			input: &entities.AuditLog{
				ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				TenantID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				EntityType: "matching_rule",
				EntityID:   uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Action:     "UPDATE",
				ActorID:    nil,
				Changes:    json.RawMessage(`{"priority": 10}`),
				CreatedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: AuditLogResponse{
				ID:         "11111111-1111-1111-1111-111111111111",
				TenantID:   "22222222-2222-2222-2222-222222222222",
				EntityType: "matching_rule",
				EntityID:   "33333333-3333-3333-3333-333333333333",
				Action:     "UPDATE",
				ActorID:    nil,
				Changes:    json.RawMessage(`{"priority": 10}`),
				CreatedAt:  "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := AuditLogToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAuditLogToResponse_TruncationMarkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		changes              json.RawMessage
		expectedTruncated    bool
		expectedOriginalSize int64
	}{
		{
			name:                 "normal audit log has zero-valued truncation fields",
			changes:              json.RawMessage(`{"entity_type":"context","changes":{"name":"new-context"}}`),
			expectedTruncated:    false,
			expectedOriginalSize: 0,
		},
		{
			name: "truncated audit log surfaces marker fields",
			changes: json.RawMessage(
				`{"entity_type":"context","changes":{"_truncated":true,"_originalSize":1572864,"_note":"audit diff exceeded 1MiB outbox cap","_maxAllowed":921600}}`,
			),
			expectedTruncated:    true,
			expectedOriginalSize: 1572864,
		},
		{
			name: "malformed _originalSize falls back to zero while preserving Truncated",
			changes: json.RawMessage(
				`{"entity_type":"context","changes":{"_truncated":true,"_originalSize":"huge","_note":"bad"}}`,
			),
			expectedTruncated:    true,
			expectedOriginalSize: 0,
		},
		{
			name: "missing _originalSize preserves Truncated with zero size",
			changes: json.RawMessage(
				`{"entity_type":"context","changes":{"_truncated":true,"_note":"bad"}}`,
			),
			expectedTruncated:    true,
			expectedOriginalSize: 0,
		},
		{
			name:                 "empty changes yields zero values",
			changes:              nil,
			expectedTruncated:    false,
			expectedOriginalSize: 0,
		},
		{
			name:                 "invalid JSON yields zero values",
			changes:              json.RawMessage(`not json`),
			expectedTruncated:    false,
			expectedOriginalSize: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input := &entities.AuditLog{
				ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				TenantID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				EntityType: "context",
				EntityID:   uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Action:     "UPDATE",
				Changes:    tt.changes,
				CreatedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			}

			result := AuditLogToResponse(input)
			assert.Equal(t, tt.expectedTruncated, result.Truncated)
			assert.Equal(t, tt.expectedOriginalSize, result.OriginalSize)
		})
	}
}

func TestAuditLogsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []*entities.AuditLog
		expected []AuditLogResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []AuditLogResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*entities.AuditLog{},
			expected: []AuditLogResponse{},
		},
		{
			name: "filters nil elements",
			input: []*entities.AuditLog{
				{
					ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					TenantID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					EntityType: "reconciliation_context",
					EntityID:   uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					Action:     "CREATE",
					Changes:    json.RawMessage(`{}`),
					CreatedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				nil,
			},
			expected: []AuditLogResponse{
				{
					ID:         "11111111-1111-1111-1111-111111111111",
					TenantID:   "22222222-2222-2222-2222-222222222222",
					EntityType: "reconciliation_context",
					EntityID:   "33333333-3333-3333-3333-333333333333",
					Action:     "CREATE",
					Changes:    json.RawMessage(`{}`),
					CreatedAt:  "2025-01-15T10:30:00Z",
				},
			},
		},
		{
			name: "converts multiple elements",
			input: []*entities.AuditLog{
				{
					ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					TenantID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					EntityType: "reconciliation_context",
					EntityID:   uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					Action:     "CREATE",
					Changes:    json.RawMessage(`{"name": "ctx1"}`),
					CreatedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				{
					ID:         uuid.MustParse("44444444-4444-4444-4444-444444444444"),
					TenantID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					EntityType: "matching_rule",
					EntityID:   uuid.MustParse("55555555-5555-5555-5555-555555555555"),
					Action:     "UPDATE",
					ActorID:    ptrString("admin@example.com"),
					Changes:    json.RawMessage(`{"priority": 5}`),
					CreatedAt:  time.Date(2025, 1, 15, 11, 0o0, 0, 0, time.UTC),
				},
			},
			expected: []AuditLogResponse{
				{
					ID:         "11111111-1111-1111-1111-111111111111",
					TenantID:   "22222222-2222-2222-2222-222222222222",
					EntityType: "reconciliation_context",
					EntityID:   "33333333-3333-3333-3333-333333333333",
					Action:     "CREATE",
					Changes:    json.RawMessage(`{"name": "ctx1"}`),
					CreatedAt:  "2025-01-15T10:30:00Z",
				},
				{
					ID:         "44444444-4444-4444-4444-444444444444",
					TenantID:   "22222222-2222-2222-2222-222222222222",
					EntityType: "matching_rule",
					EntityID:   "55555555-5555-5555-5555-555555555555",
					Action:     "UPDATE",
					ActorID:    ptrString("admin@example.com"),
					Changes:    json.RawMessage(`{"priority": 5}`),
					CreatedAt:  "2025-01-15T11:00:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := AuditLogsToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func ptrString(s string) *string {
	return &s
}
