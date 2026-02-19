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
