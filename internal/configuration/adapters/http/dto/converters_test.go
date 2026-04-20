//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestReconciliationContextToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.ReconciliationContext
		expected ReconciliationContextResponse
	}{
		{
			name:     "nil input returns empty struct",
			input:    nil,
			expected: ReconciliationContextResponse{},
		},
		{
			name: "full entity conversion",
			input: &entities.ReconciliationContext{
				ID:              uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				TenantID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Name:            "Bank Reconciliation Q1",
				Type:            value_objects.ContextTypeOneToOne,
				Interval:        "daily",
				Status:          value_objects.ContextStatusActive,
				FeeToleranceAbs: decimal.NewFromFloat(0.50),
				FeeTolerancePct: decimal.NewFromFloat(0.01),
				CreatedAt:       time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:       time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC),
			},
			expected: ReconciliationContextResponse{
				ID:              "11111111-1111-1111-1111-111111111111",
				TenantID:        "22222222-2222-2222-2222-222222222222",
				Name:            "Bank Reconciliation Q1",
				Type:            "1:1",
				Interval:        "daily",
				Status:          "ACTIVE",
				FeeToleranceAbs: "0.5",
				FeeTolerancePct: "0.01",
				CreatedAt:       "2025-01-15T10:30:00Z",
				UpdatedAt:       "2025-01-15T10:35:00Z",
			},
		},
		{
			name: "one to many paused context",
			input: &entities.ReconciliationContext{
				ID:              uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				TenantID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Name:            "FX Reconciliation",
				Type:            value_objects.ContextTypeOneToMany,
				Interval:        "weekly",
				Status:          value_objects.ContextStatusPaused,
				FeeToleranceAbs: decimal.Zero,
				FeeTolerancePct: decimal.Zero,
				CreatedAt:       time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:       time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: ReconciliationContextResponse{
				ID:              "11111111-1111-1111-1111-111111111111",
				TenantID:        "22222222-2222-2222-2222-222222222222",
				Name:            "FX Reconciliation",
				Type:            "1:N",
				Interval:        "weekly",
				Status:          "PAUSED",
				FeeToleranceAbs: "0",
				FeeTolerancePct: "0",
				CreatedAt:       "2025-01-15T10:30:00Z",
				UpdatedAt:       "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ReconciliationContextToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReconciliationContextsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []*entities.ReconciliationContext
		expected []ReconciliationContextResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []ReconciliationContextResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*entities.ReconciliationContext{},
			expected: []ReconciliationContextResponse{},
		},
		{
			name: "filters nil elements",
			input: []*entities.ReconciliationContext{
				{
					ID:              uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					TenantID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					Name:            "Test Context",
					Type:            value_objects.ContextTypeOneToOne,
					Interval:        "daily",
					Status:          value_objects.ContextStatusActive,
					FeeToleranceAbs: decimal.Zero,
					FeeTolerancePct: decimal.Zero,
					CreatedAt:       time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					UpdatedAt:       time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				nil,
			},
			expected: []ReconciliationContextResponse{
				{
					ID:              "11111111-1111-1111-1111-111111111111",
					TenantID:        "22222222-2222-2222-2222-222222222222",
					Name:            "Test Context",
					Type:            "1:1",
					Interval:        "daily",
					Status:          "ACTIVE",
					FeeToleranceAbs: "0",
					FeeTolerancePct: "0",
					CreatedAt:       "2025-01-15T10:30:00Z",
					UpdatedAt:       "2025-01-15T10:30:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ReconciliationContextsToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReconciliationSourceToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.ReconciliationSource
		expected ReconciliationSourceResponse
	}{
		{
			name:  "nil input returns empty struct",
			input: nil,
			expected: ReconciliationSourceResponse{
				Config: map[string]any{},
			},
		},
		{
			name: "full entity conversion",
			input: &entities.ReconciliationSource{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Name:      "Primary Bank Account",
				Type:      value_objects.SourceTypeBank,
				Config:    map[string]any{"account": "12345"},
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: ReconciliationSourceResponse{
				ID:        "11111111-1111-1111-1111-111111111111",
				ContextID: "22222222-2222-2222-2222-222222222222",
				Name:      "Primary Bank Account",
				Type:      "BANK",
				Config:    map[string]any{"account": "12345"},
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:30:00Z",
			},
		},
		{
			name: "nil config returns empty map",
			input: &entities.ReconciliationSource{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Name:      "Ledger Source",
				Type:      value_objects.SourceTypeLedger,
				Config:    nil,
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: ReconciliationSourceResponse{
				ID:        "11111111-1111-1111-1111-111111111111",
				ContextID: "22222222-2222-2222-2222-222222222222",
				Name:      "Ledger Source",
				Type:      "LEDGER",
				Config:    map[string]any{},
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ReconciliationSourceToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSourceWithFieldMapStatusToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        *entities.ReconciliationSource
		hasFieldMaps bool
		expected     SourceWithFieldMapStatusResponse
	}{
		{
			name:         "nil input returns empty struct",
			input:        nil,
			hasFieldMaps: false,
			expected: SourceWithFieldMapStatusResponse{
				ReconciliationSourceResponse: ReconciliationSourceResponse{
					Config: map[string]any{},
				},
				HasFieldMaps: false,
			},
		},
		{
			name: "with field maps",
			input: &entities.ReconciliationSource{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Name:      "Bank Source",
				Type:      value_objects.SourceTypeBank,
				Config:    map[string]any{},
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			hasFieldMaps: true,
			expected: SourceWithFieldMapStatusResponse{
				ReconciliationSourceResponse: ReconciliationSourceResponse{
					ID:        "11111111-1111-1111-1111-111111111111",
					ContextID: "22222222-2222-2222-2222-222222222222",
					Name:      "Bank Source",
					Type:      "BANK",
					Config:    map[string]any{},
					CreatedAt: "2025-01-15T10:30:00Z",
					UpdatedAt: "2025-01-15T10:30:00Z",
				},
				HasFieldMaps: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := SourceWithFieldMapStatusToResponse(tt.input, tt.hasFieldMaps)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSourcesToResponseWithFieldMaps(t *testing.T) {
	t.Parallel()

	sourceID1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	sourceID2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	tests := []struct {
		name                 string
		input                []*entities.ReconciliationSource
		fieldMapsMap         map[uuid.UUID]bool
		expectedCount        int
		expectedHasFieldMaps []bool
	}{
		{
			name:                 "nil slice returns empty slice",
			input:                nil,
			fieldMapsMap:         nil,
			expectedCount:        0,
			expectedHasFieldMaps: nil,
		},
		{
			name:                 "empty slice returns empty slice",
			input:                []*entities.ReconciliationSource{},
			fieldMapsMap:         nil,
			expectedCount:        0,
			expectedHasFieldMaps: nil,
		},
		{
			name: "processes field map status correctly",
			input: []*entities.ReconciliationSource{
				{
					ID:        sourceID1,
					ContextID: uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					Name:      "Source 1",
					Type:      value_objects.SourceTypeBank,
					Config:    map[string]any{},
					CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				{
					ID:        sourceID2,
					ContextID: uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					Name:      "Source 2",
					Type:      value_objects.SourceTypeLedger,
					Config:    map[string]any{},
					CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
			},
			fieldMapsMap: map[uuid.UUID]bool{
				sourceID1: true,
				sourceID2: false,
			},
			expectedCount:        2,
			expectedHasFieldMaps: []bool{true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := SourcesToResponseWithFieldMaps(tt.input, tt.fieldMapsMap)
			assert.Len(t, result, tt.expectedCount)

			for i, expected := range tt.expectedHasFieldMaps {
				assert.Equal(
					t,
					expected,
					result[i].HasFieldMaps,
					"HasFieldMaps mismatch at index %d",
					i,
				)
			}
		})
	}
}

func TestFieldMapToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.FieldMap
		expected FieldMapResponse
	}{
		{
			name:  "nil input returns empty struct",
			input: nil,
			expected: FieldMapResponse{
				Mapping: map[string]any{},
			},
		},
		{
			name: "full entity conversion",
			input: &entities.FieldMap{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				SourceID:  uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Mapping:   map[string]any{"amount": "transaction_amount"},
				Version:   3,
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC),
			},
			expected: FieldMapResponse{
				ID:        "11111111-1111-1111-1111-111111111111",
				ContextID: "22222222-2222-2222-2222-222222222222",
				SourceID:  "33333333-3333-3333-3333-333333333333",
				Mapping:   map[string]any{"amount": "transaction_amount"},
				Version:   3,
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:35:00Z",
			},
		},
		{
			name: "nil mapping returns empty map",
			input: &entities.FieldMap{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				SourceID:  uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Mapping:   nil,
				Version:   1,
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: FieldMapResponse{
				ID:        "11111111-1111-1111-1111-111111111111",
				ContextID: "22222222-2222-2222-2222-222222222222",
				SourceID:  "33333333-3333-3333-3333-333333333333",
				Mapping:   map[string]any{},
				Version:   1,
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := FieldMapToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFieldMapsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []*entities.FieldMap
		expected []FieldMapResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []FieldMapResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*entities.FieldMap{},
			expected: []FieldMapResponse{},
		},
		{
			name: "filters nil elements",
			input: []*entities.FieldMap{
				{
					ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					SourceID:  uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					Mapping:   map[string]any{},
					Version:   1,
					CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				nil,
			},
			expected: []FieldMapResponse{
				{
					ID:        "11111111-1111-1111-1111-111111111111",
					ContextID: "22222222-2222-2222-2222-222222222222",
					SourceID:  "33333333-3333-3333-3333-333333333333",
					Mapping:   map[string]any{},
					Version:   1,
					CreatedAt: "2025-01-15T10:30:00Z",
					UpdatedAt: "2025-01-15T10:30:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := FieldMapsToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchRuleToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *shared.MatchRule
		expected MatchRuleResponse
	}{
		{
			name:  "nil input returns empty struct",
			input: nil,
			expected: MatchRuleResponse{
				Config: map[string]any{},
			},
		},
		{
			name: "full entity conversion",
			input: &shared.MatchRule{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Priority:  1,
				Type:      shared.RuleTypeExact,
				Config:    map[string]any{"field": "amount"},
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: MatchRuleResponse{
				ID:        "11111111-1111-1111-1111-111111111111",
				ContextID: "22222222-2222-2222-2222-222222222222",
				Priority:  1,
				Type:      "EXACT",
				Config:    map[string]any{"field": "amount"},
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:30:00Z",
			},
		},
		{
			name: "nil config returns empty map",
			input: &shared.MatchRule{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Priority:  10,
				Type:      shared.RuleTypeTolerance,
				Config:    nil,
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: MatchRuleResponse{
				ID:        "11111111-1111-1111-1111-111111111111",
				ContextID: "22222222-2222-2222-2222-222222222222",
				Priority:  10,
				Type:      "TOLERANCE",
				Config:    map[string]any{},
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchRuleToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchRulesToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []*shared.MatchRule
		expected []MatchRuleResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []MatchRuleResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*shared.MatchRule{},
			expected: []MatchRuleResponse{},
		},
		{
			name: "filters nil elements",
			input: []*shared.MatchRule{
				{
					ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					Priority:  1,
					Type:      shared.RuleTypeExact,
					Config:    map[string]any{},
					CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				nil,
			},
			expected: []MatchRuleResponse{
				{
					ID:        "11111111-1111-1111-1111-111111111111",
					ContextID: "22222222-2222-2222-2222-222222222222",
					Priority:  1,
					Type:      "EXACT",
					Config:    map[string]any{},
					CreatedAt: "2025-01-15T10:30:00Z",
					UpdatedAt: "2025-01-15T10:30:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchRulesToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
