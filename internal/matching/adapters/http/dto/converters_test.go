// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/testutil"
)

func mustParseConfidence(val int) value_objects.ConfidenceScore {
	score, err := value_objects.ParseConfidenceScore(val)
	if err != nil {
		panic("mustParseConfidence: " + err.Error())
	}

	return score
}

func TestMatchRunToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.MatchRun
		expected MatchRunResponse
	}{
		{
			name:  "nil input returns empty struct",
			input: nil,
			expected: MatchRunResponse{
				Stats: map[string]int{},
			},
		},
		{
			name: "full entity conversion",
			input: &entities.MatchRun{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Mode:      value_objects.MatchRunModeDryRun,
				Status:    value_objects.MatchRunStatusCompleted,
				StartedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				Stats:     map[string]int{"matched": 10, "unmatched": 2},
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC),
			},
			expected: MatchRunResponse{
				ID:        "11111111-1111-1111-1111-111111111111",
				ContextID: "22222222-2222-2222-2222-222222222222",
				Mode:      "DRY_RUN",
				Status:    "COMPLETED",
				StartedAt: "2025-01-15T10:30:00Z",
				Stats:     map[string]int{"matched": 10, "unmatched": 2},
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:35:00Z",
			},
		},
		{
			name: "with completed_at and failure_reason",
			input: &entities.MatchRun{
				ID:            uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID:     uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Mode:          value_objects.MatchRunModeCommit,
				Status:        value_objects.MatchRunStatusFailed,
				StartedAt:     time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				CompletedAt:   testutil.TimePtr(time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC)),
				FailureReason: testutil.StringPtr("no matching rules"),
				Stats:         map[string]int{},
				CreatedAt:     time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:     time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC),
			},
			expected: MatchRunResponse{
				ID:            "11111111-1111-1111-1111-111111111111",
				ContextID:     "22222222-2222-2222-2222-222222222222",
				Mode:          "COMMIT",
				Status:        "FAILED",
				StartedAt:     "2025-01-15T10:30:00Z",
				CompletedAt:   testutil.StringPtr("2025-01-15T10:35:00Z"),
				FailureReason: testutil.StringPtr("no matching rules"),
				Stats:         map[string]int{},
				CreatedAt:     "2025-01-15T10:30:00Z",
				UpdatedAt:     "2025-01-15T10:35:00Z",
			},
		},
		{
			name: "nil stats returns empty map",
			input: &entities.MatchRun{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Mode:      value_objects.MatchRunModeDryRun,
				Status:    value_objects.MatchRunStatusProcessing,
				StartedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				Stats:     nil,
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: MatchRunResponse{
				ID:        "11111111-1111-1111-1111-111111111111",
				ContextID: "22222222-2222-2222-2222-222222222222",
				Mode:      "DRY_RUN",
				Status:    "PROCESSING",
				StartedAt: "2025-01-15T10:30:00Z",
				Stats:     map[string]int{},
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchRunToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchRunsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []*entities.MatchRun
		expected []MatchRunResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []MatchRunResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*entities.MatchRun{},
			expected: []MatchRunResponse{},
		},
		{
			name: "filters nil elements",
			input: []*entities.MatchRun{
				{
					ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					Mode:      value_objects.MatchRunModeDryRun,
					Status:    value_objects.MatchRunStatusProcessing,
					StartedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					Stats:     map[string]int{},
					CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					UpdatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				nil,
			},
			expected: []MatchRunResponse{
				{
					ID:        "11111111-1111-1111-1111-111111111111",
					ContextID: "22222222-2222-2222-2222-222222222222",
					Mode:      "DRY_RUN",
					Status:    "PROCESSING",
					StartedAt: "2025-01-15T10:30:00Z",
					Stats:     map[string]int{},
					CreatedAt: "2025-01-15T10:30:00Z",
					UpdatedAt: "2025-01-15T10:30:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchRunsToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func buildMatchGroupEntity(
	confidence int,
	status value_objects.MatchGroupStatus,
) *entities.MatchGroup {
	return &entities.MatchGroup{
		ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ContextID:  uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		RunID:      uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		RuleID:     uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		Confidence: mustParseConfidence(confidence),
		Status:     status,
		Items:      []*entities.MatchItem{},
		CreatedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
		UpdatedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
	}
}

func buildMatchItemEntity() *entities.MatchItem {
	return &entities.MatchItem{
		ID:                uuid.MustParse("55555555-5555-5555-5555-555555555555"),
		MatchGroupID:      uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		TransactionID:     uuid.MustParse("66666666-6666-6666-6666-666666666666"),
		AllocatedAmount:   decimal.NewFromFloat(100.50),
		AllocatedCurrency: "USD",
		ExpectedAmount:    decimal.NewFromFloat(100.50),
		AllowPartial:      false,
		CreatedAt:         time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
		UpdatedAt:         time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
	}
}

func buildMatchGroupResponse(confidence int, status string) MatchGroupResponse {
	ruleID := "44444444-4444-4444-4444-444444444444"
	return MatchGroupResponse{
		ID:         "11111111-1111-1111-1111-111111111111",
		ContextID:  "22222222-2222-2222-2222-222222222222",
		RunID:      "33333333-3333-3333-3333-333333333333",
		RuleID:     &ruleID,
		Confidence: confidence,
		Status:     status,
		Items:      []MatchItemResponse{},
		CreatedAt:  "2025-01-15T10:30:00Z",
		UpdatedAt:  "2025-01-15T10:30:00Z",
	}
}

func buildMatchItemResponse() MatchItemResponse {
	return MatchItemResponse{
		ID:                "55555555-5555-5555-5555-555555555555",
		MatchGroupID:      "11111111-1111-1111-1111-111111111111",
		TransactionID:     "66666666-6666-6666-6666-666666666666",
		AllocatedAmount:   "100.5",
		AllocatedCurrency: "USD",
		ExpectedAmount:    "100.5",
		AllowPartial:      false,
		CreatedAt:         "2025-01-15T10:30:00Z",
		UpdatedAt:         "2025-01-15T10:30:00Z",
	}
}

func TestMatchGroupToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.MatchGroup
		expected MatchGroupResponse
	}{
		{
			name:  "nil input returns empty struct",
			input: nil,
			expected: MatchGroupResponse{
				Items: []MatchItemResponse{},
			},
		},
		{
			name:     "full entity conversion",
			input:    buildMatchGroupEntity(85, value_objects.MatchGroupStatusProposed),
			expected: buildMatchGroupResponse(85, "PROPOSED"),
		},
		{
			name: "with items and rejection",
			input: func() *entities.MatchGroup {
				group := buildMatchGroupEntity(75, value_objects.MatchGroupStatusRejected)
				group.Items = []*entities.MatchItem{buildMatchItemEntity()}
				group.RejectedReason = testutil.StringPtr("amounts do not match")
				group.UpdatedAt = time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC)

				return group
			}(),
			expected: func() MatchGroupResponse {
				resp := buildMatchGroupResponse(75, "REJECTED")
				resp.Items = []MatchItemResponse{buildMatchItemResponse()}
				resp.RejectedReason = testutil.StringPtr("amounts do not match")
				resp.UpdatedAt = "2025-01-15T10:35:00Z"

				return resp
			}(),
		},
		{
			name: "nil rule id returns nil pointer",
			input: func() *entities.MatchGroup {
				group := buildMatchGroupEntity(85, value_objects.MatchGroupStatusProposed)
				group.RuleID = uuid.Nil
				return group
			}(),
			expected: func() MatchGroupResponse {
				resp := buildMatchGroupResponse(85, "PROPOSED")
				resp.RuleID = nil
				return resp
			}(),
		},
		{
			name: "with confirmed_at",
			input: func() *entities.MatchGroup {
				group := buildMatchGroupEntity(95, value_objects.MatchGroupStatusConfirmed)
				group.ConfirmedAt = testutil.TimePtr(time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC))
				group.UpdatedAt = time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC)

				return group
			}(),
			expected: func() MatchGroupResponse {
				resp := buildMatchGroupResponse(95, "CONFIRMED")
				resp.ConfirmedAt = testutil.StringPtr("2025-01-15T10:35:00Z")
				resp.UpdatedAt = "2025-01-15T10:35:00Z"

				return resp
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchGroupToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchGroupsToResponse(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    []*entities.MatchGroup
		expected []MatchGroupResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []MatchGroupResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*entities.MatchGroup{},
			expected: []MatchGroupResponse{},
		},
		{
			name: "populated slice converts all elements",
			input: []*entities.MatchGroup{
				{
					ID:         uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					RunID:      uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					ContextID:  uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					Status:     "CONFIRMED",
					Confidence: mustParseConfidence(95),
					CreatedAt:  fixedTime,
					UpdatedAt:  fixedTime,
				},
			},
			expected: []MatchGroupResponse{
				{
					ID:         "11111111-1111-1111-1111-111111111111",
					RunID:      "22222222-2222-2222-2222-222222222222",
					ContextID:  "33333333-3333-3333-3333-333333333333",
					Status:     "CONFIRMED",
					Confidence: 95,
					Items:      []MatchItemResponse{},
					CreatedAt:  "2025-01-15T10:30:00Z",
					UpdatedAt:  "2025-01-15T10:30:00Z",
				},
			},
		},
		{
			name:     "nil elements in slice are skipped",
			input:    []*entities.MatchGroup{nil, nil},
			expected: []MatchGroupResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchGroupsToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchItemToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.MatchItem
		expected MatchItemResponse
	}{
		{
			name:     "nil input returns empty struct",
			input:    nil,
			expected: MatchItemResponse{},
		},
		{
			name: "full entity conversion",
			input: &entities.MatchItem{
				ID:                uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				MatchGroupID:      uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				TransactionID:     uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				AllocatedAmount:   decimal.NewFromFloat(1000.00),
				AllocatedCurrency: "EUR",
				ExpectedAmount:    decimal.NewFromFloat(1000.00),
				AllowPartial:      true,
				CreatedAt:         time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:         time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: MatchItemResponse{
				ID:                "11111111-1111-1111-1111-111111111111",
				MatchGroupID:      "22222222-2222-2222-2222-222222222222",
				TransactionID:     "33333333-3333-3333-3333-333333333333",
				AllocatedAmount:   "1000",
				AllocatedCurrency: "EUR",
				ExpectedAmount:    "1000",
				AllowPartial:      true,
				CreatedAt:         "2025-01-15T10:30:00Z",
				UpdatedAt:         "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchItemToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchItemsToResponse(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    []*entities.MatchItem
		expected []MatchItemResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []MatchItemResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*entities.MatchItem{},
			expected: []MatchItemResponse{},
		},
		{
			name: "populated slice converts all elements",
			input: []*entities.MatchItem{
				{
					ID:                uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					MatchGroupID:      uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					TransactionID:     uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					AllocatedAmount:   decimal.NewFromFloat(500.00),
					AllocatedCurrency: "USD",
					ExpectedAmount:    decimal.NewFromFloat(500.00),
					AllowPartial:      false,
					CreatedAt:         fixedTime,
					UpdatedAt:         fixedTime,
				},
			},
			expected: []MatchItemResponse{
				{
					ID:                "11111111-1111-1111-1111-111111111111",
					MatchGroupID:      "22222222-2222-2222-2222-222222222222",
					TransactionID:     "33333333-3333-3333-3333-333333333333",
					AllocatedAmount:   "500",
					AllocatedCurrency: "USD",
					ExpectedAmount:    "500",
					AllowPartial:      false,
					CreatedAt:         "2025-01-15T10:30:00Z",
					UpdatedAt:         "2025-01-15T10:30:00Z",
				},
			},
		},
		{
			name:     "nil elements in slice are skipped",
			input:    []*entities.MatchItem{nil},
			expected: []MatchItemResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchItemsToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAdjustmentToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.Adjustment
		expected AdjustmentResponse
	}{
		{
			name:     "nil input returns empty struct",
			input:    nil,
			expected: AdjustmentResponse{},
		},
		{
			name: "full entity conversion with match group",
			input: &entities.Adjustment{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				MatchGroupID: testutil.UUIDPtr(
					uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				),
				Type:        entities.AdjustmentTypeBankFee,
				Amount:      decimal.NewFromFloat(10.50),
				Currency:    "USD",
				Description: "Bank wire fee",
				Reason:      "Processing fee charged by bank",
				CreatedBy:   "user@example.com",
				CreatedAt:   time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:   time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: AdjustmentResponse{
				ID:           "11111111-1111-1111-1111-111111111111",
				ContextID:    "22222222-2222-2222-2222-222222222222",
				MatchGroupID: testutil.StringPtr("33333333-3333-3333-3333-333333333333"),
				Type:         "BANK_FEE",
				Amount:       "10.5",
				Currency:     "USD",
				Description:  "Bank wire fee",
				Reason:       "Processing fee charged by bank",
				CreatedBy:    "user@example.com",
				CreatedAt:    "2025-01-15T10:30:00Z",
				UpdatedAt:    "2025-01-15T10:30:00Z",
			},
		},
		{
			name: "with transaction id instead of match group",
			input: &entities.Adjustment{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				TransactionID: testutil.UUIDPtr(
					uuid.MustParse("44444444-4444-4444-4444-444444444444"),
				),
				Type:        entities.AdjustmentTypeFXDifference,
				Amount:      decimal.NewFromFloat(5.25),
				Currency:    "EUR",
				Description: "FX rate difference",
				Reason:      "Rate changed between booking and settlement",
				CreatedBy:   "system",
				CreatedAt:   time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:   time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: AdjustmentResponse{
				ID:            "11111111-1111-1111-1111-111111111111",
				ContextID:     "22222222-2222-2222-2222-222222222222",
				TransactionID: testutil.StringPtr("44444444-4444-4444-4444-444444444444"),
				Type:          "FX_DIFFERENCE",
				Amount:        "5.25",
				Currency:      "EUR",
				Description:   "FX rate difference",
				Reason:        "Rate changed between booking and settlement",
				CreatedBy:     "system",
				CreatedAt:     "2025-01-15T10:30:00Z",
				UpdatedAt:     "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := AdjustmentToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAdjustmentsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []*entities.Adjustment
		expected []AdjustmentResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []AdjustmentResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*entities.Adjustment{},
			expected: []AdjustmentResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := AdjustmentsToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
