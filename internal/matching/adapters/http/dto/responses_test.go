// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchRunResponse_JSON(t *testing.T) {
	t.Parallel()

	completedAt := "2025-01-15T10:35:00Z"
	resp := MatchRunResponse{
		ID:          "550e8400-e29b-41d4-a716-446655440000",
		ContextID:   "550e8400-e29b-41d4-a716-446655440001",
		Mode:        "DRY_RUN",
		Status:      "COMPLETED",
		StartedAt:   "2025-01-15T10:30:00Z",
		CompletedAt: &completedAt,
		Stats:       map[string]int{"matched": 10, "unmatched": 2},
		CreatedAt:   "2025-01-15T10:30:00Z",
		UpdatedAt:   "2025-01-15T10:35:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded MatchRunResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Mode, decoded.Mode)
	assert.Equal(t, resp.Stats["matched"], decoded.Stats["matched"])
}

func TestMatchGroupResponse_JSON(t *testing.T) {
	t.Parallel()

	ruleID := "550e8400-e29b-41d4-a716-446655440003"
	resp := MatchGroupResponse{
		ID:         "550e8400-e29b-41d4-a716-446655440000",
		ContextID:  "550e8400-e29b-41d4-a716-446655440001",
		RunID:      "550e8400-e29b-41d4-a716-446655440002",
		RuleID:     &ruleID,
		Confidence: 85,
		Status:     "PROPOSED",
		Items:      []MatchItemResponse{},
		CreatedAt:  "2025-01-15T10:30:00Z",
		UpdatedAt:  "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded MatchGroupResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Confidence, decoded.Confidence)
}

func TestMatchItemResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := MatchItemResponse{
		ID:                "550e8400-e29b-41d4-a716-446655440000",
		MatchGroupID:      "550e8400-e29b-41d4-a716-446655440001",
		TransactionID:     "550e8400-e29b-41d4-a716-446655440002",
		AllocatedAmount:   "1000.00",
		AllocatedCurrency: "USD",
		ExpectedAmount:    "1000.00",
		AllowPartial:      false,
		CreatedAt:         "2025-01-15T10:30:00Z",
		UpdatedAt:         "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded MatchItemResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.AllocatedAmount, decoded.AllocatedAmount)
}

func TestAdjustmentResponse_JSON(t *testing.T) {
	t.Parallel()

	matchGroupID := "550e8400-e29b-41d4-a716-446655440001"
	resp := AdjustmentResponse{
		ID:           "550e8400-e29b-41d4-a716-446655440000",
		ContextID:    "550e8400-e29b-41d4-a716-446655440001",
		MatchGroupID: &matchGroupID,
		Type:         "BANK_FEE",
		Amount:       "10.50",
		Currency:     "USD",
		Description:  "Bank wire fee adjustment",
		Reason:       "Variance due to bank fee",
		CreatedBy:    "user@example.com",
		CreatedAt:    "2025-01-15T10:30:00Z",
		UpdatedAt:    "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded AdjustmentResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Type, decoded.Type)
	assert.Equal(t, resp.Amount, decoded.Amount)
}
