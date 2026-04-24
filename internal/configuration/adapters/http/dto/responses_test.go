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

func TestReconciliationContextResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := ReconciliationContextResponse{
		ID:              "550e8400-e29b-41d4-a716-446655440000",
		TenantID:        "550e8400-e29b-41d4-a716-446655440001",
		Name:            "Bank Reconciliation Q1",
		Type:            "1:1",
		Interval:        "daily",
		Status:          "ACTIVE",
		FeeToleranceAbs: "0.50",
		FeeTolerancePct: "0.01",
		CreatedAt:       "2025-01-15T10:30:00Z",
		UpdatedAt:       "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ReconciliationContextResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Name, decoded.Name)
	assert.Equal(t, resp.Type, decoded.Type)
}

func TestReconciliationSourceResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := ReconciliationSourceResponse{
		ID:        "550e8400-e29b-41d4-a716-446655440000",
		ContextID: "550e8400-e29b-41d4-a716-446655440001",
		Name:      "Primary Bank Account",
		Type:      "BANK",
		Config:    map[string]any{"key": "value"},
		CreatedAt: "2025-01-15T10:30:00Z",
		UpdatedAt: "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ReconciliationSourceResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Name, decoded.Name)
}

func TestSourceWithFieldMapStatusResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := SourceWithFieldMapStatusResponse{
		ReconciliationSourceResponse: ReconciliationSourceResponse{
			ID:   "550e8400-e29b-41d4-a716-446655440000",
			Name: "Primary Bank",
			Type: "BANK",
		},
		HasFieldMaps: true,
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded SourceWithFieldMapStatusResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.True(t, decoded.HasFieldMaps)
}

func TestFieldMapResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := FieldMapResponse{
		ID:        "550e8400-e29b-41d4-a716-446655440000",
		ContextID: "550e8400-e29b-41d4-a716-446655440001",
		SourceID:  "550e8400-e29b-41d4-a716-446655440002",
		Mapping:   map[string]any{"amount": "$.transaction.amount"},
		Version:   1,
		CreatedAt: "2025-01-15T10:30:00Z",
		UpdatedAt: "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded FieldMapResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Version, decoded.Version)
}

func TestMatchRuleResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := MatchRuleResponse{
		ID:        "550e8400-e29b-41d4-a716-446655440000",
		ContextID: "550e8400-e29b-41d4-a716-446655440001",
		Priority:  1,
		Type:      "EXACT",
		Config:    map[string]any{"field": "amount"},
		CreatedAt: "2025-01-15T10:30:00Z",
		UpdatedAt: "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded MatchRuleResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Priority, decoded.Priority)
	assert.Equal(t, resp.Type, decoded.Type)
}
