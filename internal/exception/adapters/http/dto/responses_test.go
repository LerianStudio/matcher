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

func TestExceptionResponse_JSON(t *testing.T) {
	t.Parallel()

	externalSystem := "JIRA"
	resp := ExceptionResponse{
		ID:             "550e8400-e29b-41d4-a716-446655440000",
		TransactionID:  "550e8400-e29b-41d4-a716-446655440001",
		Severity:       "HIGH",
		Status:         "OPEN",
		ExternalSystem: &externalSystem,
		CreatedAt:      "2025-01-15T10:30:00Z",
		UpdatedAt:      "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ExceptionResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Severity, decoded.Severity)
}

func TestDisputeResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := DisputeResponse{
		ID:          "550e8400-e29b-41d4-a716-446655440000",
		ExceptionID: "550e8400-e29b-41d4-a716-446655440001",
		Category:    "BANK_FEE_ERROR",
		State:       "OPEN",
		Description: "Transaction amount differs",
		OpenedBy:    "user@example.com",
		Evidence:    []EvidenceResponse{},
		CreatedAt:   "2025-01-15T10:30:00Z",
		UpdatedAt:   "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded DisputeResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Category, decoded.Category)
}

func TestEvidenceResponse_JSON(t *testing.T) {
	t.Parallel()

	fileURL := "https://storage.example.com/evidence/doc.pdf"
	resp := EvidenceResponse{
		ID:          "550e8400-e29b-41d4-a716-446655440000",
		DisputeID:   "550e8400-e29b-41d4-a716-446655440001",
		Comment:     "Bank statement",
		SubmittedBy: "user@example.com",
		FileURL:     &fileURL,
		SubmittedAt: "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded EvidenceResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Comment, decoded.Comment)
}

func TestListExceptionsResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := ListExceptionsResponse{
		Items: []ExceptionResponse{
			{ID: "exc-1", Severity: "HIGH"},
			{ID: "exc-2", Severity: "LOW"},
		},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ListExceptionsResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded.Items, 2)
}

func TestDispatchResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := DispatchResponse{
		ExceptionID:       "550e8400-e29b-41d4-a716-446655440000",
		Target:            "JIRA",
		ExternalReference: "RECON-1234",
		Acknowledged:      true,
		DispatchedAt:      "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded DispatchResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ExceptionID, decoded.ExceptionID)
	assert.Equal(t, resp.Target, decoded.Target)
	assert.True(t, decoded.Acknowledged)
}

func TestHistoryResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := HistoryResponse{
		Items: []HistoryEntryResponse{
			{ID: "hist-1", Action: "CREATE"},
			{ID: "hist-2", Action: "UPDATE"},
		},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded HistoryResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded.Items, 2)
}
