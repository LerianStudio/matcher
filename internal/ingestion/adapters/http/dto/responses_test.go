//go:build unit

package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobResponse_JSON(t *testing.T) {
	t.Parallel()

	startedAt := "2025-01-15T10:30:00Z"
	resp := JobResponse{
		ID:        "550e8400-e29b-41d4-a716-446655440000",
		ContextID: "550e8400-e29b-41d4-a716-446655440001",
		SourceID:  "550e8400-e29b-41d4-a716-446655440002",
		Status:    "PROCESSING",
		FileName:  "transactions.csv",
		TotalRows: 1000,
		StartedAt: &startedAt,
		CreatedAt: "2025-01-15T10:30:00Z",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded JobResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Status, decoded.Status)
	assert.Equal(t, resp.TotalRows, decoded.TotalRows)
}

func TestTransactionResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := TransactionResponse{
		ID:         "550e8400-e29b-41d4-a716-446655440000",
		JobID:      "550e8400-e29b-41d4-a716-446655440001",
		ExternalID: "TXN-12345",
		Amount:     "1000.50",
		Currency:   "USD",
		Status:     "PENDING",
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded TransactionResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, resp.ID, decoded.ID)
	assert.Equal(t, resp.Amount, decoded.Amount)
}

func TestListJobsResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := ListJobsResponse{
		Items: []JobResponse{
			{ID: "job-1", Status: "COMPLETED"},
			{ID: "job-2", Status: "PROCESSING"},
		},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ListJobsResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded.Items, 2)
}

func TestListTransactionsResponse_JSON(t *testing.T) {
	t.Parallel()

	resp := ListTransactionsResponse{
		Items: []TransactionResponse{
			{ID: "tx-1", Amount: "100.00"},
			{ID: "tx-2", Amount: "200.00"},
		},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ListTransactionsResponse

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Len(t, decoded.Items, 2)
}
