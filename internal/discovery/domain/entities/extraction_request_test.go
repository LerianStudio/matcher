//go:build unit

package entities_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func newExtractionRequest(t *testing.T) *entities.ExtractionRequest {
	t.Helper()

	req, err := entities.NewExtractionRequest(
		context.Background(),
		uuid.New(),
		map[string]any{"transactions": map[string]any{"columns": []string{"id", "amount"}}},
		"2026-03-01",
		"2026-03-08",
		map[string]any{"equals": map[string]any{"currency": "USD"}},
	)
	require.NoError(t, err)

	return req
}

func TestNewExtractionRequest(t *testing.T) {
	t.Parallel()

	connectionID := uuid.New()
	tables := map[string]any{"transactions": map[string]any{"columns": []string{"id", "amount"}}}
	filters := map[string]any{"equals": map[string]any{"currency": "USD"}}

	req, err := entities.NewExtractionRequest(context.Background(), connectionID, tables, "2026-03-01", "2026-03-08", filters)
	require.NoError(t, err)
	require.NotNil(t, req)

	assert.Equal(t, connectionID, req.ConnectionID)
	assert.Equal(t, vo.ExtractionStatusPending, req.Status)
	assert.Empty(t, req.FetcherJobID)
	assert.Equal(t, "2026-03-01", req.StartDate)
	assert.Equal(t, "2026-03-08", req.EndDate)
	assert.NotNil(t, req.Tables)
	assert.NotNil(t, req.Filters)
	require.Contains(t, req.Filters, "equals")
	equals, ok := req.Filters["equals"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "USD", equals["currency"])
	assert.False(t, req.CreatedAt.IsZero())
	assert.True(t, req.CreatedAt.Equal(req.UpdatedAt))

	// Defensive copy - mutating caller-owned maps must not affect the entity.
	filters["equals"].(map[string]any)["currency"] = "BRL"
	tables["transactions"] = false
	equals, ok = req.Filters["equals"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "USD", equals["currency"])
	assert.NotEqual(t, false, req.Tables["transactions"])
}

func TestNewExtractionRequest_RequiresConnectionID(t *testing.T) {
	t.Parallel()

	req, err := entities.NewExtractionRequest(context.Background(), uuid.Nil, nil, "", "", nil)
	require.Error(t, err)
	assert.Nil(t, req)
	assert.Contains(t, err.Error(), "connection id")
}

func TestExtractionRequest_Transitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(t *testing.T, req *entities.ExtractionRequest)
		status vo.ExtractionStatus
		jobID  string
		result string
		errMsg string
	}{
		{
			name: "submitted",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("job-123"))
			},
			status: vo.ExtractionStatusSubmitted,
			jobID:  "job-123",
		},
		{
			name: "extracting",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("job-123"))
				require.NoError(t, req.MarkExtracting())
			},
			status: vo.ExtractionStatusExtracting,
			jobID:  "job-123",
		},
		{
			name: "complete",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("job-123"))
				require.NoError(t, req.MarkComplete("/tmp/result.csv"))
			},
			status: vo.ExtractionStatusComplete,
			jobID:  "job-123",
			result: "/tmp/result.csv",
		},
		{
			name: "failed",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkFailed("fetcher timeout"))
			},
			status: vo.ExtractionStatusFailed,
			errMsg: "fetcher timeout",
		},
		{
			name: "cancelled",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkCancelled())
			},
			status: vo.ExtractionStatusCancelled,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := newExtractionRequest(t)
			tt.mutate(t, req)

			assert.Equal(t, tt.status, req.Status)
			assert.Equal(t, tt.jobID, req.FetcherJobID)
			assert.Equal(t, tt.result, req.ResultPath)
			assert.Equal(t, tt.errMsg, req.ErrorMessage)
		})
	}
}

func TestExtractionRequest_InvalidTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(t *testing.T, req *entities.ExtractionRequest) error
	}{
		{
			name: "submitted requires job id",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) error {
				t.Helper()
				return req.MarkSubmitted("")
			},
		},
		{
			name: "extracting requires submitted state",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) error {
				t.Helper()
				return req.MarkExtracting()
			},
		},
		{
			name: "complete requires result path",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) error {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("job-123"))
				return req.MarkComplete("")
			},
		},
		{
			name: "complete rejects path traversal",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) error {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("job-123"))
				return req.MarkComplete("/data/../secret.csv")
			},
		},
		{
			name: "failed requires message",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) error {
				t.Helper()
				return req.MarkFailed("")
			},
		},
		{
			name: "terminal cannot be failed again",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) error {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("job-123"))
				require.NoError(t, req.MarkComplete("/tmp/result.csv"))
				return req.MarkFailed("boom")
			},
		},
		{
			name: "terminal cannot be cancelled again",
			mutate: func(t *testing.T, req *entities.ExtractionRequest) error {
				t.Helper()
				require.NoError(t, req.MarkCancelled())
				return req.MarkCancelled()
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.mutate(t, newExtractionRequest(t))
			require.Error(t, err)
			assert.ErrorIs(t, err, entities.ErrInvalidTransition)
		})
	}
}

func TestExtractionRequest_JSONSerialization(t *testing.T) {
	t.Parallel()

	req := newExtractionRequest(t)

	tablesJSON, err := req.TablesJSON()
	require.NoError(t, err)

	filtersJSON, err := req.FiltersJSON()
	require.NoError(t, err)

	var tables map[string]any
	require.NoError(t, json.Unmarshal(tablesJSON, &tables))
	assert.Contains(t, tables, "transactions")

	var filters map[string]any
	require.NoError(t, json.Unmarshal(filtersJSON, &filters))
	equalsJSON, err := json.Marshal(filters["equals"])
	require.NoError(t, err)
	var equals map[string]string
	require.NoError(t, json.Unmarshal(equalsJSON, &equals))
	assert.Equal(t, "USD", equals["currency"])
}

func TestExtractionRequest_FiltersJSON_NilFiltersReturnsNil(t *testing.T) {
	t.Parallel()

	req := newExtractionRequest(t)
	req.Filters = nil

	filtersJSON, err := req.FiltersJSON()

	require.NoError(t, err)
	assert.Nil(t, filtersJSON)
}
