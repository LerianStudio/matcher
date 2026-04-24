// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
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

// TestExtractionRequest_LinkToIngestion_CompleteExtraction_Linkable verifies
// the happy-path state-machine transition for AC-F2 and AC-O2: a COMPLETE
// extraction can be linked to a downstream ingestion job exactly once. The
// UpdatedAt timestamp is bumped so UpdateIfUnchanged-style optimistic
// concurrency sees a real state change.
func TestExtractionRequest_LinkToIngestion_CompleteExtraction_Linkable(t *testing.T) {
	t.Parallel()

	req := newExtractionRequest(t)
	require.NoError(t, req.MarkSubmitted("fetcher-job-123"))
	require.NoError(t, req.MarkExtracting())
	require.NoError(t, req.MarkComplete("/data/result.json"))

	before := req.UpdatedAt
	ingestionJobID := uuid.New()

	err := req.LinkToIngestion(ingestionJobID)

	require.NoError(t, err)
	assert.Equal(t, ingestionJobID, req.IngestionJobID)
	assert.True(t, req.UpdatedAt.After(before) || req.UpdatedAt.Equal(before),
		"UpdatedAt should be bumped")
}

// TestExtractionRequest_LinkToIngestion_NilReceiver_IsNoop asserts the
// defensive nil-receiver guard does not panic. Symmetric with the entity's
// other transition methods.
func TestExtractionRequest_LinkToIngestion_NilReceiver_IsNoop(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest

	err := req.LinkToIngestion(uuid.New())
	require.NoError(t, err)
}

// TestExtractionRequest_LinkToIngestion_RejectsNilIngestionJobID guards the
// invariant that a link must name a real ingestion job. Passing uuid.Nil is
// a programmer error, surfaced as ErrInvalidTransition.
func TestExtractionRequest_LinkToIngestion_RejectsNilIngestionJobID(t *testing.T) {
	t.Parallel()

	req := newExtractionRequest(t)
	require.NoError(t, req.MarkSubmitted("fetcher-job-123"))
	require.NoError(t, req.MarkExtracting())
	require.NoError(t, req.MarkComplete("/data/result.json"))

	err := req.LinkToIngestion(uuid.Nil)

	require.ErrorIs(t, err, entities.ErrInvalidTransition)
	assert.Equal(t, uuid.Nil, req.IngestionJobID,
		"failed link should not mutate IngestionJobID")
}

// TestExtractionRequest_LinkToIngestion_RejectsNonCompleteState enforces the
// invariant that only COMPLETE extractions have trustworthy output to link.
// Table-driven to cover every non-terminal-success state.
func TestExtractionRequest_LinkToIngestion_RejectsNonCompleteState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(t *testing.T, req *entities.ExtractionRequest)
	}{
		{
			name:    "pending extraction",
			prepare: func(t *testing.T, _ *entities.ExtractionRequest) { t.Helper() },
		},
		{
			name: "submitted extraction",
			prepare: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("fetcher-job-123"))
			},
		},
		{
			name: "extracting extraction",
			prepare: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("fetcher-job-123"))
				require.NoError(t, req.MarkExtracting())
			},
		},
		{
			name: "failed extraction",
			prepare: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkSubmitted("fetcher-job-123"))
				require.NoError(t, req.MarkExtracting())
				require.NoError(t, req.MarkFailed("boom"))
			},
		},
		{
			name: "cancelled extraction",
			prepare: func(t *testing.T, req *entities.ExtractionRequest) {
				t.Helper()
				require.NoError(t, req.MarkCancelled())
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := newExtractionRequest(t)
			tt.prepare(t, req)

			err := req.LinkToIngestion(uuid.New())

			require.ErrorIs(t, err, entities.ErrInvalidTransition)
			assert.Equal(t, uuid.Nil, req.IngestionJobID,
				"failed link should not mutate IngestionJobID")
			assert.NotEqual(t, vo.ExtractionStatusComplete, req.Status,
				"precondition: state must not be COMPLETE")
		})
	}
}

// TestExtractionRequest_LinkToIngestion_RejectsRelinkWithDifferentID enforces
// the 1:1 extraction→ingestion invariant at the domain layer. This is the
// belt that pairs with the adapter's atomic UPDATE ... WHERE ingestion_job_id
// IS NULL suspenders — both layers agree on rejection semantics.
func TestExtractionRequest_LinkToIngestion_RejectsRelinkWithDifferentID(t *testing.T) {
	t.Parallel()

	req := newExtractionRequest(t)
	require.NoError(t, req.MarkSubmitted("fetcher-job-123"))
	require.NoError(t, req.MarkExtracting())
	require.NoError(t, req.MarkComplete("/data/result.json"))

	firstJobID := uuid.New()
	require.NoError(t, req.LinkToIngestion(firstJobID))

	secondJobID := uuid.New()
	err := req.LinkToIngestion(secondJobID)

	// Cross-job collision now surfaces the canonical
	// sharedPorts.ErrExtractionAlreadyLinked sentinel (Fix 6) so callers
	// can errors.Is on the same identity used by the atomic SQL guard.
	require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)
	assert.Equal(t, firstJobID, req.IngestionJobID,
		"rejected re-link must not overwrite the existing linkage")
}

// TestExtractionRequest_LinkToIngestion_IdempotentSameID preserves the
// adapter-level idempotency semantics at the domain layer: linking with the
// same id the extraction already has is a no-op success, not an error. This
// lets the bridge worker retry safely after a partial failure downstream of
// the link write.
func TestExtractionRequest_LinkToIngestion_IdempotentSameID(t *testing.T) {
	t.Parallel()

	req := newExtractionRequest(t)
	require.NoError(t, req.MarkSubmitted("fetcher-job-123"))
	require.NoError(t, req.MarkExtracting())
	require.NoError(t, req.MarkComplete("/data/result.json"))

	jobID := uuid.New()
	require.NoError(t, req.LinkToIngestion(jobID))

	err := req.LinkToIngestion(jobID)

	require.NoError(t, err, "same-id re-link must be idempotent no-op")
	assert.Equal(t, jobID, req.IngestionJobID)
}
