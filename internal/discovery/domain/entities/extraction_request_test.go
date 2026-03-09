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

func TestNewExtractionRequest_ValidInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tables := map[string]interface{}{
		"transactions": map[string]interface{}{
			"columns": []string{"id", "amount", "date"},
		},
	}

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", tables)
	require.NoError(t, err)
	require.NotNil(t, req)

	assert.NotEmpty(t, req.ID)
	assert.Equal(t, uuid.Nil, req.IngestionJobID) // Not set at creation time
	assert.Equal(t, "fetcher-conn-1", req.FetcherConnID)
	assert.Equal(t, vo.ExtractionStatusPending, req.Status)
	assert.NotNil(t, req.Tables)
	assert.Empty(t, req.FetcherJobID)
	assert.Empty(t, req.ResultPath)
	assert.Empty(t, req.ErrorMessage)
	assert.False(t, req.CreatedAt.IsZero())
	assert.False(t, req.UpdatedAt.IsZero())
	assert.Equal(t, req.CreatedAt, req.UpdatedAt)
}

func TestNewExtractionRequest_EmptyFetcherConnID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "", nil)
	require.Error(t, err)
	assert.Nil(t, req)
	assert.Contains(t, err.Error(), "fetcher connection id")
}

func TestNewExtractionRequest_NilTables(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)
	require.NotNil(t, req)
	// Tables is initialized to empty map when nil is passed (consistent with DB roundtrip).
	assert.NotNil(t, req.Tables)
	assert.Empty(t, req.Tables)
}

func TestExtractionRequest_SetIngestionJobID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)
	require.NotNil(t, req)

	// Initially, IngestionJobID is not set (zero UUID).
	assert.Equal(t, uuid.Nil, req.IngestionJobID)

	jobID := uuid.New()
	originalUpdatedAt := req.UpdatedAt

	req.SetIngestionJobID(jobID)

	assert.Equal(t, jobID, req.IngestionJobID)
	assert.False(t, req.UpdatedAt.Before(originalUpdatedAt))
}

func TestExtractionRequest_SetIngestionJobID_NilReceiver(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest
	assert.NotPanics(t, func() {
		req.SetIngestionJobID(uuid.New())
	})
}

func TestExtractionRequest_MarkSubmitted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkSubmitted("fetcher-job-456")
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusSubmitted, req.Status)
	assert.Equal(t, "fetcher-job-456", req.FetcherJobID)
}

func TestExtractionRequest_MarkSubmitted_NilReceiver(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest
	assert.NotPanics(t, func() {
		err := req.MarkSubmitted("fetcher-job-456")
		assert.NoError(t, err)
	})
}

func TestExtractionRequest_MarkSubmitted_InvalidTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkSubmitted("fetcher-job-456")
	require.NoError(t, err)

	// Attempt second MarkSubmitted should fail
	err = req.MarkSubmitted("fetcher-job-789")
	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrInvalidTransition)
}

func TestExtractionRequest_MarkExtracting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkSubmitted("fetcher-job-456")
	require.NoError(t, err)

	err = req.MarkExtracting()
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusExtracting, req.Status)
}

func TestExtractionRequest_MarkExtracting_FromPending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	// Can also go PENDING -> EXTRACTING
	err = req.MarkExtracting()
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusExtracting, req.Status)
}

func TestExtractionRequest_MarkExtracting_InvalidTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkComplete("/path/to/result")
	require.Error(t, err) // Can't complete from PENDING

	// Force to COMPLETE state for test
	err = req.MarkSubmitted("job-1")
	require.NoError(t, err)

	err = req.MarkComplete("/path/to/result")
	require.NoError(t, err)

	// Now try to MarkExtracting from COMPLETE (should fail)
	err = req.MarkExtracting()
	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrInvalidTransition)
}

func TestExtractionRequest_MarkExtracting_NilReceiver(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest
	assert.NotPanics(t, func() {
		err := req.MarkExtracting()
		assert.NoError(t, err)
	})
}

func TestExtractionRequest_MarkComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	// Must go through SUBMITTED or EXTRACTING first
	err = req.MarkSubmitted("job-1")
	require.NoError(t, err)

	err = req.MarkComplete("/data/output/results.csv")
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusComplete, req.Status)
	assert.Equal(t, "/data/output/results.csv", req.ResultPath)
	assert.True(t, req.Status.IsTerminal())
}

func TestExtractionRequest_MarkComplete_FromExtracting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkExtracting()
	require.NoError(t, err)

	err = req.MarkComplete("/data/output/results.csv")
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusComplete, req.Status)
}

func TestExtractionRequest_MarkComplete_InvalidTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	// Cannot complete from PENDING
	err = req.MarkComplete("/data/output/results.csv")
	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrInvalidTransition)
}

func TestExtractionRequest_MarkComplete_NilReceiver(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest
	assert.NotPanics(t, func() {
		err := req.MarkComplete("/some/path")
		assert.NoError(t, err)
	})
}

func TestExtractionRequest_MarkFailed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkFailed("connection timeout after 30s")
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusFailed, req.Status)
	assert.Equal(t, "connection timeout after 30s", req.ErrorMessage)
	assert.True(t, req.Status.IsTerminal())
}

func TestExtractionRequest_MarkFailed_InvalidTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkSubmitted("job-1")
	require.NoError(t, err)

	err = req.MarkComplete("/path")
	require.NoError(t, err)

	// Cannot fail from terminal state
	err = req.MarkFailed("some error")
	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrInvalidTransition)
}

func TestExtractionRequest_MarkFailed_NilReceiver(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest
	assert.NotPanics(t, func() {
		err := req.MarkFailed("some error")
		assert.NoError(t, err)
	})
}

func TestExtractionRequest_MarkCancelled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkCancelled()
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusCancelled, req.Status)
	assert.True(t, req.Status.IsTerminal())
}

func TestExtractionRequest_MarkCancelled_FromSubmitted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkSubmitted("job-1")
	require.NoError(t, err)

	err = req.MarkCancelled()
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusCancelled, req.Status)
}

func TestExtractionRequest_MarkCancelled_FromExtracting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkExtracting()
	require.NoError(t, err)

	err = req.MarkCancelled()
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusCancelled, req.Status)
}

func TestExtractionRequest_MarkCancelled_InvalidTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	err = req.MarkSubmitted("job-1")
	require.NoError(t, err)

	err = req.MarkComplete("/path")
	require.NoError(t, err)

	// Cannot cancel from terminal state
	err = req.MarkCancelled()
	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrInvalidTransition)
}

func TestExtractionRequest_MarkCancelled_NilReceiver(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest
	assert.NotPanics(t, func() {
		err := req.MarkCancelled()
		assert.NoError(t, err)
	})
}

func TestExtractionRequest_StatusTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	assert.Equal(t, vo.ExtractionStatusPending, req.Status)
	assert.False(t, req.Status.IsTerminal())

	err = req.MarkSubmitted("job-1")
	require.NoError(t, err)
	assert.Equal(t, vo.ExtractionStatusSubmitted, req.Status)
	assert.False(t, req.Status.IsTerminal())

	err = req.MarkExtracting()
	require.NoError(t, err)
	assert.Equal(t, vo.ExtractionStatusExtracting, req.Status)
	assert.False(t, req.Status.IsTerminal())

	err = req.MarkComplete("/output/data.csv")
	require.NoError(t, err)
	assert.Equal(t, vo.ExtractionStatusComplete, req.Status)
	assert.True(t, req.Status.IsTerminal())
}

func TestExtractionRequest_TablesJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tables := map[string]interface{}{
		"transactions": map[string]interface{}{
			"columns": []string{"id", "amount"},
		},
	}

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", tables)
	require.NoError(t, err)

	data, err := req.TablesJSON()
	require.NoError(t, err)
	require.NotNil(t, data)

	var parsed map[string]interface{}
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Contains(t, parsed, "transactions")
}

func TestExtractionRequest_TablesJSON_NilTables(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	data, err := req.TablesJSON()
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data))
}

func TestExtractionRequest_TablesJSON_NilReceiver(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest

	data, err := req.TablesJSON()
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data))
}

func TestExtractionRequest_FiltersJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	req.Filters = map[string]interface{}{
		"date_range": map[string]interface{}{
			"start": "2026-01-01",
			"end":   "2026-01-31",
		},
	}

	data, err := req.FiltersJSON()
	require.NoError(t, err)
	require.NotNil(t, data)

	var parsed map[string]interface{}
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Contains(t, parsed, "date_range")
}

func TestExtractionRequest_FiltersJSON_NilFilters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	req, err := entities.NewExtractionRequest(ctx, "fetcher-conn-1", nil)
	require.NoError(t, err)

	data, err := req.FiltersJSON()
	require.NoError(t, err)
	assert.Equal(t, []byte("null"), data)
}

func TestExtractionRequest_FiltersJSON_NilReceiver(t *testing.T) {
	t.Parallel()

	var req *entities.ExtractionRequest

	data, err := req.FiltersJSON()
	require.NoError(t, err)
	assert.Equal(t, []byte("null"), data)
}
