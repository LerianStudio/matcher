//go:build unit

package command

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func testConnectionEntity() *entities.FetcherConnection {
	return &entities.FetcherConnection{
		ID:            uuid.New(),
		FetcherConnID: "fetcher-conn-1",
		ConfigName:    "config-1",
		DatabaseType:  "postgresql",
	}
}

func TestStartExtraction_FetcherUnhealthy(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: false},
		&mockConnectionRepo{findByIDConn: testConnectionEntity()},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		uuid.New(),
		map[string]interface{}{"transactions": true},
		sharedPorts.ExtractionParams{},
	)

	require.ErrorIs(t, err, ErrFetcherUnavailable)
}

func TestStartExtraction_SubmitJobError(t *testing.T) {
	t.Parallel()

	submitErr := errors.New("submit failed")

	connection := testConnectionEntity()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, submitErr: submitErr},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]interface{}{"transactions": true},
		sharedPorts.ExtractionParams{Filters: map[string]interface{}{"currency": "USD"}},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "submit extraction job")
}

func TestStartExtraction_PersistError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{createErr: errors.New("db error")}
	connection := testConnectionEntity()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, submitJobID: "job-abc"},
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]interface{}{"transactions": true},
		sharedPorts.ExtractionParams{},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist pending extraction request")
}

func TestStartExtraction_Success(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{}
	fetcherClient := &mockFetcherClient{healthy: true, submitJobID: "job-123"}
	connection := testConnectionEntity()

	uc, err := NewUseCase(
		fetcherClient,
		&mockConnectionRepo{findByIDConn: connection},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	extraction, err := uc.StartExtraction(
		context.Background(),
		connection.ID,
		map[string]interface{}{
			"transactions": map[string]any{"columns": []any{"id", "amount", 99}},
		},
		sharedPorts.ExtractionParams{
			StartDate: "2026-03-01",
			EndDate:   "2026-03-08",
			Filters:   map[string]interface{}{"currency": "USD"},
		},
	)

	require.NoError(t, err)
	require.NotNil(t, extraction)
	assert.Equal(t, 1, fetcherClient.submitCallCount)
	assert.Equal(t, 1, extractionRepo.createCount)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, connection.ID, extraction.ConnectionID)
	assert.Equal(t, "job-123", extraction.FetcherJobID)
	assert.Equal(t, vo.ExtractionStatusSubmitted, extraction.Status)
	assert.Equal(t, connection.FetcherConnID, fetcherClient.lastSubmitInput.ConnectionID)
	assert.Equal(t, map[string]interface{}{"currency": "USD"}, fetcherClient.lastSubmitInput.Filters)
	require.Contains(t, fetcherClient.lastSubmitInput.Tables, "transactions")
	assert.Equal(t, []string{"id", "amount"}, fetcherClient.lastSubmitInput.Tables["transactions"].Columns)
	assert.Equal(t, "2026-03-01", fetcherClient.lastSubmitInput.Tables["transactions"].StartDate)
	assert.Equal(t, "2026-03-08", fetcherClient.lastSubmitInput.Tables["transactions"].EndDate)
}

func TestBuildTableConfig(t *testing.T) {
	t.Parallel()

	params := sharedPorts.ExtractionParams{StartDate: "2026-03-01", EndDate: "2026-03-08"}

	tests := []struct {
		name     string
		input    any
		expected sharedPorts.ExtractionTableConfig
	}{
		{
			name:  "non_map_uses_dates_only",
			input: true,
			expected: sharedPorts.ExtractionTableConfig{
				StartDate: "2026-03-01",
				EndDate:   "2026-03-08",
			},
		},
		{
			name:  "string_slice_is_preserved",
			input: map[string]any{"columns": []string{"id", "amount"}},
			expected: sharedPorts.ExtractionTableConfig{
				Columns:   []string{"id", "amount"},
				StartDate: "2026-03-01",
				EndDate:   "2026-03-08",
			},
		},
		{
			name:  "any_slice_filters_non_strings",
			input: map[string]any{"columns": []any{"id", 42, "amount", false}},
			expected: sharedPorts.ExtractionTableConfig{
				Columns:   []string{"id", "amount"},
				StartDate: "2026-03-01",
				EndDate:   "2026-03-08",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, buildTableConfig(tt.input, params))
		})
	}
}

func TestStartExtraction_ConnectionNotFound(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{findByIDErr: repositories.ErrConnectionNotFound},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.StartExtraction(
		context.Background(),
		uuid.New(),
		map[string]any{"transactions": true},
		sharedPorts.ExtractionParams{},
	)

	require.ErrorIs(t, err, ErrConnectionNotFound)
}

func TestPollExtractionStatus_FindError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{findByIDErr: repositories.ErrExtractionNotFound}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrExtractionNotFound)
}

func TestPollExtractionStatus_AlreadyComplete(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-done",
		Status:       vo.ExtractionStatusComplete,
		ResultPath:   "/data/output.csv",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	// No update should be made since the status is already terminal.
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_AlreadyFailed(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-failed",
		Status:       vo.ExtractionStatusFailed,
		ErrorMessage: "disk full",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_TransitionsToRunning(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-running",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:  "job-running",
				Status: "RUNNING",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusExtracting, req.Status)
}

func TestPollExtractionStatus_TransitionsToComplete(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-complete",
		Status:       vo.ExtractionStatusExtracting,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:      "job-complete",
				Status:     "COMPLETE",
				ResultPath: "/data/result.csv",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusComplete, req.Status)
	assert.Equal(t, "/data/result.csv", req.ResultPath)
}

func TestPollExtractionStatus_TransitionsToFailed(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-fail",
		Status:       vo.ExtractionStatusExtracting,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:        "job-fail",
				Status:       "FAILED",
				ErrorMessage: "connection refused",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusFailed, req.Status)
	assert.Equal(t, "connection refused", req.ErrorMessage)
}

func TestPollExtractionStatus_GetStatusError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-x",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy:      true,
			jobStatusErr: errors.New("fetcher timeout"),
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get extraction job status")
}

func TestPollExtractionStatus_UpdateError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-up",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{
		findByIDReq: req,
		updateErr:   errors.New("update failed"),
	}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:  "job-up",
				Status: "RUNNING",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update extraction request")
}

func TestPollExtractionStatus_UnknownStatus_PersistsWithoutTransition(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-weird",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{
			healthy: true,
			jobStatus: &sharedPorts.ExtractionJobStatus{
				JobID:  "job-weird",
				Status: "SUSPENDED",
			},
		},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	assert.Equal(t, 0, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusSubmitted, req.Status)
}

func TestPollExtractionStatus_NilStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-nil",
		Status:       vo.ExtractionStatusSubmitted,
		ConnectionID: uuid.New(),
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: nil},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil extraction status")
}
