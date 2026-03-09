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
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestStartExtraction_FetcherUnhealthy(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: false},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	err = uc.StartExtraction(
		context.Background(),
		"conn-1",
		map[string]interface{}{"transactions": true},
		sharedPorts.ExtractionParams{},
	)

	require.ErrorIs(t, err, ErrFetcherUnavailable)
}

func TestStartExtraction_SubmitJobError(t *testing.T) {
	t.Parallel()

	submitErr := errors.New("submit failed")

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, submitErr: submitErr},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	err = uc.StartExtraction(
		context.Background(),
		"conn-1",
		map[string]interface{}{"transactions": true},
		sharedPorts.ExtractionParams{Filters: map[string]interface{}{"currency": "USD"}},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "submit extraction job")
}

func TestStartExtraction_PersistError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{createErr: errors.New("db error")}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, submitJobID: "job-abc"},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	err = uc.StartExtraction(
		context.Background(),
		"conn-1",
		map[string]interface{}{"transactions": true},
		sharedPorts.ExtractionParams{},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist extraction request")
}

func TestStartExtraction_Success(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{}
	fetcherClient := &mockFetcherClient{healthy: true, submitJobID: "job-123"}

	uc, err := NewUseCase(
		fetcherClient,
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	err = uc.StartExtraction(
		context.Background(),
		"conn-1",
		map[string]interface{}{"transactions": true},
		sharedPorts.ExtractionParams{
			StartDate: "2026-03-01",
			EndDate:   "2026-03-08",
			Filters:   map[string]interface{}{"currency": "USD"},
		},
	)

	require.NoError(t, err)
	assert.Equal(t, 1, fetcherClient.submitCallCount)
	assert.Equal(t, 1, extractionRepo.createCount)
}

func TestPollExtractionStatus_FindError(t *testing.T) {
	t.Parallel()

	extractionRepo := &mockExtractionRepo{findByIDErr: errors.New("not found")}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	err = uc.PollExtractionStatus(context.Background(), uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "find extraction request")
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_TransitionsToRunning(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:            uuid.New(),
		FetcherJobID:  "job-running",
		Status:        vo.ExtractionStatusSubmitted,
		FetcherConnID: "conn-1",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusExtracting, req.Status)
}

func TestPollExtractionStatus_TransitionsToComplete(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:            uuid.New(),
		FetcherJobID:  "job-complete",
		Status:        vo.ExtractionStatusExtracting,
		FetcherConnID: "conn-1",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusComplete, req.Status)
	assert.Equal(t, "/data/result.csv", req.ResultPath)
}

func TestPollExtractionStatus_TransitionsToFailed(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:            uuid.New(),
		FetcherJobID:  "job-fail",
		Status:        vo.ExtractionStatusExtracting,
		FetcherConnID: "conn-1",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusFailed, req.Status)
	assert.Equal(t, "connection refused", req.ErrorMessage)
}

func TestPollExtractionStatus_GetStatusError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:            uuid.New(),
		FetcherJobID:  "job-x",
		Status:        vo.ExtractionStatusSubmitted,
		FetcherConnID: "conn-1",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get extraction job status")
}

func TestPollExtractionStatus_UpdateError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:            uuid.New(),
		FetcherJobID:  "job-up",
		Status:        vo.ExtractionStatusSubmitted,
		FetcherConnID: "conn-1",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update extraction request")
}

func TestPollExtractionStatus_UnknownStatus_PersistsWithoutTransition(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:            uuid.New(),
		FetcherJobID:  "job-weird",
		Status:        vo.ExtractionStatusSubmitted,
		FetcherConnID: "conn-1",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusSubmitted, req.Status)
}

func TestPollExtractionStatus_NilStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{
		ID:            uuid.New(),
		FetcherJobID:  "job-nil",
		Status:        vo.ExtractionStatusSubmitted,
		FetcherConnID: "conn-1",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
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

	err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil extraction status")
}
