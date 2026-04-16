//go:build unit

package command

import (
	"context"
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

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-done", Status: vo.ExtractionStatusComplete, ResultPath: "/data/output.csv", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(&mockFetcherClient{healthy: true}, &mockConnectionRepo{}, &mockSchemaRepo{}, extractionRepo, &libLog.NopLogger{})
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_AlreadyFailed(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-failed", Status: vo.ExtractionStatusFailed, ErrorMessage: "disk full", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(&mockFetcherClient{healthy: true}, &mockConnectionRepo{}, &mockSchemaRepo{}, extractionRepo, &libLog.NopLogger{})
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_TransitionsToRunning(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-running", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-running", Status: "RUNNING"}},
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

func TestPollExtractionStatus_QueuedStatusDoesNotUpdate(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-queued", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-queued", Status: "SUBMITTED"}},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, req, result)
	assert.Equal(t, vo.ExtractionStatusSubmitted, req.Status)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_TransitionsToComplete(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-complete", Status: vo.ExtractionStatusExtracting, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-complete", Status: "COMPLETE", ResultPath: "/data/result.csv"}},
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

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-fail", Status: vo.ExtractionStatusExtracting, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-fail", Status: "FAILED"}},
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
	assert.Equal(t, entities.SanitizedExtractionFailureMessage, req.ErrorMessage)
}

func TestPollExtractionStatus_TransitionsToCancelled(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-cancelled", Status: vo.ExtractionStatusExtracting, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-cancelled", Status: "CANCELLED"}},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, vo.ExtractionStatusCancelled, result.Status)
	assert.Equal(t, 1, extractionRepo.updateCount)
}

func TestPollExtractionStatus_EchoFieldsDivergence_DoesNotFail(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-echo", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), Tables: map[string]any{"transactions": map[string]any{"columns": []string{"id", "amount"}}}, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-echo", Status: "RUNNING", MappedFields: map[string]map[string][]string{"prod-db": {"public.transactions": {"id", "amount"}}}}},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, vo.ExtractionStatusExtracting, req.Status)
}

func TestPollExtractionStatus_EchoFieldsMatch_NoLogNoPanic(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-match", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), Tables: map[string]any{"transactions": map[string]any{"columns": []string{"id"}}}, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-match", Status: "RUNNING", MappedFields: map[string]map[string][]string{"any-config": {"transactions": {"id"}}}}},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestPollExtractionStatus_NoEchoFields_SkipsDivergenceCheck(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-noecho", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), Tables: map[string]any{"transactions": map[string]any{"columns": []string{"id"}}}, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-noecho", Status: "RUNNING"}},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	require.NotNil(t, result)
}
