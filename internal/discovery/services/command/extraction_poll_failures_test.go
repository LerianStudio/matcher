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

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestPollExtractionStatus_GetStatusError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-x", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatusErr: errors.New("fetcher timeout")},
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

func TestPollExtractionStatus_MissingFetcherJobID(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), Status: vo.ExtractionStatusPending, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(&mockFetcherClient{healthy: true}, &mockConnectionRepo{}, &mockSchemaRepo{}, extractionRepo, &libLog.NopLogger{})
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.ErrorIs(t, err, ErrExtractionTrackingIncomplete)
	assert.Equal(t, 0, extractionRepo.updateCount)
}

func TestPollExtractionStatus_FetcherUnavailable(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-unavailable", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatusErr: sharedPorts.ErrFetcherUnavailable},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.ErrorIs(t, err, ErrFetcherUnavailable)
}

func TestPollExtractionStatus_RemoteNotFoundCancelsExtraction(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-missing", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatusErr: sharedPorts.ErrFetcherResourceNotFound},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), req.ID)

	require.NoError(t, err)
	assert.Equal(t, vo.ExtractionStatusCancelled, result.Status)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Empty(t, result.ErrorMessage)
}

func TestPollExtractionStatus_ConcurrentUpdateReturnsLatestState(t *testing.T) {
	t.Parallel()

	staleReq := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-complete", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC().Add(-time.Minute), UpdatedAt: time.Now().UTC().Add(-time.Second)}
	latestReq := &entities.ExtractionRequest{ID: staleReq.ID, FetcherJobID: staleReq.FetcherJobID, Status: vo.ExtractionStatusComplete, ConnectionID: staleReq.ConnectionID, ResultPath: "/data/already-complete.csv", CreatedAt: staleReq.CreatedAt, UpdatedAt: time.Now().UTC()}

	var findCount int
	extractionRepo := &mockExtractionRepo{
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
			return repositories.ErrExtractionConflict
		},
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
			findCount++
			if findCount == 1 {
				return staleReq, nil
			}

			return latestReq, nil
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: staleReq.FetcherJobID, Status: "COMPLETE", ResultPath: "/data/newer.csv"}},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		extractionRepo,
		&libLog.NopLogger{},
	)
	require.NoError(t, err)

	result, err := uc.PollExtractionStatus(context.Background(), staleReq.ID)

	require.NoError(t, err)
	assert.Same(t, latestReq, result)
	assert.Equal(t, 2, findCount)
	assert.Equal(t, 1, extractionRepo.updateCount)
	assert.Equal(t, vo.ExtractionStatusComplete, latestReq.Status)
	assert.Equal(t, "/data/already-complete.csv", latestReq.ResultPath)
}

func TestPollExtractionStatus_UpdateError(t *testing.T) {
	t.Parallel()

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-up", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req, updateErr: errors.New("update failed")}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-up", Status: "RUNNING"}},
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

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-weird", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true, jobStatus: &sharedPorts.ExtractionJobStatus{ID: "job-weird", Status: "SUSPENDED"}},
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

	req := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-nil", Status: vo.ExtractionStatusSubmitted, ConnectionID: uuid.New(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	extractionRepo := &mockExtractionRepo{findByIDReq: req}

	uc, err := NewUseCase(&mockFetcherClient{healthy: true, jobStatus: nil}, &mockConnectionRepo{}, &mockSchemaRepo{}, extractionRepo, &libLog.NopLogger{})
	require.NoError(t, err)

	_, err = uc.PollExtractionStatus(context.Background(), req.ID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil extraction status")
}
