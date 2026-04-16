//go:build unit

package worker

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// --- stub extraction repository ---

// stubExtractionRepo implements repositories.ExtractionRepository for poller tests.
type stubExtractionRepo struct {
	entity         *entities.ExtractionRequest
	createFn       func(ctx context.Context, req *entities.ExtractionRequest) error
	createWithTxFn func(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error
	updateFn       func(ctx context.Context, req *entities.ExtractionRequest) error
	updateIfFn     func(ctx context.Context, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error
	updateWithTxFn func(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error
	findByIDFn     func(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error)
}

var _ repositories.ExtractionRepository = (*stubExtractionRepo)(nil)

func (m *stubExtractionRepo) Create(ctx context.Context, req *entities.ExtractionRequest) error {
	if m.createFn != nil {
		return m.createFn(ctx, req)
	}

	return nil
}

func (m *stubExtractionRepo) CreateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	if m.createWithTxFn != nil {
		return m.createWithTxFn(ctx, tx, req)
	}

	return nil
}

func (m *stubExtractionRepo) Update(ctx context.Context, req *entities.ExtractionRequest) error {
	m.entity = req
	if m.updateFn != nil {
		return m.updateFn(ctx, req)
	}

	return nil
}

func (m *stubExtractionRepo) UpdateIfUnchanged(ctx context.Context, req *entities.ExtractionRequest, expectedUpdatedAt time.Time) error {
	if m.updateIfFn != nil {
		if err := m.updateIfFn(ctx, req, expectedUpdatedAt); err != nil {
			return err
		}
	}

	m.entity = req

	return m.Update(ctx, req)
}

func (m *stubExtractionRepo) UpdateIfUnchangedWithTx(
	ctx context.Context,
	_ *sql.Tx,
	req *entities.ExtractionRequest,
	expectedUpdatedAt time.Time,
) error {
	return m.UpdateIfUnchanged(ctx, req, expectedUpdatedAt)
}

func (m *stubExtractionRepo) UpdateWithTx(ctx context.Context, tx *sql.Tx, req *entities.ExtractionRequest) error {
	if m.updateWithTxFn != nil {
		return m.updateWithTxFn(ctx, tx, req)
	}

	return nil
}

func (m *stubExtractionRepo) FindByID(ctx context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}

	if m.entity != nil && m.entity.ID == id {
		return m.entity, nil
	}

	return nil, sql.ErrNoRows
}

func (m *stubExtractionRepo) LinkIfUnlinked(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}

func (m *stubExtractionRepo) FindEligibleForBridge(_ context.Context, _ int) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

// --- NewExtractionPoller tests ---

func TestNewExtractionPoller_NilFetcherClient(t *testing.T) {
	t.Parallel()

	p, err := NewExtractionPoller(
		nil,
		&stubExtractionRepo{},
		ExtractionPollerConfig{},
		&stubLogger{},
	)

	assert.Nil(t, p)
	require.ErrorIs(t, err, ErrNilFetcherClient)
}

func TestNewExtractionPoller_NilExtractionRepo(t *testing.T) {
	t.Parallel()

	p, err := NewExtractionPoller(
		&stubFetcherClient{},
		nil,
		ExtractionPollerConfig{},
		&stubLogger{},
	)

	assert.Nil(t, p)
	require.ErrorIs(t, err, ErrNilExtractionRepository)
}

func TestNewExtractionPoller_DefaultPollInterval(t *testing.T) {
	t.Parallel()

	p, err := NewExtractionPoller(
		&stubFetcherClient{},
		&stubExtractionRepo{},
		ExtractionPollerConfig{PollInterval: 0, Timeout: time.Minute},
		&stubLogger{},
	)

	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Equal(t, 5*time.Second, p.cfg.PollInterval)
}

func TestNewExtractionPoller_DefaultTimeout(t *testing.T) {
	t.Parallel()

	p, err := NewExtractionPoller(
		&stubFetcherClient{},
		&stubExtractionRepo{},
		ExtractionPollerConfig{PollInterval: time.Second, Timeout: 0},
		&stubLogger{},
	)

	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Equal(t, 10*time.Minute, p.cfg.Timeout)
}

func TestNewExtractionPoller_NilLogger_UsesNop(t *testing.T) {
	t.Parallel()

	p, err := NewExtractionPoller(
		&stubFetcherClient{},
		&stubExtractionRepo{},
		ExtractionPollerConfig{PollInterval: time.Second, Timeout: time.Minute},
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, p)
}

func TestNewExtractionPoller_Success(t *testing.T) {
	t.Parallel()

	fetcher := &stubFetcherClient{}
	repo := &stubExtractionRepo{}
	logger := &stubLogger{}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 2 * time.Second, Timeout: 5 * time.Minute},
		logger,
	)

	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Equal(t, fetcher, p.fetcherClient)
	assert.Equal(t, repo, p.extractionRepo)
	assert.Equal(t, logger, p.logger)
	assert.Equal(t, 2*time.Second, p.cfg.PollInterval)
	assert.Equal(t, 5*time.Minute, p.cfg.Timeout)
}

// --- doPoll tests ---

func TestExtractionPoller_DoPoll_ImmediateComplete(t *testing.T) {
	t.Parallel()

	var completeCalled atomic.Bool

	var resultPathReceived string

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{
				Status:     "COMPLETE",
				ResultPath: "/data/output.csv",
			}, nil
		},
	}

	repo := &stubExtractionRepo{}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-complete",
		Status:       vo.ExtractionStatusSubmitted, // Valid source state for COMPLETE transition
	}
	repo.entity = extraction

	p.doPoll(
		context.Background(),
		extraction.ID,
		func(_ context.Context, path string) error {
			completeCalled.Store(true)
			resultPathReceived = path

			return nil
		},
		nil,
	)

	assert.True(t, completeCalled.Load(), "onComplete should be called")
	assert.Equal(t, "/data/output.csv", resultPathReceived)
	assert.Equal(t, "COMPLETE", string(extraction.Status))
}

func TestExtractionPoller_DoPoll_ImmediateFailed(t *testing.T) {
	t.Parallel()

	var failedCalled atomic.Bool

	var failErrMsg string

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{
				Status: "FAILED",
			}, nil
		},
	}

	repo := &stubExtractionRepo{}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-failed",
		Status:       vo.ExtractionStatusSubmitted, // Valid source state for FAILED transition
	}
	repo.entity = extraction

	p.doPoll(
		context.Background(),
		extraction.ID,
		nil,
		func(_ context.Context, errMsg string) {
			failedCalled.Store(true)
			failErrMsg = errMsg
		},
	)

	assert.True(t, failedCalled.Load(), "onFailed should be called")
	assert.Equal(t, entities.SanitizedExtractionFailureMessage, failErrMsg)
	assert.Equal(t, "FAILED", string(extraction.Status))
}

func TestExtractionPoller_DoPoll_EventualComplete(t *testing.T) {
	t.Parallel()

	var pollCount atomic.Int32

	var completeCalled atomic.Bool

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			count := pollCount.Add(1)
			if count < 3 {
				return &sharedPorts.ExtractionJobStatus{Status: "RUNNING"}, nil
			}

			return &sharedPorts.ExtractionJobStatus{
				Status:     "COMPLETE",
				ResultPath: "/data/eventual.csv",
			}, nil
		},
	}

	repo := &stubExtractionRepo{}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: 5 * time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-eventual",
		Status:       vo.ExtractionStatusSubmitted, // Valid source state for transitions
	}
	repo.entity = extraction

	p.doPoll(
		context.Background(),
		extraction.ID,
		func(_ context.Context, _ string) error {
			completeCalled.Store(true)

			return nil
		},
		nil,
	)

	assert.True(t, completeCalled.Load(), "onComplete should eventually be called")
	assert.GreaterOrEqual(t, pollCount.Load(), int32(3), "should poll at least 3 times")
}

func TestExtractionPoller_PollOnce_RemoteNotFoundCancelsExtraction(t *testing.T) {
	t.Parallel()

	repo := &stubExtractionRepo{}
	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return nil, sharedPorts.ErrFetcherResourceNotFound
		},
	}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-missing", Status: vo.ExtractionStatusSubmitted}
	repo.entity = extraction

	done := p.pollOnce(context.Background(), extraction.ID, nil, nil)
	assert.True(t, done)
	assert.Equal(t, vo.ExtractionStatusCancelled, extraction.Status)
}

func TestExtractionPoller_PollOnce_CancelledStatusStopsPolling(t *testing.T) {
	t.Parallel()

	var failedCalled atomic.Bool
	var failErrMsg string

	repo := &stubExtractionRepo{}
	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{Status: "CANCELLED", ID: "job-cancelled"}, nil
		},
	}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{ID: uuid.New(), FetcherJobID: "job-cancelled", Status: vo.ExtractionStatusExtracting}
	repo.entity = extraction

	done := p.pollOnce(context.Background(), extraction.ID, nil, func(_ context.Context, msg string) {
		failedCalled.Store(true)
		failErrMsg = msg
	})

	assert.True(t, done)
	assert.True(t, failedCalled.Load())
	assert.Equal(t, "extraction cancelled", failErrMsg)
	assert.Equal(t, vo.ExtractionStatusCancelled, extraction.Status)
}

func TestExtractionPoller_DoPoll_Timeout(t *testing.T) {
	t.Parallel()

	var failedCalled atomic.Bool

	var failErrMsg string

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{Status: "RUNNING"}, nil
		},
	}

	repo := &stubExtractionRepo{}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: 50 * time.Millisecond},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-timeout",
		Status:       vo.ExtractionStatusSubmitted, // Valid source state for FAILED transition
	}
	repo.entity = extraction

	p.doPoll(
		context.Background(),
		extraction.ID,
		nil,
		func(_ context.Context, errMsg string) {
			failedCalled.Store(true)
			failErrMsg = errMsg
		},
	)

	assert.True(t, failedCalled.Load(), "onFailed should be called on timeout")
	assert.Equal(t, "extraction timed out", failErrMsg)
	assert.Equal(t, "FAILED", string(extraction.Status))
}

func TestExtractionPoller_DoPoll_ContextCancelled(t *testing.T) {
	t.Parallel()

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{Status: "RUNNING"}, nil
		},
	}

	repo := &stubExtractionRepo{}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: 5 * time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-cancel",
		Status:       vo.ExtractionStatusSubmitted, // Valid source state
	}
	repo.entity = extraction

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately.
	cancel()

	p.doPoll(ctx, extraction.ID, nil, nil)

	// Should return without hanging. If this test doesn't time out, it passes.
}

func TestExtractionPoller_DoPoll_StatusPollError_ContinuesPolling(t *testing.T) {
	t.Parallel()

	var pollCount atomic.Int32

	var completeCalled atomic.Bool

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			count := pollCount.Add(1)
			if count == 1 {
				return nil, errors.New("temporary network error")
			}

			return &sharedPorts.ExtractionJobStatus{
				Status:     "COMPLETE",
				ResultPath: "/data/recovered.csv",
			}, nil
		},
	}

	repo := &stubExtractionRepo{}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: 5 * time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-recover",
		Status:       vo.ExtractionStatusSubmitted, // Valid source state for transitions
	}
	repo.entity = extraction

	p.doPoll(
		context.Background(),
		extraction.ID,
		func(_ context.Context, _ string) error {
			completeCalled.Store(true)

			return nil
		},
		nil,
	)

	assert.True(t, completeCalled.Load(), "should eventually complete after transient error")
	assert.GreaterOrEqual(t, pollCount.Load(), int32(2))
}

func TestExtractionPoller_DoPoll_CompleteCallbackError_StillCompletes(t *testing.T) {
	t.Parallel()

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{
				Status:     "COMPLETE",
				ResultPath: "/data/callback-fail.csv",
			}, nil
		},
	}

	repo := &stubExtractionRepo{}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-cb-err",
		Status:       vo.ExtractionStatusSubmitted, // Valid source state for COMPLETE transition
	}
	repo.entity = extraction

	p.doPoll(
		context.Background(),
		extraction.ID,
		func(_ context.Context, _ string) error {
			return errors.New("callback failed")
		},
		nil,
	)

	// Should still mark complete even when callback errors.
	assert.Equal(t, "COMPLETE", string(extraction.Status))
}

func TestExtractionPoller_PollOnce_CompleteUpdateFailure_RollsBackState(t *testing.T) {
	t.Parallel()

	repoCalls := 0

	repo := &stubExtractionRepo{
		updateFn: func(_ context.Context, _ *entities.ExtractionRequest) error {
			repoCalls++
			if repoCalls == 1 {
				return errors.New("temporary db failure")
			}

			return nil
		},
	}

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{Status: "COMPLETE", ResultPath: "/data/final.csv"}, nil
		},
	}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-complete-rollback",
		Status:       vo.ExtractionStatusSubmitted,
	}
	repo.entity = extraction

	done := p.pollOnce(context.Background(), extraction.ID, nil, nil)
	assert.False(t, done, "poller should retry after DB failure")
	assert.Equal(t, vo.ExtractionStatusSubmitted, extraction.Status, "state must rollback when update fails")
	assert.Empty(t, extraction.ResultPath)

	done = p.pollOnce(context.Background(), extraction.ID, nil, nil)
	assert.True(t, done)
	assert.Equal(t, vo.ExtractionStatusComplete, extraction.Status)
	assert.Equal(t, "/data/final.csv", extraction.ResultPath)
}

func TestExtractionPoller_PollOnce_FailedUpdateFailure_RollsBackState(t *testing.T) {
	t.Parallel()

	repoCalls := 0

	repo := &stubExtractionRepo{
		updateFn: func(_ context.Context, _ *entities.ExtractionRequest) error {
			repoCalls++
			if repoCalls == 1 {
				return errors.New("temporary db failure")
			}

			return nil
		},
	}

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{Status: "FAILED"}, nil
		},
	}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-failed-rollback",
		Status:       vo.ExtractionStatusSubmitted,
	}
	repo.entity = extraction

	done := p.pollOnce(context.Background(), extraction.ID, nil, nil)
	assert.False(t, done, "poller should retry after DB failure")
	assert.Equal(t, vo.ExtractionStatusSubmitted, extraction.Status, "state must rollback when update fails")
	assert.Empty(t, extraction.ErrorMessage)

	done = p.pollOnce(context.Background(), extraction.ID, nil, nil)
	assert.True(t, done)
	assert.Equal(t, vo.ExtractionStatusFailed, extraction.Status)
	assert.Equal(t, entities.SanitizedExtractionFailureMessage, extraction.ErrorMessage)
}

func TestExtractionPoller_PollOnce_ReloadsLatestEntityState(t *testing.T) {
	t.Parallel()

	fetcherCalls := 0
	extractionID := uuid.New()

	repo := &stubExtractionRepo{
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
			return &entities.ExtractionRequest{
				ID:           id,
				FetcherJobID: "job-cancelled",
				Status:       vo.ExtractionStatusCancelled,
			}, nil
		},
	}

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			fetcherCalls++
			return &sharedPorts.ExtractionJobStatus{Status: "RUNNING"}, nil
		},
	}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	done := p.pollOnce(context.Background(), extractionID, nil, nil)
	assert.True(t, done)
	assert.Equal(t, 0, fetcherCalls, "poller must stop on reloaded terminal state without calling fetcher")
}

func TestExtractionPoller_PollOnce_ConcurrentUpdateStopsOnReloadedTerminalState(t *testing.T) {
	t.Parallel()

	staleExtraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-conflict",
		Status:       vo.ExtractionStatusSubmitted,
		UpdatedAt:    time.Now().UTC().Add(-time.Second),
	}
	latestExtraction := &entities.ExtractionRequest{
		ID:           staleExtraction.ID,
		FetcherJobID: staleExtraction.FetcherJobID,
		Status:       vo.ExtractionStatusComplete,
		ResultPath:   "/data/existing.csv",
		UpdatedAt:    time.Now().UTC(),
	}

	var findCount int
	repo := &stubExtractionRepo{
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
			findCount++
			if findCount == 1 {
				return staleExtraction, nil
			}

			return latestExtraction, nil
		},
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
			return repositories.ErrExtractionConflict
		},
	}

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, _ string) (*sharedPorts.ExtractionJobStatus, error) {
			return &sharedPorts.ExtractionJobStatus{Status: "FAILED"}, nil
		},
	}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	done := p.pollOnce(context.Background(), staleExtraction.ID, nil, nil)
	assert.True(t, done)
	assert.Equal(t, 2, findCount)
	assert.Equal(t, vo.ExtractionStatusComplete, latestExtraction.Status)
	assert.Equal(t, "/data/existing.csv", latestExtraction.ResultPath)
}

func TestExtractionPoller_PollOnce_QueuedStatusKeepsPollingWithoutUpdate(t *testing.T) {
	t.Parallel()

	extraction := &entities.ExtractionRequest{
		ID:           uuid.New(),
		FetcherJobID: "job-queued",
		Status:       vo.ExtractionStatusSubmitted,
		UpdatedAt:    time.Now().UTC(),
	}

	updateCalled := false
	repo := &stubExtractionRepo{
		findByIDFn: func(_ context.Context, id uuid.UUID) (*entities.ExtractionRequest, error) {
			assert.Equal(t, extraction.ID, id)
			return extraction, nil
		},
		updateIfFn: func(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
			updateCalled = true
			return nil
		},
	}

	fetcher := &stubFetcherClient{
		getExtractionJobStatusFn: func(_ context.Context, jobID string) (*sharedPorts.ExtractionJobStatus, error) {
			assert.Equal(t, extraction.FetcherJobID, jobID)
			return &sharedPorts.ExtractionJobStatus{ID: jobID, Status: "PENDING"}, nil
		},
	}

	p, err := NewExtractionPoller(
		fetcher,
		repo,
		ExtractionPollerConfig{PollInterval: 10 * time.Millisecond, Timeout: time.Second},
		&stubLogger{},
	)
	require.NoError(t, err)

	done := p.pollOnce(context.Background(), extraction.ID, nil, nil)

	assert.False(t, done)
	assert.False(t, updateCalled)
	assert.Equal(t, vo.ExtractionStatusSubmitted, extraction.Status)
}

// --- Sentinel errors ---

func TestExtractionPollerErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrNilFetcherClient,
		ErrNilExtractionRepository,
	}

	seen := make(map[string]string)
	for _, e := range errs {
		msg := e.Error()
		if prev, exists := seen[msg]; exists {
			t.Errorf("duplicate sentinel error message %q: both %q and current", msg, prev)
		}

		seen[msg] = msg
	}
}

func TestExtractionPoller_PollUntilComplete_NilReceiverInvokesFailureCallback(t *testing.T) {
	t.Parallel()

	var poller *ExtractionPoller
	called := false

	poller.PollUntilComplete(context.Background(), uuid.New(), nil, func(_ context.Context, errMsg string) {
		called = true
		assert.Equal(t, "extraction poller unavailable", errMsg)
	})

	assert.True(t, called)
}
