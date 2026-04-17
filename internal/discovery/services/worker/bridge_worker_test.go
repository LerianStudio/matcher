// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// stubBridgeOrchestrator records every call to BridgeExtraction for the
// bridge worker tests. It exercises the worker loop in isolation from the
// real retrieve/verify/custody/ingest/link pipeline.
type stubBridgeOrchestrator struct {
	mu         sync.Mutex
	calls      []sharedPorts.BridgeExtractionInput
	returnFn   func(ctx context.Context, input sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error)
	callsCount atomic.Int64
}

func (s *stubBridgeOrchestrator) BridgeExtraction(
	ctx context.Context,
	input sharedPorts.BridgeExtractionInput,
) (*sharedPorts.BridgeExtractionOutcome, error) {
	s.mu.Lock()
	s.calls = append(s.calls, input)
	s.mu.Unlock()

	s.callsCount.Add(1)

	if s.returnFn != nil {
		return s.returnFn(ctx, input)
	}

	return &sharedPorts.BridgeExtractionOutcome{
		IngestionJobID:   uuid.New(),
		TransactionCount: 1,
		CustodyDeleted:   true,
	}, nil
}

// stubBridgeTenantLister returns a canned list of tenants without touching
// pg_namespace. Tests call the worker's tickOnce helper directly so this
// does not need to talk to Postgres at all.
type stubBridgeTenantLister struct {
	tenants []string
	err     error
}

func (s *stubBridgeTenantLister) ListTenants(_ context.Context) ([]string, error) {
	return s.tenants, s.err
}

// stubBridgeExtractionRepo provides FindEligibleForBridge hits. Other
// methods are satisfied with zero values since the worker only invokes the
// eligible-find path directly.
// incrementCall records an IncrementBridgeAttempts call (Polish Fix 3).
// transient-failure tests assert against this counter rather than the
// pre-fix .updatedExtractions slice, because the worker no longer takes the
// wide Update path for transient retries.
type incrementCall struct {
	ID       uuid.UUID
	Attempts int
}

type stubBridgeExtractionRepo struct {
	eligibleByTenant   map[string][]*entities.ExtractionRequest
	eligibleErr        error
	mu                 sync.Mutex
	observedTenants    []string
	markedFailures     []entities.ExtractionRequest
	markBridgeFailedFn func(req *entities.ExtractionRequest) error
	updatedExtractions []entities.ExtractionRequest
	updateFn           func(req *entities.ExtractionRequest) error
	incrementCalls     []incrementCall
	incrementFn        func(id uuid.UUID, attempts int) error
	// custodyDeletedCalls records MarkCustodyDeleted invocations so tests
	// can assert convergence behavior (Polish Fix 1, T-006).
	custodyDeletedCalls []custodyDeletedCall
	custodyDeletedFn    func(id uuid.UUID, deletedAt time.Time) error
}

// custodyDeletedCall captures a single MarkCustodyDeleted invocation for
// test assertions.
type custodyDeletedCall struct {
	ID        uuid.UUID
	DeletedAt time.Time
}

func (s *stubBridgeExtractionRepo) FindEligibleForBridge(
	ctx context.Context,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	if s.eligibleErr != nil {
		return nil, s.eligibleErr
	}

	tid, _ := ctx.Value(auth.TenantIDKey).(string)

	s.mu.Lock()
	s.observedTenants = append(s.observedTenants, tid)
	s.mu.Unlock()

	return s.eligibleByTenant[tid], nil
}

// Unused repository methods — must implement the interface for compile.
func (s *stubBridgeExtractionRepo) Create(_ context.Context, _ *entities.ExtractionRequest) error {
	return nil
}

func (s *stubBridgeExtractionRepo) CreateWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (s *stubBridgeExtractionRepo) Update(_ context.Context, req *entities.ExtractionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.updateFn != nil {
		return s.updateFn(req)
	}

	if req != nil {
		s.updatedExtractions = append(s.updatedExtractions, *req)
	}

	return nil
}

func (s *stubBridgeExtractionRepo) UpdateIfUnchanged(_ context.Context, _ *entities.ExtractionRequest, _ time.Time) error {
	return nil
}

func (s *stubBridgeExtractionRepo) UpdateIfUnchangedWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest, _ time.Time) error {
	return nil
}

func (s *stubBridgeExtractionRepo) UpdateWithTx(_ context.Context, _ sharedPorts.Tx, _ *entities.ExtractionRequest) error {
	return nil
}

func (s *stubBridgeExtractionRepo) FindByID(_ context.Context, _ uuid.UUID) (*entities.ExtractionRequest, error) {
	return nil, nil
}

func (s *stubBridgeExtractionRepo) LinkIfUnlinked(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}

func (s *stubBridgeExtractionRepo) MarkBridgeFailed(_ context.Context, req *entities.ExtractionRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.markBridgeFailedFn != nil {
		return s.markBridgeFailedFn(req)
	}

	if req != nil {
		s.markedFailures = append(s.markedFailures, *req)
	}

	return nil
}

func (s *stubBridgeExtractionRepo) MarkBridgeFailedWithTx(_ context.Context, _ sharedPorts.Tx, req *entities.ExtractionRequest) error {
	return s.MarkBridgeFailed(context.Background(), req)
}

func (s *stubBridgeExtractionRepo) IncrementBridgeAttempts(_ context.Context, id uuid.UUID, attempts int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.incrementFn != nil {
		return s.incrementFn(id, attempts)
	}

	s.incrementCalls = append(s.incrementCalls, incrementCall{ID: id, Attempts: attempts})

	return nil
}

func (s *stubBridgeExtractionRepo) IncrementBridgeAttemptsWithTx(_ context.Context, _ sharedPorts.Tx, id uuid.UUID, attempts int) error {
	return s.IncrementBridgeAttempts(context.Background(), id, attempts)
}

func (s *stubBridgeExtractionRepo) CountBridgeReadiness(_ context.Context, _ time.Duration) (repositories.BridgeReadinessCounts, error) {
	return repositories.BridgeReadinessCounts{}, nil
}

func (s *stubBridgeExtractionRepo) ListBridgeCandidates(
	_ context.Context,
	_ string,
	_ time.Duration,
	_ time.Time,
	_ uuid.UUID,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (s *stubBridgeExtractionRepo) FindBridgeRetentionCandidates(
	_ context.Context,
	_ time.Duration,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	return nil, nil
}

func (s *stubBridgeExtractionRepo) MarkCustodyDeleted(
	_ context.Context,
	id uuid.UUID,
	deletedAt time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Record the attempt BEFORE invoking custodyDeletedFn so tests can
	// assert the call was made even when the fn simulates a failure
	// (e.g. the MarkerFailureIsNonFatal regression guard).
	s.custodyDeletedCalls = append(s.custodyDeletedCalls, custodyDeletedCall{
		ID:        id,
		DeletedAt: deletedAt,
	})

	if s.custodyDeletedFn != nil {
		return s.custodyDeletedFn(id, deletedAt)
	}

	return nil
}

func (s *stubBridgeExtractionRepo) MarkCustodyDeletedWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	id uuid.UUID,
	deletedAt time.Time,
) error {
	return s.MarkCustodyDeleted(context.Background(), id, deletedAt)
}

// completeExtraction builds a COMPLETE, unlinked extraction for the worker
// to pick up.
func completeExtraction(id, connID uuid.UUID) *entities.ExtractionRequest {
	return &entities.ExtractionRequest{
		ID:           id,
		ConnectionID: connID,
		Status:       vo.ExtractionStatusComplete,
		FetcherJobID: "fetcher-job-" + id.String()[:8],
		ResultPath:   "/data/" + id.String() + ".json",
		CreatedAt:    time.Now().UTC().Add(-time.Minute),
		UpdatedAt:    time.Now().UTC(),
	}
}

func TestNewBridgeWorker_NilOrchestrator_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	repo := &stubBridgeExtractionRepo{}
	tenantLister := &stubBridgeTenantLister{}

	w, err := NewBridgeWorker(nil, repo, tenantLister, &stubInfraProvider{}, BridgeWorkerConfig{}, nil)
	require.Nil(t, w)
	require.ErrorIs(t, err, sharedPorts.ErrNilBridgeOrchestrator)
}

func TestNewBridgeWorker_NilExtractionRepo_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	orch := &stubBridgeOrchestrator{}
	lister := &stubBridgeTenantLister{}

	w, err := NewBridgeWorker(orch, nil, lister, &stubInfraProvider{}, BridgeWorkerConfig{}, nil)
	require.Nil(t, w)
	require.ErrorIs(t, err, ErrNilBridgeExtractionRepo)
}

func TestNewBridgeWorker_NilTenantLister_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{}

	w, err := NewBridgeWorker(orch, repo, nil, &stubInfraProvider{}, BridgeWorkerConfig{}, nil)
	require.Nil(t, w)
	require.ErrorIs(t, err, ErrNilBridgeTenantLister)
}

func TestNormalizeBridgeConfig_AppliesDefaults(t *testing.T) {
	t.Parallel()

	cfg := normalizeBridgeConfig(BridgeWorkerConfig{})
	assert.Equal(t, bridgeDefaultInterval, cfg.Interval)
	assert.Equal(t, bridgeDefaultBatchSize, cfg.BatchSize)
}

func TestBridgeLockTTL_RespectsFloor(t *testing.T) {
	t.Parallel()

	ttl := bridgeLockTTL(100 * time.Millisecond)
	assert.GreaterOrEqual(t, ttl, bridgeMinLockTTL)

	ttl = bridgeLockTTL(30 * time.Second)
	assert.Equal(t, 60*time.Second, ttl)
}

// TestBridgeWorker_UpdateRuntimeConfig_RejectedWhileRunning asserts that
// attempting to rewrite the worker config while the loop is active is a
// programming error (WorkerManager serialises stop→config→start).
func TestBridgeWorker_UpdateRuntimeConfig_RejectedWhileRunning(t *testing.T) {
	t.Parallel()

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{}
	lister := &stubBridgeTenantLister{}

	w, err := NewBridgeWorker(orch, repo, lister, &stubInfraProvider{}, BridgeWorkerConfig{
		Interval:  time.Second,
		BatchSize: 10,
	}, nil)

	// We can't actually Start the worker without a real infraProvider, so we
	// simulate the running state directly via the atomic bool.
	require.NoError(t, err)
	require.NotNil(t, w)

	w.running.Store(true)
	defer w.running.Store(false)

	updateErr := w.UpdateRuntimeConfig(BridgeWorkerConfig{Interval: time.Minute, BatchSize: 100})
	require.ErrorIs(t, updateErr, ErrBridgeRuntimeConfigUpdateRunning)
}

// TestBridgeWorker_UpdateRuntimeConfig_AppliedWhileStopped asserts that
// config updates replace the stored interval/batch-size when the worker is
// not running (the WorkerManager always serialises stop→config→start).
func TestBridgeWorker_UpdateRuntimeConfig_AppliedWhileStopped(t *testing.T) {
	t.Parallel()

	w, err := NewBridgeWorker(
		&stubBridgeOrchestrator{},
		&stubBridgeExtractionRepo{},
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		BridgeWorkerConfig{Interval: time.Second, BatchSize: 10},
		nil,
	)
	require.NoError(t, err)

	err = w.UpdateRuntimeConfig(BridgeWorkerConfig{Interval: time.Minute, BatchSize: 100})
	require.NoError(t, err)

	assert.Equal(t, time.Minute, w.cfg.Interval)
	assert.Equal(t, 100, w.cfg.BatchSize)
}

// TestBridgeWorker_Stop_ReturnsNotRunningWhenNeverStarted exercises the
// defensive Stop() path: calling Stop without Start is a programmer error
// surfaced as a sentinel, not a panic.
func TestBridgeWorker_Stop_ReturnsNotRunningWhenNeverStarted(t *testing.T) {
	t.Parallel()

	w, err := NewBridgeWorker(
		&stubBridgeOrchestrator{},
		&stubBridgeExtractionRepo{},
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		BridgeWorkerConfig{},
		nil,
	)
	require.NoError(t, err)

	err = w.Stop()
	require.ErrorIs(t, err, ErrBridgeWorkerNotRunning)
}

// TestBridgeWorker_Stop_ConcurrentStopsReturnExactlyOneNil exercises the
// CompareAndSwap-at-top fix (Fix 5). N concurrent goroutines call Stop()
// on a single running worker; exactly ONE must observe a nil return (the
// CAS winner), and the remaining N-1 must observe ErrBridgeWorkerNotRunning
// without blocking forever on doneCh.
//
// The pre-fix layout (Load → close → CAS) had a TOCTOU window where two
// goroutines could observe running=true, both proceed past the load, and
// race to close stopCh — one would close it (via stopOnce) and the other
// would block on doneCh until the run loop exited, then both return nil
// and the caller has no way to know which one actually performed the stop.
func TestBridgeWorker_Stop_ConcurrentStopsReturnExactlyOneNil(t *testing.T) {
	t.Parallel()

	w, err := NewBridgeWorker(
		&stubBridgeOrchestrator{},
		&stubBridgeExtractionRepo{},
		&stubBridgeTenantLister{tenants: nil},
		&stubInfraProvider{},
		BridgeWorkerConfig{Interval: time.Hour}, // long interval — only the immediate-cycle runs.
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, w.Start(context.Background()))

	// Start() sets running=true via CompareAndSwap BEFORE spawning the run
	// goroutine, so the CAS contract under concurrent Stops is already
	// testable the instant Start returns. The in-flight immediate cycle
	// (if still running) will observe stopCh closed at the next select
	// boundary — Stop() blocks on <-doneCh until the cycle exits, so the
	// test remains deterministic without any sleep-based synchronization.
	const concurrentStops = 16

	var (
		wg     sync.WaitGroup
		nilCnt atomic.Int64
	)

	wg.Add(concurrentStops)

	for i := 0; i < concurrentStops; i++ {
		go func() {
			defer wg.Done()

			if stopErr := w.Stop(); stopErr == nil {
				nilCnt.Add(1)
			}
		}()
	}

	wg.Wait()

	require.Equal(t, int64(1), nilCnt.Load(),
		"exactly one Stop() must win the CompareAndSwap and return nil; got %d nil returns", nilCnt.Load())
}
