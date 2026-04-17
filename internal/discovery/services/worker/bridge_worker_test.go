// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

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

// TestBridgeWorker_ProcessTenant_EligibleExtraction_BridgesEndToEnd is the
// cornerstone test: one tenant, one COMPLETE+unlinked extraction, worker
// processTenant drives it through the orchestrator.
func TestBridgeWorker_ProcessTenant_EligibleExtraction_BridgesEndToEnd(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()
	connID := uuid.New()

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {completeExtraction(extractionID, connID)},
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 1, count, "one extraction should be processed")

	orch.mu.Lock()
	calls := orch.calls
	orch.mu.Unlock()

	require.Len(t, calls, 1)
	assert.Equal(t, extractionID, calls[0].ExtractionID)
	assert.Equal(t, tenantID, calls[0].TenantID)
}

// TestBridgeWorker_ProcessTenant_IneligibleExtraction_IsSwallowedAsIdempotent
// exercises the orchestrator returning ErrBridgeExtractionIneligible (a
// concurrent worker won the race). The worker treats this as idempotent
// success and counts the extraction as "processed" for reporting purposes.
func TestBridgeWorker_ProcessTenant_IneligibleExtraction_IsSwallowedAsIdempotent(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrBridgeExtractionIneligible
		},
	}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {completeExtraction(extractionID, uuid.New())},
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	// Ineligible is idempotent success: processed=1 (no error surfaced).
	assert.Equal(t, 1, count)
}

// TestBridgeWorker_ProcessTenant_AlreadyLinked_IsSwallowedAsIdempotent
// mirrors the ineligible case but via ErrExtractionAlreadyLinked, which
// occurs when a concurrent worker wrote the link between our eligible-find
// and our orchestrator call.
func TestBridgeWorker_ProcessTenant_AlreadyLinked_IsSwallowedAsIdempotent(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, sharedPorts.ErrExtractionAlreadyLinked
		},
	}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {completeExtraction(extractionID, uuid.New())},
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 1, count)
}

// TestBridgeWorker_ProcessTenant_TransientError_IsCountedNegatively
// verifies that terminal orchestrator failures (neither idempotent signal)
// are NOT counted as processed. The extraction stays unlinked and the next
// cycle will re-attempt it.
func TestBridgeWorker_ProcessTenant_TransientError_IsCountedNegatively(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()

	orch := &stubBridgeOrchestrator{
		returnFn: func(_ context.Context, _ sharedPorts.BridgeExtractionInput) (*sharedPorts.BridgeExtractionOutcome, error) {
			return nil, errors.New("transient retrieval boom")
		},
	}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {completeExtraction(extractionID, uuid.New())},
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 0, count, "transient failure should not count as processed")
}

// TestBridgeWorker_ProcessTenant_EmptyTenantBatch_ProcessesZero ensures a
// tenant with no eligible extractions is cheap — the worker returns 0 and
// does not invoke the orchestrator at all.
func TestBridgeWorker_ProcessTenant_EmptyTenantBatch_ProcessesZero(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 0, count)
	assert.Equal(t, int64(0), orch.callsCount.Load())
}

// TestBridgeWorker_ProcessTenant_RepoError_LogsAndReturnsZero ensures a
// FindEligibleForBridge failure does not crash the worker — the cycle
// moves on to the next tenant.
func TestBridgeWorker_ProcessTenant_RepoError_LogsAndReturnsZero(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{
		eligibleErr: errors.New("db down"),
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 0, count)
	assert.Equal(t, int64(0), orch.callsCount.Load())
}

// TestBridgeWorker_ProcessTenant_MultipleExtractions_AllProcessed verifies
// the batch loop iterates through every eligible extraction.
func TestBridgeWorker_ProcessTenant_MultipleExtractions_AllProcessed(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	connID := uuid.New()

	extractions := []*entities.ExtractionRequest{
		completeExtraction(uuid.New(), connID),
		completeExtraction(uuid.New(), connID),
		completeExtraction(uuid.New(), connID),
	}

	orch := &stubBridgeOrchestrator{}
	repo := &stubBridgeExtractionRepo{
		eligibleByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: extractions,
		},
	}

	w := &BridgeWorker{
		orchestrator:   orch,
		extractionRepo: repo,
		tenantLister:   &stubBridgeTenantLister{tenants: []string{tenantID}},
		cfg:            BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("bridge_worker_test")

	count := w.processTenant(context.Background(), tenantID)
	assert.Equal(t, 3, count)
	assert.Equal(t, int64(3), orch.callsCount.Load())
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

	// Allow the immediate-cycle to settle — it bails out quickly because
	// tenants is empty and the lock acquire path uses the stub provider.
	time.Sleep(50 * time.Millisecond)

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
