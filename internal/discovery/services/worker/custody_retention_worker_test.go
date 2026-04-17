// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// stubRetentionExtractionRepo provides FindBridgeRetentionCandidates hits.
// Other methods are zero-value; only the retention path is exercised.
type stubRetentionExtractionRepo struct {
	stubBridgeExtractionRepo

	mu                   sync.Mutex
	candidatesByTenant   map[string][]*entities.ExtractionRequest
	candidatesErr        error
	observedTenants      []string
	observedGracePeriods []time.Duration
	// markedDeletedByTenant tracks extraction IDs already marked as
	// custody-deleted — simulates the migration 000027 convergence guard
	// so a second sweep on the same row returns no candidates. Populated by
	// MarkCustodyDeleted.
	markedDeletedByTenant map[string]map[uuid.UUID]struct{}
}

// FindBridgeRetentionCandidates resolves the tenant from ctx (mirroring the
// real Postgres adapter, which uses WithTenantTxProvider to scope the
// SELECT). Resolving from ctx eliminates non-determinism that would
// otherwise arise from iterating Go's unordered map — under the old
// round-robin logic, a Parallel test could see tenant A's candidates on
// the call meant for tenant B. Polish Fix 3, T-006.
func (s *stubRetentionExtractionRepo) FindBridgeRetentionCandidates(
	ctx context.Context,
	gracePeriod time.Duration,
	_ int,
) ([]*entities.ExtractionRequest, error) {
	s.mu.Lock()
	s.observedGracePeriods = append(s.observedGracePeriods, gracePeriod)
	s.mu.Unlock()

	if s.candidatesErr != nil {
		return nil, s.candidatesErr
	}

	tenantID, _ := ctx.Value(auth.TenantIDKey).(string)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !contains(s.observedTenants, tenantID) {
		s.observedTenants = append(s.observedTenants, tenantID)
	}

	candidates := s.candidatesByTenant[tenantID]
	if len(candidates) == 0 {
		return nil, nil
	}

	// Filter out candidates whose custody has already been marked deleted —
	// this simulates the migration 000027 WHERE clause (custody_deleted_at
	// IS NULL) so the second sweep on a cleaned-up row is a no-op.
	marked := s.markedDeletedByTenant[tenantID]

	result := make([]*entities.ExtractionRequest, 0, len(candidates))

	for _, c := range candidates {
		if c == nil {
			result = append(result, nil)
			continue
		}

		if _, already := marked[c.ID]; already {
			continue
		}

		result = append(result, c)
	}

	return result, nil
}

// MarkCustodyDeleted records the delete marker by tenant+extraction ID so
// subsequent FindBridgeRetentionCandidates calls filter the row out.
// Overrides the embedded stubBridgeExtractionRepo's no-op implementation.
func (s *stubRetentionExtractionRepo) MarkCustodyDeleted(
	ctx context.Context,
	id uuid.UUID,
	deletedAt time.Time,
) error {
	// Record the call on the embedded stub for any assertions about call
	// count / args. We call via the embedded method because the field is
	// private and we want the stub's tracking to remain authoritative.
	if err := s.stubBridgeExtractionRepo.MarkCustodyDeleted(ctx, id, deletedAt); err != nil {
		return err
	}

	tenantID, _ := ctx.Value(auth.TenantIDKey).(string)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.markedDeletedByTenant == nil {
		s.markedDeletedByTenant = make(map[string]map[uuid.UUID]struct{})
	}

	if _, ok := s.markedDeletedByTenant[tenantID]; !ok {
		s.markedDeletedByTenant[tenantID] = make(map[uuid.UUID]struct{})
	}

	s.markedDeletedByTenant[tenantID][id] = struct{}{}

	return nil
}

func contains(haystack []string, needle string) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}

	return false
}

// stubCustodyStore records Delete calls and returns canned responses.
type stubCustodyStore struct {
	mu          sync.Mutex
	deleteCalls []sharedPorts.ArtifactCustodyReference
	deleteFn    func(ref sharedPorts.ArtifactCustodyReference) error
}

var _ sharedPorts.ArtifactCustodyStore = (*stubCustodyStore)(nil)

func (s *stubCustodyStore) Store(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyWriteInput,
) (*sharedPorts.ArtifactCustodyReference, error) {
	return nil, nil
}

func (s *stubCustodyStore) Open(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyReference,
) (io.ReadCloser, error) {
	return nil, nil
}

func (s *stubCustodyStore) Delete(
	_ context.Context,
	ref sharedPorts.ArtifactCustodyReference,
) error {
	s.mu.Lock()
	s.deleteCalls = append(s.deleteCalls, ref)
	s.mu.Unlock()

	if s.deleteFn != nil {
		return s.deleteFn(ref)
	}

	return nil
}

// terminalFailedExtraction returns an extraction with a terminal bridge
// failure (TERMINAL retention bucket).
func terminalFailedExtraction(id, connID uuid.UUID) *entities.ExtractionRequest {
	return &entities.ExtractionRequest{
		ID:                     id,
		ConnectionID:           connID,
		Status:                 vo.ExtractionStatusComplete,
		FetcherJobID:           "fetcher-job-" + id.String()[:8],
		ResultPath:             "/data/" + id.String() + ".json",
		BridgeAttempts:         3,
		BridgeLastError:        vo.BridgeErrorClassArtifactNotFound,
		BridgeLastErrorMessage: "404 from Fetcher",
		BridgeFailedAt:         time.Now().UTC().Add(-2 * time.Hour),
		CreatedAt:              time.Now().UTC().Add(-3 * time.Hour),
		UpdatedAt:              time.Now().UTC().Add(-2 * time.Hour),
	}
}

// lateLinkedExtraction returns an extraction that's been ingested (linked)
// long enough ago that it should fall into the LATE_LINKED retention bucket.
func lateLinkedExtraction(id, connID uuid.UUID) *entities.ExtractionRequest {
	return &entities.ExtractionRequest{
		ID:             id,
		ConnectionID:   connID,
		IngestionJobID: uuid.New(),
		Status:         vo.ExtractionStatusComplete,
		FetcherJobID:   "fetcher-job-" + id.String()[:8],
		ResultPath:     "/data/" + id.String() + ".json",
		CreatedAt:      time.Now().UTC().Add(-3 * time.Hour),
		UpdatedAt:      time.Now().UTC().Add(-2 * time.Hour),
	}
}

// liveExtraction returns an extraction that is COMPLETE+unlinked with no
// terminal error — i.e. the bridge worker still owns it. It must NOT be
// returned by FindBridgeRetentionCandidates.
func liveExtraction(id, connID uuid.UUID) *entities.ExtractionRequest {
	return &entities.ExtractionRequest{
		ID:           id,
		ConnectionID: connID,
		Status:       vo.ExtractionStatusComplete,
		FetcherJobID: "fetcher-job-" + id.String()[:8],
		ResultPath:   "/data/" + id.String() + ".json",
		CreatedAt:    time.Now().UTC().Add(-time.Minute),
		UpdatedAt:    time.Now().UTC().Add(-time.Minute),
	}
}

// --- Constructor tests ---

func TestNewCustodyRetentionWorker_NilExtractionRepo(t *testing.T) {
	t.Parallel()

	w, err := NewCustodyRetentionWorker(
		nil,
		&stubCustodyStore{},
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilCustodyRetentionExtractionRepo)
}

func TestNewCustodyRetentionWorker_NilCustody(t *testing.T) {
	t.Parallel()

	w, err := NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		nil,
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilCustodyRetentionCustody)
}

func TestNewCustodyRetentionWorker_NilTenantLister(t *testing.T) {
	t.Parallel()

	w, err := NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		&stubCustodyStore{},
		nil,
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilCustodyRetentionTenantLister)
}

func TestNewCustodyRetentionWorker_NilInfraProvider(t *testing.T) {
	t.Parallel()

	w, err := NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		&stubCustodyStore{},
		&stubBridgeTenantLister{},
		nil,
		CustodyRetentionWorkerConfig{},
		nil,
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilCustodyRetentionInfraProvider)
}

func TestNewCustodyRetentionWorker_NilLoggerCoercedToNop(t *testing.T) {
	t.Parallel()

	w, err := NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		&stubCustodyStore{},
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, w)
	assert.NotNil(t, w.logger, "nil logger must be coerced to a Nop logger so the worker is safe to use")
}

func TestNormalizeCustodyRetentionConfig_AppliesDefaults(t *testing.T) {
	t.Parallel()

	cfg := normalizeCustodyRetentionConfig(CustodyRetentionWorkerConfig{})

	assert.Equal(t, custodyRetentionDefaultInterval, cfg.Interval)
	assert.Equal(t, custodyRetentionDefaultGracePeriod, cfg.GracePeriod)
	assert.Equal(t, custodyRetentionDefaultBatchSize, cfg.BatchSize)
}

func TestNormalizeCustodyRetentionConfig_PreservesPositiveValues(t *testing.T) {
	t.Parallel()

	cfg := normalizeCustodyRetentionConfig(CustodyRetentionWorkerConfig{
		Interval:    7 * time.Minute,
		GracePeriod: 30 * time.Minute,
		BatchSize:   42,
	})

	assert.Equal(t, 7*time.Minute, cfg.Interval)
	assert.Equal(t, 30*time.Minute, cfg.GracePeriod)
	assert.Equal(t, 42, cfg.BatchSize)
}

func TestCustodyRetentionLockTTL_Minimum(t *testing.T) {
	t.Parallel()

	// Sub-second interval: TTL clamped at the minimum.
	got := custodyRetentionLockTTL(100 * time.Millisecond)
	assert.Equal(t, custodyRetentionMinLockTTL, got)
}

func TestCustodyRetentionLockTTL_ProportionalToInterval(t *testing.T) {
	t.Parallel()

	got := custodyRetentionLockTTL(10 * time.Minute)
	assert.Equal(t, 20*time.Minute, got, "TTL must be 2× interval")
}

// --- sweepOne tests (TDD cornerstone) ---

// TestCustodyRetentionWorker_SweepsOrphansFromTerminalFailedExtractions is the
// TDD cornerstone: a terminally-failed extraction's custody object must be
// deleted by the sweep.
func TestCustodyRetentionWorker_SweepsOrphansFromTerminalFailedExtractions(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()
	connID := uuid.New()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {terminalFailedExtraction(extractionID, connID)},
		},
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	count := w.sweepTenant(context.Background(), tenantID)

	assert.Equal(t, 1, count, "one terminally-failed extraction's custody must be deleted")

	custody.mu.Lock()
	calls := custody.deleteCalls
	custody.mu.Unlock()

	require.Len(t, calls, 1)
	assert.Contains(t, calls[0].Key, extractionID.String(), "delete must target the extraction's custody key")
	assert.Contains(t, calls[0].Key, tenantID, "delete key must be tenant-scoped")
}

// TestCustodyRetentionWorker_SweepsLateLinkedExtractions verifies the
// LATE_LINKED bucket: extraction was ingested but the cleanupCustody hook
// failed; the sweep is the safety net.
func TestCustodyRetentionWorker_SweepsLateLinkedExtractions(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()
	connID := uuid.New()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {lateLinkedExtraction(extractionID, connID)},
		},
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	count := w.sweepTenant(context.Background(), tenantID)

	assert.Equal(t, 1, count)

	custody.mu.Lock()
	defer custody.mu.Unlock()
	require.Len(t, custody.deleteCalls, 1)
	assert.Contains(t, custody.deleteCalls[0].Key, extractionID.String())
}

// TestCustodyRetentionWorker_PreservesOrphansStillInFlight verifies that
// the repository's filter excludes COMPLETE+unlinked+no-terminal-error rows
// (the bridge worker's responsibility). When the repo returns no candidates,
// the worker doesn't delete anything.
func TestCustodyRetentionWorker_PreservesOrphansStillInFlight(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		// No candidates returned — repo's WHERE clause excludes live
		// extractions (no terminal error AND no late-linked).
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: nil,
		},
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	count := w.sweepTenant(context.Background(), tenantID)

	assert.Equal(t, 0, count, "live extraction's custody must NOT be deleted")

	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Empty(t, custody.deleteCalls, "no Delete call should be made when the repo returns no candidates")
}

// TestCustodyRetentionWorker_HandlesTransientS3Failure verifies that a
// transient Delete failure is logged but does not crash the cycle. The next
// tick (driven by the worker loop) will retry naturally because the
// candidate row is still in the orphan population.
func TestCustodyRetentionWorker_HandlesTransientS3Failure(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()

	transientErr := errors.New("s3 timeout: i/o timeout")
	custody := &stubCustodyStore{
		deleteFn: func(_ sharedPorts.ArtifactCustodyReference) error {
			return transientErr
		},
	}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {terminalFailedExtraction(extractionID, uuid.New())},
		},
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	count := w.sweepTenant(context.Background(), tenantID)

	assert.Equal(t, 0, count, "failed delete must not be counted as deleted")

	custody.mu.Lock()
	defer custody.mu.Unlock()
	require.Len(t, custody.deleteCalls, 1, "delete was attempted exactly once for the candidate")
}

// TestCustodyRetentionWorker_RespectsGracePeriod is a unit-level proxy: the
// worker passes the configured grace period through to the repository.
// The repository's SQL WHERE clause is what enforces the grace period;
// integration tests cover the SQL semantics. Here we assert the wiring.
func TestCustodyRetentionWorker_RespectsGracePeriod(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: nil,
		},
	}

	customGrace := 45 * time.Minute
	w := newTestCustodyRetentionWorkerWithCfg(repo, custody, CustodyRetentionWorkerConfig{
		Interval:    15 * time.Minute,
		GracePeriod: customGrace,
		BatchSize:   100,
	})

	w.sweepTenant(context.Background(), tenantID)

	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.observedGracePeriods, 1, "exactly one find call expected")
	assert.Equal(t, customGrace, repo.observedGracePeriods[0],
		"worker must pass the configured grace period through to the repository")
}

// TestCustodyRetentionWorker_HandlesRepoError swallows repo errors and
// returns zero deletions — this matches BridgeWorker.processTenant's
// degrade-gracefully posture.
func TestCustodyRetentionWorker_HandlesRepoError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesErr: errors.New("connection refused"),
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	count := w.sweepTenant(context.Background(), tenantID)

	assert.Equal(t, 0, count)

	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Empty(t, custody.deleteCalls, "no deletes when find fails")
}

// TestCustodyRetentionWorker_SkipsNilCandidates verifies defensive
// nil-checking on items returned from the repo (a bug-resilient invariant).
func TestCustodyRetentionWorker_SkipsNilCandidates(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {nil, terminalFailedExtraction(uuid.New(), uuid.New()), nil},
		},
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	count := w.sweepTenant(context.Background(), tenantID)

	assert.Equal(t, 1, count, "nil candidates must be skipped, real one deleted")

	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Len(t, custody.deleteCalls, 1)
}

// TestCustodyRetentionWorker_MarksCustodyDeletedAfterDelete asserts the
// convergence marker is persisted (Polish Fix 1, T-006): after a successful
// Delete, the worker MUST call MarkCustodyDeleted so subsequent sweeps skip
// this row via the migration 000027 partial index.
func TestCustodyRetentionWorker_MarksCustodyDeletedAfterDelete(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	extractionID := uuid.New()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {terminalFailedExtraction(extractionID, uuid.New())},
		},
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	before := time.Now().UTC()
	count := w.sweepTenant(context.Background(), tenantID)
	after := time.Now().UTC()

	assert.Equal(t, 1, count, "terminal orphan must be deleted and counted")

	// Convergence marker must have been persisted for this exact extraction.
	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.custodyDeletedCalls, 1,
		"MarkCustodyDeleted must be called exactly once per successful Delete")
	assert.Equal(t, extractionID, repo.custodyDeletedCalls[0].ID,
		"marker must target the swept extraction's ID")
	// Timestamp must be a real non-zero UTC value from the sweep window.
	gotAt := repo.custodyDeletedCalls[0].DeletedAt
	assert.False(t, gotAt.IsZero(), "DeletedAt must be a real timestamp, not zero-value")
	assert.False(t, gotAt.Before(before), "DeletedAt must be >= sweep start")
	assert.False(t, gotAt.After(after.Add(time.Second)), "DeletedAt must be <= sweep end")
}

// TestCustodyRetentionWorker_ConvergesToIdle is the cornerstone convergence
// test (Polish Fix 1, T-006): after one successful sweep, a second sweep on
// the same tenant MUST return zero candidates and make zero delete calls.
// Without the custody_deleted_at marker, the LATE-LINKED bucket would match
// the same rows forever — this test fails without migration 000027.
func TestCustodyRetentionWorker_ConvergesToIdle(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()
	terminalID := uuid.New()
	lateLinkedID := uuid.New()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {
				terminalFailedExtraction(terminalID, uuid.New()),
				lateLinkedExtraction(lateLinkedID, uuid.New()),
			},
		},
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	// First sweep: both orphans deleted AND marked.
	firstCount := w.sweepTenant(context.Background(), tenantID)
	assert.Equal(t, 2, firstCount, "first sweep must delete both orphan buckets")

	custody.mu.Lock()
	firstDeleteCount := len(custody.deleteCalls)
	custody.mu.Unlock()
	assert.Equal(t, 2, firstDeleteCount, "first sweep: one Delete per candidate")

	// Second sweep: rows are already marked, stub filters them out,
	// worker does nothing. This is the convergence property.
	secondCount := w.sweepTenant(context.Background(), tenantID)
	assert.Equal(t, 0, secondCount, "second sweep MUST converge to zero deletions")

	custody.mu.Lock()
	secondDeleteCount := len(custody.deleteCalls)
	custody.mu.Unlock()
	assert.Equal(t, firstDeleteCount, secondDeleteCount,
		"second sweep MUST NOT call Delete again — the convergence marker is the guard")
}

// TestCustodyRetentionWorker_MarkerFailureIsNonFatal asserts that a
// MarkCustodyDeleted failure is logged but does not roll back the delete
// count: custody is already gone, the next sweep will retry the marker.
func TestCustodyRetentionWorker_MarkerFailureIsNonFatal(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New().String()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {terminalFailedExtraction(uuid.New(), uuid.New())},
		},
	}
	// Force the marker write to fail — simulates a DB blip.
	repo.custodyDeletedFn = func(_ uuid.UUID, _ time.Time) error {
		return errors.New("simulated db error")
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	count := w.sweepTenant(context.Background(), tenantID)

	// Delete succeeded even though marker write failed: count must reflect
	// that (retention is best-effort; we prefer retrying the marker over
	// rolling back a successful S3 delete).
	assert.Equal(t, 1, count,
		"marker write failure must not un-count a successful Delete")

	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Len(t, custody.deleteCalls, 1, "Delete was called before the marker write")
}

// TestCustodyRetentionWorker_BuildKeyFailureLoggedAndSkipped exercises the
// guard against malformed tenant IDs (e.g. with '/' or control bytes).
// custody.BuildObjectKey rejects them; the sweep moves on.
func TestCustodyRetentionWorker_BuildKeyFailureLoggedAndSkipped(t *testing.T) {
	t.Parallel()

	// Tenant ID with '/' triggers BuildObjectKey rejection.
	badTenant := "tenant/with/slash"
	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			badTenant: {terminalFailedExtraction(uuid.New(), uuid.New())},
		},
	}

	w := newTestCustodyRetentionWorker(repo, custody)

	count := w.sweepTenant(context.Background(), badTenant)

	assert.Equal(t, 0, count, "build-key failure must not count as delete")

	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Empty(t, custody.deleteCalls,
		"Delete must not be called when the tenant-scoped key cannot be built")
}

// --- Lifecycle tests ---

func TestCustodyRetentionWorker_Stop_ReturnsNotRunningWhenNeverStarted(t *testing.T) {
	t.Parallel()

	w := newTestCustodyRetentionWorker(&stubRetentionExtractionRepo{}, &stubCustodyStore{})

	err := w.Stop()
	require.ErrorIs(t, err, ErrCustodyRetentionWorkerNotRunning)
}

func TestCustodyRetentionWorker_UpdateRuntimeConfig_RejectedWhileRunning(t *testing.T) {
	t.Parallel()

	w := newTestCustodyRetentionWorker(&stubRetentionExtractionRepo{}, &stubCustodyStore{})
	w.running.Store(true)
	defer w.running.Store(false)

	err := w.UpdateRuntimeConfig(CustodyRetentionWorkerConfig{Interval: time.Minute})
	require.ErrorIs(t, err, ErrCustodyRetentionRuntimeUpdateBusy)
}

func TestCustodyRetentionWorker_UpdateRuntimeConfig_AppliedWhileStopped(t *testing.T) {
	t.Parallel()

	w := newTestCustodyRetentionWorker(&stubRetentionExtractionRepo{}, &stubCustodyStore{})

	newCfg := CustodyRetentionWorkerConfig{
		Interval:    9 * time.Minute,
		GracePeriod: 90 * time.Minute,
		BatchSize:   77,
	}
	require.NoError(t, w.UpdateRuntimeConfig(newCfg))

	assert.Equal(t, 9*time.Minute, w.cfg.Interval)
	assert.Equal(t, 90*time.Minute, w.cfg.GracePeriod)
	assert.Equal(t, 77, w.cfg.BatchSize)
}

func TestCustodyRetentionWorker_NilReceiverGuards(t *testing.T) {
	t.Parallel()

	var w *CustodyRetentionWorker

	require.ErrorIs(t, w.Start(context.Background()), ErrNilCustodyRetentionExtractionRepo)
	require.ErrorIs(t, w.Stop(), ErrCustodyRetentionWorkerNotRunning)
	require.ErrorIs(t, w.UpdateRuntimeConfig(CustodyRetentionWorkerConfig{}), ErrCustodyRetentionWorkerNotRunning)
}

// --- retentionBucket classification tests ---

func TestRetentionBucket_Terminal(t *testing.T) {
	t.Parallel()

	got := retentionBucket(terminalFailedExtraction(uuid.New(), uuid.New()))
	assert.Equal(t, "terminal", got)
}

func TestRetentionBucket_LateLinked(t *testing.T) {
	t.Parallel()

	got := retentionBucket(lateLinkedExtraction(uuid.New(), uuid.New()))
	assert.Equal(t, "late_linked", got)
}

func TestRetentionBucket_Unknown(t *testing.T) {
	t.Parallel()

	got := retentionBucket(liveExtraction(uuid.New(), uuid.New()))
	assert.Equal(t, "unknown", got)
}

func TestRetentionBucket_NilExtraction(t *testing.T) {
	t.Parallel()

	got := retentionBucket(nil)
	assert.Equal(t, "unknown", got)
}

// --- Test helpers ---

// newTestCustodyRetentionWorker builds a worker bypassing the constructor's
// nil-check chain. Tests use this to exercise sweepTenant / sweepOne in
// isolation without standing up a real Redis or repo. Mirrors the test-
// helper pattern in bridge_worker_test.go.
func newTestCustodyRetentionWorker(
	repo *stubRetentionExtractionRepo,
	custody *stubCustodyStore,
) *CustodyRetentionWorker {
	return newTestCustodyRetentionWorkerWithCfg(repo, custody, CustodyRetentionWorkerConfig{
		Interval:    15 * time.Minute,
		GracePeriod: time.Hour,
		BatchSize:   100,
	})
}

func newTestCustodyRetentionWorkerWithCfg(
	repo *stubRetentionExtractionRepo,
	custody *stubCustodyStore,
	cfg CustodyRetentionWorkerConfig,
) *CustodyRetentionWorker {
	w := &CustodyRetentionWorker{
		extractionRepo: repo,
		custody:        custody,
		tenantLister:   &stubBridgeTenantLister{},
		infraProvider:  &stubInfraProvider{},
		cfg:            cfg,
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("custody_retention_worker_test")

	return w
}
