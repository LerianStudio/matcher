// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
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
	defer s.mu.Unlock()

	s.observedGracePeriods = append(s.observedGracePeriods, gracePeriod)

	if s.candidatesErr != nil {
		return nil, s.candidatesErr
	}

	tenantID, _ := ctx.Value(auth.TenantIDKey).(string)

	if !slices.Contains(s.observedTenants, tenantID) {
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

// stubCustodyStore records Delete calls and returns canned responses.
// It implements both sharedPorts.ArtifactCustodyStore and
// sharedPorts.CustodyKeyBuilder so the worker can be wired against real
// port interfaces without importing the custody adapter package.
type stubCustodyStore struct {
	mu          sync.Mutex
	deleteCalls []sharedPorts.ArtifactCustodyReference
	deleteFn    func(ref sharedPorts.ArtifactCustodyReference) error
}

var (
	_ sharedPorts.ArtifactCustodyStore = (*stubCustodyStore)(nil)
	_ sharedPorts.CustodyKeyBuilder    = (*stubCustodyStore)(nil)
)

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

// BuildObjectKey mirrors the custody adapter's tenant-scoped key layout
// (including its validation rules) so sweep tests exercise the same
// rejection paths (empty tenant, '/' in tenant, control bytes, nil
// extraction id) the production adapter enforces.
func (s *stubCustodyStore) BuildObjectKey(
	tenantID string,
	extractionID uuid.UUID,
) (string, error) {
	return stubBuildObjectKey(tenantID, extractionID)
}

// stubBuildObjectKey replicates custody.BuildObjectKey's validation so the
// worker unit tests stay independent of the custody adapter package. Any
// change to the adapter's contract should be reflected here.
func stubBuildObjectKey(tenantID string, extractionID uuid.UUID) (string, error) {
	trimmed := strings.TrimSpace(tenantID)
	if trimmed == "" {
		return "", sharedPorts.ErrArtifactTenantIDRequired
	}

	if strings.ContainsRune(trimmed, '/') {
		return "", sharedPorts.ErrArtifactTenantIDRequired
	}

	for i := 0; i < len(trimmed); i++ {
		b := trimmed[i]
		if b < 0x20 || b == 0x7F {
			return "", sharedPorts.ErrArtifactTenantIDRequired
		}
	}

	if extractionID == uuid.Nil {
		return "", sharedPorts.ErrArtifactExtractionIDRequired
	}

	return trimmed + "/fetcher-artifacts/" + extractionID.String() + ".json", nil
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

	custodyStub := &stubCustodyStore{}

	w, err := NewCustodyRetentionWorker(
		nil,
		custodyStub,
		custodyStub,
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
		&stubCustodyStore{},
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilCustodyRetentionCustody)
}

func TestNewCustodyRetentionWorker_NilKeyBuilder(t *testing.T) {
	t.Parallel()

	w, err := NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		&stubCustodyStore{},
		nil,
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)

	assert.Nil(t, w)
	require.ErrorIs(t, err, ErrNilCustodyRetentionKeyBuilder)
}

func TestNewCustodyRetentionWorker_NilTenantLister(t *testing.T) {
	t.Parallel()

	custodyStub := &stubCustodyStore{}

	w, err := NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		custodyStub,
		custodyStub,
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

	custodyStub := &stubCustodyStore{}

	w, err := NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		custodyStub,
		custodyStub,
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

	custodyStub := &stubCustodyStore{}

	w, err := NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		custodyStub,
		custodyStub,
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
	assert.Equal(t, CustodyRetentionDefaultBatchSize, cfg.BatchSize)
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

	// The marker write MUST have been attempted — "non-fatal" means the
	// failure is swallowed, not that the attempt was skipped. Without this
	// assertion a regression could silently stop calling MarkCustodyDeleted
	// entirely and the test would still pass via deleteCalls alone.
	repo.mu.Lock()
	markAttempts := len(repo.custodyDeletedCalls)
	repo.mu.Unlock()
	assert.Equal(t, 1, markAttempts,
		"MarkCustodyDeleted must be attempted exactly once even though it fails")

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

	// All lifecycle methods on a nil receiver return the worker-nil
	// sentinel. This distinguishes "worker itself is nil" from "a
	// dependency was nil at construction" (ErrNilCustodyRetention*) and
	// from "worker exists but is not running" (ErrCustodyRetentionWorker
	// NotRunning).
	require.ErrorIs(t, w.Start(context.Background()), ErrCustodyRetentionWorkerNil)
	require.ErrorIs(t, w.Stop(), ErrCustodyRetentionWorkerNil)
	require.ErrorIs(t, w.UpdateRuntimeConfig(CustodyRetentionWorkerConfig{}), ErrCustodyRetentionWorkerNil)

	// Done on a nil receiver returns a pre-closed channel so callers
	// race-free observe "already done" rather than blocking forever.
	done := w.Done()
	require.NotNil(t, done, "Done() on nil receiver must not return nil")

	select {
	case <-done:
		// pre-closed as expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Done() on nil receiver must be pre-closed")
	}
}

// TestCustodyRetentionWorker_ConstructorSentinelsNotConfusedWithWorkerNil
// guards against a regression where the nil-receiver sentinel and the
// nil-dependency sentinels could be confused. Each constructor nil-arg path
// MUST return its specific dependency sentinel, never ErrCustodyRetention
// WorkerNil — the worker-nil sentinel is reserved for lifecycle methods
// called on a nil *CustodyRetentionWorker.
func TestCustodyRetentionWorker_ConstructorSentinelsNotConfusedWithWorkerNil(t *testing.T) {
	t.Parallel()

	custodyStub := &stubCustodyStore{}

	// nil extraction repo: must be ErrNilCustodyRetentionExtractionRepo, NOT ErrCustodyRetentionWorkerNil.
	_, err := NewCustodyRetentionWorker(
		nil,
		custodyStub,
		custodyStub,
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)
	require.ErrorIs(t, err, ErrNilCustodyRetentionExtractionRepo)
	require.NotErrorIs(t, err, ErrCustodyRetentionWorkerNil)

	// nil custody: must be ErrNilCustodyRetentionCustody, NOT ErrCustodyRetentionWorkerNil.
	_, err = NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		nil,
		custodyStub,
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)
	require.ErrorIs(t, err, ErrNilCustodyRetentionCustody)
	require.NotErrorIs(t, err, ErrCustodyRetentionWorkerNil)

	// nil key builder: must be ErrNilCustodyRetentionKeyBuilder, NOT ErrCustodyRetentionWorkerNil.
	_, err = NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		custodyStub,
		nil,
		&stubBridgeTenantLister{},
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)
	require.ErrorIs(t, err, ErrNilCustodyRetentionKeyBuilder)
	require.NotErrorIs(t, err, ErrCustodyRetentionWorkerNil)

	// nil tenant lister: must be ErrNilCustodyRetentionTenantLister, NOT ErrCustodyRetentionWorkerNil.
	_, err = NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		custodyStub,
		custodyStub,
		nil,
		&stubInfraProvider{},
		CustodyRetentionWorkerConfig{},
		nil,
	)
	require.ErrorIs(t, err, ErrNilCustodyRetentionTenantLister)
	require.NotErrorIs(t, err, ErrCustodyRetentionWorkerNil)

	// nil infra provider: must be ErrNilCustodyRetentionInfraProvider, NOT ErrCustodyRetentionWorkerNil.
	_, err = NewCustodyRetentionWorker(
		&stubRetentionExtractionRepo{},
		custodyStub,
		custodyStub,
		&stubBridgeTenantLister{},
		nil,
		CustodyRetentionWorkerConfig{},
		nil,
	)
	require.ErrorIs(t, err, ErrNilCustodyRetentionInfraProvider)
	require.NotErrorIs(t, err, ErrCustodyRetentionWorkerNil)
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

// TestRetentionBucket_TerminalWinsOverLateLinked asserts the implicit
// priority in retentionBucket: when BOTH BridgeLastError and
// IngestionJobID are set on the same row, the bucket is "terminal" (the
// earlier branch). Documents the invariant that a bridge failure
// followed by a late ingestion link keeps the row classified as
// terminal, not late_linked.
func TestRetentionBucket_TerminalWinsOverLateLinked(t *testing.T) {
	t.Parallel()

	// Start from a terminal extraction (BridgeLastError set) and add an
	// IngestionJobID so both discriminators are populated simultaneously.
	extraction := terminalFailedExtraction(uuid.New(), uuid.New())
	extraction.IngestionJobID = uuid.New()

	got := retentionBucket(extraction)
	assert.Equal(t, "terminal", got,
		"terminal must win over late_linked when both discriminators are set")
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
		keyBuilder:     custody,
		tenantLister:   &stubBridgeTenantLister{},
		infraProvider:  &stubInfraProvider{},
		cfg:            cfg,
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("custody_retention_worker_test")

	return w
}

// newTestCustodyRetentionWorkerFull is a wiring helper for sweepCycle tests.
// Unlike newTestCustodyRetentionWorker, it accepts the tenantLister and
// infraProvider so tests can inject miniredis-backed lock stubs and tenant
// lists. Everything else defaults to the same config the smaller helper
// uses, so tests only pay attention to the collaborators they care about.
func newTestCustodyRetentionWorkerFull(
	repo *stubRetentionExtractionRepo,
	custody *stubCustodyStore,
	tenantLister sharedPorts.TenantLister,
	infraProvider sharedPorts.InfrastructureProvider,
) *CustodyRetentionWorker {
	w := &CustodyRetentionWorker{
		extractionRepo: repo,
		custody:        custody,
		keyBuilder:     custody,
		tenantLister:   tenantLister,
		infraProvider:  infraProvider,
		cfg: CustodyRetentionWorkerConfig{
			Interval:    15 * time.Minute,
			GracePeriod: time.Hour,
			BatchSize:   100,
		},
	}
	w.logger = &stubLogger{}
	w.tracer = otel.Tracer("custody_retention_worker_test")

	return w
}

// newMiniredisInfraProvider wires a miniredis instance to a testutil
// MockInfrastructureProvider so sweepCycle's acquireLock path runs against a
// real (in-process) Redis server. Mirrors the pattern used in
// internal/matching/adapters/redis/lock_manager_test.go. Returns both the
// provider and the miniredis handle — the latter lets tests inspect keys or
// inject failures.
func newMiniredisInfraProvider(t *testing.T) (*testutil.MockInfrastructureProvider, *miniredis.Miniredis) {
	t.Helper()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}

	return provider, srv
}

// --- sweepCycle tests ---

// TestSweepCycle_LockNotAcquired_EarlyReturn verifies that when another
// replica already holds the retention lock, sweepCycle exits without
// touching tenants. The SetNX call observes the pre-existing key and
// returns false, short-circuiting tenant iteration. This is the primary
// coordination property of the worker — without it, N replicas would each
// run a full sweep cycle every tick.
func TestSweepCycle_LockNotAcquired_EarlyReturn(t *testing.T) {
	t.Parallel()

	provider, srv := newMiniredisInfraProvider(t)

	// Pre-seed the lock with a value held by an imaginary peer replica,
	// with TTL well past the sweepCycle's SetNX TTL. This forces SetNX
	// from the worker to return false.
	require.NoError(t, srv.Set(custodyRetentionLockKey, "peer-token"))
	srv.SetTTL(custodyRetentionLockKey, time.Hour)

	tenantID := uuid.New().String()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {terminalFailedExtraction(uuid.New(), uuid.New())},
		},
	}

	lister := &stubBridgeTenantLister{tenants: []string{tenantID}}

	w := newTestCustodyRetentionWorkerFull(repo, custody, lister, provider)

	w.sweepCycle(context.Background())

	// Lock was NOT acquired → worker must NOT have iterated tenants and
	// MUST NOT have issued any custody deletes.
	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Empty(t, custody.deleteCalls,
		"sweepCycle must not call Delete when the distributed lock is held by another replica")

	repo.mu.Lock()
	defer repo.mu.Unlock()
	assert.Empty(t, repo.observedTenants,
		"sweepCycle must not reach FindBridgeRetentionCandidates when the lock is held elsewhere")
}

// TestSweepCycle_LockAcquisitionError_WarnLogNoDeletes verifies that when
// GetRedisConnection surfaces an error (e.g. infra provider failure),
// sweepCycle logs a warning and exits without touching tenants. This is
// the degrade-gracefully posture: a transient Redis hiccup must not crash
// the worker or trigger a sweep without lock protection.
func TestSweepCycle_LockAcquisitionError_WarnLogNoDeletes(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		RedisErr: errors.New("redis infra down"),
	}

	tenantID := uuid.New().String()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			tenantID: {terminalFailedExtraction(uuid.New(), uuid.New())},
		},
	}

	lister := &stubBridgeTenantLister{tenants: []string{tenantID}}

	w := newTestCustodyRetentionWorkerFull(repo, custody, lister, provider)

	// Must not panic — the worker's acquireLock returns the error and
	// sweepCycle's lock branch logs WARN and returns.
	w.sweepCycle(context.Background())

	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Empty(t, custody.deleteCalls,
		"lock acquisition error must short-circuit the cycle before any tenant work")

	repo.mu.Lock()
	defer repo.mu.Unlock()
	assert.Empty(t, repo.observedTenants,
		"lock acquisition error must not reach FindBridgeRetentionCandidates")
}

// TestSweepCycle_TenantListError_WarnLogNoDeletes verifies that a
// ListTenants failure is logged but does not crash the cycle and does not
// sweep any tenants. This matches BridgeWorker.pollCycle's tenant-list
// error handling. Without this guard, a transient pg_namespace hiccup
// would take the worker goroutine down with it.
func TestSweepCycle_TenantListError_WarnLogNoDeletes(t *testing.T) {
	t.Parallel()

	provider, _ := newMiniredisInfraProvider(t)

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{}

	lister := &stubBridgeTenantLister{
		err: errors.New("pg_namespace query failed"),
	}

	w := newTestCustodyRetentionWorkerFull(repo, custody, lister, provider)

	w.sweepCycle(context.Background())

	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Empty(t, custody.deleteCalls,
		"tenant list error must short-circuit the cycle before per-tenant work")

	repo.mu.Lock()
	defer repo.mu.Unlock()
	assert.Empty(t, repo.observedTenants,
		"tenant list error must not reach FindBridgeRetentionCandidates")
}

// TestSweepCycle_MultiTenantAggregation_IncludesDefault is the load-bearing
// correctness test: sweepCycle must process the default tenant just like
// any UUID-named tenant. This guards against the 2026-02-06 regression
// where background workers enumerating via pg_namespace excluded the
// default tenant (which lives in the public schema, not a UUID schema).
// auth.DefaultTenantID in the list MUST be swept, and per-tenant counts
// aggregate correctly across all three tenants.
func TestSweepCycle_MultiTenantAggregation_IncludesDefault(t *testing.T) {
	t.Parallel()

	provider, _ := newMiniredisInfraProvider(t)

	defaultTenant := auth.DefaultTenantID
	tenantA := uuid.New().String()
	tenantB := uuid.New().String()

	// Each tenant gets a different number of terminal orphans so the
	// aggregation assertion is meaningful: default=1, A=2, B=3 (total=6).
	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			defaultTenant: {
				terminalFailedExtraction(uuid.New(), uuid.New()),
			},
			tenantA: {
				terminalFailedExtraction(uuid.New(), uuid.New()),
				terminalFailedExtraction(uuid.New(), uuid.New()),
			},
			tenantB: {
				terminalFailedExtraction(uuid.New(), uuid.New()),
				terminalFailedExtraction(uuid.New(), uuid.New()),
				terminalFailedExtraction(uuid.New(), uuid.New()),
			},
		},
	}

	lister := &stubBridgeTenantLister{
		tenants: []string{defaultTenant, tenantA, tenantB},
	}

	w := newTestCustodyRetentionWorkerFull(repo, custody, lister, provider)

	w.sweepCycle(context.Background())

	// All three tenants must have been observed (meaning the default
	// tenant was NOT skipped, which is the load-bearing guarantee).
	repo.mu.Lock()
	observed := append([]string(nil), repo.observedTenants...)
	repo.mu.Unlock()

	assert.Contains(t, observed, defaultTenant,
		"default tenant MUST be processed — regression guard against pg_namespace enumeration excluding public schema")
	assert.Contains(t, observed, tenantA, "tenantA must be processed")
	assert.Contains(t, observed, tenantB, "tenantB must be processed")
	assert.Len(t, observed, 3, "exactly three tenants processed, none skipped")

	// Cross-tenant aggregation: total Delete calls == 1 + 2 + 3 = 6.
	custody.mu.Lock()
	defer custody.mu.Unlock()
	assert.Len(t, custody.deleteCalls, 6,
		"cross-tenant delete count must aggregate every tenant's orphans")

	// And each tenant's deletes appear in the aggregated key list (the
	// key prefix is the tenant ID via stubBuildObjectKey).
	for _, tenant := range []string{defaultTenant, tenantA, tenantB} {
		found := 0

		for _, call := range custody.deleteCalls {
			if strings.HasPrefix(call.Key, tenant+"/fetcher-artifacts/") {
				found++
			}
		}

		assert.Positive(t, found,
			"tenant %s must have at least one delete key prefixed with its id", tenant)
	}
}

// TestSweepCycle_EmptyTenantStringSkipped verifies the empty-string tenant
// guard in sweepCycle. A degenerate list with an empty string entry must
// not produce a custody sweep for that empty tenant — BuildObjectKey would
// reject it anyway, but skipping it earlier saves a spurious tracing span.
func TestSweepCycle_EmptyTenantStringSkipped(t *testing.T) {
	t.Parallel()

	provider, _ := newMiniredisInfraProvider(t)

	realTenant := uuid.New().String()

	custody := &stubCustodyStore{}
	repo := &stubRetentionExtractionRepo{
		candidatesByTenant: map[string][]*entities.ExtractionRequest{
			realTenant: {terminalFailedExtraction(uuid.New(), uuid.New())},
		},
	}

	// List with an empty string in the middle — sweepCycle's `if
	// tenantID == "" { continue }` must filter it out.
	lister := &stubBridgeTenantLister{
		tenants: []string{realTenant, "", realTenant + "-noop"},
	}

	w := newTestCustodyRetentionWorkerFull(repo, custody, lister, provider)

	w.sweepCycle(context.Background())

	// Only the real tenant had candidates, and the empty string must
	// never reach FindBridgeRetentionCandidates.
	repo.mu.Lock()
	defer repo.mu.Unlock()

	for _, tid := range repo.observedTenants {
		assert.NotEmpty(t, tid,
			"sweepCycle must skip empty tenant IDs before reaching the repository")
	}

	custody.mu.Lock()
	defer custody.mu.Unlock()
	// Exactly the real tenant's one orphan was deleted; the empty string
	// contributed nothing.
	assert.Len(t, custody.deleteCalls, 1,
		"exactly one delete for the real tenant; empty-string tenant contributes zero work")
	assert.True(t,
		strings.HasPrefix(custody.deleteCalls[0].Key, realTenant+"/fetcher-artifacts/"),
		"the single delete must target the real tenant's key")
}
