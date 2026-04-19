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

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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

// concurrencyTrackingOrchestrator is a BridgeOrchestrator that records the
// peak number of in-flight BridgeExtraction calls. Used by the parallelism
// tests to assert that TenantConcurrency bounds the tenant-level fan-out
// without resorting to brittle wall-clock timing. Each call holds
// `hold` long, which lets the worker load up several tenant goroutines at
// once without any individual call returning first. The gauge only ever
// observes the true peak of concurrent calls — no sleeps in the assertions.
type concurrencyTrackingOrchestrator struct {
	hold        time.Duration
	mu          sync.Mutex
	inFlight    int64
	maxInFlight int64
	totalCalls  atomic.Int64
}

func (c *concurrencyTrackingOrchestrator) BridgeExtraction(
	_ context.Context,
	_ sharedPorts.BridgeExtractionInput,
) (*sharedPorts.BridgeExtractionOutcome, error) {
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > c.maxInFlight {
		c.maxInFlight = c.inFlight
	}
	c.mu.Unlock()

	c.totalCalls.Add(1)

	if c.hold > 0 {
		time.Sleep(c.hold)
	}

	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()

	return &sharedPorts.BridgeExtractionOutcome{
		IngestionJobID:   uuid.New(),
		TransactionCount: 1,
		CustodyDeleted:   true,
	}, nil
}

func (c *concurrencyTrackingOrchestrator) peakConcurrent() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.maxInFlight
}

// TestBridgeWorker_PollCycle_FansOutTenants_UpToConcurrencyCeiling drives the
// full pollCycle with a miniredis-backed lock and three tenants × ten
// extractions each. With TenantConcurrency=3 we expect the worker to hold
// three orchestrator calls in flight at once — one per tenant. Extractions
// inside a tenant stay sequential (processTenant iterates serially), so
// the in-flight gauge tops out at exactly TenantConcurrency, never higher.
//
// Why not a wall-clock timing assertion: the fan-out cuts cycle time, but
// on shared CI runners a timing-based assertion would be flaky. The
// orchestrator's peak-in-flight counter is race-safe and captures exactly
// the property we care about — that tenants ran in parallel, bounded by
// the configured ceiling.
func TestBridgeWorker_PollCycle_FansOutTenants_UpToConcurrencyCeiling(t *testing.T) {
	t.Parallel()

	const (
		tenantCount       = 3
		extractionsPer    = 10
		tenantConcurrency = 3
		perCallHold       = 20 * time.Millisecond
	)

	tenants := make([]string, tenantCount)
	eligibleByTenant := make(map[string][]*entities.ExtractionRequest, tenantCount)

	for i := range tenantCount {
		tenantID := uuid.New().String()
		tenants[i] = tenantID

		extractions := make([]*entities.ExtractionRequest, 0, extractionsPer)
		connID := uuid.New()

		for range extractionsPer {
			extractions = append(extractions, completeExtraction(uuid.New(), connID))
		}

		eligibleByTenant[tenantID] = extractions
	}

	orch := &concurrencyTrackingOrchestrator{hold: perCallHold}
	repo := &stubBridgeExtractionRepo{eligibleByTenant: eligibleByTenant}
	lister := &stubBridgeTenantLister{tenants: tenants}

	w, _ := newBridgeWorkerWithMiniredis(t)
	w.orchestrator = orch
	w.extractionRepo = repo
	w.tenantLister = lister
	w.cfg.TenantConcurrency = tenantConcurrency
	w.cfg.BatchSize = extractionsPer

	w.pollCycle(context.Background())

	assert.Equal(t, int64(tenantCount*extractionsPer), orch.totalCalls.Load(),
		"every eligible extraction across every tenant must have been processed")

	peak := orch.peakConcurrent()
	assert.LessOrEqual(t, peak, int64(tenantConcurrency),
		"in-flight orchestrator calls must never exceed TenantConcurrency; peak=%d ceiling=%d",
		peak, tenantConcurrency)

	// Positive assertion: the cycle must actually have fanned out — if peak
	// were 1, TenantConcurrency would be doing nothing. Require at least 2
	// concurrent calls to prove the fan-out is live. With 3 tenants and
	// perCallHold=20ms, the first tenant's first extraction is still in
	// flight when the second tenant's first extraction starts, so peak≥2
	// is a lower-bound guarantee — not a timing approximation.
	assert.GreaterOrEqual(t, peak, int64(2),
		"tenant fan-out must produce concurrent orchestrator calls; peak=%d suggests serial processing",
		peak)
}

// TestBridgeWorker_PollCycle_ConcurrencyOne_RunsSequentially is the dual of
// the fan-out test: setting TenantConcurrency=1 forces fully serial
// behaviour, matching the pre-refactor contract. Guards against someone
// accidentally inlining a fixed concurrency and breaking the knob.
func TestBridgeWorker_PollCycle_ConcurrencyOne_RunsSequentially(t *testing.T) {
	t.Parallel()

	const (
		tenantCount    = 3
		extractionsPer = 2
		perCallHold    = 10 * time.Millisecond
	)

	tenants := make([]string, tenantCount)
	eligibleByTenant := make(map[string][]*entities.ExtractionRequest, tenantCount)

	for i := range tenantCount {
		tenantID := uuid.New().String()
		tenants[i] = tenantID

		extractions := make([]*entities.ExtractionRequest, 0, extractionsPer)
		connID := uuid.New()

		for range extractionsPer {
			extractions = append(extractions, completeExtraction(uuid.New(), connID))
		}

		eligibleByTenant[tenantID] = extractions
	}

	orch := &concurrencyTrackingOrchestrator{hold: perCallHold}
	repo := &stubBridgeExtractionRepo{eligibleByTenant: eligibleByTenant}
	lister := &stubBridgeTenantLister{tenants: tenants}

	w, _ := newBridgeWorkerWithMiniredis(t)
	w.orchestrator = orch
	w.extractionRepo = repo
	w.tenantLister = lister
	w.cfg.TenantConcurrency = 1
	w.cfg.BatchSize = extractionsPer

	w.pollCycle(context.Background())

	assert.Equal(t, int64(tenantCount*extractionsPer), orch.totalCalls.Load())
	assert.Equal(t, int64(1), orch.peakConcurrent(),
		"TenantConcurrency=1 must serialize every orchestrator call; any higher peak means the knob is broken")
}

// TestNormalizeBridgeConfig_FillsDefaultTenantConcurrency documents that a
// zero or negative TenantConcurrency falls back to
// bridgeDefaultTenantConcurrency rather than collapsing pollCycle to
// sequential behaviour silently. Mirrors the other normalization guards.
func TestNormalizeBridgeConfig_FillsDefaultTenantConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want int
	}{
		{"zero defaults to bridgeDefaultTenantConcurrency", 0, bridgeDefaultTenantConcurrency},
		{"negative defaults to bridgeDefaultTenantConcurrency", -1, bridgeDefaultTenantConcurrency},
		{"positive value preserved", 7, 7},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			normalized := normalizeBridgeConfig(BridgeWorkerConfig{
				TenantConcurrency: tc.in,
			})
			assert.Equal(t, tc.want, normalized.TenantConcurrency)
		})
	}
}
