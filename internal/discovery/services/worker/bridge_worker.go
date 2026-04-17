// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/chanutil"
)

const (
	// bridgeWorkerLockKey is the global distributed lock key for the bridge
	// worker. A single global lock (not per-tenant) is sufficient because
	// the orchestrator's atomic link write prevents duplicate outcomes even
	// if two instances briefly race on the same extraction.
	bridgeWorkerLockKey = "matcher:fetcher_bridge:cycle"

	// bridgeLockTTLMultiplier bounds the lock TTL at twice the poll
	// interval, matching scheduler/archival convention so a stuck worker
	// auto-releases before two cycles elapse.
	bridgeLockTTLMultiplier = 2

	// bridgeMinLockTTL is the floor applied to the lock TTL to avoid
	// degenerate sub-second values in test scenarios.
	bridgeMinLockTTL = 5 * time.Second

	// bridgeDefaultInterval is the default poll interval when none is
	// configured. 30s is a balance between freshness for a newly-completed
	// extraction and load on the discovery schema.
	bridgeDefaultInterval = 30 * time.Second

	// bridgeDefaultBatchSize is the default per-tenant batch size. 50 rows
	// per tenant per cycle keeps per-cycle runtime bounded on a busy
	// deployment while still draining reasonable backlog.
	bridgeDefaultBatchSize = 50
)

// Sentinel errors for bridge worker construction / lifecycle.
//
// The nil-orchestrator sentinel lives in shared ports (sharedPorts.
// ErrNilBridgeOrchestrator) so the worker constructor and the orchestrator's
// own nil-receiver guard surface the SAME identity to callers using
// errors.Is. Keeping a duplicate package-local copy here would confuse
// callers and silently break errors.Is comparisons.
var (
	ErrNilBridgeExtractionRepo          = errors.New("bridge worker requires extraction repository")
	ErrNilBridgeTenantLister            = errors.New("bridge worker requires tenant lister")
	ErrNilBridgeInfraProvider           = errors.New("bridge worker requires infrastructure provider")
	ErrBridgeWorkerAlreadyRunning       = errors.New("bridge worker already running")
	ErrBridgeWorkerNotRunning           = errors.New("bridge worker not running")
	ErrBridgeRuntimeConfigUpdateRunning = errors.New("bridge worker runtime config update requires stopped worker")
	ErrBridgeRedisConnectionNil         = errors.New("bridge worker: redis connection is nil")
)

// BridgeWorkerConfig holds the tunables for the bridge worker.
type BridgeWorkerConfig struct {
	// Interval between poll cycles. Falls back to bridgeDefaultInterval
	// when <= 0.
	Interval time.Duration
	// BatchSize caps how many extractions we process per tenant per cycle.
	// Falls back to bridgeDefaultBatchSize when <= 0.
	BatchSize int
	// Retry holds the retry-and-backoff schedule the worker applies to
	// transient bridgeOne failures (T-005). Zero values get sane defaults
	// from BridgeRetryBackoff.Normalize.
	Retry BridgeRetryBackoff
}

func normalizeBridgeConfig(cfg BridgeWorkerConfig) BridgeWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = bridgeDefaultInterval
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = bridgeDefaultBatchSize
	}

	cfg.Retry = cfg.Retry.Normalize()

	return cfg
}

// bridgeLockTTL returns the lock TTL proportional to the poll interval.
// See scheduler_worker.go for the same pattern.
func bridgeLockTTL(interval time.Duration) time.Duration {
	ttl := time.Duration(bridgeLockTTLMultiplier) * interval
	if ttl < bridgeMinLockTTL {
		return bridgeMinLockTTL
	}

	return ttl
}

// BridgeWorker periodically scans each tenant for COMPLETE + unlinked
// extractions and drives them through the bridge orchestrator until linked.
//
// Concurrency model:
//   - A single Redis distributed lock gates the whole cycle. With multiple
//     matcher replicas, only one runs a given cycle.
//   - Within a cycle, tenants are processed sequentially to keep span
//     trees readable and to avoid fan-out spikes against the orchestrator's
//     downstream dependencies (Fetcher, object storage, ingestion).
//   - The orchestrator's atomic link write is the ultimate defense against
//     duplicate outcomes — even if two replicas briefly disagree about the
//     lock, at most one can write the ingestion_job_id.
type BridgeWorker struct {
	mu             sync.Mutex
	orchestrator   sharedPorts.BridgeOrchestrator
	extractionRepo repositories.ExtractionRepository
	tenantLister   sharedPorts.TenantLister
	infraProvider  sharedPorts.InfrastructureProvider
	cfg            BridgeWorkerConfig
	logger         libLog.Logger
	tracer         trace.Tracer

	running  atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewBridgeWorker constructs the worker with validated dependencies.
func NewBridgeWorker(
	orchestrator sharedPorts.BridgeOrchestrator,
	extractionRepo repositories.ExtractionRepository,
	tenantLister sharedPorts.TenantLister,
	infraProvider sharedPorts.InfrastructureProvider,
	cfg BridgeWorkerConfig,
	logger libLog.Logger,
) (*BridgeWorker, error) {
	if orchestrator == nil {
		return nil, sharedPorts.ErrNilBridgeOrchestrator
	}

	if extractionRepo == nil {
		return nil, ErrNilBridgeExtractionRepo
	}

	if tenantLister == nil {
		return nil, ErrNilBridgeTenantLister
	}

	if infraProvider == nil {
		return nil, ErrNilBridgeInfraProvider
	}

	cfg = normalizeBridgeConfig(cfg)

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &BridgeWorker{
		orchestrator:   orchestrator,
		extractionRepo: extractionRepo,
		tenantLister:   tenantLister,
		infraProvider:  infraProvider,
		cfg:            cfg,
		logger:         logger,
		tracer:         otel.Tracer("discovery.bridge_worker"),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}, nil
}

// Start begins the worker loop. The goroutine is supervised by
// runtime.SafeGoWithContextAndComponent, which recovers panics and restarts
// the loop per the KeepRunning policy.
func (w *BridgeWorker) Start(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return ErrBridgeWorkerAlreadyRunning
	}

	w.prepareRunState()

	runtime.SafeGoWithContextAndComponent(
		ctx,
		w.logger,
		"discovery",
		"bridge_worker",
		runtime.KeepRunning,
		w.run,
	)

	return nil
}

// Stop signals the worker to exit and blocks until the run loop has
// terminated. Safe to call from any goroutine; multiple concurrent callers
// race on the leading CompareAndSwap so exactly one observes the running→
// stopped transition and returns nil. The losers see ErrBridgeWorkerNotRunning
// without blocking on doneCh, eliminating the load→close→CAS TOCTOU window
// where two concurrent stops could both close stopCh or both report success.
func (w *BridgeWorker) Stop() error {
	if !w.running.CompareAndSwap(true, false) {
		return ErrBridgeWorkerNotRunning
	}

	w.stopOnce.Do(func() {
		close(w.stopCh)
	})
	<-w.doneCh

	w.logger.Log(context.Background(), libLog.LevelInfo, "bridge worker stopped")

	return nil
}

// Done returns a channel closed when the run loop terminates. The mutex is
// taken because prepareRunState may swap w.doneCh under the same lock during
// a Stop→Start cycle; reading without the lock could hand callers a stale
// channel that never closes (nil-safety H2).
func (w *BridgeWorker) Done() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.doneCh
}

// UpdateRuntimeConfig swaps the tick interval / batch size for the next
// start cycle. The worker manager always stop→starts on config change, so
// we reject updates while running to avoid races with the ticker.
func (w *BridgeWorker) UpdateRuntimeConfig(cfg BridgeWorkerConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running.Load() {
		return ErrBridgeRuntimeConfigUpdateRunning
	}

	w.cfg = normalizeBridgeConfig(cfg)

	return nil
}

func (w *BridgeWorker) prepareRunState() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.stopOnce = sync.Once{}

	if chanutil.ClosedSignalChannel(w.stopCh) {
		w.stopCh = make(chan struct{})
	}

	if chanutil.ClosedSignalChannel(w.doneCh) {
		w.doneCh = make(chan struct{})
	}
}

func (w *BridgeWorker) run(ctx context.Context) {
	defer runtime.RecoverAndLogWithContext(ctx, w.logger, "discovery", "bridge_worker.run")
	defer close(w.doneCh)

	// Run one cycle immediately so a freshly-deployed worker does not wait a
	// full interval before draining backlog.
	w.pollCycle(ctx)

	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.pollCycle(ctx)
		}
	}
}

// pollCycle acquires the distributed lock, lists tenants (INCLUDING the
// default tenant), and drives each tenant's eligible extractions through
// the orchestrator.
func (w *BridgeWorker) pollCycle(ctx context.Context) {
	logger, tracer := w.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.bridge.poll_cycle")
	defer span.End()

	acquired, token, err := w.acquireLock(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "bridge lock acquire failed", err)
		logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: lock acquire failed")

		return
	}

	if !acquired {
		return
	}
	defer w.releaseLock(ctx, token)

	tenants, err := w.tenantLister.ListTenants(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "bridge: list tenants failed", err)
		logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelError, "bridge: failed to list tenants")

		return
	}

	span.SetAttributes(attribute.Int("bridge.tenant_count", len(tenants)))

	processed := 0

	for _, tenantID := range tenants {
		if tenantID == "" {
			continue
		}

		count := w.processTenant(ctx, tenantID)
		processed += count
	}

	span.SetAttributes(attribute.Int("bridge.extractions_processed", processed))
}

// processTenant drives bridge work for a single tenant. Returns the number
// of extractions that completed the pipeline (successfully or with a
// terminal idempotent signal) so the cycle-level span can report totals.
func (w *BridgeWorker) processTenant(parentCtx context.Context, tenantID string) int {
	ctx := context.WithValue(parentCtx, auth.TenantIDKey, tenantID)
	logger, tracer := w.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.bridge.process_tenant")
	defer span.End()

	span.SetAttributes(attribute.String("tenant.id", tenantID))

	batchSize := w.cfg.BatchSize

	extractions, err := w.extractionRepo.FindEligibleForBridge(ctx, batchSize)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "bridge: find eligible extractions failed", err)
		logger.With(
			libLog.String("tenant.id", tenantID),
			libLog.String("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "bridge: failed to find eligible extractions")

		return 0
	}

	span.SetAttributes(attribute.Int("bridge.eligible_count", len(extractions)))

	processed := 0

	for _, extraction := range extractions {
		if extraction == nil {
			continue
		}

		if err := w.bridgeOne(ctx, extraction, tenantID); err != nil {
			w.logBridgeError(ctx, logger, extraction.ID, tenantID, err)

			continue
		}

		processed++
	}

	return processed
}

// bridgeOne runs a single extraction through the orchestrator. Wraps each
// call in its own span so operators can see per-extraction timing even when
// the tenant batch is large.
//
// T-005 retry semantics:
//  1. Idempotent signals (already-linked, ineligible) → silent success.
//  2. Terminal classifications (integrity / 404) → persist
//     MarkBridgeFailed; the row exits the eligibility queue.
//  3. Transient classifications (custody / network / source-unresolvable) →
//     increment bridge_attempts; if attempts ≥ max, escalate to terminal.
//
// Backoff strategy: PASSIVE — the worker does NOT sleep between retries and
// has no exponential-backoff math. Backoff is enforced by
// FindEligibleForBridge ordering by `updated_at ASC`: every attempt bumps
// the row's updated_at, pushing it to the tail of the eligibility queue so
// newer rows drain first. The tick cadence (BridgeWorkerConfig.Interval)
// IS the retry cadence; MaxAttempts caps total retries before terminal
// escalation. This is simpler, race-free, and avoids the dual-clock confusion
// of an in-process backoff timer racing the DB queue ordering.
func (w *BridgeWorker) bridgeOne(ctx context.Context, extraction *entities.ExtractionRequest, tenantID string) error {
	if extraction == nil {
		return nil
	}

	_, tracer := w.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.bridge.bridge_one")
	defer span.End()

	span.SetAttributes(
		attribute.String("extraction.id", extraction.ID.String()),
		attribute.String("tenant.id", tenantID),
		attribute.Int("bridge.attempts_before", extraction.BridgeAttempts),
	)

	outcome, err := w.orchestrator.BridgeExtraction(ctx, sharedPorts.BridgeExtractionInput{
		ExtractionID: extraction.ID,
		TenantID:     tenantID,
	})

	classification := ClassifyBridgeError(err)
	span.SetAttributes(attribute.String("bridge.retry_policy", classification.Policy.String()))

	switch classification.Policy {
	case RetryIdempotent:
		// Either no error (happy path) or a benign concurrent-write signal.
		if outcome != nil {
			span.SetAttributes(
				attribute.String("ingestion.job_id", outcome.IngestionJobID.String()),
				attribute.Int("ingestion.transaction_count", outcome.TransactionCount),
				attribute.Bool("bridge.custody_deleted", outcome.CustodyDeleted),
			)
		}

		return nil

	case RetryTerminal:
		w.persistTerminalFailure(ctx, extraction, classification.Class, err)

		return err

	case RetryTransient:
		w.handleTransientFailure(ctx, extraction, err)

		return err
	}

	return err
}

// persistTerminalFailure records the BridgeErrorClass on the extraction and
// removes it from the eligibility queue. The persist failure path itself is
// best-effort: if the DB write fails, the next tick will pick up the same
// extraction, classify again, and try the persist again. Logging the persist
// failure separately so operators can spot a stuck-in-loop pattern.
func (w *BridgeWorker) persistTerminalFailure(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	class vo.BridgeErrorClass,
	originalErr error,
) {
	logger, _ := w.tracking(ctx)

	extraction.RecordBridgeAttempt()

	message := terminalFailureMessage(originalErr)
	if markErr := extraction.MarkBridgeFailed(class, message); markErr != nil {
		logger.With(
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.String("class", string(class)),
			libLog.String("error", markErr.Error()),
		).Log(ctx, libLog.LevelError, "bridge: domain mark-failed rejected (wiring bug)")

		return
	}

	if persistErr := w.extractionRepo.MarkBridgeFailed(ctx, extraction); persistErr != nil {
		logger.With(
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.String("class", string(class)),
			libLog.String("bridge.class", string(class)),
			libLog.String("error", persistErr.Error()),
		).Log(ctx, libLog.LevelError, "bridge: persist terminal failure failed")
	}
}

// handleTransientFailure increments bridge_attempts, escalates to terminal
// if the configured ceiling is reached, otherwise persists just the bumped
// attempts via the existing Update path.
func (w *BridgeWorker) handleTransientFailure(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	originalErr error,
) {
	logger, _ := w.tracking(ctx)

	attempts := extraction.RecordBridgeAttempt()

	if w.cfg.Retry.ShouldEscalate(attempts) {
		escalated := EscalateAfterMaxAttempts(originalErr)
		message := fmt.Sprintf(
			"escalated to terminal after %d attempts: %s",
			attempts,
			terminalFailureMessage(originalErr),
		)

		if markErr := extraction.MarkBridgeFailed(escalated, message); markErr != nil {
			logger.With(
				libLog.String("extraction.id", extraction.ID.String()),
				libLog.String("class", string(escalated)),
				libLog.String("error", markErr.Error()),
			).Log(ctx, libLog.LevelError, "bridge: domain mark-failed rejected during escalation")

			return
		}

		if persistErr := w.extractionRepo.MarkBridgeFailed(ctx, extraction); persistErr != nil {
			logger.With(
				libLog.String("extraction.id", extraction.ID.String()),
				libLog.String("class", string(escalated)),
				libLog.String("bridge.class", string(escalated)),
				libLog.String("error", persistErr.Error()),
			).Log(ctx, libLog.LevelError, "bridge: persist escalated failure failed")
		}

		return
	}

	// Below the ceiling: persist ONLY the bumped attempts + updated_at via
	// the narrow IncrementBridgeAttempts UPDATE (Polish Fix 3). The wide
	// Update path could otherwise clobber a concurrent link write under a
	// lock-TTL-expiry edge case — the narrow UPDATE is gated by
	// `ingestion_job_id IS NULL` so that race produces ErrExtractionAlreadyLinked
	// (logged at info level, not warn) instead of silent data corruption.
	persistErr := w.extractionRepo.IncrementBridgeAttempts(ctx, extraction.ID, attempts)
	if persistErr == nil {
		return
	}

	if errors.Is(persistErr, sharedPorts.ErrExtractionAlreadyLinked) {
		// Concurrent link won the race. The link itself is the desired
		// outcome — stop retrying. Log at info because this is benign
		// concurrency, not an error.
		logger.With(
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.Any("attempts", attempts),
		).Log(ctx, libLog.LevelInfo, "bridge: transient retry skipped — concurrent link won")

		return
	}

	logger.With(
		libLog.String("extraction.id", extraction.ID.String()),
		libLog.Any("attempts", attempts),
		libLog.String("error", persistErr.Error()),
	).Log(ctx, libLog.LevelWarn, "bridge: failed to persist transient attempt counter")
}

// terminalFailureMessage builds the operator-facing message persisted in
// bridge_last_error_message. Bounded by the entity's MaxBridgeFailureMessageLength.
func terminalFailureMessage(err error) string {
	if err == nil {
		return "unknown failure"
	}

	return err.Error()
}

func (w *BridgeWorker) logBridgeError(
	ctx context.Context,
	logger libLog.Logger,
	extractionID uuid.UUID,
	tenantID string,
	err error,
) {
	level := libLog.LevelError

	// Source-unresolvable is a config gap, not a transient failure. Log
	// at WARN so operators see it without page-worthy urgency.
	if errors.Is(err, sharedPorts.ErrBridgeSourceUnresolvable) {
		level = libLog.LevelWarn
	}

	logger.With(
		libLog.String("tenant.id", tenantID),
		libLog.String("extraction.id", extractionID.String()),
		libLog.String("error", err.Error()),
	).Log(ctx, level, "bridge: extraction failed")
}

// acquireLock is a thin wrapper over the infrastructure provider's Redis
// client. Mirrors the pattern in scheduler/archival/discovery workers.
func (w *BridgeWorker) acquireLock(ctx context.Context) (bool, string, error) {
	connLease, err := w.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return false, "", ErrBridgeRedisConnectionNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis client: %w", err)
	}

	token := uuid.New().String()

	ok, err := rdb.SetNX(ctx, bridgeWorkerLockKey, token, bridgeLockTTL(w.cfg.Interval)).Result()
	if err != nil {
		return false, "", fmt.Errorf("redis setnx for bridge lock: %w", err)
	}

	return ok, token, nil
}

// releaseLock uses a Lua script to avoid releasing a lock that has already
// expired and been re-acquired by another instance.
func (w *BridgeWorker) releaseLock(ctx context.Context, token string) {
	connLease, err := w.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		w.logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: failed to get redis connection for lock release")

		return
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return
	}

	rdb, clientErr := conn.GetClient(ctx)
	if clientErr != nil {
		w.logger.With(libLog.Any("error", clientErr.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: failed to get redis client for lock release")

		return
	}

	script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`

	if _, err := rdb.Eval(ctx, script, []string{bridgeWorkerLockKey}, token).Result(); err != nil {
		w.logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: failed to release lock")
	}
}

func (w *BridgeWorker) tracking(ctx context.Context) (libLog.Logger, trace.Tracer) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if logger == nil {
		logger = w.logger
	}

	if tracer == nil {
		tracer = w.tracer
	}

	return logger, tracer
}
