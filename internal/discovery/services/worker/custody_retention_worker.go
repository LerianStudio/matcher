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

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/chanutil"
)

const (
	// custodyRetentionLockKey is the global distributed lock key for the
	// custody retention sweep. A single global lock is sufficient because
	// the sweep is idempotent (Delete on a non-existent key is a no-op for
	// every supported backend) so a brief race between replicas is safe.
	custodyRetentionLockKey = "matcher:fetcher_bridge:custody_retention"

	// custodyRetentionLockTTLMultiplier mirrors the bridge worker
	// convention: TTL = 2× tick interval, floored at 5s.
	custodyRetentionLockTTLMultiplier = 2

	// custodyRetentionMinLockTTL guards against degenerate sub-second TTLs
	// in test scenarios with tight intervals.
	custodyRetentionMinLockTTL = 5 * time.Second

	// custodyRetentionDefaultInterval is the fallback tick cadence when the
	// configured interval is non-positive. 15 minutes balances orphan-
	// drain responsiveness against log/metric noise on idle deployments.
	custodyRetentionDefaultInterval = 15 * time.Minute

	// custodyRetentionDefaultGracePeriod is the fallback grace window
	// applied to LATE-LINKED candidates. One hour is long enough to ride
	// out a typical S3 outage but short enough that a stuck happy-path
	// cleanupCustody still gets swept within an operational SLO.
	custodyRetentionDefaultGracePeriod = time.Hour

	// CustodyRetentionDefaultBatchSize caps how many retention candidates
	// the sweep examines per tenant per cycle. Bounded so a single cycle
	// can't dominate the lock TTL when a tenant has a large orphan
	// backlog. Exported so the bootstrap wiring can pass it explicitly.
	CustodyRetentionDefaultBatchSize = 100
)

// Sentinel errors for custody retention worker construction / lifecycle.
var (
	ErrCustodyRetentionWorkerNil          = errors.New("custody retention worker is nil")
	ErrNilCustodyRetentionExtractionRepo  = errors.New("custody retention worker requires extraction repository")
	ErrNilCustodyRetentionCustody         = errors.New("custody retention worker requires custody store")
	ErrNilCustodyRetentionKeyBuilder      = errors.New("custody retention worker requires custody key builder")
	ErrNilCustodyRetentionTenantLister    = errors.New("custody retention worker requires tenant lister")
	ErrNilCustodyRetentionInfraProvider   = errors.New("custody retention worker requires infrastructure provider")
	ErrCustodyRetentionWorkerAlreadyRun   = errors.New("custody retention worker already running")
	ErrCustodyRetentionWorkerNotRunning   = errors.New("custody retention worker not running")
	ErrCustodyRetentionRuntimeUpdateBusy  = errors.New("custody retention worker runtime config update requires stopped worker")
	ErrCustodyRetentionRedisConnectionNil = errors.New("custody retention worker: redis connection is nil")
)

// CustodyRetentionWorkerConfig holds the tunables for the custody retention
// sweep worker. Mirrors BridgeWorkerConfig in shape so operators have one
// mental model for fetcher-bridge worker tuning.
type CustodyRetentionWorkerConfig struct {
	// Interval between sweep cycles. Falls back to
	// custodyRetentionDefaultInterval when <= 0.
	Interval time.Duration
	// GracePeriod is the delay applied to LATE-LINKED candidates before
	// the sweep deletes their custody object. Protects the orchestrator's
	// happy-path cleanupCustody from racing the sweep on freshly-linked
	// extractions. Falls back to custodyRetentionDefaultGracePeriod when
	// <= 0.
	GracePeriod time.Duration
	// BatchSize caps how many retention candidates are examined per
	// tenant per cycle. Falls back to CustodyRetentionDefaultBatchSize
	// when <= 0.
	BatchSize int
}

func normalizeCustodyRetentionConfig(cfg CustodyRetentionWorkerConfig) CustodyRetentionWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = custodyRetentionDefaultInterval
	}

	if cfg.GracePeriod <= 0 {
		cfg.GracePeriod = custodyRetentionDefaultGracePeriod
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = CustodyRetentionDefaultBatchSize
	}

	return cfg
}

// custodyRetentionLockTTL returns the lock TTL proportional to the sweep
// interval. Mirrors bridgeLockTTL.
func custodyRetentionLockTTL(interval time.Duration) time.Duration {
	ttl := time.Duration(custodyRetentionLockTTLMultiplier) * interval
	if ttl < custodyRetentionMinLockTTL {
		return custodyRetentionMinLockTTL
	}

	return ttl
}

// CustodyRetentionWorker periodically sweeps orphan custody objects.
//
// Two orphan populations are handled (see ExtractionRepository.
// FindBridgeRetentionCandidates):
//
//  1. TERMINAL: extractions with bridge_last_error set. The bridge
//     orchestrator's cleanupCustody hook only runs on the happy path, so
//     terminally-failed extractions leave their custody object behind.
//  2. LATE-LINKED: extractions linked successfully but whose custody
//     delete hook failed (S3 outage, network blip). The grace period
//     prevents racing the orchestrator on freshly-linked rows.
//
// Concurrency model:
//   - One Redis distributed lock gates the whole cycle. Multiple replicas
//     coordinate via SETNX + Lua release.
//   - Tenants processed sequentially within a cycle for span readability.
//   - Per-candidate Delete is idempotent: a missing object is a no-op,
//     not an error.
type CustodyRetentionWorker struct {
	mu             sync.Mutex
	extractionRepo repositories.ExtractionRepository
	custody        sharedPorts.ArtifactCustodyStore
	keyBuilder     sharedPorts.CustodyKeyBuilder
	tenantLister   sharedPorts.TenantLister
	infraProvider  sharedPorts.InfrastructureProvider
	cfg            CustodyRetentionWorkerConfig
	logger         libLog.Logger
	tracer         trace.Tracer

	running  atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewCustodyRetentionWorker constructs the worker with validated dependencies.
func NewCustodyRetentionWorker(
	extractionRepo repositories.ExtractionRepository,
	custodyStore sharedPorts.ArtifactCustodyStore,
	keyBuilder sharedPorts.CustodyKeyBuilder,
	tenantLister sharedPorts.TenantLister,
	infraProvider sharedPorts.InfrastructureProvider,
	cfg CustodyRetentionWorkerConfig,
	logger libLog.Logger,
) (*CustodyRetentionWorker, error) {
	if extractionRepo == nil {
		return nil, ErrNilCustodyRetentionExtractionRepo
	}

	if custodyStore == nil {
		return nil, ErrNilCustodyRetentionCustody
	}

	if keyBuilder == nil {
		return nil, ErrNilCustodyRetentionKeyBuilder
	}

	if tenantLister == nil {
		return nil, ErrNilCustodyRetentionTenantLister
	}

	if infraProvider == nil {
		return nil, ErrNilCustodyRetentionInfraProvider
	}

	cfg = normalizeCustodyRetentionConfig(cfg)

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &CustodyRetentionWorker{
		extractionRepo: extractionRepo,
		custody:        custodyStore,
		keyBuilder:     keyBuilder,
		tenantLister:   tenantLister,
		infraProvider:  infraProvider,
		cfg:            cfg,
		logger:         logger,
		tracer:         otel.Tracer("discovery.custody_retention_worker"),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}, nil
}

// Start begins the worker loop. The goroutine is supervised by
// runtime.SafeGoWithContextAndComponent, which recovers panics and restarts
// the loop per the KeepRunning policy.
func (worker *CustodyRetentionWorker) Start(ctx context.Context) error {
	if worker == nil {
		return ErrCustodyRetentionWorkerNil
	}

	if !worker.running.CompareAndSwap(false, true) {
		return ErrCustodyRetentionWorkerAlreadyRun
	}

	worker.prepareRunState()

	runtime.SafeGoWithContextAndComponent(
		ctx,
		worker.logger,
		"discovery",
		"custody_retention_worker",
		runtime.KeepRunning,
		worker.run,
	)

	return nil
}

// Stop signals the worker to exit and blocks until the run loop has
// terminated. Mirrors BridgeWorker.Stop's CAS-at-top pattern so concurrent
// Stop callers cannot both close stopCh.
func (worker *CustodyRetentionWorker) Stop() error {
	if worker == nil {
		return ErrCustodyRetentionWorkerNil
	}

	if !worker.running.CompareAndSwap(true, false) {
		return ErrCustodyRetentionWorkerNotRunning
	}

	worker.stopOnce.Do(func() {
		close(worker.stopCh)
	})
	<-worker.doneCh

	worker.logger.Log(context.Background(), libLog.LevelInfo, "custody retention worker stopped")

	return nil
}

// Done returns the channel for the current run cycle. Callers must re-query
// Done() after Start if the worker was previously Stopped: each Start→Stop
// cycle allocates a fresh doneCh, and a reference captured from a prior
// cycle will remain closed (signalling terminated) even while the next
// cycle is active.
//
// The mutex is taken because prepareRunState may swap worker.doneCh under the
// same lock during a Stop→Start cycle; reading without the lock could hand
// callers a stale channel that never closes.
//
// A nil receiver returns a pre-closed channel so callers that race against
// worker construction observe "already done" rather than blocking forever.
func (worker *CustodyRetentionWorker) Done() <-chan struct{} {
	if worker == nil {
		c := make(chan struct{})
		close(c)

		return c
	}

	worker.mu.Lock()
	defer worker.mu.Unlock()

	return worker.doneCh
}

// UpdateRuntimeConfig swaps the tick interval / grace period / batch size
// for the next start cycle. The worker manager always stop→starts on config
// change, so we reject updates while running to avoid races with the ticker.
func (worker *CustodyRetentionWorker) UpdateRuntimeConfig(cfg CustodyRetentionWorkerConfig) error {
	if worker == nil {
		return ErrCustodyRetentionWorkerNil
	}

	worker.mu.Lock()
	defer worker.mu.Unlock()

	if worker.running.Load() {
		return ErrCustodyRetentionRuntimeUpdateBusy
	}

	worker.cfg = normalizeCustodyRetentionConfig(cfg)

	return nil
}

func (worker *CustodyRetentionWorker) prepareRunState() {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	worker.stopOnce = sync.Once{}

	if chanutil.ClosedSignalChannel(worker.stopCh) {
		worker.stopCh = make(chan struct{})
	}

	if chanutil.ClosedSignalChannel(worker.doneCh) {
		worker.doneCh = make(chan struct{})
	}
}

func (worker *CustodyRetentionWorker) run(ctx context.Context) {
	defer runtime.RecoverAndLogWithContext(ctx, worker.logger, "discovery", "custody_retention_worker.run")
	defer close(worker.doneCh)

	// Run one cycle immediately so a freshly-deployed worker does not
	// wait a full interval before draining accumulated orphans.
	worker.sweepCycle(ctx)

	ticker := time.NewTicker(worker.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-worker.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.sweepCycle(ctx)
		}
	}
}

// sweepCycle acquires the distributed lock, lists tenants, and sweeps
// orphan custody objects for each.
func (worker *CustodyRetentionWorker) sweepCycle(ctx context.Context) {
	logger, tracer := worker.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.custody_retention.sweep_cycle")
	defer span.End()

	acquired, token, err := worker.acquireLock(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "custody retention lock acquire failed", err)
		logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "custody retention: lock acquire failed")

		return
	}

	if !acquired {
		return
	}

	defer worker.releaseLock(ctx, token)

	tenants, err := worker.tenantLister.ListTenants(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "custody retention: list tenants failed", err)
		logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelError, "custody retention: failed to list tenants")

		return
	}

	span.SetAttributes(attribute.Int("custody_retention.tenant_count", len(tenants)))

	deleted := 0

	for _, tenantID := range tenants {
		if tenantID == "" {
			continue
		}

		count := worker.sweepTenant(ctx, tenantID)
		deleted += count
	}

	span.SetAttributes(attribute.Int("custody_retention.objects_deleted", deleted))
}

// sweepTenant runs the retention sweep for a single tenant. Returns the
// number of custody objects actually deleted (excluding no-op deletes for
// objects that were already gone).
func (worker *CustodyRetentionWorker) sweepTenant(parentCtx context.Context, tenantID string) int {
	ctx := context.WithValue(parentCtx, auth.TenantIDKey, tenantID)
	logger, tracer := worker.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.custody_retention.sweep_tenant")
	defer span.End()

	span.SetAttributes(attribute.String("tenant.id", tenantID))

	candidates, err := worker.extractionRepo.FindBridgeRetentionCandidates(ctx, worker.cfg.GracePeriod, worker.cfg.BatchSize)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "custody retention: find candidates failed", err)
		logger.With(
			libLog.String("tenant.id", tenantID),
			libLog.String("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "custody retention: failed to find candidates")

		return 0
	}

	span.SetAttributes(attribute.Int("custody_retention.candidate_count", len(candidates)))

	deleted := 0

	for _, extraction := range candidates {
		if extraction == nil {
			continue
		}

		if worker.sweepOne(ctx, tenantID, extraction) {
			deleted++
		}
	}

	span.SetAttributes(attribute.Int("custody_retention.tenant_deleted", deleted))

	return deleted
}

// sweepOne deletes a single orphan custody object. Returns true when a
// delete actually removed bytes (false when the object was already gone or
// when the delete failed transiently — failures are logged for the next
// cycle to retry).
//
// Idempotency: ArtifactCustodyStore.Delete on an object key that no longer
// exists is a no-op for the S3-compatible backends Matcher targets. We do
// not pre-check Exists because the additional round trip doubles cost
// without changing semantics — Delete is the authoritative outcome.
func (worker *CustodyRetentionWorker) sweepOne(
	ctx context.Context,
	tenantID string,
	extraction *entities.ExtractionRequest,
) bool {
	logger, tracer := worker.tracking(ctx)

	ctx, span := tracer.Start(ctx, "discovery.custody_retention.sweep_one")
	defer span.End()

	key, err := worker.keyBuilder.BuildObjectKey(tenantID, extraction.ID)
	if err != nil {
		// Build failure is a config bug (e.g. invalid tenant id), not a
		// retention concern. Log and skip this candidate; future cycles
		// will pick it up if the underlying issue is fixed.
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "build object key failed", err)
		logger.With(
			libLog.String("tenant.id", tenantID),
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.String("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "custody retention: build object key failed")

		return false
	}

	span.SetAttributes(
		attribute.String("tenant.id", tenantID),
		attribute.String("extraction.id", extraction.ID.String()),
		attribute.String("custody.key", key),
		attribute.String("retention.bucket", retentionBucket(extraction)),
	)

	ref := sharedPorts.ArtifactCustodyReference{Key: key}

	if delErr := worker.custody.Delete(ctx, ref); delErr != nil {
		// Transient failure: log at warn so next cycle retries. Do not
		// mark it terminal — retention is a best-effort housekeeping
		// task.
		libOpentelemetry.HandleSpanError(span, "custody delete failed", delErr)
		logger.With(
			libLog.String("tenant.id", tenantID),
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.String("custody.key", key),
			libLog.String("error", delErr.Error()),
		).Log(ctx, libLog.LevelWarn, "custody retention: delete failed (will retry)")

		return false
	}

	// Persist the convergence marker (Polish Fix 1, T-006). Without this
	// write, the extraction row would re-appear in FindBridgeRetention
	// Candidates on every subsequent sweep cycle — the Delete call above
	// would then be a no-op for a missing S3 key but we'd still burn a
	// round-trip per row per tick forever. If the DB write fails, log
	// WARN and continue: the next sweep will just hit the same already-
	// missing S3 key again, which is safe (idempotent no-op).
	if markErr := worker.extractionRepo.MarkCustodyDeleted(ctx, extraction.ID, time.Now().UTC()); markErr != nil {
		libOpentelemetry.HandleSpanError(span, "mark custody deleted failed", markErr)
		logger.With(
			libLog.String("tenant.id", tenantID),
			libLog.String("extraction.id", extraction.ID.String()),
			libLog.String("custody.key", key),
			libLog.String("error", markErr.Error()),
		).Log(ctx, libLog.LevelWarn, "custody retention: mark custody deleted failed (will retry next sweep)")
	}

	// Success log at Debug. Info-level noise becomes a cost at scale after
	// this row drops out of future sweeps via the convergence marker;
	// aggregate signal lives in the sweepCycle span's objects_deleted
	// attribute (Polish Fix 4).
	logger.With(
		libLog.String("tenant.id", tenantID),
		libLog.String("extraction.id", extraction.ID.String()),
		libLog.String("custody.key", key),
		libLog.String("retention.bucket", retentionBucket(extraction)),
	).Log(ctx, libLog.LevelDebug, "custody retention: object deleted")

	return true
}

// retentionBucket classifies a candidate for observability. TERMINAL =
// extraction has bridge_last_error; LATE_LINKED = extraction has an
// ingestion job link (so cleanupCustody should have run but didn't). The
// classification is informational — both buckets get the same delete
// treatment.
func retentionBucket(extraction *entities.ExtractionRequest) string {
	if extraction == nil {
		return unknownRetentionBucket
	}

	if extraction.BridgeLastError != "" {
		return "terminal"
	}

	if extraction.IngestionJobID != uuid.Nil {
		return "late_linked"
	}

	return unknownRetentionBucket
}

// acquireLock is a thin wrapper over the infrastructure provider's Redis
// client. Mirrors BridgeWorker.acquireLock.
func (worker *CustodyRetentionWorker) acquireLock(ctx context.Context) (bool, string, error) {
	connLease, err := worker.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return false, "", ErrCustodyRetentionRedisConnectionNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis client: %w", err)
	}

	token := uuid.New().String()

	ok, err := rdb.SetNX(ctx, custodyRetentionLockKey, token, custodyRetentionLockTTL(worker.cfg.Interval)).Result()
	if err != nil {
		return false, "", fmt.Errorf("redis setnx for custody retention lock: %w", err)
	}

	return ok, token, nil
}

// releaseLock uses a Lua script to avoid releasing a lock that has already
// expired and been re-acquired by another instance.
func (worker *CustodyRetentionWorker) releaseLock(ctx context.Context, token string) {
	connLease, err := worker.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		worker.logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "custody retention: failed to get redis connection for lock release")

		return
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return
	}

	rdb, clientErr := conn.GetClient(ctx)
	if clientErr != nil {
		worker.logger.With(libLog.Any("error", clientErr.Error())).
			Log(ctx, libLog.LevelWarn, "custody retention: failed to get redis client for lock release")

		return
	}

	if _, err := rdb.Eval(ctx, redisLockReleaseLua, []string{custodyRetentionLockKey}, token).Result(); err != nil {
		worker.logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "custody retention: failed to release lock")
	}
}

func (worker *CustodyRetentionWorker) tracking(ctx context.Context) (libLog.Logger, trace.Tracer) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if logger == nil {
		logger = worker.logger
	}

	if tracer == nil {
		tracer = worker.tracer
	}

	return logger, tracer
}
