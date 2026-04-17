// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
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

	// bridgeHeartbeatTTLMultiplier scales the poll interval to derive the
	// TTL on the liveness heartbeat key. Three cycles is the sweet spot:
	// one transient Redis or worker stall will NOT blank the dashboard,
	// but two consecutive misses will. Used only when the optional
	// heartbeat writer is wired. C15.
	bridgeHeartbeatTTLMultiplier = 3

	// bridgeMinHeartbeatTTL is the floor applied to the heartbeat TTL. The
	// minimum deliberately exceeds bridgeMinLockTTL so tests using sub-
	// second intervals still exercise the happy path without immediate
	// expiry, while staying small enough to expire cleanly between runs.
	bridgeMinHeartbeatTTL = 15 * time.Second
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

// bridgeHeartbeatTTL returns the heartbeat TTL proportional to the poll
// interval. Three intervals is chosen deliberately — see the constant's
// doc comment for the reasoning. A floor keeps the key alive long enough
// for short-interval test scenarios to observe it.
func bridgeHeartbeatTTL(interval time.Duration) time.Duration {
	ttl := time.Duration(bridgeHeartbeatTTLMultiplier) * interval
	if ttl < bridgeMinHeartbeatTTL {
		return bridgeMinHeartbeatTTL
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
	mu              sync.Mutex
	orchestrator    sharedPorts.BridgeOrchestrator
	extractionRepo  repositories.ExtractionRepository
	tenantLister    sharedPorts.TenantLister
	infraProvider   sharedPorts.InfrastructureProvider
	heartbeatWriter discoveryPorts.BridgeHeartbeatWriter // optional liveness emitter (C15)
	cfg             BridgeWorkerConfig
	logger          libLog.Logger
	tracer          trace.Tracer

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
func (worker *BridgeWorker) Start(ctx context.Context) error {
	if !worker.running.CompareAndSwap(false, true) {
		return ErrBridgeWorkerAlreadyRunning
	}

	worker.prepareRunState()

	runtime.SafeGoWithContextAndComponent(
		ctx,
		worker.logger,
		"discovery",
		"bridge_worker",
		runtime.KeepRunning,
		worker.run,
	)

	return nil
}

// Stop signals the worker to exit and blocks until the run loop has
// terminated. Safe to call from any goroutine; multiple concurrent callers
// race on the leading CompareAndSwap so exactly one observes the running→
// stopped transition and returns nil. The losers see ErrBridgeWorkerNotRunning
// without blocking on doneCh, eliminating the load→close→CAS TOCTOU window
// where two concurrent stops could both close stopCh or both report success.
func (worker *BridgeWorker) Stop() error {
	if !worker.running.CompareAndSwap(true, false) {
		return ErrBridgeWorkerNotRunning
	}

	worker.stopOnce.Do(func() {
		close(worker.stopCh)
	})
	<-worker.doneCh

	worker.logger.Log(context.Background(), libLog.LevelInfo, "bridge worker stopped")

	return nil
}

// Done returns a channel closed when the run loop terminates. The mutex is
// taken because prepareRunState may swap worker.doneCh under the same lock during
// a Stop→Start cycle; reading without the lock could hand callers a stale
// channel that never closes (nil-safety H2).
func (worker *BridgeWorker) Done() <-chan struct{} {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	return worker.doneCh
}

// WithHeartbeatWriter wires the optional liveness emitter. Called by
// bootstrap after construction so the bridge worker can keep its
// NewBridgeWorker signature stable. A nil writer is explicitly permitted
// and turns the heartbeat path into a no-op — useful for unit tests and
// for deployments where Redis is momentarily unreachable at boot.
//
// Not safe to call while the worker is running; the worker manager always
// stop→starts on config change so this stays on the cold path. C15.
func (worker *BridgeWorker) WithHeartbeatWriter(writer discoveryPorts.BridgeHeartbeatWriter) {
	if worker == nil {
		return
	}

	worker.mu.Lock()
	defer worker.mu.Unlock()

	worker.heartbeatWriter = writer
}

// UpdateRuntimeConfig swaps the tick interval / batch size for the next
// start cycle. The worker manager always stop→starts on config change, so
// we reject updates while running to avoid races with the ticker.
func (worker *BridgeWorker) UpdateRuntimeConfig(cfg BridgeWorkerConfig) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	if worker.running.Load() {
		return ErrBridgeRuntimeConfigUpdateRunning
	}

	worker.cfg = normalizeBridgeConfig(cfg)

	return nil
}

func (worker *BridgeWorker) prepareRunState() {
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

func (worker *BridgeWorker) run(ctx context.Context) {
	defer runtime.RecoverAndLogWithContext(ctx, worker.logger, "discovery", "bridge_worker.run")
	defer close(worker.doneCh)

	// Run one cycle immediately so a freshly-deployed worker does not wait a
	// full interval before draining backlog.
	worker.pollCycle(ctx)

	ticker := time.NewTicker(worker.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-worker.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.pollCycle(ctx)
		}
	}
}

func (worker *BridgeWorker) tracking(ctx context.Context) (libLog.Logger, trace.Tracer) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if logger == nil {
		logger = worker.logger
	}

	if tracer == nil {
		tracer = worker.tracer
	}

	return logger, tracer
}

// writeHeartbeat records the worker's "I ticked at T" signal. Called at
// the tail of every pollCycle (whether lock was acquired or not) so the
// dashboard can distinguish "worker is alive but backlog is growing" from
// "worker is dead". Non-fatal on error — a momentarily unavailable Redis
// must not prevent the bridge from processing extractions on the next
// tick. C15.
func (worker *BridgeWorker) writeHeartbeat(ctx context.Context) {
	if worker == nil || worker.heartbeatWriter == nil {
		return
	}

	ttl := bridgeHeartbeatTTL(worker.cfg.Interval)
	if err := worker.heartbeatWriter.WriteLastTickAt(ctx, time.Now().UTC(), ttl); err != nil {
		worker.logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: heartbeat write failed")
	}
}
