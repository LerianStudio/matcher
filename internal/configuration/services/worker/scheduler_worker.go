// Package worker provides background workers for the configuration context.
package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/chanutil"
)

const (
	// schedulerLockKeyPrefix is the Redis lock key prefix for schedule execution.
	schedulerLockKeyPrefix = "matcher:scheduler:schedule:"

	// schedulerLockExpiryMultiplier is the factor applied to the poll interval
	// to determine lock TTL. 2× ensures the lock outlives one full cycle while
	// auto-expiring well before two cycles pass.
	schedulerLockExpiryMultiplier = 2

	// schedulerMinLockExpiry is the minimum lock expiry to prevent degenerate
	// TTLs when the poll interval is very short (e.g. sub-second in tests).
	schedulerMinLockExpiry = 5 * time.Second
)

// schedulerLockExpiry returns the lock TTL proportional to the poll interval:
// max(2 × interval, 5s). This prevents the lock from expiring mid-execution
// while ensuring it auto-releases before the next cycle would stall.
func schedulerLockExpiry(interval time.Duration) time.Duration {
	expiry := time.Duration(schedulerLockExpiryMultiplier) * interval
	if expiry < schedulerMinLockExpiry {
		return schedulerMinLockExpiry
	}

	return expiry
}

// ErrNilScheduleRepository is re-exported from the command package
// to avoid duplicate declarations across the configuration context.
var ErrNilScheduleRepository = configCommand.ErrNilScheduleRepository

// Sentinel errors for scheduler worker.
var (
	ErrNilMatchTrigger                 = errors.New("match trigger is required")
	ErrNilLockManager                  = errors.New("lock manager is required")
	ErrWorkerAlreadyRunning            = errors.New("scheduler worker already running")
	ErrWorkerNotRunning                = errors.New("scheduler worker not running")
	ErrRuntimeConfigUpdateWhileRunning = errors.New("worker runtime config update requires stopped worker")
)

// SchedulerWorkerConfig holds configuration for the scheduler worker.
type SchedulerWorkerConfig struct {
	// Interval between poll cycles.
	Interval time.Duration
}

// SchedulerWorker polls for due schedules and triggers match runs.
type SchedulerWorker struct {
	mu           sync.Mutex
	scheduleRepo ports.ScheduleRepository
	matchTrigger sharedPorts.MatchTrigger
	lockManager  libRedis.LockManager
	cfg          SchedulerWorkerConfig
	logger       libLog.Logger
	tracer       trace.Tracer

	running  atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func normalizeSchedulerWorkerConfig(cfg SchedulerWorkerConfig) SchedulerWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}

	return cfg
}

// NewSchedulerWorker creates a new scheduler worker.
func NewSchedulerWorker(
	scheduleRepo ports.ScheduleRepository,
	matchTrigger sharedPorts.MatchTrigger,
	lockManager libRedis.LockManager,
	cfg SchedulerWorkerConfig,
	logger libLog.Logger,
) (*SchedulerWorker, error) {
	if scheduleRepo == nil {
		return nil, ErrNilScheduleRepository
	}

	if matchTrigger == nil {
		return nil, ErrNilMatchTrigger
	}

	if lockManager == nil {
		return nil, ErrNilLockManager
	}

	cfg = normalizeSchedulerWorkerConfig(cfg)

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &SchedulerWorker{
		scheduleRepo: scheduleRepo,
		matchTrigger: matchTrigger,
		lockManager:  lockManager,
		cfg:          cfg,
		logger:       logger,
		tracer:       otel.Tracer("configuration.scheduler_worker"),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}, nil
}

// prepareRunState reinitialises the worker's stop/done channels and sync.Once for
// re-entrant Start→Stop→Start cycles. SAFETY: The caller (WorkerManager) MUST ensure
// Stop() has fully completed before calling Start(), which calls prepareRunState().
// The WorkerManager serialises all lifecycle transitions via its mutex.
func (worker *SchedulerWorker) prepareRunState() {
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

// UpdateRuntimeConfig updates the worker runtime configuration used on the next start/restart.
// NOTE: This does NOT affect a currently running worker's ticker. The WorkerManager
// always performs a full stop→start cycle when config changes, ensuring the new
// config is picked up when the worker's run() loop creates a fresh ticker.
func (worker *SchedulerWorker) UpdateRuntimeConfig(cfg SchedulerWorkerConfig) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	if worker.running.Load() {
		return ErrRuntimeConfigUpdateWhileRunning
	}

	worker.cfg = normalizeSchedulerWorkerConfig(cfg)

	return nil
}

// Start begins the scheduler worker.
func (worker *SchedulerWorker) Start(ctx context.Context) error {
	if !worker.running.CompareAndSwap(false, true) {
		return ErrWorkerAlreadyRunning
	}

	worker.prepareRunState()

	runtime.SafeGoWithContextAndComponent(
		ctx,
		worker.logger,
		"configuration",
		"scheduler_worker",
		runtime.KeepRunning,
		worker.run,
	)

	return nil
}

// Stop gracefully shuts down the worker.
func (worker *SchedulerWorker) Stop() error {
	if !worker.running.Load() {
		return ErrWorkerNotRunning
	}

	worker.stopOnce.Do(func() {
		close(worker.stopCh)
	})
	<-worker.doneCh

	if !worker.running.CompareAndSwap(true, false) {
		return ErrWorkerNotRunning
	}

	worker.logger.Log(context.Background(), libLog.LevelInfo, "scheduler worker stopped")

	return nil
}

// Done returns a channel that is closed when the worker stops.
func (worker *SchedulerWorker) Done() <-chan struct{} {
	return worker.doneCh
}

func (worker *SchedulerWorker) run(ctx context.Context) {
	defer runtime.RecoverAndLogWithContext(ctx, worker.logger, "configuration", "scheduler_worker.run")
	defer close(worker.doneCh)

	// Run one cycle immediately on start.
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

// pollCycle finds all due schedules and triggers match runs.
func (worker *SchedulerWorker) pollCycle(ctx context.Context) {
	logger, tracer := worker.tracking(ctx)

	ctx, span := tracer.Start(ctx, "configuration.scheduler.poll_cycle")
	defer span.End()

	now := time.Now().UTC()

	schedules, err := worker.scheduleRepo.FindDueSchedules(ctx, now)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find due schedules", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "scheduler: failed to find due schedules")

		return
	}

	span.SetAttributes(attribute.Int("scheduler.due_count", len(schedules)))

	for _, schedule := range schedules {
		worker.processSchedule(ctx, schedule, now)
	}
}

// processSchedule acquires a lock and triggers a match run for a single schedule.
func (worker *SchedulerWorker) processSchedule(
	ctx context.Context,
	schedule *entities.ReconciliationSchedule,
	now time.Time,
) {
	logger, tracer := worker.tracking(ctx)

	ctx, span := tracer.Start(ctx, "configuration.scheduler.process_schedule")
	defer span.End()

	span.SetAttributes(
		attribute.String("schedule.id", schedule.ID.String()),
		attribute.String("schedule.context_id", schedule.ContextID.String()),
		attribute.String("schedule.cron", schedule.CronExpression),
	)

	lockKey := schedulerLockKeyPrefix + schedule.ID.String()

	// Use WithLockOptions with Tries=1 for non-blocking semantics (same as TryLock)
	// but with a proportional TTL: 2× the poll interval ensures the lock outlives
	// a single cycle while auto-expiring before the next would stall.
	lockOpts := libRedis.LockOptions{
		Expiry:      schedulerLockExpiry(worker.cfg.Interval),
		Tries:       1,
		RetryDelay:  libRedis.DefaultLockOptions().RetryDelay,
		DriftFactor: libRedis.DefaultLockOptions().DriftFactor,
	}

	lockErr := worker.lockManager.WithLockOptions(ctx, lockKey, lockOpts, func(lockCtx context.Context) error {
		// Trigger match run using the tenant ID resolved via JOIN in FindDueSchedules.
		worker.matchTrigger.TriggerMatchForContext(lockCtx, schedule.TenantID, schedule.ContextID)

		// Update schedule after run.
		schedule.MarkRun(now)

		if _, updateErr := worker.scheduleRepo.Update(lockCtx, schedule); updateErr != nil {
			logger.With(
				libLog.String("schedule.id", schedule.ID.String()),
				libLog.Any("error", updateErr.Error()),
			).Log(lockCtx, libLog.LevelWarn, "scheduler: failed to update schedule after run")
		}

		logger.With(
			libLog.String("context.id", schedule.ContextID.String()),
			libLog.String("schedule.id", schedule.ID.String()),
		).Log(lockCtx, libLog.LevelInfo, "scheduler: triggered match run for context")

		return nil
	})
	if lockErr != nil {
		logger.With(
			libLog.String("schedule.id", schedule.ID.String()),
			libLog.Any("error", lockErr.Error()),
		).Log(ctx, libLog.LevelWarn, "scheduler: lock error for schedule")
	}
}

// tracking extracts observability primitives from context.
func (worker *SchedulerWorker) tracking(ctx context.Context) (libLog.Logger, trace.Tracer) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if logger == nil {
		logger = worker.logger
	}

	if tracer == nil {
		tracer = worker.tracer
	}

	return logger, tracer
}
