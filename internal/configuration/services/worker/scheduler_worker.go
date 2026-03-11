// Package worker provides background workers for the configuration context.
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

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	// schedulerLockKeyPrefix is the Redis lock key prefix for schedule execution.
	schedulerLockKeyPrefix = "matcher:scheduler:schedule:"

	// lockTTLMultiplier is applied to the worker interval to compute the lock TTL.
	lockTTLMultiplier = 2
)

// ErrNilScheduleRepository is re-exported from the command package
// to avoid duplicate declarations across the configuration context.
var ErrNilScheduleRepository = configCommand.ErrNilScheduleRepository

// Sentinel errors for scheduler worker.
var (
	ErrNilMatchTrigger      = errors.New("match trigger is required")
	ErrNilInfraProvider     = errors.New("infrastructure provider is required")
	ErrWorkerAlreadyRunning = errors.New("scheduler worker already running")
	ErrWorkerNotRunning     = errors.New("scheduler worker not running")
	ErrRedisClientNil       = errors.New("redis client is nil")
)

// SchedulerWorkerConfig holds configuration for the scheduler worker.
type SchedulerWorkerConfig struct {
	// Interval between poll cycles.
	Interval time.Duration
}

// SchedulerWorker polls for due schedules and triggers match runs.
type SchedulerWorker struct {
	mu            sync.Mutex
	scheduleRepo  ports.ScheduleRepository
	matchTrigger  sharedPorts.MatchTrigger
	infraProvider sharedPorts.InfrastructureProvider
	cfg           SchedulerWorkerConfig
	logger        libLog.Logger
	tracer        trace.Tracer

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
	infraProvider sharedPorts.InfrastructureProvider,
	cfg SchedulerWorkerConfig,
	logger libLog.Logger,
) (*SchedulerWorker, error) {
	if scheduleRepo == nil {
		return nil, ErrNilScheduleRepository
	}

	if matchTrigger == nil {
		return nil, ErrNilMatchTrigger
	}

	if infraProvider == nil {
		return nil, ErrNilInfraProvider
	}

	cfg = normalizeSchedulerWorkerConfig(cfg)

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &SchedulerWorker{
		scheduleRepo:  scheduleRepo,
		matchTrigger:  matchTrigger,
		infraProvider: infraProvider,
		cfg:           cfg,
		logger:        logger,
		tracer:        otel.Tracer("configuration.scheduler_worker"),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
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

	if schedulerChannelClosed(worker.stopCh) {
		worker.stopCh = make(chan struct{})
	}

	if schedulerChannelClosed(worker.doneCh) {
		worker.doneCh = make(chan struct{})
	}
}

func schedulerChannelClosed(ch <-chan struct{}) bool {
	if ch == nil {
		return true
	}

	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// UpdateRuntimeConfig updates the worker runtime configuration used on the next start/restart.
// NOTE: This does NOT affect a currently running worker's ticker. The WorkerManager
// always performs a full stop→start cycle when config changes, ensuring the new
// config is picked up when the worker's run() loop creates a fresh ticker.
func (worker *SchedulerWorker) UpdateRuntimeConfig(cfg SchedulerWorkerConfig) {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	worker.cfg = normalizeSchedulerWorkerConfig(cfg)
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
	if !worker.running.CompareAndSwap(true, false) {
		return ErrWorkerNotRunning
	}

	worker.stopOnce.Do(func() {
		close(worker.stopCh)
	})
	<-worker.doneCh

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

	acquired, token, err := worker.acquireLock(ctx, lockKey)
	if err != nil {
		logger.With(
			libLog.String("schedule.id", schedule.ID.String()),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "scheduler: lock error for schedule")

		return
	}

	if !acquired {
		return
	}

	defer worker.releaseLock(ctx, lockKey, token)

	// Trigger match run using the tenant ID resolved via JOIN in FindDueSchedules.
	worker.matchTrigger.TriggerMatchForContext(ctx, schedule.TenantID, schedule.ContextID)

	// Update schedule after run
	schedule.MarkRun(now)

	if _, err := worker.scheduleRepo.Update(ctx, schedule); err != nil {
		logger.With(
			libLog.String("schedule.id", schedule.ID.String()),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "scheduler: failed to update schedule after run")
	}

	logger.With(
		libLog.String("context.id", schedule.ContextID.String()),
		libLog.String("schedule.id", schedule.ID.String()),
	).Log(ctx, libLog.LevelInfo, "scheduler: triggered match run for context")
}

// acquireLock attempts to acquire a Redis distributed lock.
func (worker *SchedulerWorker) acquireLock(ctx context.Context, key string) (bool, string, error) {
	conn, err := worker.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis connection: %w", err)
	}

	if conn == nil {
		return false, "", ErrRedisClientNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis client for lock acquire: %w", err)
	}

	lockTTL := lockTTLMultiplier * worker.cfg.Interval
	token := uuid.New().String()

	ok, err := rdb.SetNX(ctx, key, token, lockTTL).Result()
	if err != nil {
		return false, "", fmt.Errorf("redis setnx: %w", err)
	}

	return ok, token, nil
}

// releaseLock releases a Redis distributed lock.
func (worker *SchedulerWorker) releaseLock(ctx context.Context, key, token string) {
	conn, err := worker.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		return
	}

	if conn == nil {
		return
	}

	rdb, rdbErr := conn.GetClient(ctx)
	if rdbErr != nil {
		worker.logger.With(
			libLog.String("lock.key", key),
			libLog.Any("error", rdbErr.Error()),
		).Log(ctx, libLog.LevelWarn, "scheduler: failed to acquire redis client for lock release")

		return
	}

	script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`
	if _, err := rdb.Eval(ctx, script, []string{key}, token).Result(); err != nil {
		worker.logger.With(
			libLog.String("lock.key", key),
			libLog.Any("error", err.Error()),
		).Log(ctx, libLog.LevelWarn, "scheduler: failed to release lock")
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
