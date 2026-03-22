// Package worker provides background job processing for reporting.
package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/reporting/ports"
	"github.com/LerianStudio/matcher/pkg/chanutil"
)

const (
	defaultCleanupInterval = 1 * time.Hour
	defaultCleanupBatch    = 100
	defaultFileDeleteGrace = 1 * time.Hour
)

// CleanupWorkerConfig contains configuration for the cleanup worker.
type CleanupWorkerConfig struct {
	Interval              time.Duration
	BatchSize             int
	FileDeleteGracePeriod time.Duration
}

// CleanupWorker removes expired export files and updates job status.
type CleanupWorker struct {
	mu      sync.Mutex
	jobRepo repositories.ExportJobRepository
	storage ports.ObjectStorageClient
	cfg     CleanupWorkerConfig
	logger  libLog.Logger
	tracer  trace.Tracer

	running  atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func normalizeCleanupWorkerConfig(cfg CleanupWorkerConfig) CleanupWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultCleanupInterval
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultCleanupBatch
	}

	if cfg.FileDeleteGracePeriod <= 0 {
		cfg.FileDeleteGracePeriod = defaultFileDeleteGrace
	}

	return cfg
}

// NewCleanupWorker creates a new cleanup worker.
func NewCleanupWorker(
	jobRepo repositories.ExportJobRepository,
	storage ports.ObjectStorageClient,
	cfg CleanupWorkerConfig,
	logger libLog.Logger,
) (*CleanupWorker, error) {
	if jobRepo == nil {
		return nil, ErrNilJobRepository
	}

	if storage == nil {
		return nil, ErrNilStorageClient
	}

	cfg = normalizeCleanupWorkerConfig(cfg)

	return &CleanupWorker{
		jobRepo: jobRepo,
		storage: storage,
		cfg:     cfg,
		logger:  logger,
		tracer:  otel.Tracer("reporting.cleanup_worker"),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}, nil
}

// prepareRunState reinitialises the worker's stop/done channels and sync.Once for
// re-entrant Start→Stop→Start cycles. SAFETY: The caller (WorkerManager) MUST ensure
// Stop() has fully completed before calling Start(), which calls prepareRunState().
// The WorkerManager serialises all lifecycle transitions via its mutex.
func (worker *CleanupWorker) prepareRunState() {
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
func (worker *CleanupWorker) UpdateRuntimeConfig(cfg CleanupWorkerConfig) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	if worker.running.Load() {
		return ErrRuntimeConfigUpdateWhileRunning
	}

	worker.cfg = normalizeCleanupWorkerConfig(cfg)

	return nil
}

// Start begins the cleanup worker.
func (worker *CleanupWorker) Start(ctx context.Context) error {
	if !worker.running.CompareAndSwap(false, true) {
		return ErrWorkerAlreadyRunning
	}

	worker.prepareRunState()

	runtime.SafeGoWithContextAndComponent(
		ctx,
		worker.logger,
		"reporting",
		"cleanup_worker",
		runtime.KeepRunning,
		worker.run,
	)

	return nil
}

// Stop gracefully shuts down the worker.
func (worker *CleanupWorker) Stop() error {
	if !worker.running.Load() {
		return ErrWorkerNotRunning
	}

	worker.stopOnce.Do(func() {
		close(worker.stopCh)
	})
	<-worker.doneCh

	worker.running.Store(false)

	worker.logger.Log(context.Background(), libLog.LevelInfo, "cleanup worker stopped")

	return nil
}

// Done returns a channel that is closed when the worker stops.
func (worker *CleanupWorker) Done() <-chan struct{} {
	return worker.doneCh
}

func (worker *CleanupWorker) run(ctx context.Context) {
	defer close(worker.doneCh)
	defer runtime.RecoverAndLogWithContext(ctx, worker.logger, "cleanup_worker", "run")

	worker.cleanupExpired(ctx)

	ticker := time.NewTicker(worker.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-worker.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.cleanupExpired(ctx)
		}
	}
}

func (worker *CleanupWorker) cleanupExpired(ctx context.Context) {
	ctx, span := worker.tracer.Start(ctx, "cleanup_worker.cleanup_expired")
	defer span.End()

	ctx = libCommons.ContextWithLogger(ctx, worker.logger)
	ctx = libCommons.ContextWithTracer(ctx, worker.tracer)

	jobs, err := worker.jobRepo.ListExpired(ctx, worker.cfg.BatchSize)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list expired jobs", err)

		worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list expired jobs: %v", err))

		return
	}

	if len(jobs) == 0 {
		return
	}

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("cleaning up %d expired export jobs", len(jobs)))

	now := time.Now().UTC()

	for _, job := range jobs {
		worker.cleanupJob(ctx, job, now)
	}
}

func (worker *CleanupWorker) cleanupJob(ctx context.Context, job *entities.ExportJob, now time.Time) {
	// Phase 1: Mark job as expired (prevents new presigned URLs).
	worker.markJobExpiredIfNeeded(ctx, job)

	// Phase 2: Delete S3 file only after the grace period has elapsed.
	// This allows presigned URLs issued before expiry to complete.
	worker.deleteExpiredFileIfReady(ctx, job, now)
}

func (worker *CleanupWorker) markJobExpiredIfNeeded(ctx context.Context, job *entities.ExportJob) {
	if job.Status == entities.ExportJobStatusExpired {
		return
	}

	job.MarkExpired()

	if err := worker.jobRepo.Update(ctx, job); err != nil {
		worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark job %s as expired: %v", job.ID, err))

		return
	}

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("marked job %s as expired", job.ID))
}

func (worker *CleanupWorker) deleteExpiredFileIfReady(ctx context.Context, job *entities.ExportJob, now time.Time) {
	if job.FileKey == "" || !now.After(job.ExpiresAt.Add(worker.cfg.FileDeleteGracePeriod)) {
		return
	}

	if err := worker.storage.Delete(ctx, job.FileKey); err != nil {
		worker.logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to delete file for job %s: %v", job.ID, err))

		return
	}

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("deleted file for expired job %s", job.ID))
}
