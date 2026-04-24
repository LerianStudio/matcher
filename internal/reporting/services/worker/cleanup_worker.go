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

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	workermetrics "github.com/LerianStudio/matcher/internal/shared/observability/workermetrics"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/chanutil"
)

// cleanupWorkerName is the stable label value emitted on matcher.worker.*
// metrics from this worker.
const cleanupWorkerName = "cleanup_worker"

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
	storage objectstorage.Backend
	cfg     CleanupWorkerConfig
	logger  libLog.Logger
	tracer  trace.Tracer
	metrics *workermetrics.Recorder

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
	storage objectstorage.Backend,
	cfg CleanupWorkerConfig,
	logger libLog.Logger,
) (*CleanupWorker, error) {
	if jobRepo == nil {
		return nil, ErrNilJobRepository
	}

	if sharedPorts.IsNilValue(storage) {
		return nil, ErrNilStorageClient
	}

	cfg = normalizeCleanupWorkerConfig(cfg)

	return &CleanupWorker{
		jobRepo: jobRepo,
		storage: storage,
		cfg:     cfg,
		logger:  logger,
		tracer:  otel.Tracer("reporting.cleanup_worker"),
		metrics: workermetrics.NewRecorder(cleanupWorkerName),
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
	defer runtime.RecoverAndLogWithContext(ctx, worker.logger, "cleanup_worker", "run")
	defer close(worker.doneCh)

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

	startedAt := time.Now()
	outcome := workermetrics.OutcomeSkipped

	var processed, failed int

	defer func() {
		worker.metrics.RecordCycle(ctx, startedAt, outcome)
		worker.metrics.RecordItems(ctx, processed, failed)
	}()

	ctx = libCommons.ContextWithLogger(ctx, worker.logger)
	ctx = libCommons.ContextWithTracer(ctx, worker.tracer)

	jobs, err := worker.jobRepo.ListExpired(ctx, worker.cfg.BatchSize)
	if err != nil {
		outcome = workermetrics.OutcomeFailure

		libOpentelemetry.HandleSpanError(span, "failed to list expired jobs", err)

		libLog.SafeError(worker.logger, ctx, "failed to list expired jobs", err, runtime.IsProductionMode())

		return
	}

	if len(jobs) == 0 {
		return
	}

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("cleaning up %d expired export jobs", len(jobs)))

	now := time.Now().UTC()

	// Parallelize cleanupJob across the listed expired jobs. Each call does
	// two DB updates plus at most one S3 Delete; on a batch of 100 jobs the
	// sequential walk can take 10-30s, holding open a worker goroutine and
	// eating pool capacity the whole time. cleanupJob is idempotent and each
	// job's work is scoped to its own record, so independent goroutines are
	// safe. We cap concurrency to cleanupJobConcurrency to avoid blowing up
	// the S3 / DB connection pool under a surprise large batch.
	//
	// No errgroup: cleanupJob already logs per-job failures internally and
	// returns nothing — we explicitly want to process every job regardless
	// of peer failures, which is the opposite of errgroup's fail-fast.
	processed, failed = worker.runCleanupJobsParallel(ctx, jobs, now)
	outcome = workermetrics.OutcomeSuccess
}

// cleanupJobConcurrency caps the number of in-flight cleanupJob goroutines.
// Each goroutine issues two DB updates + one S3 delete; 10 matches the
// connection-pool headroom tested against POSTGRES_MAX_OPEN_CONNS defaults
// and leaves plenty of slack for other workers sharing the pool.
const cleanupJobConcurrency = 10

// runCleanupJobsParallel runs cleanupJob on every non-nil entry in jobs and
// returns the aggregate (processed, failed) counts. A job counts as
// processed when every stage it attempted succeeded; a job counts as
// failed if either the expire-transition or the delete step surfaced an
// error. Skipped-because-grace-period is processed, not failed.
func (worker *CleanupWorker) runCleanupJobsParallel(
	ctx context.Context,
	jobs []*entities.ExportJob,
	now time.Time,
) (int, int) {
	limit := cleanupJobConcurrency
	if len(jobs) < limit {
		limit = len(jobs)
	}

	sem := make(chan struct{}, limit)

	var wg sync.WaitGroup

	var processed, failed atomic.Int64

	for _, job := range jobs {
		if job == nil {
			continue
		}

		wg.Add(1)

		sem <- struct{}{}

		runtime.SafeGoWithContextAndComponent(
			ctx,
			worker.logger,
			"reporting",
			"cleanup_worker.job",
			runtime.KeepRunning,
			func(goCtx context.Context) {
				defer wg.Done()
				defer func() { <-sem }()

				if worker.cleanupJob(goCtx, job, now) {
					processed.Add(1)
				} else {
					failed.Add(1)
				}
			},
		)
	}

	wg.Wait()

	return int(processed.Load()), int(failed.Load())
}

// cleanupJob runs the two-phase cleanup for one expired export job.
// Returns true when every stage the job reached succeeded (including the
// happy no-op paths where the job was already expired or the grace period
// had not elapsed yet); returns false when either phase surfaced an error.
func (worker *CleanupWorker) cleanupJob(ctx context.Context, job *entities.ExportJob, now time.Time) bool {
	// Phase 1: Mark job as expired (prevents new presigned URLs).
	markOK := worker.markJobExpiredIfNeeded(ctx, job)

	// Phase 2: Delete S3 file only after the grace period has elapsed.
	// This allows presigned URLs issued before expiry to complete.
	deleteOK := worker.deleteExpiredFileIfReady(ctx, job, now)

	return markOK && deleteOK
}

// markJobExpiredIfNeeded transitions a job to EXPIRED if it is not
// already. Returns true when the transition succeeded OR the job was
// already expired (no-op success); returns false when the domain
// transition or persistence errored.
func (worker *CleanupWorker) markJobExpiredIfNeeded(ctx context.Context, job *entities.ExportJob) bool {
	if job.Status == entities.ExportJobStatusExpired {
		return true
	}

	if err := job.MarkExpired(); err != nil {
		libLog.SafeError(worker.logger, ctx, fmt.Sprintf("failed to transition job %s to expired", job.ID), err, runtime.IsProductionMode())

		return false
	}

	if err := worker.jobRepo.Update(ctx, job); err != nil {
		libLog.SafeError(worker.logger, ctx, fmt.Sprintf("failed to persist job %s expired status", job.ID), err, runtime.IsProductionMode())

		return false
	}

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("marked job %s as expired", job.ID))

	return true
}

// deleteExpiredFileIfReady removes the S3 object if the grace period has
// elapsed. Returns true when the delete ran successfully OR the object
// was not yet ready to delete (no-op success); returns false only when
// the backend rejected the delete.
func (worker *CleanupWorker) deleteExpiredFileIfReady(ctx context.Context, job *entities.ExportJob, now time.Time) bool {
	if job.FileKey == "" || !now.After(job.ExpiresAt.Add(worker.cfg.FileDeleteGracePeriod)) {
		return true
	}

	if err := worker.storage.Delete(ctx, job.FileKey); err != nil {
		worker.logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to delete file for job %s: %v", job.ID, err))

		return false
	}

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("deleted file for expired job %s", job.ID))

	return true
}
