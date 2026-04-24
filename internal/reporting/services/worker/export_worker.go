// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package worker provides background job processing for reporting.
package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	workermetrics "github.com/LerianStudio/matcher/internal/shared/observability/workermetrics"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/chanutil"
)

// exportWorkerName is the stable label value emitted on matcher.worker.*
// metrics from this worker.
const exportWorkerName = "export_worker"

const (
	defaultPollInterval      = 5 * time.Second
	defaultPageSize          = 1000
	progressUpdateEvery      = 5000
	tempFilePrefix           = "export-"
	defaultMaxRetries        = 3
	defaultInitialBackoff    = 1 * time.Second
	defaultMaxBackoff        = 5 * time.Minute
	defaultBackoffMultiplier = 2.0
)

var (
	// ErrWorkerAlreadyRunning indicates the worker is already started.
	ErrWorkerAlreadyRunning = errors.New("worker already running")
	// ErrWorkerNotRunning indicates the worker is not started.
	ErrWorkerNotRunning = errors.New("worker not running")
	// ErrRuntimeConfigUpdateWhileRunning indicates runtime config can only change while stopped.
	ErrRuntimeConfigUpdateWhileRunning = errors.New("worker runtime config update requires stopped worker")
	// ErrNilJobRepository indicates job repository is nil.
	ErrNilJobRepository = errors.New("job repository is required")
	// ErrNilReportRepository indicates report repository is nil.
	ErrNilReportRepository = errors.New("report repository is required")
	// ErrNilStorageClient indicates storage client is nil.
	ErrNilStorageClient = errors.New("storage client is required")
	// ErrUnsupportedReportType indicates an unsupported report type.
	ErrUnsupportedReportType = errors.New("unsupported report type")
	// ErrUnsupportedFormat indicates an unsupported format for streaming.
	ErrUnsupportedFormat = errors.New("unsupported format for streaming")
)

// ExportWorkerConfig contains configuration for the export worker.
type ExportWorkerConfig struct {
	PollInterval      time.Duration
	PageSize          int
	TempDir           string
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
}

// ExportWorker processes queued export jobs in the background.
type ExportWorker struct {
	mu         sync.Mutex
	jobRepo    repositories.ExportJobRepository
	reportRepo repositories.ReportRepository
	storage    objectstorage.Backend
	cfg        ExportWorkerConfig
	logger     libLog.Logger
	tracer     trace.Tracer
	metrics    *workermetrics.Recorder

	running    atomic.Bool
	stopOnce   sync.Once
	stopCh     chan struct{}
	doneCh     chan struct{}
	cancelFunc context.CancelFunc
}

func normalizeExportWorkerConfig(cfg ExportWorkerConfig) ExportWorkerConfig {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if cfg.PageSize <= 0 {
		cfg.PageSize = defaultPageSize
	}

	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}

	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}

	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultInitialBackoff
	}

	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}

	if cfg.BackoffMultiplier <= 0 {
		cfg.BackoffMultiplier = defaultBackoffMultiplier
	}

	return cfg
}

// NewExportWorker creates a new export worker.
func NewExportWorker(
	jobRepo repositories.ExportJobRepository,
	reportRepo repositories.ReportRepository,
	storage objectstorage.Backend,
	cfg ExportWorkerConfig,
	logger libLog.Logger,
) (*ExportWorker, error) {
	if jobRepo == nil {
		return nil, ErrNilJobRepository
	}

	if reportRepo == nil {
		return nil, ErrNilReportRepository
	}

	if sharedPorts.IsNilValue(storage) {
		return nil, ErrNilStorageClient
	}

	cfg = normalizeExportWorkerConfig(cfg)

	return &ExportWorker{
		jobRepo:    jobRepo,
		reportRepo: reportRepo,
		storage:    storage,
		cfg:        cfg,
		logger:     logger,
		tracer:     otel.Tracer("reporting.export_worker"),
		metrics:    workermetrics.NewRecorder(exportWorkerName),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}, nil
}

// prepareRunState reinitialises the worker's stop/done channels and sync.Once for
// re-entrant Start→Stop→Start cycles. SAFETY: The caller (WorkerManager) MUST ensure
// Stop() has fully completed before calling Start(), which calls prepareRunState().
// The WorkerManager serialises all lifecycle transitions via its mutex.
func (worker *ExportWorker) prepareRunState(ctx context.Context) context.Context {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	worker.stopOnce = sync.Once{}

	if chanutil.ClosedSignalChannel(worker.stopCh) {
		worker.stopCh = make(chan struct{})
	}

	if chanutil.ClosedSignalChannel(worker.doneCh) {
		worker.doneCh = make(chan struct{})
	}

	// Cancel any previous context before creating a new one to prevent
	// leaked goroutines from a prior Start→Stop→Start cycle.
	if worker.cancelFunc != nil {
		worker.cancelFunc()
	}

	runCtx, cancel := context.WithCancel(ctx)
	worker.cancelFunc = cancel

	return runCtx
}

// UpdateRuntimeConfig updates the worker runtime configuration used on the next start/restart.
// NOTE: This does NOT affect a currently running worker's ticker. The WorkerManager
// always performs a full stop→start cycle when config changes, ensuring the new
// config is picked up when the worker's run() loop creates a fresh ticker.
func (worker *ExportWorker) UpdateRuntimeConfig(cfg ExportWorkerConfig) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	if worker.running.Load() {
		return ErrRuntimeConfigUpdateWhileRunning
	}

	worker.cfg = normalizeExportWorkerConfig(cfg)

	return nil
}

// Start begins processing export jobs.
func (worker *ExportWorker) Start(ctx context.Context) error {
	if !worker.running.CompareAndSwap(false, true) {
		return ErrWorkerAlreadyRunning
	}

	runCtx := worker.prepareRunState(ctx)

	runtime.SafeGoWithContextAndComponent(
		runCtx,
		worker.logger,
		"reporting",
		"export_worker",
		runtime.KeepRunning,
		worker.run,
	)

	return nil
}

// Stop gracefully shuts down the worker.
func (worker *ExportWorker) Stop() error {
	if !worker.running.Load() {
		return ErrWorkerNotRunning
	}

	worker.stopOnce.Do(func() {
		if worker.cancelFunc != nil {
			worker.cancelFunc()
		}

		close(worker.stopCh)
	})
	<-worker.doneCh

	worker.running.Store(false)

	worker.logger.Log(context.Background(), libLog.LevelInfo, "export worker stopped")

	return nil
}

func (worker *ExportWorker) run(ctx context.Context) {
	defer runtime.RecoverAndLogWithContext(ctx, worker.logger, "export_worker", "run")
	defer close(worker.doneCh)

	ticker := time.NewTicker(worker.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-worker.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			worker.pollAndProcess(ctx)
		}
	}
}

func (worker *ExportWorker) pollAndProcess(ctx context.Context) {
	ctx, span := worker.tracer.Start(ctx, "export_worker.poll")
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

	job, jobCtx, err := worker.claimNextQueuedJob(ctx)
	if err != nil {
		outcome = workermetrics.OutcomeFailure

		libOpentelemetry.HandleSpanError(span, "failed to claim job", err)

		libLog.SafeError(worker.logger, ctx, "failed to claim queued job", err, runtime.IsProductionMode())

		return
	}

	if job == nil {
		return
	}

	if worker.processJob(jobCtx, job) {
		processed = 1
		outcome = workermetrics.OutcomeSuccess
	} else {
		failed = 1
		outcome = workermetrics.OutcomeFailure
	}
}

func (worker *ExportWorker) claimNextQueuedJob(ctx context.Context) (*entities.ExportJob, context.Context, error) {
	if tenantLister, ok := worker.jobRepo.(sharedPorts.TenantLister); ok {
		job, jobCtx, err := worker.claimNextQueuedJobAcrossTenants(ctx, tenantLister)
		if job != nil || err != nil {
			return job, jobCtx, err
		}
	}

	job, err := worker.jobRepo.ClaimNextQueued(ctx)
	if err != nil {
		return nil, ctx, err
	}

	if job == nil {
		return nil, ctx, nil
	}

	return job, withTenantContext(ctx, job.TenantID.String()), nil
}

func (worker *ExportWorker) claimNextQueuedJobAcrossTenants(
	ctx context.Context,
	tenantLister sharedPorts.TenantLister,
) (*entities.ExportJob, context.Context, error) {
	tenants, err := tenantLister.ListTenants(ctx)
	if err != nil {
		return nil, ctx, fmt.Errorf("list export worker tenants: %w", err)
	}

	var firstErr error

	for _, tenantID := range tenants {
		tenantCtx := withTenantContext(ctx, tenantID)

		job, claimErr := worker.jobRepo.ClaimNextQueued(tenantCtx)
		if claimErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("claim export job for tenant %s: %w", tenantID, claimErr)
			}

			continue
		}

		if job != nil {
			return job, withTenantContext(tenantCtx, job.TenantID.String()), nil
		}
	}

	if firstErr != nil {
		return nil, ctx, firstErr
	}

	return nil, ctx, nil
}

func withTenantContext(ctx context.Context, tenantID string) context.Context {
	if tenantID == "" {
		return ctx
	}

	return context.WithValue(ctx, auth.TenantIDKey, tenantID)
}
