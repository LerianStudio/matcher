// Package worker provides background job processing for reporting.
//
//nolint:wrapcheck // internal package streaming writers are tightly coupled
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	"github.com/LerianStudio/matcher/internal/reporting/services/query/exports"
)

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
	jobRepo    repositories.ExportJobRepository
	reportRepo repositories.ReportRepository
	storage    ports.ObjectStorageClient
	cfg        ExportWorkerConfig
	logger     libLog.Logger
	tracer     trace.Tracer

	running    atomic.Bool
	stopOnce   sync.Once
	stopCh     chan struct{}
	doneCh     chan struct{}
	cancelFunc context.CancelFunc
}

// NewExportWorker creates a new export worker.
func NewExportWorker(
	jobRepo repositories.ExportJobRepository,
	reportRepo repositories.ReportRepository,
	storage ports.ObjectStorageClient,
	cfg ExportWorkerConfig,
	logger libLog.Logger,
) (*ExportWorker, error) {
	if jobRepo == nil {
		return nil, ErrNilJobRepository
	}

	if reportRepo == nil {
		return nil, ErrNilReportRepository
	}

	if storage == nil {
		return nil, ErrNilStorageClient
	}

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

	return &ExportWorker{
		jobRepo:    jobRepo,
		reportRepo: reportRepo,
		storage:    storage,
		cfg:        cfg,
		logger:     logger,
		tracer:     otel.Tracer("reporting.export_worker"),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}, nil
}

// Start begins processing export jobs.
func (worker *ExportWorker) Start(ctx context.Context) error {
	if !worker.running.CompareAndSwap(false, true) {
		return ErrWorkerAlreadyRunning
	}

	ctx, cancel := context.WithCancel(ctx)
	worker.cancelFunc = cancel

	runtime.SafeGoWithContextAndComponent(
		ctx,
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
	defer close(worker.doneCh)
	defer runtime.RecoverAndLogWithContext(ctx, worker.logger, "export_worker", "run")

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

	ctx = libCommons.ContextWithLogger(ctx, worker.logger)
	ctx = libCommons.ContextWithTracer(ctx, worker.tracer)

	job, err := worker.jobRepo.ClaimNextQueued(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to claim job", err)

		worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to claim queued job: %v", err))

		return
	}

	if job == nil {
		return
	}

	worker.processJob(ctx, job)
}

func (worker *ExportWorker) processJob(ctx context.Context, job *entities.ExportJob) {
	ctx, span := worker.tracer.Start(ctx, "export_worker.process_job")
	defer span.End()

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf(
		"processing export job %s (type=%s, format=%s)",
		job.ID,
		job.ReportType,
		job.Format,
	))

	tempFile, err := os.CreateTemp(worker.cfg.TempDir, tempFilePrefix+job.ID.String()+"-*")
	if err != nil {
		worker.failJob(ctx, job, fmt.Errorf("creating temp file: %w", err))

		return
	}

	tempPath := tempFile.Name()

	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
	}()

	hashWriter := sha256.New()
	multiWriter := io.MultiWriter(tempFile, hashWriter)

	recordCount, err := worker.streamExport(ctx, job, multiWriter)
	if err != nil {
		worker.failJob(ctx, job, err)

		return
	}

	if err := tempFile.Sync(); err != nil {
		worker.failJob(ctx, job, fmt.Errorf("syncing temp file: %w", err))

		return
	}

	fileInfo, err := tempFile.Stat()
	if err != nil {
		worker.failJob(ctx, job, fmt.Errorf("getting file info: %w", err))

		return
	}

	bytesWritten := fileInfo.Size()
	sha256Hash := hex.EncodeToString(hashWriter.Sum(nil))

	if _, err := tempFile.Seek(0, 0); err != nil {
		worker.failJob(ctx, job, fmt.Errorf("seeking temp file: %w", err))

		return
	}

	fileKey := worker.generateFileKey(job)
	fileName := entities.GenerateFileName(
		job.ReportType,
		job.Format,
		job.ContextID,
		job.Filter.DateFrom,
		job.Filter.DateTo,
	)
	contentType := worker.getContentType(job.Format)

	if _, err := worker.storage.Upload(ctx, fileKey, tempFile, contentType); err != nil {
		worker.failJob(ctx, job, fmt.Errorf("uploading to storage: %w", err))

		return
	}

	job.MarkSucceeded(fileKey, fileName, sha256Hash, recordCount, bytesWritten)

	if err := worker.jobRepo.Update(ctx, job); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update job status", err)

		worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark job %s as succeeded: %v", job.ID, err))

		return
	}

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf(
		"completed export job %s: %d records, %d bytes",
		job.ID,
		recordCount,
		bytesWritten,
	))
}

func (worker *ExportWorker) streamExport(
	ctx context.Context,
	job *entities.ExportJob,
	writer io.Writer,
) (int64, error) {
	filter := entities.ReportFilter{
		ContextID: job.ContextID,
		DateFrom:  job.Filter.DateFrom,
		DateTo:    job.Filter.DateTo,
		SourceID:  job.Filter.SourceID,
	}

	switch job.ReportType {
	case entities.ExportReportTypeMatched:
		return worker.streamMatched(ctx, job, filter, writer)
	case entities.ExportReportTypeUnmatched:
		return worker.streamUnmatched(ctx, job, filter, writer)
	case entities.ExportReportTypeVariance:
		return worker.streamVariance(ctx, job, writer)
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedReportType, job.ReportType)
	}
}

func (worker *ExportWorker) streamMatched(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	switch job.Format {
	case entities.ExportFormatCSV:
		return worker.streamMatchedCSV(ctx, job, filter, writer)
	case entities.ExportFormatJSON:
		return worker.streamMatchedJSON(ctx, job, filter, writer)
	case entities.ExportFormatXML:
		return worker.streamMatchedXML(ctx, job, filter, writer)
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedFormat, job.Format)
	}
}

func (worker *ExportWorker) streamMatchedCSV(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	csvWriter := exports.NewStreamingCSVWriter(writer)

	if err := csvWriter.WriteMatchedHeader(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListMatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching matched page: %w", err)
		}

		for _, item := range items {
			if err := csvWriter.WriteMatchedRow(item); err != nil {
				return 0, err
			}
		}

		if csvWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, csvWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, csvWriter.RecordCount(), 0)

	if err := csvWriter.Flush(); err != nil {
		return 0, err
	}

	return csvWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamMatchedJSON(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	jsonWriter := exports.NewStreamingJSONWriter(writer)

	if err := jsonWriter.WriteArrayStart(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListMatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching matched page: %w", err)
		}

		for _, item := range items {
			if err := jsonWriter.WriteRow(item); err != nil {
				return 0, err
			}
		}

		if jsonWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, jsonWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, jsonWriter.RecordCount(), 0)

	if err := jsonWriter.WriteArrayEnd(); err != nil {
		return 0, err
	}

	return jsonWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamMatchedXML(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	xmlWriter := exports.NewStreamingXMLWriter(writer)

	if err := xmlWriter.WriteHeader("matchedItems"); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListMatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching matched page: %w", err)
		}

		for _, item := range items {
			if err := xmlWriter.WriteRow("item", item); err != nil {
				return 0, err
			}
		}

		if xmlWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, xmlWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, xmlWriter.RecordCount(), 0)

	if err := xmlWriter.WriteFooter("matchedItems"); err != nil {
		return 0, err
	}

	if err := xmlWriter.Flush(); err != nil {
		return 0, err
	}

	return xmlWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamUnmatched(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	switch job.Format {
	case entities.ExportFormatCSV:
		return worker.streamUnmatchedCSV(ctx, job, filter, writer)
	case entities.ExportFormatJSON:
		return worker.streamUnmatchedJSON(ctx, job, filter, writer)
	case entities.ExportFormatXML:
		return worker.streamUnmatchedXML(ctx, job, filter, writer)
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedFormat, job.Format)
	}
}

func (worker *ExportWorker) streamUnmatchedCSV(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	csvWriter := exports.NewStreamingCSVWriter(writer)

	if err := csvWriter.WriteUnmatchedHeader(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListUnmatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching unmatched page: %w", err)
		}

		for _, item := range items {
			if err := csvWriter.WriteUnmatchedRow(item); err != nil {
				return 0, err
			}
		}

		if csvWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, csvWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, csvWriter.RecordCount(), 0)

	if err := csvWriter.Flush(); err != nil {
		return 0, err
	}

	return csvWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamUnmatchedJSON(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	jsonWriter := exports.NewStreamingJSONWriter(writer)

	if err := jsonWriter.WriteArrayStart(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListUnmatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching unmatched page: %w", err)
		}

		for _, item := range items {
			if err := jsonWriter.WriteRow(item); err != nil {
				return 0, err
			}
		}

		if jsonWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, jsonWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, jsonWriter.RecordCount(), 0)

	if err := jsonWriter.WriteArrayEnd(); err != nil {
		return 0, err
	}

	return jsonWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamUnmatchedXML(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.ReportFilter,
	writer io.Writer,
) (int64, error) {
	xmlWriter := exports.NewStreamingXMLWriter(writer)

	if err := xmlWriter.WriteHeader("unmatchedItems"); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListUnmatchedPage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching unmatched page: %w", err)
		}

		for _, item := range items {
			if err := xmlWriter.WriteRow("item", item); err != nil {
				return 0, err
			}
		}

		if xmlWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, xmlWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, xmlWriter.RecordCount(), 0)

	if err := xmlWriter.WriteFooter("unmatchedItems"); err != nil {
		return 0, err
	}

	if err := xmlWriter.Flush(); err != nil {
		return 0, err
	}

	return xmlWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamVariance(
	ctx context.Context,
	job *entities.ExportJob,
	writer io.Writer,
) (int64, error) {
	filter := entities.VarianceReportFilter{
		ContextID: job.ContextID,
		DateFrom:  job.Filter.DateFrom,
		DateTo:    job.Filter.DateTo,
		SourceID:  job.Filter.SourceID,
	}

	switch job.Format {
	case entities.ExportFormatCSV:
		return worker.streamVarianceCSV(ctx, job, filter, writer)
	case entities.ExportFormatJSON:
		return worker.streamVarianceJSON(ctx, job, filter, writer)
	case entities.ExportFormatXML:
		return worker.streamVarianceXML(ctx, job, filter, writer)
	default:
		return 0, fmt.Errorf("%w: %s", ErrUnsupportedFormat, job.Format)
	}
}

func (worker *ExportWorker) streamVarianceCSV(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.VarianceReportFilter,
	writer io.Writer,
) (int64, error) {
	csvWriter := exports.NewStreamingCSVWriter(writer)

	if err := csvWriter.WriteVarianceHeader(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListVariancePage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching variance page: %w", err)
		}

		for _, item := range items {
			if err := csvWriter.WriteVarianceRow(item); err != nil {
				return 0, err
			}
		}

		if csvWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, csvWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, csvWriter.RecordCount(), 0)

	if err := csvWriter.Flush(); err != nil {
		return 0, err
	}

	return csvWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamVarianceJSON(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.VarianceReportFilter,
	writer io.Writer,
) (int64, error) {
	jsonWriter := exports.NewStreamingJSONWriter(writer)

	if err := jsonWriter.WriteArrayStart(); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListVariancePage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching variance page: %w", err)
		}

		for _, item := range items {
			if err := jsonWriter.WriteRow(item); err != nil {
				return 0, err
			}
		}

		if jsonWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, jsonWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, jsonWriter.RecordCount(), 0)

	if err := jsonWriter.WriteArrayEnd(); err != nil {
		return 0, err
	}

	return jsonWriter.RecordCount(), nil
}

func (worker *ExportWorker) streamVarianceXML(
	ctx context.Context,
	job *entities.ExportJob,
	filter entities.VarianceReportFilter,
	writer io.Writer,
) (int64, error) {
	xmlWriter := exports.NewStreamingXMLWriter(writer)

	if err := xmlWriter.WriteHeader("varianceRows"); err != nil {
		return 0, err
	}

	var afterKey string

	for {
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("export cancelled: %w", err)
		}

		items, nextKey, err := worker.reportRepo.ListVariancePage(
			ctx,
			filter,
			afterKey,
			worker.cfg.PageSize,
		)
		if err != nil {
			return 0, fmt.Errorf("fetching variance page: %w", err)
		}

		for _, item := range items {
			if err := xmlWriter.WriteRow("row", item); err != nil {
				return 0, err
			}
		}

		if xmlWriter.RecordCount()%progressUpdateEvery == 0 {
			_ = worker.jobRepo.UpdateProgress(ctx, job.ID, xmlWriter.RecordCount(), 0)
		}

		if nextKey == "" {
			break
		}

		afterKey = nextKey
	}

	_ = worker.jobRepo.UpdateProgress(ctx, job.ID, xmlWriter.RecordCount(), 0)

	if err := xmlWriter.WriteFooter("varianceRows"); err != nil {
		return 0, err
	}

	if err := xmlWriter.Flush(); err != nil {
		return 0, err
	}

	return xmlWriter.RecordCount(), nil
}

func (worker *ExportWorker) failJob(ctx context.Context, job *entities.ExportJob, err error) {
	_, span := worker.tracer.Start(ctx, "export_worker.fail_job")
	defer span.End()

	libOpentelemetry.HandleSpanError(span, "export job failed", err)

	if job.Attempts <= worker.cfg.MaxRetries {
		if updateErr := worker.requeueForRetry(ctx, job, err); updateErr != nil {
			libOpentelemetry.HandleSpanError(
				span,
				"failed to requeue export job for retry",
				updateErr,
			)

			worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf(
				"failed to requeue export job %s for retry: %v",
				job.ID,
				updateErr,
			))
		}

		return
	}

	worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf(
		"export job %s permanently failed after %d attempts: %v",
		job.ID,
		job.Attempts,
		err,
	))

	job.MarkFailed(err.Error())

	if updateErr := worker.jobRepo.UpdateStatus(ctx, job); updateErr != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update job status to failed", updateErr)

		worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to update job %s status to failed: %v", job.ID, updateErr))
	}
}

func (worker *ExportWorker) requeueForRetry(
	ctx context.Context,
	job *entities.ExportJob,
	err error,
) error {
	backoffDuration := worker.calculateBackoff(job.Attempts)
	nextRetry := time.Now().UTC().Add(backoffDuration)

	worker.logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("export job %s failed (attempt %d/%d), retrying in %v: %v",
		job.ID, job.Attempts, worker.cfg.MaxRetries, backoffDuration, err))

	job.MarkForRetry(err.Error(), nextRetry)

	updateErr := worker.jobRepo.RequeueForRetry(ctx, job)
	if updateErr == nil {
		return nil
	}

	worker.handleRequeueFailure(ctx, job, err, updateErr)

	return updateErr
}

func (worker *ExportWorker) handleRequeueFailure(
	ctx context.Context,
	job *entities.ExportJob,
	originalErr, updateErr error,
) {
	worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to requeue job %s for retry: %v", job.ID, updateErr))

	errMsg := fmt.Sprintf(
		"failed to requeue export job after error: %v (requeue error: %v)",
		originalErr,
		updateErr,
	)
	job.MarkFailed(errMsg)

	if failErr := worker.jobRepo.UpdateStatus(ctx, job); failErr != nil {
		worker.logger.Log(ctx, libLog.LevelError, fmt.Sprintf(
			"failed to mark job %s as failed after requeue error: %v",
			job.ID,
			failErr,
		))
	}
}

func (worker *ExportWorker) calculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return worker.cfg.InitialBackoff
	}

	backoff := float64(worker.cfg.InitialBackoff)
	for i := 1; i < attempt; i++ {
		backoff *= worker.cfg.BackoffMultiplier
	}

	if time.Duration(backoff) > worker.cfg.MaxBackoff {
		return worker.cfg.MaxBackoff
	}

	return time.Duration(backoff)
}

func (worker *ExportWorker) generateFileKey(job *entities.ExportJob) string {
	return filepath.Join(
		"exports",
		job.TenantID.String(),
		job.ContextID.String(),
		fmt.Sprintf("%s-%s.%s", job.ID.String(), job.ReportType, worker.getExtension(job.Format)),
	)
}

func (worker *ExportWorker) getExtension(format entities.ExportFormat) string {
	switch format {
	case entities.ExportFormatCSV:
		return "csv"
	case entities.ExportFormatJSON:
		return "json"
	case entities.ExportFormatXML:
		return "xml"
	default:
		return "dat"
	}
}

func (worker *ExportWorker) getContentType(format entities.ExportFormat) string {
	switch format {
	case entities.ExportFormatCSV:
		return "text/csv"
	case entities.ExportFormatJSON:
		return "application/json"
	case entities.ExportFormatXML:
		return "application/xml"
	default:
		return "application/octet-stream"
	}
}
