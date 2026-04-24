// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	libS3 "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/s3"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	reportingMetrics "github.com/LerianStudio/matcher/internal/reporting/services/metrics"
)

// processJob runs one export job to completion. Returns true when the job
// uploaded, was marked succeeded, and the status update persisted; false
// when any stage failed (temp-file / stream / sync / stat / seek / keygen /
// upload / MarkSucceeded / persist). A MarkSucceeded/persist failure after a
// successful upload is counted as a failed cycle item even though the file
// is already in object storage — the user-facing job record never flipped
// to SUCCEEDED, so operators should see it in items_failed_total.
func (worker *ExportWorker) processJob(ctx context.Context, job *entities.ExportJob) bool {
	ctx = withTenantContext(ctx, job.TenantID.String())

	ctx, span := worker.tracer.Start(ctx, "export_worker.process_job")
	defer span.End()

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf(
		"processing export job %s (type=%s, format=%s)",
		job.ID,
		job.ReportType,
		job.Format,
	))

	// The repository's ClaimNextQueued already flips the row to RUNNING at
	// the atomic UPDATE; emit the RUNNING lifecycle transition here so the
	// counter lands once per claim attempt (retries included).
	reportingMetrics.RecordExportJobTransition(
		ctx,
		string(job.Format),
		string(entities.ExportJobStatusRunning),
	)

	tempFile, err := os.CreateTemp(worker.cfg.TempDir, tempFilePrefix+job.ID.String()+"-*")
	if err != nil {
		worker.failJob(ctx, job, fmt.Errorf("creating temp file: %w", err))

		return false
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

		return false
	}

	if err := tempFile.Sync(); err != nil {
		worker.failJob(ctx, job, fmt.Errorf("syncing temp file: %w", err))

		return false
	}

	fileInfo, err := tempFile.Stat()
	if err != nil {
		worker.failJob(ctx, job, fmt.Errorf("getting file info: %w", err))

		return false
	}

	bytesWritten := fileInfo.Size()
	sha256Hash := hex.EncodeToString(hashWriter.Sum(nil))

	if _, err := tempFile.Seek(0, 0); err != nil {
		worker.failJob(ctx, job, fmt.Errorf("seeking temp file: %w", err))

		return false
	}

	fileKey, err := worker.generateFileKey(job)
	if err != nil {
		worker.failJob(ctx, job, fmt.Errorf("build export storage key: %w", err))

		return false
	}

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

		return false
	}

	if err := job.MarkSucceeded(fileKey, fileName, sha256Hash, recordCount, bytesWritten); err != nil {
		libLog.SafeError(worker.logger, ctx, fmt.Sprintf("failed to mark job %s as succeeded", job.ID), err, runtime.IsProductionMode())

		return false
	}

	if err := worker.jobRepo.Update(ctx, job); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update job status", err)

		libLog.SafeError(worker.logger, ctx, fmt.Sprintf("failed to persist job %s succeeded status", job.ID), err, runtime.IsProductionMode())

		return false
	}

	worker.logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf(
		"completed export job %s: %d records, %d bytes",
		job.ID,
		recordCount,
		bytesWritten,
	))

	// Success-path lifecycle metric + duration histogram. Duration is
	// measured from CreatedAt (queue time) not StartedAt (claim time) so
	// dashboards surface end-to-end export latency — queue time is usually
	// the dominant component and is what operators notice.
	reportingMetrics.RecordExportJobTransition(
		ctx,
		string(job.Format),
		string(entities.ExportJobStatusSucceeded),
	)
	reportingMetrics.RecordExportDuration(
		ctx,
		string(job.Format),
		float64(time.Since(job.CreatedAt).Milliseconds()),
	)

	return true
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

			libLog.SafeError(
				worker.logger,
				ctx,
				fmt.Sprintf("failed to requeue export job %s for retry", job.ID),
				updateErr,
				runtime.IsProductionMode(),
			)
		}

		return
	}

	libLog.SafeError(
		worker.logger,
		ctx,
		fmt.Sprintf("export job %s permanently failed after %d attempts", job.ID, job.Attempts),
		err,
		runtime.IsProductionMode(),
	)

	if markErr := job.MarkFailed(err.Error()); markErr != nil {
		libLog.SafeError(worker.logger, ctx, fmt.Sprintf("failed to mark job %s as failed", job.ID), markErr, runtime.IsProductionMode())

		return
	}

	if updateErr := worker.jobRepo.UpdateStatus(ctx, job); updateErr != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update job status to failed", updateErr)

		libLog.SafeError(worker.logger, ctx, fmt.Sprintf("failed to update job %s status to failed", job.ID), updateErr, runtime.IsProductionMode())
	}

	// Terminal-failure lifecycle metric + duration histogram. Emitted after
	// the FAILED transition persists so the counter matches what the DB
	// reports. Duration covers the full queued → failed window.
	reportingMetrics.RecordExportJobTransition(
		ctx,
		string(job.Format),
		string(entities.ExportJobStatusFailed),
	)
	reportingMetrics.RecordExportDuration(
		ctx,
		string(job.Format),
		float64(time.Since(job.CreatedAt).Milliseconds()),
	)
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

	if markErr := job.MarkForRetry(err.Error(), nextRetry); markErr != nil {
		return fmt.Errorf("mark export job for retry: %w", markErr)
	}

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
	libLog.SafeError(worker.logger, ctx, fmt.Sprintf("failed to requeue job %s for retry", job.ID), updateErr, runtime.IsProductionMode())

	errMsg := fmt.Sprintf(
		"failed to requeue export job after error: %v (requeue error: %v)",
		originalErr,
		updateErr,
	)
	if markErr := job.MarkFailed(errMsg); markErr != nil {
		libLog.SafeError(
			worker.logger,
			ctx,
			fmt.Sprintf("failed to mark job %s as failed after requeue error (transition rejected)", job.ID),
			markErr,
			runtime.IsProductionMode(),
		)

		return
	}

	if failErr := worker.jobRepo.UpdateStatus(ctx, job); failErr != nil {
		libLog.SafeError(
			worker.logger,
			ctx,
			fmt.Sprintf("failed to persist job %s failed status after requeue error", job.ID),
			failErr,
			runtime.IsProductionMode(),
		)
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

func (worker *ExportWorker) generateFileKey(job *entities.ExportJob) (string, error) {
	originalKey := fmt.Sprintf("exports/%s/%s-%s.%s",
		job.ContextID.String(),
		job.ID.String(),
		job.ReportType,
		worker.getExtension(job.Format),
	)

	//nolint:wrapcheck // caller (processJob) wraps with "build export storage key" context
	return libS3.GetObjectStorageKey(job.TenantID.String(), originalKey)
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
