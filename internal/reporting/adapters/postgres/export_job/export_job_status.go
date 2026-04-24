// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package export_job

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Update persists changes to an existing export job.
func (repo *Repository) Update(ctx context.Context, job *entities.ExportJob) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.update")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeUpdate(ctx, tx, job)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update export job: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update export job", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to update export job", wrappedErr, runtime.IsProductionMode())

		return wrappedErr
	}

	return nil
}

// UpdateWithTx persists changes to an existing export job using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.ExportJob,
) error {
	return repo.executeUpdate(ctx, tx, job)
}

func (repo *Repository) executeUpdate(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.ExportJob,
) error {
	filterJSON, err := job.Filter.ToJSON()
	if err != nil {
		return fmt.Errorf("marshal filter: %w", err)
	}

	query, args, err := squirrel.
		Update(exportJobsTable).
		Set("status", job.Status).
		Set("records_written", job.RecordsWritten).
		Set("bytes_written", job.BytesWritten).
		Set("file_key", pgcommon.StringToNullString(job.FileKey)).
		Set("file_name", pgcommon.StringToNullString(job.FileName)).
		Set("sha256", pgcommon.StringToNullString(job.SHA256)).
		Set("error", pgcommon.StringToNullString(job.Error)).
		Set("attempts", job.Attempts).
		Set("next_retry_at", pgcommon.TimePtrToNullTime(job.NextRetryAt)).
		Set("filter", filterJSON).
		Set("started_at", pgcommon.TimePtrToNullTime(job.StartedAt)).
		Set("finished_at", pgcommon.TimePtrToNullTime(job.FinishedAt)).
		Set("updated_at", job.UpdatedAt).
		Where(squirrel.Eq{"id": job.ID}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if err != nil {
		return fmt.Errorf("build update query: %w", err)
	}

	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec update: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rows == 0 {
		return repositories.ErrExportJobNotFound
	}

	return nil
}

// UpdateStatus updates only the status and related fields of a job.
func (repo *Repository) UpdateStatus(ctx context.Context, job *entities.ExportJob) error {
	return repo.Update(ctx, job)
}

// UpdateStatusWithTx updates only the status and related fields of a job using the provided transaction.
func (repo *Repository) UpdateStatusWithTx(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.ExportJob,
) error {
	return repo.UpdateWithTx(ctx, tx, job)
}

// UpdateProgress updates the progress counters of a running job.
func (repo *Repository) UpdateProgress(
	ctx context.Context,
	id uuid.UUID,
	recordsWritten, bytesWritten int64,
) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.update_progress")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeUpdateProgress(ctx, tx, id, recordsWritten, bytesWritten)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("update export job progress: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update export job progress", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to update export job progress", wrappedErr, runtime.IsProductionMode())

		return wrappedErr
	}

	return nil
}

// UpdateProgressWithTx updates the progress counters of a running job using the provided transaction.
func (repo *Repository) UpdateProgressWithTx(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
	recordsWritten, bytesWritten int64,
) error {
	return repo.executeUpdateProgress(ctx, tx, id, recordsWritten, bytesWritten)
}

func (repo *Repository) executeUpdateProgress(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
	recordsWritten, bytesWritten int64,
) error {
	query, args, err := squirrel.
		Update(exportJobsTable).
		Set("records_written", recordsWritten).
		Set("bytes_written", bytesWritten).
		Set("updated_at", time.Now().UTC()).
		Where(squirrel.Eq{"id": id}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if err != nil {
		return fmt.Errorf("build update query: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("exec update: %w", err)
	}

	return nil
}

// ClaimNextQueued atomically claims the next queued job for processing.
// Only claims jobs where next_retry_at is NULL or in the past.
func (repo *Repository) ClaimNextQueued(ctx context.Context) (*entities.ExportJob, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.claim_next_queued")

	defer span.End()

	job, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ExportJob, error) {
			now := time.Now().UTC()

			// #nosec G202 -- exportJobColumnsString() returns a constant, not user input
			query := `
			UPDATE export_jobs
			SET status = $1, started_at = $2, attempts = attempts + 1, updated_at = $2
			WHERE id = (
				SELECT id FROM export_jobs
				WHERE status = $3
				AND (next_retry_at IS NULL OR next_retry_at <= $4)
				ORDER BY created_at ASC
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING ` + exportJobColumnsString()

			row := tx.QueryRowContext(ctx, query,
				entities.ExportJobStatusRunning,
				now,
				entities.ExportJobStatusQueued,
				now,
			)

			return scanExportJob(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, repositories.ErrExportJobNotFound) {
			return nil, nil
		}

		wrappedErr := fmt.Errorf("claim next queued export job: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to claim queued job", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to claim next queued export job", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return job, nil
}

// RequeueForRetry updates a job for retry with a scheduled next attempt time.
func (repo *Repository) RequeueForRetry(
	ctx context.Context,
	job *entities.ExportJob,
) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.requeue_for_retry")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		query, args, err := squirrel.
			Update(exportJobsTable).
			Set("status", job.Status).
			Set("error", pgcommon.StringToNullString(job.Error)).
			Set("next_retry_at", pgcommon.TimePtrToNullTime(job.NextRetryAt)).
			Set("updated_at", job.UpdatedAt).
			Where(squirrel.Eq{"id": job.ID}).
			PlaceholderFormat(squirrel.Dollar).
			ToSql()
		if err != nil {
			return struct{}{}, fmt.Errorf("build update query: %w", err)
		}

		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return struct{}{}, fmt.Errorf("exec update: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return struct{}{}, fmt.Errorf("get rows affected: %w", err)
		}

		if rows == 0 {
			return struct{}{}, repositories.ErrExportJobNotFound
		}

		return struct{}{}, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("requeue export job for retry: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to requeue export job", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to requeue export job for retry", wrappedErr, runtime.IsProductionMode())

		return wrappedErr
	}

	return nil
}
