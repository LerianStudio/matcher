// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package export_job

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Create persists a new export job.
func (repo *Repository) Create(ctx context.Context, job *entities.ExportJob) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.create")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeCreate(ctx, tx, job)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("create export job: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create export job", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to create export job", wrappedErr, runtime.IsProductionMode())

		return wrappedErr
	}

	return nil
}

// CreateWithTx persists a new export job using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.ExportJob,
) error {
	return repo.executeCreate(ctx, tx, job)
}

func (repo *Repository) executeCreate(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.ExportJob,
) error {
	filterJSON, err := job.Filter.ToJSON()
	if err != nil {
		return fmt.Errorf("marshal filter: %w", err)
	}

	query, args, err := squirrel.
		Insert(exportJobsTable).
		Columns(
			"id", "tenant_id", "context_id", "report_type", "format", "filter",
			"status", "records_written", "bytes_written", "file_key", "file_name",
			"sha256", "error", "attempts", "next_retry_at", "created_at", "started_at", "finished_at", "expires_at", "updated_at",
		).
		Values(
			job.ID, job.TenantID, job.ContextID, job.ReportType, job.Format, filterJSON,
			job.Status, job.RecordsWritten, job.BytesWritten, pgcommon.StringToNullString(job.FileKey), pgcommon.StringToNullString(job.FileName),
			pgcommon.StringToNullString(job.SHA256),
			pgcommon.StringToNullString(job.Error),
			job.Attempts, pgcommon.TimePtrToNullTime(job.NextRetryAt),
			job.CreatedAt, pgcommon.TimePtrToNullTime(job.StartedAt), pgcommon.TimePtrToNullTime(job.FinishedAt), job.ExpiresAt, job.UpdatedAt,
		).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if err != nil {
		return fmt.Errorf("build insert query: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("exec insert: %w", err)
	}

	return nil
}

// GetByID retrieves an export job by its ID.
func (repo *Repository) GetByID(
	ctx context.Context,
	id uuid.UUID,
) (*entities.ExportJob, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.get_by_id")

	defer span.End()

	job, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ExportJob, error) {
			return repo.getByIDTx(ctx, tx, id)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get export job by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get export job", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get export job", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return job, nil
}

func (repo *Repository) getByIDTx(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
) (*entities.ExportJob, error) {
	query, args, err := squirrel.
		Select(exportJobColumns()...).
		From(exportJobsTable).
		Where(squirrel.Eq{"id": id}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build select query: %w", err)
	}

	row := tx.QueryRowContext(ctx, query, args...)

	return scanExportJob(row)
}

// Delete removes an export job by ID.
func (repo *Repository) Delete(ctx context.Context, id uuid.UUID) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.delete")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeDelete(ctx, tx, id)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("delete export job: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to delete export job", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to delete export job", wrappedErr, runtime.IsProductionMode())

		return wrappedErr
	}

	return nil
}

// DeleteWithTx removes an export job by ID using the provided transaction.
func (repo *Repository) DeleteWithTx(ctx context.Context, tx *sql.Tx, id uuid.UUID) error {
	return repo.executeDelete(ctx, tx, id)
}

func (repo *Repository) executeDelete(
	ctx context.Context,
	tx *sql.Tx,
	id uuid.UUID,
) error {
	query, args, err := squirrel.
		Delete(exportJobsTable).
		Where(squirrel.Eq{"id": id}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if err != nil {
		return fmt.Errorf("build delete query: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("exec delete: %w", err)
	}

	return nil
}
