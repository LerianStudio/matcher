package export_job

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const exportJobsTable = "export_jobs"

// safeLimit converts a non-negative int to uint64 for squirrel's Limit method.
// Returns 0 for negative values (which squirrel treats as no limit).
func safeLimit(n int) uint64 {
	if n <= 0 {
		return 0
	}

	return uint64(n)
}

// Repository persists export jobs in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new export job repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create export job: %v", wrappedErr))

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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get export job: %v", wrappedErr))

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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to update export job: %v", wrappedErr))

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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to update export job progress: %v", wrappedErr))

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

// List retrieves export jobs for the tenant in context with optional status filter.
// Tenant is extracted from context using auth.GetTenantID(ctx).
// cursor is the ID of the last item from the previous page (nil for first page).
// Results are ordered by created_at DESC and cursor-based pagination uses (created_at, id) keyset.
func (repo *Repository) List(
	ctx context.Context,
	status *string,
	cursor *libHTTP.TimestampCursor,
	limit int,
) ([]*entities.ExportJob, libHTTP.CursorPagination, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.list")

	defer span.End()

	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		wrappedErr := fmt.Errorf("parse tenant ID from context: %w", err)
		libOpentelemetry.HandleSpanError(span, "invalid tenant ID in context", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("invalid tenant ID in context: %v", wrappedErr))

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	var pagination libHTTP.CursorPagination

	limit = libHTTP.ValidateLimit(limit, libHTTP.DefaultLimit, libHTTP.MaxLimit)

	jobs, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*entities.ExportJob, error) {
			queryLimit := limit + 1
			builder := squirrel.
				Select(exportJobColumns()...).
				From(exportJobsTable).
				Where(squirrel.Eq{"tenant_id": tenantID}).
				OrderBy("created_at DESC", "id DESC").
				Limit(safeLimit(queryLimit)).
				PlaceholderFormat(squirrel.Dollar)

			if status != nil {
				builder = builder.Where(squirrel.Eq{"status": *status})
			}

			// Apply cursor-based pagination using keyset (created_at, id)
			if cursor != nil {
				builder = builder.Where(
					squirrel.Or{
						squirrel.Lt{"created_at": cursor.Timestamp},
						squirrel.And{
							squirrel.Eq{"created_at": cursor.Timestamp},
							squirrel.Lt{"id": cursor.ID},
						},
					},
				)
			}

			query, args, err := builder.ToSql()
			if err != nil {
				return nil, fmt.Errorf("build select query: %w", err)
			}

			records, err := scanExportJobs(tx.QueryContext(ctx, query, args...))
			if err != nil {
				return nil, err
			}

			hasMore := len(records) > limit
			if hasMore {
				records = records[:limit]
				last := records[len(records)-1]

				pagination.Next = libHTTP.EncodeTimestampCursor(last.CreatedAt, last.ID)
			}

			return records, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list export jobs: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list export jobs", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list export jobs: %v", wrappedErr))

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return jobs, pagination, nil
}

// ListByContext retrieves export jobs for a specific context.
func (repo *Repository) ListByContext(
	ctx context.Context,
	contextID uuid.UUID,
	limit int,
) ([]*entities.ExportJob, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.list_by_context")

	defer span.End()

	jobs, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*entities.ExportJob, error) {
			query, args, err := squirrel.
				Select(exportJobColumns()...).
				From(exportJobsTable).
				Where(squirrel.Eq{"context_id": contextID}).
				OrderBy("created_at DESC").
				Limit(safeLimit(limit)).
				PlaceholderFormat(squirrel.Dollar).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("build select query: %w", err)
			}

			return scanExportJobs(tx.QueryContext(ctx, query, args...))
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list export jobs by context: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list export jobs", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list export jobs: %v", wrappedErr))

		return nil, wrappedErr
	}

	return jobs, nil
}

// ListExpired retrieves jobs that have passed their expiration time.
func (repo *Repository) ListExpired(
	ctx context.Context,
	limit int,
) ([]*entities.ExportJob, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.export_job.list_expired")

	defer span.End()

	jobs, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) ([]*entities.ExportJob, error) {
			query, args, err := squirrel.
				Select(exportJobColumns()...).
				From(exportJobsTable).
				Where(squirrel.Eq{"status": entities.ExportJobStatusSucceeded}).
				Where(squirrel.Lt{"expires_at": time.Now().UTC()}).
				OrderBy("expires_at ASC").
				Limit(safeLimit(limit)).
				PlaceholderFormat(squirrel.Dollar).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("build select query: %w", err)
			}

			return scanExportJobs(tx.QueryContext(ctx, query, args...))
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list expired export jobs: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list expired jobs", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list expired export jobs: %v", wrappedErr))

		return nil, wrappedErr
	}

	return jobs, nil
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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to claim next queued export job: %v", wrappedErr))

		return nil, wrappedErr
	}

	return job, nil
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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to delete export job: %v", wrappedErr))

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

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to requeue export job for retry: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

func exportJobColumns() []string {
	return []string{
		"id", "tenant_id", "context_id", "report_type", "format", "filter",
		"status", "records_written", "bytes_written", "file_key", "file_name",
		"sha256", "error", "attempts", "next_retry_at", "created_at", "started_at", "finished_at", "expires_at", "updated_at",
	}
}

func exportJobColumnsString() string {
	return "id, tenant_id, context_id, report_type, format, filter, status, records_written, bytes_written, file_key, file_name, sha256, error, attempts, next_retry_at, created_at, started_at, finished_at, expires_at, updated_at"
}

func scanExportJob(row *sql.Row) (*entities.ExportJob, error) {
	var job entities.ExportJob

	var filterJSON []byte

	var fileKey, fileName, sha256Hash, errMsg sql.NullString

	var nextRetryAt, startedAt, finishedAt sql.NullTime

	err := row.Scan(
		&job.ID,
		&job.TenantID,
		&job.ContextID,
		&job.ReportType,
		&job.Format,
		&filterJSON,
		&job.Status,
		&job.RecordsWritten,
		&job.BytesWritten,
		&fileKey,
		&fileName,
		&sha256Hash,
		&errMsg,
		&job.Attempts,
		&nextRetryAt,
		&job.CreatedAt,
		&startedAt,
		&finishedAt,
		&job.ExpiresAt,
		&job.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, repositories.ErrExportJobNotFound
		}

		return nil, fmt.Errorf("scan export job: %w", err)
	}

	if err := json.Unmarshal(filterJSON, &job.Filter); err != nil {
		return nil, fmt.Errorf("unmarshal filter: %w", err)
	}

	job.FileKey = fileKey.String
	job.FileName = fileName.String
	job.SHA256 = sha256Hash.String
	job.Error = errMsg.String

	if nextRetryAt.Valid {
		job.NextRetryAt = &nextRetryAt.Time
	}

	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}

	if finishedAt.Valid {
		job.FinishedAt = &finishedAt.Time
	}

	return &job, nil
}

func scanExportJobs(rows *sql.Rows, err error) ([]*entities.ExportJob, error) {
	if err != nil {
		return nil, fmt.Errorf("query export jobs: %w", err)
	}

	defer rows.Close()

	var jobs []*entities.ExportJob

	for rows.Next() {
		var job entities.ExportJob

		var filterJSON []byte

		var fileKey, fileName, sha256Hash, errMsg sql.NullString

		var nextRetryAt, startedAt, finishedAt sql.NullTime

		err := rows.Scan(
			&job.ID,
			&job.TenantID,
			&job.ContextID,
			&job.ReportType,
			&job.Format,
			&filterJSON,
			&job.Status,
			&job.RecordsWritten,
			&job.BytesWritten,
			&fileKey,
			&fileName,
			&sha256Hash,
			&errMsg,
			&job.Attempts,
			&nextRetryAt,
			&job.CreatedAt,
			&startedAt,
			&finishedAt,
			&job.ExpiresAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan export job row: %w", err)
		}

		if err := json.Unmarshal(filterJSON, &job.Filter); err != nil {
			return nil, fmt.Errorf("unmarshal filter: %w", err)
		}

		job.FileKey = fileKey.String
		job.FileName = fileName.String
		job.SHA256 = sha256Hash.String
		job.Error = errMsg.String

		if nextRetryAt.Valid {
			job.NextRetryAt = &nextRetryAt.Time
		}

		if startedAt.Valid {
			job.StartedAt = &startedAt.Time
		}

		if finishedAt.Valid {
			job.FinishedAt = &finishedAt.Time
		}

		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate export jobs: %w", err)
	}

	return jobs, nil
}
