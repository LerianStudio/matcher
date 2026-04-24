// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package job

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const jobColumns = "id, context_id, source_id, status, started_at, completed_at, metadata, created_at, updated_at"

// Column name constants for sort operations.
const (
	columnCreatedAt   = "created_at"
	columnStartedAt   = "started_at"
	columnCompletedAt = "completed_at"
	columnStatus      = "status"
)

// Repository is a PostgreSQL implementation of JobRepository.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new job repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// WithTx executes a function within a database transaction.
func (repo *Repository) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if repo == nil || repo.provider == nil {
		return errRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.job_transaction")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, fn(tx)
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to run job transaction", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to run job transaction")

		return fmt.Errorf("job transaction failed: %w", err)
	}

	return nil
}

// Create persists a new ingestion job.
func (repo *Repository) Create(
	ctx context.Context,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return repo.create(ctx, nil, job)
}

// CreateWithTx persists a new ingestion job within a transaction.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return repo.create(ctx, tx, job)
}

func (repo *Repository) create(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	if repo == nil || repo.provider == nil {
		return nil, errRepoNotInit
	}

	if job == nil {
		return nil, errJobEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_ingestion_job")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) (*entities.IngestionJob, error) {
			model, err := NewJobPostgreSQLModel(job)
			if err != nil {
				return nil, err
			}

			query := `INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, completed_at, metadata, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING ` + jobColumns
			row := execTx.QueryRowContext(ctx, query,
				model.ID,
				model.ContextID,
				model.SourceID,
				model.Status,
				model.StartedAt,
				model.CompletedAt,
				model.Metadata,
				model.CreatedAt,
				model.UpdatedAt,
			)

			return scanJob(row)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create ingestion job", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to create ingestion job")

		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	return result, nil
}

// FindByID retrieves a job by its ID.
func (repo *Repository) FindByID(
	ctx context.Context,
	id uuid.UUID,
) (*entities.IngestionJob, error) {
	if repo == nil || repo.provider == nil {
		return nil, errRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_ingestion_job_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.IngestionJob, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+jobColumns+" FROM ingestion_jobs WHERE id = $1",
				id.String(),
			)

			return scanJob(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find ingestion job", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find ingestion job")
		}

		return nil, fmt.Errorf("failed to find job: %w", err)
	}

	return result, nil
}

// FindByContextID retrieves jobs by context ID with cursor pagination.
//
//nolint:cyclop // pagination logic is inherently complex
func (repo *Repository) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
	filter repositories.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, errRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_ingestion_jobs_by_context")

	defer span.End()

	var pagination libHTTP.CursorPagination

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (jobs []*entities.IngestionJob, err error) {
			orderDirection := libHTTP.ValidateSortDirection(filter.SortOrder)
			limit := libHTTP.ValidateLimit(
				filter.Limit,
				constants.DefaultPaginationLimit,
				constants.MaximumPaginationLimit,
			)

			sortColumn := normalizeJobSortColumn(filter.SortBy)
			useIDCursor := sortColumn == "id"

			findAll := squirrel.Select(strings.Split(jobColumns, ", ")...).
				From("ingestion_jobs").
				Where(squirrel.Eq{"context_id": contextID.String()}).
				PlaceholderFormat(squirrel.Dollar)

			var cursorDirection string

			findAll, _, cursorDirection, err = pgcommon.ApplyCursorPagination(
				findAll, filter.Cursor, sortColumn, orderDirection, limit, useIDCursor, "ingestion_jobs",
			)
			if err != nil {
				return nil, fmt.Errorf("apply cursor pagination: %w", err)
			}

			query, args, err := findAll.ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build SQL: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to query jobs: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			jobs = make([]*entities.IngestionJob, 0, limit+1)

			for rows.Next() {
				jobEntity, err := scanJob(rows)
				if err != nil {
					return nil, err
				}

				jobs = append(jobs, jobEntity)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate rows: %w", err)
			}

			hasPagination := len(jobs) > limit
			isFirstPage := filter.Cursor == "" || (!hasPagination && cursorDirection == libHTTP.CursorDirectionPrev)

			jobs = libHTTP.PaginateRecords(
				isFirstPage,
				hasPagination,
				cursorDirection,
				jobs,
				limit,
			)

			pagination, err = calculateJobPagination(
				jobs, useIDCursor, isFirstPage, hasPagination, cursorDirection, sortColumn,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate cursor: %w", err)
			}

			return jobs, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list ingestion jobs", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list ingestion jobs")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf(
			"failed to list jobs by context: %w",
			err,
		)
	}

	return result, pagination, nil
}

// Update persists changes to an existing ingestion job.
func (repo *Repository) Update(
	ctx context.Context,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return repo.update(ctx, nil, job)
}

// UpdateWithTx persists changes to an existing ingestion job within a transaction.
func (repo *Repository) UpdateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return repo.update(ctx, tx, job)
}

func (repo *Repository) update(
	ctx context.Context,
	tx *sql.Tx,
	job *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	if repo == nil || repo.provider == nil {
		return nil, errRepoNotInit
	}

	if job == nil {
		return nil, errJobEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.update_ingestion_job")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) (*entities.IngestionJob, error) {
			model, err := NewJobPostgreSQLModel(job)
			if err != nil {
				return nil, err
			}

			query := `UPDATE ingestion_jobs SET status = $1, started_at = $2, completed_at = $3, metadata = $4, updated_at = $5 WHERE id = $6
			RETURNING ` + jobColumns
			row := execTx.QueryRowContext(ctx, query,
				model.Status,
				model.StartedAt,
				model.CompletedAt,
				model.Metadata,
				model.UpdatedAt,
				model.ID,
			)

			return scanJob(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update ingestion job", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to update ingestion job")
		}

		return nil, fmt.Errorf("failed to update job: %w", err)
	}

	return result, nil
}

func jobSortValue(job *entities.IngestionJob, column string) string {
	if job == nil {
		return ""
	}

	switch column {
	case columnCreatedAt:
		return job.CreatedAt.UTC().Format(time.RFC3339Nano)
	case columnStartedAt:
		if job.StartedAt.IsZero() {
			return ""
		}

		return job.StartedAt.UTC().Format(time.RFC3339Nano)
	case columnCompletedAt:
		if job.CompletedAt != nil {
			return job.CompletedAt.UTC().Format(time.RFC3339Nano)
		}

		return ""
	case columnStatus:
		return job.Status.String()
	default:
		return job.ID.String()
	}
}

// allowedJobSortColumns lists columns valid for sort operations.
var allowedJobSortColumns = []string{"id", columnCreatedAt, columnStartedAt, columnCompletedAt, columnStatus}

func normalizeJobSortColumn(sortBy string) string {
	return libHTTP.ValidateSortColumn(strings.TrimSpace(sortBy), allowedJobSortColumns, "id")
}

// calculateJobPagination computes cursor pagination metadata for a job result set.
func calculateJobPagination(
	jobs []*entities.IngestionJob,
	useIDCursor, isFirstPage, hasPagination bool,
	cursorDirection string,
	sortColumn string,
) (libHTTP.CursorPagination, error) {
	if len(jobs) == 0 {
		return libHTTP.CursorPagination{}, nil
	}

	first, last := jobs[0], jobs[len(jobs)-1]
	if err := sharedpg.ValidateSortCursorBoundaries(first, last); err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("validate job pagination boundaries: %w", err)
	}

	if useIDCursor {
		pagination, err := libHTTP.CalculateCursor(
			isFirstPage, hasPagination, cursorDirection,
			first.ID.String(), last.ID.String(),
		)
		if err != nil {
			return libHTTP.CursorPagination{}, fmt.Errorf("calculate cursor: %w", err)
		}

		return pagination, nil
	}

	return calculateJobSortPagination(
		isFirstPage,
		hasPagination,
		cursorDirection == libHTTP.CursorDirectionNext,
		sortColumn,
		jobSortValue(first, sortColumn),
		first.ID.String(),
		jobSortValue(last, sortColumn),
		last.ID.String(),
		libHTTP.CalculateSortCursorPagination,
	)
}

func calculateJobSortPagination(
	isFirstPage, hasPagination, pointsNext bool,
	sortColumn,
	firstSortValue,
	firstID,
	lastSortValue,
	lastID string,
	calculateSortCursor sharedpg.SortCursorCalculator,
) (libHTTP.CursorPagination, error) {
	pagination, err := sharedpg.CalculateSortCursorPaginationWrapped(
		isFirstPage,
		hasPagination,
		pointsNext,
		sortColumn,
		firstSortValue,
		firstID,
		lastSortValue,
		lastID,
		calculateSortCursor,
		"calculate sort cursor pagination",
	)
	if err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("calculate job sort cursor pagination: %w", err)
	}

	return pagination, nil
}

func scanJob(scanner interface{ Scan(dest ...any) error }) (*entities.IngestionJob, error) {
	var model pgcommon.JobPostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.SourceID,
		&model.Status,
		&model.StartedAt,
		&model.CompletedAt,
		&model.Metadata,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("failed to scan job: %w", err)
	}

	return jobModelToEntity(&model)
}
