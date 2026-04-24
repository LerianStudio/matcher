// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package match_run provides PostgreSQL repository implementation for match run persistence.
package match_run

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// columns defines the match run table projection.
const columns = "id, context_id, mode, status, started_at, completed_at, stats, failure_reason, created_at, updated_at"

// Repository persists match runs in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new match run repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a match run without an explicit transaction.
func (repo *Repository) Create(
	ctx context.Context,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return repo.create(ctx, nil, entity)
}

// CreateWithTx inserts a match run using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRunEntityNeeded
	}

	if tx == nil {
		return nil, ErrInvalidTx
	}

	return repo.create(ctx, tx, entity)
}

func (repo *Repository) create(
	ctx context.Context,
	tx *sql.Tx,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRunEntityNeeded
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_match_run")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) (*matchingEntities.MatchRun, error) {
			model, err := NewPostgreSQLModel(entity)
			if err != nil {
				return nil, err
			}

			_, err = execTx.ExecContext(
				ctx,
				`INSERT INTO match_runs (id, context_id, mode, status, started_at, completed_at, stats, failure_reason, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
				model.ID,
				model.ContextID,
				model.Mode,
				model.Status,
				model.StartedAt,
				model.CompletedAt,
				model.Stats,
				model.FailureReason,
				model.CreatedAt,
				model.UpdatedAt,
			)
			if err != nil {
				return nil, fmt.Errorf("insert match run: %w", err)
			}

			row := execTx.QueryRowContext(
				ctx,
				"SELECT "+columns+" FROM match_runs WHERE context_id=$1 AND id=$2",
				model.ContextID,
				model.ID,
			)

			return scan(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create match run transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create match run", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create match run")

		return nil, wrappedErr
	}

	return result, nil
}

// Update modifies a match run without an explicit transaction.
func (repo *Repository) Update(
	ctx context.Context,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return repo.update(ctx, nil, entity)
}

// UpdateWithTx modifies a match run using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRunEntityNeeded
	}

	if tx == nil {
		return nil, ErrInvalidTx
	}

	return repo.update(ctx, tx, entity)
}

func (repo *Repository) update(
	ctx context.Context,
	tx *sql.Tx,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrMatchRunEntityNeeded
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.update_match_run")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) (*matchingEntities.MatchRun, error) {
			model, err := NewPostgreSQLModel(entity)
			if err != nil {
				return nil, err
			}

			res, err := execTx.ExecContext(ctx,
				`UPDATE match_runs
			 SET status=$1, completed_at=$2, stats=$3, failure_reason=$4, updated_at=$5
			 WHERE context_id=$6 AND id=$7`,
				model.Status,
				model.CompletedAt,
				model.Stats,
				model.FailureReason,
				model.UpdatedAt,
				model.ContextID,
				model.ID,
			)
			if err != nil {
				return nil, fmt.Errorf("update match run: %w", err)
			}

			affected, err := res.RowsAffected()
			if err != nil {
				return nil, fmt.Errorf("rows affected: %w", err)
			}

			if affected == 0 {
				return nil, sql.ErrNoRows
			}

			row := execTx.QueryRowContext(
				ctx,
				"SELECT "+columns+" FROM match_runs WHERE context_id=$1 AND id=$2",
				model.ContextID,
				model.ID,
			)

			return scan(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update match run transaction: %w", err)
		if !errors.Is(wrappedErr, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update match run", wrappedErr)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update match run")
		}

		return nil, wrappedErr
	}

	return result, nil
}

// FindByID returns a match run by id.
func (repo *Repository) FindByID(
	ctx context.Context,
	contextID, runID uuid.UUID,
) (*matchingEntities.MatchRun, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_match_run_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*matchingEntities.MatchRun, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+columns+" FROM match_runs WHERE context_id=$1 AND id=$2",
				contextID.String(),
				runID.String(),
			)

			return scan(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("find match run transaction: %w", err)
		if errors.Is(wrappedErr, sql.ErrNoRows) {
			return nil, wrappedErr
		}

		libOpentelemetry.HandleSpanError(span, "failed to find match run by id", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to find match run by id")

		return nil, wrappedErr
	}

	return result, nil
}

// ListByContextID returns match runs for a context with cursor pagination.
//
//nolint:gocognit,gocyclo,cyclop // cursor pagination with sorting requires complex logic
func (repo *Repository) ListByContextID(
	ctx context.Context,
	contextID uuid.UUID,
	filter matchingRepos.CursorFilter,
) ([]*matchingEntities.MatchRun, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_match_runs_by_context")

	defer span.End()

	var pagination libHTTP.CursorPagination

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (runs []*matchingEntities.MatchRun, err error) {
			orderDirection := libHTTP.ValidateSortDirection(filter.SortOrder)

			limit := filter.Limit
			if limit <= 0 {
				limit = constants.DefaultPaginationLimit
			}

			decodedCursor := libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}

			if filter.Cursor != "" {
				cursor, err := libHTTP.DecodeCursor(filter.Cursor)
				if err != nil {
					return nil, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
				}

				decodedCursor = cursor
			}

			findAll := squirrel.Select(strings.Split(columns, ", ")...).
				From("match_runs").
				Where(squirrel.Eq{"context_id": contextID.String()}).
				PlaceholderFormat(squirrel.Dollar)

			operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(orderDirection, decodedCursor.Direction)
			if dirErr != nil {
				return nil, fmt.Errorf("cursor direction rules: %w", dirErr)
			}

			if decodedCursor.ID != "" {
				findAll = findAll.Where(squirrel.Expr("id "+operator+" ?", decodedCursor.ID))
			}

			findAll = findAll.
				OrderBy("id " + effectiveOrder).
				Limit(uint64(limit + 1))

			query, args, err := findAll.ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build SQL: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to query match runs: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			runs = make([]*matchingEntities.MatchRun, 0, limit+1)

			for rows.Next() {
				entity, scanErr := scan(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				runs = append(runs, entity)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate rows: %w", err)
			}

			hasPagination := len(runs) > limit
			isFirstPage := filter.Cursor == "" || (!hasPagination && decodedCursor.Direction == libHTTP.CursorDirectionPrev)

			runs = libHTTP.PaginateRecords(
				isFirstPage,
				hasPagination,
				decodedCursor.Direction,
				runs,
				limit,
			)

			if len(runs) > 0 {
				pagination, err = libHTTP.CalculateCursor(
					isFirstPage,
					hasPagination,
					decodedCursor.Direction,
					runs[0].ID.String(),
					runs[len(runs)-1].ID.String(),
				)
				if err != nil {
					return nil, fmt.Errorf("failed to calculate cursor: %w", err)
				}
			}

			return runs, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list match runs transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list match runs", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to list match runs")

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result, pagination, nil
}

// WithTx executes a callback within a tenant transaction.
func (repo *Repository) WithTx(ctx context.Context, fn func(matchingRepos.Tx) error) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if fn == nil {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.with_match_run_tx")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		if err := fn(tx); err != nil {
			return struct{}{}, err
		}

		return struct{}{}, nil
	})
	if err != nil {
		wrappedErr := fmt.Errorf("match run transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to run match run transaction", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to run match run transaction")

		return wrappedErr
	}

	return nil
}

// scan converts a SQL row into a match run entity.
func scan(scanner interface{ Scan(dest ...any) error }) (*matchingEntities.MatchRun, error) {
	var model PostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.Mode,
		&model.Status,
		&model.StartedAt,
		&model.CompletedAt,
		&model.Stats,
		&model.FailureReason,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

var _ matchingRepos.MatchRunRepository = (*Repository)(nil)
