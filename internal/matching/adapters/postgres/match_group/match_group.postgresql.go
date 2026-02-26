// Package match_group provides PostgreSQL persistence for match group entities.
package match_group

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	// columns defines the match group table projection.
	columns = "id, context_id, run_id, rule_id, confidence, status, rejected_reason, confirmed_at, created_at, updated_at"

	// sortColumnCreatedAt is the sort column for created_at.
	sortColumnCreatedAt = "created_at"
	// sortColumnStatus is the sort column for status.
	sortColumnStatus = "status"
)

// Repository persists match groups in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new match group repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// CreateBatch inserts match groups without an explicit transaction.
func (repo *Repository) CreateBatch(
	ctx context.Context,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return repo.createBatch(ctx, nil, groups)
}

// CreateBatchWithTx inserts match groups using the provided transaction.
func (repo *Repository) CreateBatchWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrInvalidTx
	}

	sqlTx, ok := tx.(*sql.Tx)
	if !ok || sqlTx == nil {
		return nil, ErrInvalidTx
	}

	return repo.createBatch(ctx, sqlTx, groups)
}

func (repo *Repository) createBatch(
	ctx context.Context,
	tx *sql.Tx,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if len(groups) == 0 {
		return nil, nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_match_group_batch")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) ([]*matchingEntities.MatchGroup, error) {
			ids, err := insertBatch(ctx, execTx, groups)
			if err != nil {
				return nil, err
			}

			return fetchBatch(ctx, execTx, ids)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create match group batch transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create match group batch", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create match group batch")

		return nil, wrappedErr
	}

	return result, nil
}

func insertBatch(
	ctx context.Context,
	execTx *sql.Tx,
	groups []*matchingEntities.MatchGroup,
) (ids []uuid.UUID, retErr error) {
	stmt, err := execTx.PrepareContext(
		ctx,
		`INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status, rejected_reason, confirmed_at, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
	)
	if err != nil {
		return nil, fmt.Errorf("prepare insert match group: %w", err)
	}

	defer func() {
		if closeErr := stmt.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("close prepared statement: %w", closeErr)
		}
	}()

	ids = make([]uuid.UUID, 0, len(groups))
	for _, group := range groups {
		if group == nil {
			continue
		}

		model, err := NewPostgreSQLModel(group)
		if err != nil {
			return nil, err
		}

		if _, err := stmt.ExecContext(ctx,
			model.ID,
			model.ContextID,
			model.RunID,
			model.RuleID,
			model.Confidence,
			model.Status,
			model.RejectedReason,
			model.ConfirmedAt,
			model.CreatedAt,
			model.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("insert match group: %w", err)
		}

		ids = append(ids, group.ID)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	return ids, nil
}

func fetchBatch(
	ctx context.Context,
	execTx *sql.Tx,
	ids []uuid.UUID,
) (result []*matchingEntities.MatchGroup, retErr error) {
	if len(ids) == 0 {
		return []*matchingEntities.MatchGroup{}, nil
	}

	rows, err := execTx.QueryContext(
		ctx,
		"SELECT "+columns+" FROM match_groups WHERE id = ANY($1::uuid[]) ORDER BY created_at ASC",
		ids,
	)
	if err != nil {
		return nil, fmt.Errorf("select match group batch: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil && retErr == nil {
			retErr = fmt.Errorf("close rows: %w", closeErr)
		}
	}()

	persisted := make([]*matchingEntities.MatchGroup, 0, len(ids))

	for rows.Next() {
		entity, scanErr := scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		persisted = append(persisted, entity)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate match group rows: %w", err)
	}

	return persisted, nil
}

// ListByRunID returns match groups for a run with cursor pagination.
//
//nolint:gocognit,gocyclo,cyclop // cursor pagination with sorting requires complex logic
func (repo *Repository) ListByRunID(
	ctx context.Context,
	contextID, runID uuid.UUID,
	filter matchingRepos.CursorFilter,
) ([]*matchingEntities.MatchGroup, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_match_groups_by_run")

	defer span.End()

	var pagination libHTTP.CursorPagination

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (groups []*matchingEntities.MatchGroup, err error) {
			orderDirection := libHTTP.ValidateSortDirection(filter.SortOrder)

			limit := libHTTP.ValidateLimit(
				filter.Limit,
				constants.DefaultPaginationLimit,
				constants.MaximumPaginationLimit,
			)

			sortColumn := normalizeSortColumn(filter.SortBy)
			useIDCursor := sortColumn == "id"

			var cursorDirection string

			findAll := squirrel.Select(strings.Split(columns, ", ")...).
				From("match_groups").
				Where(squirrel.Eq{"context_id": contextID.String()}).
				Where(squirrel.Eq{"run_id": runID.String()}).
				PlaceholderFormat(squirrel.Dollar)

			var cursorErr error

			if useIDCursor {
				findAll, _, cursorDirection, cursorErr = pgcommon.ApplyIDCursorPagination(
					findAll, filter.Cursor, orderDirection, limit,
				)
			} else {
				findAll, _, cursorDirection, cursorErr = pgcommon.ApplySortCursorPagination(
					findAll, filter.Cursor, sortColumn, orderDirection, "match_groups", limit,
				)
			}

			if cursorErr != nil {
				return nil, fmt.Errorf("apply cursor pagination: %w", cursorErr)
			}

			query, args, err := findAll.ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build SQL: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to query match groups: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			groups = make([]*matchingEntities.MatchGroup, 0, limit+1)

			for rows.Next() {
				entity, err := scan(rows)
				if err != nil {
					return nil, err
				}

				groups = append(groups, entity)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate rows: %w", err)
			}

			hasPagination := len(groups) > limit
			isFirstPage := filter.Cursor == "" || (!hasPagination && cursorDirection == libHTTP.CursorDirectionPrev)

			groups = libHTTP.PaginateRecords(
				isFirstPage,
				hasPagination,
				cursorDirection,
				groups,
				limit,
			)

			if len(groups) == 0 {
				return groups, nil
			}

			first, last := groups[0], groups[len(groups)-1]
			if boundaryErr := pgcommon.ValidateSortCursorBoundaries(first, last); boundaryErr != nil {
				return nil, fmt.Errorf("validate match group pagination boundaries: %w", boundaryErr)
			}

			if useIDCursor {
				pagination, err = libHTTP.CalculateCursor(
					isFirstPage,
					hasPagination,
					cursorDirection,
					first.ID.String(),
					last.ID.String(),
				)
				if err != nil {
					return nil, fmt.Errorf("failed to calculate cursor: %w", err)
				}
			} else {
				calculatedPagination, calculateErr := calculateMatchGroupSortPagination(
					isFirstPage,
					hasPagination,
					cursorDirection == libHTTP.CursorDirectionNext,
					sortColumn,
					matchGroupSortValue(first, sortColumn),
					first.ID.String(),
					matchGroupSortValue(last, sortColumn),
					last.ID.String(),
					libHTTP.CalculateSortCursorPagination,
				)
				if calculateErr != nil {
					return nil, fmt.Errorf("failed to calculate sort cursor pagination: %w", calculateErr)
				}

				pagination = calculatedPagination
			}

			return groups, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to list match groups via WithTenantTx: %w", err)
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to list match groups", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to list match groups")
		}

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result, pagination, nil
}

func matchGroupSortValue(group *matchingEntities.MatchGroup, column string) string {
	if group == nil {
		return ""
	}

	switch column {
	case sortColumnCreatedAt:
		return group.CreatedAt.UTC().Format(time.RFC3339Nano)
	case sortColumnStatus:
		return string(group.Status)
	default:
		return group.ID.String()
	}
}

// allowedMatchGroupSortColumns lists columns valid for sort operations.
var allowedMatchGroupSortColumns = []string{"id", sortColumnCreatedAt, sortColumnStatus}

func normalizeSortColumn(sortBy string) string {
	return libHTTP.ValidateSortColumn(sortBy, allowedMatchGroupSortColumns, "id")
}

func calculateMatchGroupSortPagination(
	isFirstPage, hasPagination, pointsNext bool,
	sortColumn,
	firstSortValue,
	firstID,
	lastSortValue,
	lastID string,
	calculateSortCursor pgcommon.SortCursorCalculator,
) (libHTTP.CursorPagination, error) {
	pagination, err := pgcommon.CalculateSortCursorPaginationWrapped(
		isFirstPage,
		hasPagination,
		pointsNext,
		sortColumn,
		firstSortValue,
		firstID,
		lastSortValue,
		lastID,
		calculateSortCursor,
		"calculate match group cursor pagination",
	)
	if err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("calculate match group sort cursor pagination: %w", err)
	}

	return pagination, nil
}

// scan converts a SQL row into a match group entity.
func scan(scanner interface{ Scan(dest ...any) error }) (*matchingEntities.MatchGroup, error) {
	var model PostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.RunID,
		&model.RuleID,
		&model.Confidence,
		&model.Status,
		&model.RejectedReason,
		&model.ConfirmedAt,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// FindByID returns a match group by ID.
func (repo *Repository) FindByID(
	ctx context.Context,
	contextID, id uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_match_group_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*matchingEntities.MatchGroup, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+columns+" FROM match_groups WHERE context_id=$1 AND id=$2",
				contextID.String(),
				id.String(),
			)

			return scan(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("find match group transaction: %w", err)
		if errors.Is(wrappedErr, sql.ErrNoRows) {
			return nil, wrappedErr
		}

		libOpentelemetry.HandleSpanError(span, "failed to find match group by id", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to find match group by id")

		return nil, wrappedErr
	}

	return result, nil
}

// Update modifies a match group.
func (repo *Repository) Update(
	ctx context.Context,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if group == nil {
		return nil, ErrMatchGroupEntityNeeded
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.update_match_group")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*matchingEntities.MatchGroup, error) {
			return repo.executeUpdate(ctx, tx, group)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update match group transaction: %w", err)
		if !errors.Is(wrappedErr, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update match group", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update match group")
		}

		return nil, wrappedErr
	}

	return result, nil
}

// UpdateWithTx modifies a match group using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx context.Context,
	tx matchingRepos.Tx,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if group == nil {
		return nil, ErrMatchGroupEntityNeeded
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	sqlTx, ok := tx.(*sql.Tx)
	if !ok || sqlTx == nil {
		return nil, ErrInvalidTx
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.update_match_group_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		sqlTx,
		func(innerTx *sql.Tx) (*matchingEntities.MatchGroup, error) {
			return repo.executeUpdate(ctx, innerTx, group)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update match group transaction: %w", err)
		if !errors.Is(wrappedErr, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to update match group", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to update match group")
		}

		return nil, wrappedErr
	}

	return result, nil
}

// executeUpdate performs the actual match group update within a transaction.
func (repo *Repository) executeUpdate(
	ctx context.Context,
	tx *sql.Tx,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	model, err := NewPostgreSQLModel(group)
	if err != nil {
		return nil, err
	}

	res, err := tx.ExecContext(ctx,
		`UPDATE match_groups
		 SET status=$1, rejected_reason=$2, confirmed_at=$3, updated_at=$4
		 WHERE context_id=$5 AND id=$6`,
		model.Status,
		model.RejectedReason,
		model.ConfirmedAt,
		model.UpdatedAt,
		model.ContextID,
		model.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("update match group: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("rows affected: %w", err)
	}

	if affected == 0 {
		return nil, sql.ErrNoRows
	}

	row := tx.QueryRowContext(
		ctx,
		"SELECT "+columns+" FROM match_groups WHERE context_id=$1 AND id=$2",
		model.ContextID,
		model.ID,
	)

	return scan(row)
}

var _ matchingRepos.MatchGroupRepository = (*Repository)(nil)
