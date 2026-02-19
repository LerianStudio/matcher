// Package adjustment provides PostgreSQL persistence for adjustment entities.
package adjustment

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
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	columns = "id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at"

	// sortColumnCreatedAt is the sort column for created_at.
	sortColumnCreatedAt = "created_at"
	// sortColumnType is the sort column for type.
	sortColumnType = "type"

	// defaultBatchCapacity is the default pre-allocation capacity for batch queries.
	defaultBatchCapacity = 32
)

// Repository persists adjustments in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new adjustment repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a new adjustment.
func (repo *Repository) Create(
	ctx context.Context,
	adjustment *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if adjustment == nil {
		return nil, ErrAdjustmentEntityNeeded
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_adjustment")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*matchingEntities.Adjustment, error) {
			model, err := NewPostgreSQLModel(adjustment)
			if err != nil {
				return nil, err
			}

			_, err = tx.ExecContext(
				ctx,
				`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
				model.ID,
				model.ContextID,
				model.MatchGroupID,
				model.TransactionID,
				model.Type,
				model.Direction,
				model.Amount,
				model.Currency,
				model.Description,
				model.Reason,
				model.CreatedBy,
				model.CreatedAt,
				model.UpdatedAt,
			)
			if err != nil {
				return nil, fmt.Errorf("insert adjustment: %w", err)
			}

			row := tx.QueryRowContext(
				ctx,
				"SELECT "+columns+" FROM adjustments WHERE id=$1",
				model.ID,
			)

			return scan(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create adjustment transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create adjustment", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create adjustment")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx creates an adjustment within the provided transaction.
// This enables atomic operations where adjustment creation and audit logging
// must succeed or fail together (SOX compliance).
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx any,
	adjustment *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if adjustment == nil {
		return nil, ErrAdjustmentEntityNeeded
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	sqlTx, ok := tx.(*sql.Tx)
	if !ok {
		return nil, ErrInvalidTransactionType
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_adjustment_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		sqlTx,
		func(innerTx *sql.Tx) (*matchingEntities.Adjustment, error) {
			model, err := NewPostgreSQLModel(adjustment)
			if err != nil {
				return nil, err
			}

			_, err = innerTx.ExecContext(
				ctx,
				`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
				model.ID,
				model.ContextID,
				model.MatchGroupID,
				model.TransactionID,
				model.Type,
				model.Direction,
				model.Amount,
				model.Currency,
				model.Description,
				model.Reason,
				model.CreatedBy,
				model.CreatedAt,
				model.UpdatedAt,
			)
			if err != nil {
				return nil, fmt.Errorf("insert adjustment: %w", err)
			}

			row := innerTx.QueryRowContext(
				ctx,
				"SELECT "+columns+" FROM adjustments WHERE id=$1",
				model.ID,
			)

			return scan(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create adjustment with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create adjustment with tx", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to create adjustment with tx")

		return nil, wrappedErr
	}

	return result, nil
}

// FindByID returns an adjustment by ID.
func (repo *Repository) FindByID(
	ctx context.Context,
	contextID, id uuid.UUID,
) (*matchingEntities.Adjustment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_adjustment_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*matchingEntities.Adjustment, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+columns+" FROM adjustments WHERE context_id=$1 AND id=$2",
				contextID.String(),
				id.String(),
			)

			return scan(row)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("find adjustment transaction: %w", err)
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find adjustment by id", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to find adjustment by id")
		}

		return nil, wrappedErr
	}

	return result, nil
}

// ListByContextID returns adjustments for a context with cursor pagination.
//
//nolint:gocognit,gocyclo,cyclop // cursor pagination with sorting requires complex logic
func (repo *Repository) ListByContextID(
	ctx context.Context,
	contextID uuid.UUID,
	filter matchingRepos.CursorFilter,
) ([]*matchingEntities.Adjustment, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_adjustments_by_context")

	defer span.End()

	var pagination libHTTP.CursorPagination

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (_ []*matchingEntities.Adjustment, err error) {
			orderDirection := libHTTP.ValidateSortDirection(filter.SortOrder)

			limit := filter.Limit
			if limit <= 0 {
				limit = 20
			}

			sortColumn := normalizeSortColumn(filter.SortBy)
			useIDCursor := sortColumn == "id"

			var cursorDirection string

			findAll := squirrel.Select(strings.Split(columns, ", ")...).
				From("adjustments").
				Where(squirrel.Eq{"context_id": contextID.String()}).
				PlaceholderFormat(squirrel.Dollar)

			var cursorErr error

			if useIDCursor {
				findAll, _, cursorDirection, cursorErr = pgcommon.ApplyIDCursorPagination(
					findAll, filter.Cursor, orderDirection, limit,
				)
			} else {
				findAll, _, cursorDirection, cursorErr = pgcommon.ApplySortCursorPagination(
					findAll, filter.Cursor, sortColumn, orderDirection, "adjustments", limit,
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
				return nil, fmt.Errorf("failed to query adjustments: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			adjustments := make([]*matchingEntities.Adjustment, 0, limit+1)

			for rows.Next() {
				entity, scanErr := scan(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				adjustments = append(adjustments, entity)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate rows: %w", err)
			}

			hasPagination := len(adjustments) > limit
			isFirstPage := filter.Cursor == "" || (!hasPagination && cursorDirection == libHTTP.CursorDirectionPrev)

			adjustments = libHTTP.PaginateRecords(
				isFirstPage,
				hasPagination,
				cursorDirection,
				adjustments,
				limit,
			)

			if len(adjustments) == 0 {
				return adjustments, nil
			}

			if useIDCursor {
				pagination, err = libHTTP.CalculateCursor(
					isFirstPage,
					hasPagination,
					cursorDirection,
					adjustments[0].ID.String(),
					adjustments[len(adjustments)-1].ID.String(),
				)
				if err != nil {
					return nil, fmt.Errorf("failed to calculate cursor: %w", err)
				}
			} else {
				first, last := adjustments[0], adjustments[len(adjustments)-1]

				next, prev := libHTTP.CalculateSortCursorPagination(
					isFirstPage, hasPagination, cursorDirection == libHTTP.CursorDirectionNext,
					sortColumn,
					adjustmentSortValue(first, sortColumn), first.ID.String(),
					adjustmentSortValue(last, sortColumn), last.ID.String(),
				)

				pagination = libHTTP.CursorPagination{Next: next, Prev: prev}
			}

			return adjustments, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to list adjustments via WithTenantTx: %w", err)
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to list adjustments", wrappedErr)

			logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to list adjustments")
		}

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result, pagination, nil
}

// ListByMatchGroupID returns all adjustments for a match group.
func (repo *Repository) ListByMatchGroupID(
	ctx context.Context,
	contextID, matchGroupID uuid.UUID,
) ([]*matchingEntities.Adjustment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_adjustments_by_match_group")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (_ []*matchingEntities.Adjustment, err error) {
			rows, err := qe.QueryContext(
				ctx,
				"SELECT "+columns+" FROM adjustments WHERE context_id=$1 AND match_group_id=$2 ORDER BY created_at ASC",
				contextID.String(),
				matchGroupID.String(),
			)
			if err != nil {
				return nil, fmt.Errorf("query adjustments: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			adjustments := make([]*matchingEntities.Adjustment, 0, defaultBatchCapacity)

			for rows.Next() {
				entity, scanErr := scan(rows)
				if scanErr != nil {
					return nil, scanErr
				}

				adjustments = append(adjustments, entity)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterate rows: %w", err)
			}

			return adjustments, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list adjustments by match group transaction: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list adjustments by match group",
			wrappedErr,
		)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to list adjustments by match group")

		return nil, wrappedErr
	}

	return result, nil
}

func adjustmentSortValue(adj *matchingEntities.Adjustment, column string) string {
	switch column {
	case sortColumnCreatedAt:
		return adj.CreatedAt.UTC().Format(time.RFC3339Nano)
	case sortColumnType:
		return string(adj.Type)
	default:
		return adj.ID.String()
	}
}

// allowedAdjustmentSortColumns lists columns valid for sort operations.
var allowedAdjustmentSortColumns = []string{"id", sortColumnCreatedAt, sortColumnType}

func normalizeSortColumn(sortBy string) string {
	return libHTTP.ValidateSortColumn(sortBy, allowedAdjustmentSortColumns, "id")
}

func scan(scanner interface{ Scan(dest ...any) error }) (*matchingEntities.Adjustment, error) {
	var model PostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.MatchGroupID,
		&model.TransactionID,
		&model.Type,
		&model.Direction,
		&model.Amount,
		&model.Currency,
		&model.Description,
		&model.Reason,
		&model.CreatedBy,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

var _ matchingRepos.AdjustmentRepository = (*Repository)(nil)
