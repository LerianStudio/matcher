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

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	governanceRepositories "github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
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
	provider     ports.InfrastructureProvider
	auditLogRepo governanceRepositories.AuditLogRepository
}

// NewRepository creates a new adjustment repository.
func NewRepository(
	provider ports.InfrastructureProvider,
	auditLogRepo governanceRepositories.AuditLogRepository,
) *Repository {
	return &Repository{provider: provider, auditLogRepo: auditLogRepo}
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
			return repo.insertWithTx(ctx, tx, adjustment)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create adjustment transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create adjustment", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create adjustment")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx creates an adjustment within the provided transaction.
// This enables atomic operations where adjustment creation and audit logging
// must succeed or fail together (SOX compliance).
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
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

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_adjustment_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*matchingEntities.Adjustment, error) {
			return repo.insertWithTx(ctx, innerTx, adjustment)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create adjustment with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create adjustment with tx", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create adjustment with tx")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithAuditLog atomically persists an adjustment and its corresponding audit log
// in a single transaction. This ensures SOX compliance: both records are committed
// together or both are rolled back on failure.
func (repo *Repository) CreateWithAuditLog(
	ctx context.Context,
	adjustment *matchingEntities.Adjustment,
	auditLog *sharedDomain.AuditLog,
) (*matchingEntities.Adjustment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if adjustment == nil {
		return nil, ErrAdjustmentEntityNeeded
	}

	if repo.auditLogRepo == nil {
		return nil, ErrAuditLogRepoRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_adjustment_with_audit_log")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (*matchingEntities.Adjustment, error) {
		return repo.CreateWithAuditLogWithTx(ctx, tx, adjustment, auditLog)
	})
	if err != nil {
		wrappedErr := fmt.Errorf("create adjustment with audit log: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create adjustment with audit log", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create adjustment with audit log")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithAuditLogWithTx atomically persists an adjustment and its corresponding audit log
// using a caller-owned transaction. The caller is responsible for commit/rollback.
// This enables composing the adjustment+audit operation inside a larger transaction.
func (repo *Repository) CreateWithAuditLogWithTx(
	ctx context.Context,
	tx *sql.Tx,
	adjustment *matchingEntities.Adjustment,
	auditLog *sharedDomain.AuditLog,
) (*matchingEntities.Adjustment, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if adjustment == nil {
		return nil, ErrAdjustmentEntityNeeded
	}

	if repo.auditLogRepo == nil {
		return nil, ErrAuditLogRepoRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	if auditLog == nil {
		return nil, fmt.Errorf("audit log is required for SOX compliance: %w", ErrAuditLogRequired)
	}

	created, createErr := repo.insertWithTx(ctx, tx, adjustment)
	if createErr != nil {
		return nil, fmt.Errorf("persist adjustment: %w", createErr)
	}

	if _, auditErr := repo.auditLogRepo.CreateWithTx(ctx, tx, auditLog); auditErr != nil {
		return nil, fmt.Errorf("persist audit log: %w", auditErr)
	}

	return created, nil
}

// insertWithTx performs the actual insert and SELECT within a transaction.
func (repo *Repository) insertWithTx(ctx context.Context, tx *sql.Tx, adjustment *matchingEntities.Adjustment) (*matchingEntities.Adjustment, error) {
	if tx == nil {
		return nil, ErrTransactionRequired
	}

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

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to find adjustment by id")
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

			limit := libHTTP.ValidateLimit(
				filter.Limit,
				constants.DefaultPaginationLimit,
				constants.MaximumPaginationLimit,
			)

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

			first, last := adjustments[0], adjustments[len(adjustments)-1]
			if boundaryErr := pgcommon.ValidateSortCursorBoundaries(first, last); boundaryErr != nil {
				return nil, fmt.Errorf("validate adjustment pagination boundaries: %w", boundaryErr)
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
				calculatedPagination, calculateErr := calculateAdjustmentSortPagination(
					isFirstPage,
					hasPagination,
					cursorDirection == libHTTP.CursorDirectionNext,
					sortColumn,
					adjustmentSortValue(first, sortColumn),
					first.ID.String(),
					adjustmentSortValue(last, sortColumn),
					last.ID.String(),
					libHTTP.CalculateSortCursorPagination,
				)
				if calculateErr != nil {
					return nil, fmt.Errorf("failed to calculate sort cursor pagination: %w", calculateErr)
				}

				pagination = calculatedPagination
			}

			return adjustments, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to list adjustments via WithTenantTx: %w", err)
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to list adjustments", wrappedErr)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to list adjustments")
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

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to list adjustments by match group")

		return nil, wrappedErr
	}

	return result, nil
}

func adjustmentSortValue(adj *matchingEntities.Adjustment, column string) string {
	if adj == nil {
		return ""
	}

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

func calculateAdjustmentSortPagination(
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
		"calculate adjustment cursor pagination",
	)
	if err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("calculate adjustment sort cursor pagination: %w", err)
	}

	return pagination, nil
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
