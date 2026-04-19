package context

import (
	stdctx "context"
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

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const contextColumns = "id, tenant_id, name, type, interval, status, fee_tolerance_abs, fee_tolerance_pct, fee_normalization, auto_match_on_upload, created_at, updated_at"

// Repository provides PostgreSQL operations for reconciliation contexts.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new context repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a new reconciliation context into the database.
func (repo *Repository) Create(
	ctx stdctx.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_reconciliation_context")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationContext, error) {
			return repo.executeCreate(ctx, tx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create reconciliation context: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to create reconciliation context",
			wrappedErr,
		)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create reconciliation context")

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new reconciliation context using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.create_reconciliation_context_with_tx")
	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationContext, error) {
			return repo.executeCreate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create reconciliation context: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to create reconciliation context",
			wrappedErr,
		)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to create reconciliation context")

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual context creation within a transaction.
func (repo *Repository) executeCreate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	model, err := NewContextPostgreSQLModel(entity)
	if err != nil {
		return nil, fmt.Errorf("create context model: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO reconciliation_contexts (id, tenant_id, name, type, interval, status, fee_tolerance_abs, fee_tolerance_pct, fee_normalization, auto_match_on_upload, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		model.ID,
		model.TenantID,
		model.Name,
		model.Type,
		model.Interval,
		model.Status,
		model.FeeToleranceAbs,
		model.FeeTolerancePct,
		model.FeeNormalization,
		model.AutoMatchOnUpload,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert reconciliation context: %w", err)
	}

	return model.ToEntity()
}

// FindByID retrieves a reconciliation context by tenant and context ID.
func (repo *Repository) FindByID(
	ctx stdctx.Context,
	id uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_reconciliation_context_by_id")
	defer span.End()

	tenantID := auth.GetTenantID(ctx)

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (*entities.ReconciliationContext, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+contextColumns+" FROM reconciliation_contexts WHERE tenant_id = $1 AND id = $2",
				tenantID,
				id.String(),
			)

			return scanContext(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find reconciliation context by id", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find reconciliation context by id")
		}

		return nil, fmt.Errorf("find reconciliation context by id: %w", err)
	}

	return result, nil
}

// FindByIDWithTx retrieves a reconciliation context by tenant and context ID using the provided transaction.
func (repo *Repository) FindByIDWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	id uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	tenantID := auth.GetTenantID(ctx)
	row := tx.QueryRowContext(
		ctx,
		"SELECT "+contextColumns+" FROM reconciliation_contexts WHERE tenant_id = $1 AND id = $2",
		tenantID,
		id.String(),
	)

	result, err := scanContext(row)
	if err != nil {
		return nil, fmt.Errorf("find reconciliation context by id with tx: %w", err)
	}

	return result, nil
}

// FindByName retrieves a reconciliation context by tenant and name.
func (repo *Repository) FindByName(
	ctx stdctx.Context,
	name string,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_reconciliation_context_by_name")
	defer span.End()

	tenantID := auth.GetTenantID(ctx)

	result, err := common.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe common.QueryExecutor) (*entities.ReconciliationContext, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+contextColumns+" FROM reconciliation_contexts WHERE tenant_id = $1 AND name = $2",
				tenantID,
				name,
			)

			return scanContext(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		libOpentelemetry.HandleSpanError(
			span,
			"failed to find reconciliation context by name",
			err,
		)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find reconciliation context by name")

		return nil, fmt.Errorf("find reconciliation context by name: %w", err)
	}

	return result, nil
}

// FindAll retrieves all reconciliation contexts for a tenant with optional filters using cursor-based pagination.
func (repo *Repository) FindAll(
	ctx stdctx.Context,
	cursor string,
	limit int,
	contextType *value_objects.ContextType,
	status *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.find_all_reconciliation_contexts")
	defer span.End()

	tenantID := auth.GetTenantID(ctx)

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	decodedCursor, err := decodeCursorParam(cursor)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	var pagination libHTTP.CursorPagination

	result, err := common.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (contexts []*entities.ReconciliationContext, err error) {
			findAll := buildContextQuery(tenantID, contextType, status)

			orderDirection := libHTTP.ValidateSortDirection("ASC")

			operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(orderDirection, decodedCursor.Direction)
			if dirErr != nil {
				return nil, fmt.Errorf("cursor direction rules: %w", dirErr)
			}

			if decodedCursor.ID != "" {
				findAll = findAll.Where(squirrel.Expr("id "+operator+" ?", decodedCursor.ID))
			}

			findAll = findAll.
				OrderBy("id " + effectiveOrder).
				Limit(pgcommon.SafeIntToUint64(limit) + 1)

			contexts, err = executeContextQuery(ctx, tx, findAll, limit)
			if err != nil {
				return nil, err
			}

			contexts, pagination, err = processPaginatedContexts(
				contexts, cursor, decodedCursor, limit,
			)
			if err != nil {
				return nil, err
			}

			return contexts, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation contexts", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list reconciliation contexts")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("find all reconciliation contexts: %w", err)
	}

	return result, pagination, nil
}

// Update modifies an existing reconciliation context.
func (repo *Repository) Update(
	ctx stdctx.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_reconciliation_context")
	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.ReconciliationContext, error) {
			return repo.executeUpdate(ctx, tx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("update reconciliation context: %w", err)
			libOpentelemetry.HandleSpanError(
				span,
				"failed to update reconciliation context",
				wrappedErr,
			)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update reconciliation context")

			return nil, wrappedErr
		}

		return nil, fmt.Errorf("update reconciliation context: %w", err)
	}

	return result, nil
}

// UpdateWithTx modifies an existing reconciliation context using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if entity == nil {
		return nil, ErrContextEntityRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.update_reconciliation_context_with_tx")
	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.ReconciliationContext, error) {
			return repo.executeUpdate(ctx, innerTx, entity)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("update reconciliation context: %w", err)
			libOpentelemetry.HandleSpanError(
				span,
				"failed to update reconciliation context",
				wrappedErr,
			)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to update reconciliation context")

			return nil, wrappedErr
		}

		return nil, fmt.Errorf("update reconciliation context: %w", err)
	}

	return result, nil
}

// executeUpdate performs the actual context update within a transaction.
func (repo *Repository) executeUpdate(
	ctx stdctx.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	entity.UpdatedAt = time.Now().UTC()

	model, err := NewContextPostgreSQLModel(entity)
	if err != nil {
		return nil, fmt.Errorf("create context model: %w", err)
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE reconciliation_contexts SET name = $1, type = $2, interval = $3, status = $4, fee_tolerance_abs = $5, fee_tolerance_pct = $6, fee_normalization = $7, auto_match_on_upload = $8, updated_at = $9
		WHERE tenant_id = $10 AND id = $11`,
		model.Name,
		model.Type,
		model.Interval,
		model.Status,
		model.FeeToleranceAbs,
		model.FeeTolerancePct,
		model.FeeNormalization,
		model.AutoMatchOnUpload,
		model.UpdatedAt,
		model.TenantID,
		model.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("update reconciliation context: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return nil, sql.ErrNoRows
	}

	return model.ToEntity()
}

// Delete removes a reconciliation context from the database.
func (repo *Repository) Delete(ctx stdctx.Context, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_reconciliation_context")
	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (bool, error) {
		return repo.executeDelete(ctx, tx, id)
	})
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("delete reconciliation context: %w", err)
			libOpentelemetry.HandleSpanError(
				span,
				"failed to delete reconciliation context",
				wrappedErr,
			)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete reconciliation context")

			return wrappedErr
		}

		return fmt.Errorf("delete reconciliation context: %w", err)
	}

	return nil
}

// DeleteWithTx removes a reconciliation context using the provided transaction.
func (repo *Repository) DeleteWithTx(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.delete_reconciliation_context_with_tx")
	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (bool, error) {
			return repo.executeDelete(ctx, innerTx, id)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			wrappedErr := fmt.Errorf("delete reconciliation context: %w", err)
			libOpentelemetry.HandleSpanError(
				span,
				"failed to delete reconciliation context",
				wrappedErr,
			)

			logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to delete reconciliation context")

			return wrappedErr
		}

		return fmt.Errorf("delete reconciliation context: %w", err)
	}

	return nil
}

// executeDelete performs the actual context deletion within a transaction.
func (repo *Repository) executeDelete(ctx stdctx.Context, tx *sql.Tx, id uuid.UUID) (bool, error) {
	tenantID := auth.GetTenantID(ctx)

	result, err := tx.ExecContext(
		ctx,
		"DELETE FROM reconciliation_contexts WHERE tenant_id = $1 AND id = $2",
		tenantID,
		id.String(),
	)
	if err != nil {
		return false, fmt.Errorf("delete reconciliation context: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return false, sql.ErrNoRows
	}

	return true, nil
}

// Count returns the total number of reconciliation contexts for a tenant.
func (repo *Repository) Count(ctx stdctx.Context) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "postgres.count_reconciliation_contexts")
	defer span.End()

	tenantID := auth.GetTenantID(ctx)

	result, err := common.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (int64, error) {
		row := tx.QueryRowContext(
			ctx,
			"SELECT COUNT(1) FROM reconciliation_contexts WHERE tenant_id = $1",
			tenantID,
		)

		var count int64

		if err := row.Scan(&count); err != nil {
			return 0, err
		}

		return count, nil
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to count reconciliation contexts", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to count reconciliation contexts")

		return 0, fmt.Errorf("count reconciliation contexts: %w", err)
	}

	return result, nil
}

func scanContext(
	scanner interface{ Scan(dest ...any) error },
) (*entities.ReconciliationContext, error) {
	var model ContextPostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.TenantID,
		&model.Name,
		&model.Type,
		&model.Interval,
		&model.Status,
		&model.FeeToleranceAbs,
		&model.FeeTolerancePct,
		&model.FeeNormalization,
		&model.AutoMatchOnUpload,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}

// decodeCursorParam decodes a cursor string into a Cursor struct.
// Returns a default cursor pointing forward if the cursor is empty.
func decodeCursorParam(cursor string) (libHTTP.Cursor, error) {
	if cursor == "" {
		return libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext}, nil
	}

	parsedCursor, err := libHTTP.DecodeCursor(cursor)
	if err != nil {
		return libHTTP.Cursor{}, fmt.Errorf("%w: %w", libHTTP.ErrInvalidCursor, err)
	}

	return parsedCursor, nil
}

// buildContextQuery creates the base query for listing contexts with optional filters.
func buildContextQuery(
	tenantID string,
	contextType *value_objects.ContextType,
	status *value_objects.ContextStatus,
) squirrel.SelectBuilder {
	query := squirrel.Select(strings.Split(contextColumns, ", ")...).
		From("reconciliation_contexts").
		Where(squirrel.Eq{"tenant_id": tenantID}).
		PlaceholderFormat(squirrel.Dollar)

	if contextType != nil {
		query = query.Where(squirrel.Eq{"type": contextType.String()})
	}

	if status != nil {
		query = query.Where(squirrel.Eq{"status": status.String()})
	}

	return query
}

// executeContextQuery executes the query and scans results into context entities.
func executeContextQuery(
	ctx stdctx.Context,
	tx *sql.Tx,
	queryBuilder squirrel.SelectBuilder,
	limit int,
) ([]*entities.ReconciliationContext, error) {
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build list contexts query: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	contexts := make([]*entities.ReconciliationContext, 0, limit)

	for rows.Next() {
		contextEntity, scanErr := scanContext(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		contexts = append(contexts, contextEntity)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return contexts, nil
}

// processPaginatedContexts applies pagination logic to the context results.
func processPaginatedContexts(
	contexts []*entities.ReconciliationContext,
	cursor string,
	decodedCursor libHTTP.Cursor,
	limit int,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	hasPagination := len(contexts) > limit
	isFirstPage := cursor == "" || (!hasPagination && decodedCursor.Direction == libHTTP.CursorDirectionPrev)

	contexts = libHTTP.PaginateRecords(
		isFirstPage,
		hasPagination,
		decodedCursor.Direction,
		contexts,
		limit,
	)

	var pagination libHTTP.CursorPagination

	if len(contexts) > 0 {
		page, err := libHTTP.CalculateCursor(
			isFirstPage,
			hasPagination,
			decodedCursor.Direction,
			contexts[0].ID.String(),
			contexts[len(contexts)-1].ID.String(),
		)
		if err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("calculate cursor: %w", err)
		}

		pagination = page
	}

	return contexts, pagination, nil
}
