package dispute

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Repository persists disputes in PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new dispute repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create inserts a new dispute.
func (repo *Repository) Create(
	ctx context.Context,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if disputeEntity == nil {
		return nil, ErrDisputeNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.create")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*dispute.Dispute, error) {
			return repo.executeCreate(ctx, tx, disputeEntity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create dispute: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to create dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new dispute using the provided transaction.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if disputeEntity == nil {
		return nil, ErrDisputeNil
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.create_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*dispute.Dispute, error) {
			return repo.executeCreate(ctx, innerTx, disputeEntity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create dispute: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to create dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual create operation within a transaction.
func (repo *Repository) executeCreate(
	ctx context.Context,
	tx *sql.Tx,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	evidenceJSON, err := json.Marshal(disputeEntity.Evidence)
	if err != nil {
		return nil, fmt.Errorf("marshal evidence: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO disputes (
			id, exception_id, category, state, description,
			opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`,
		disputeEntity.ID.String(),
		disputeEntity.ExceptionID.String(),
		disputeEntity.Category.String(),
		disputeEntity.State.String(),
		disputeEntity.Description,
		disputeEntity.OpenedBy,
		pgcommon.StringPtrToNullString(disputeEntity.Resolution),
		pgcommon.StringPtrToNullString(disputeEntity.ReopenReason),
		evidenceJSON,
		disputeEntity.CreatedAt,
		disputeEntity.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert dispute: %w", err)
	}

	return repo.findByIDExec(ctx, tx, disputeEntity.ID)
}

// FindByID retrieves a dispute by its ID.
func (repo *Repository) FindByID(ctx context.Context, id uuid.UUID) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.find_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*dispute.Dispute, error) {
			return repo.findByIDExec(ctx, qe, id)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDisputeNotFound
		}

		wrappedErr := fmt.Errorf("find dispute by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to find dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// FindByExceptionID retrieves a dispute by its exception ID.
func (repo *Repository) FindByExceptionID(
	ctx context.Context,
	exceptionID uuid.UUID,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.find_by_exception_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*dispute.Dispute, error) {
			row := qe.QueryRowContext(ctx, `
			SELECT id, exception_id, category, state, description,
			       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
			FROM disputes
			WHERE exception_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`, exceptionID.String())

			return scanDispute(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDisputeNotFound
		}

		wrappedErr := fmt.Errorf("find dispute by exception id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find dispute by exception", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to find dispute by exception", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// Update updates an existing dispute.
func (repo *Repository) Update(
	ctx context.Context,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if disputeEntity == nil {
		return nil, ErrDisputeNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.update")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*dispute.Dispute, error) {
			return repo.executeUpdate(ctx, tx, disputeEntity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update dispute: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to update dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// UpdateWithTx updates an existing dispute using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if disputeEntity == nil {
		return nil, ErrDisputeNil
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.update_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*dispute.Dispute, error) {
			return repo.executeUpdate(ctx, innerTx, disputeEntity)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update dispute: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update dispute", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to update dispute", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// executeUpdate performs the actual update operation within a transaction.
func (repo *Repository) executeUpdate(
	ctx context.Context,
	tx *sql.Tx,
	disputeEntity *dispute.Dispute,
) (*dispute.Dispute, error) {
	evidenceJSON, err := json.Marshal(disputeEntity.Evidence)
	if err != nil {
		return nil, fmt.Errorf("marshal evidence: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE disputes SET
			category = $2,
			state = $3,
			description = $4,
			opened_by = $5,
			resolution = $6,
			reopen_reason = $7,
			evidence = $8,
			updated_at = $9
		WHERE id = $1
	`,
		disputeEntity.ID.String(),
		disputeEntity.Category.String(),
		disputeEntity.State.String(),
		disputeEntity.Description,
		disputeEntity.OpenedBy,
		pgcommon.StringPtrToNullString(disputeEntity.Resolution),
		pgcommon.StringPtrToNullString(disputeEntity.ReopenReason),
		evidenceJSON,
		disputeEntity.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("update dispute: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return nil, ErrDisputeNotFound
	}

	return repo.findByIDExec(ctx, tx, disputeEntity.ID)
}

func (repo *Repository) findByIDExec(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	id uuid.UUID,
) (*dispute.Dispute, error) {
	row := qe.QueryRowContext(ctx, `
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`, id.String())

	return scanDispute(row)
}

// List retrieves disputes with optional filters and cursor pagination.
func (repo *Repository) List(
	ctx context.Context,
	filter repositories.DisputeFilter,
	cursor repositories.CursorFilter,
) ([]*dispute.Dispute, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.list")

	defer span.End()

	params := prepareDisputeListParams(cursor)

	result, pagination, err := repo.executeDisputeListQuery(ctx, filter, cursor, params, logger)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to list disputes: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list disputes", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list disputes", wrappedErr, runtime.IsProductionMode())

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result, pagination, nil
}

func (repo *Repository) executeDisputeListQuery(
	ctx context.Context,
	filter repositories.DisputeFilter,
	cursor repositories.CursorFilter,
	params disputeListQueryParams,
	logger libLog.Logger,
) ([]*dispute.Dispute, libHTTP.CursorPagination, error) {
	var pagination libHTTP.CursorPagination

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*dispute.Dispute, error) {
			disputes, cursorDirection, err := queryDisputes(ctx, qe, filter, params, logger)
			if err != nil {
				return nil, err
			}

			hasPagination := len(disputes) > params.limit
			isFirstPage := cursor.Cursor == "" ||
				(!hasPagination && cursorDirection == libHTTP.CursorDirectionPrev)

			disputes = libHTTP.PaginateRecords(
				isFirstPage,
				hasPagination,
				cursorDirection,
				disputes,
				params.limit,
			)

			pagination, err = calculateDisputePagination(disputes, isFirstPage, hasPagination, params, cursorDirection)
			if err != nil {
				return nil, fmt.Errorf("calculate dispute pagination: %w", err)
			}

			return disputes, nil
		},
	)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("execute dispute list query: %w", err)
	}

	return result, pagination, nil
}

func queryDisputes(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter repositories.DisputeFilter,
	params disputeListQueryParams,
	logger libLog.Logger,
) ([]*dispute.Dispute, string, error) {
	query, args, cursorDirection, err := buildDisputeListQuery(filter, params)
	if err != nil {
		return nil, "", fmt.Errorf("failed to build SQL: %w", err)
	}

	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to query disputes: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close rows: %v", closeErr))
		}
	}()

	disputes, err := scanAllDisputeRows(rows, params.limit+1)
	if err != nil {
		return nil, "", err
	}

	return disputes, cursorDirection, nil
}

func scanAllDisputeRows(rows *sql.Rows, capacity int) ([]*dispute.Dispute, error) {
	disputes := make([]*dispute.Dispute, 0, capacity)

	for rows.Next() {
		entity, err := scanDisputeRows(rows)
		if err != nil {
			return nil, err
		}

		disputes = append(disputes, entity)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate rows: %w", err)
	}

	return disputes, nil
}

type disputeListQueryParams struct {
	limit          int
	sortColumn     string
	orderDirection string
	useIDCursor    bool
	cursorStr      string
}

func prepareDisputeListParams(cursor repositories.CursorFilter) disputeListQueryParams {
	orderDirection := libHTTP.ValidateSortDirection(cursor.SortOrder)
	limit := libHTTP.ValidateLimit(cursor.Limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	sortColumn := normalizeDisputeSortColumn(cursor.SortBy)

	return disputeListQueryParams{
		limit:          limit,
		sortColumn:     sortColumn,
		orderDirection: orderDirection,
		useIDCursor:    sortColumn == "id",
		cursorStr:      cursor.Cursor,
	}
}

var allowedDisputeSortColumns = map[string]bool{
	"id":         true,
	"created_at": true,
	"updated_at": true,
	"state":      true,
	"category":   true,
}

func normalizeDisputeSortColumn(column string) string {
	if column == "" {
		return "id"
	}

	if allowedDisputeSortColumns[column] {
		return column
	}

	return "id"
}

func buildDisputeListQuery(
	filter repositories.DisputeFilter,
	params disputeListQueryParams,
) (string, []any, string, error) {
	disputeColumns := []string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence",
		"created_at", "updated_at",
	}

	findAll := squirrel.Select(disputeColumns...).
		From("disputes").
		PlaceholderFormat(squirrel.Dollar)

	findAll = applyDisputeFilters(findAll, filter)

	var (
		cursorDirection string
		cursorErr       error
	)

	if params.useIDCursor {
		findAll, _, cursorDirection, cursorErr = pgcommon.ApplyIDCursorPagination(
			findAll, params.cursorStr, params.orderDirection, params.limit,
		)
	} else {
		findAll, _, cursorDirection, cursorErr = pgcommon.ApplySortCursorPagination(
			findAll, params.cursorStr, params.sortColumn, params.orderDirection, "disputes", params.limit,
		)
	}

	if cursorErr != nil {
		return "", nil, "", fmt.Errorf("apply cursor pagination: %w", cursorErr)
	}

	query, args, sqlErr := findAll.ToSql()
	if sqlErr != nil {
		return "", nil, "", fmt.Errorf("build SQL: %w", sqlErr)
	}

	return query, args, cursorDirection, nil
}

func applyDisputeFilters(
	query squirrel.SelectBuilder,
	filter repositories.DisputeFilter,
) squirrel.SelectBuilder {
	if filter.State != nil {
		query = query.Where(squirrel.Eq{"state": filter.State.String()})
	}

	if filter.Category != nil {
		query = query.Where(squirrel.Eq{"category": filter.Category.String()})
	}

	if filter.DateFrom != nil {
		query = query.Where(squirrel.GtOrEq{"created_at": *filter.DateFrom})
	}

	if filter.DateTo != nil {
		query = query.Where(squirrel.LtOrEq{"created_at": *filter.DateTo})
	}

	return query
}

func calculateDisputePagination(
	disputes []*dispute.Dispute,
	isFirstPage, hasPagination bool,
	params disputeListQueryParams,
	cursorDirection string,
) (libHTTP.CursorPagination, error) {
	if len(disputes) == 0 {
		return libHTTP.CursorPagination{}, nil
	}

	first, last := disputes[0], disputes[len(disputes)-1]
	if err := pgcommon.ValidateSortCursorBoundaries(first, last); err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("validate dispute pagination boundaries: %w", err)
	}

	if params.useIDCursor {
		pagination, err := libHTTP.CalculateCursor(
			isFirstPage,
			hasPagination,
			cursorDirection,
			first.ID.String(),
			last.ID.String(),
		)
		if err != nil {
			return libHTTP.CursorPagination{}, fmt.Errorf("calculate ID cursor pagination: %w", err)
		}

		return pagination, nil
	}

	return calculateDisputeSortPagination(disputes, isFirstPage, hasPagination, params, cursorDirection, libHTTP.CalculateSortCursorPagination)
}

func calculateDisputeSortPagination(
	disputes []*dispute.Dispute,
	isFirstPage, hasPagination bool,
	params disputeListQueryParams,
	cursorDirection string,
	calculateSortCursor pgcommon.SortCursorCalculator,
) (libHTTP.CursorPagination, error) {
	if len(disputes) == 0 {
		return libHTTP.CursorPagination{}, nil
	}

	first, last := disputes[0], disputes[len(disputes)-1]
	if err := pgcommon.ValidateSortCursorBoundaries(first, last); err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("validate dispute pagination boundaries: %w", err)
	}

	pointsNext := cursorDirection == libHTTP.CursorDirectionNext

	pagination, err := pgcommon.CalculateSortCursorPaginationWrapped(
		isFirstPage, hasPagination, pointsNext,
		params.sortColumn,
		disputeSortValue(first, params.sortColumn), first.ID.String(),
		disputeSortValue(last, params.sortColumn), last.ID.String(),
		calculateSortCursor,
		"calculate sort cursor pagination",
	)
	if err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("calculate dispute sort cursor pagination: %w", err)
	}

	return pagination, nil
}

func disputeSortValue(disp *dispute.Dispute, column string) string {
	if disp == nil {
		return ""
	}

	switch column {
	case "created_at":
		return disp.CreatedAt.UTC().Format(time.RFC3339Nano)
	case "updated_at":
		return disp.UpdatedAt.UTC().Format(time.RFC3339Nano)
	case "state":
		return disp.State.String()
	case "category":
		return disp.Category.String()
	default:
		return disp.ID.String()
	}
}

// ExistsForTenant checks if a dispute with the given ID exists in the current tenant's schema.
// This method uses tenant-scoped read queries for schema isolation.
func (repo *Repository) ExistsForTenant(ctx context.Context, id uuid.UUID) (bool, error) {
	if repo == nil || repo.provider == nil {
		return false, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.dispute.exists_for_tenant")

	defer span.End()

	exists, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (bool, error) {
			var found bool

			err := qe.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM disputes WHERE id = $1)`, id.String()).
				Scan(&found)
			if err != nil {
				return false, fmt.Errorf("check dispute existence: %w", err)
			}

			return found, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to check dispute existence: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to check dispute existence", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to check dispute existence", wrappedErr, runtime.IsProductionMode())

		return false, wrappedErr
	}

	return exists, nil
}

var _ repositories.DisputeRepository = (*Repository)(nil)
