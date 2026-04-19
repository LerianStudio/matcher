package exception

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

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Repository persists exceptions in PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new exception repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// FindByID retrieves an exception by its ID.
func (repo *Repository) FindByID(ctx context.Context, id uuid.UUID) (*entities.Exception, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.find_by_id")

	defer span.End()

	exception, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.Exception, error) {
			row := qe.QueryRowContext(ctx, `
			SELECT id, transaction_id, severity, status, external_system, external_issue_id,
			       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
			       reason, version, created_at, updated_at
			FROM exceptions
			WHERE id = $1
		`, id.String())

			return scanException(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, entities.ErrExceptionNotFound
		}

		wrappedErr := fmt.Errorf("failed to find exception: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to find exception", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to find exception: %v", wrappedErr))

		return nil, wrappedErr
	}

	return exception, nil
}

type listQueryParams struct {
	limit          int
	sortColumn     string
	orderDirection string
	useIDCursor    bool
	cursorStr      string
}

func prepareListParams(cursor repositories.CursorFilter) listQueryParams {
	orderDirection := libHTTP.ValidateSortDirection(cursor.SortOrder)
	limit := libHTTP.ValidateLimit(cursor.Limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	sortColumn := normalizeSortColumn(cursor.SortBy)

	return listQueryParams{
		limit:          limit,
		sortColumn:     sortColumn,
		orderDirection: orderDirection,
		useIDCursor:    sortColumn == "id",
		cursorStr:      cursor.Cursor,
	}
}

func buildListQuery(
	filter repositories.ExceptionFilter,
	params listQueryParams,
) (string, []any, string, error) {
	findAll := squirrel.Select(strings.Split(columns, ", ")...).
		From("exceptions").
		PlaceholderFormat(squirrel.Dollar)

	findAll = applyFilters(findAll, filter)

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
			findAll, params.cursorStr, params.sortColumn, params.orderDirection, "exceptions", params.limit,
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

// List retrieves exceptions with optional filters and cursor pagination.
func (repo *Repository) List(
	ctx context.Context,
	filter repositories.ExceptionFilter,
	cursor repositories.CursorFilter,
) ([]*entities.Exception, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.list")

	defer span.End()

	params := prepareListParams(cursor)

	result, pagination, err := repo.executeListQuery(ctx, filter, cursor, params, logger)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to list exceptions: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list exceptions", wrappedErr)

		logError(ctx, logger, "failed to list exceptions: %v", wrappedErr)

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result, pagination, nil
}

func logError(ctx context.Context, logger libLog.Logger, format string, args ...any) {
	logger.Log(ctx, libLog.LevelError, fmt.Sprintf(format, args...))
}

func (repo *Repository) executeListQuery(
	ctx context.Context,
	filter repositories.ExceptionFilter,
	cursor repositories.CursorFilter,
	params listQueryParams,
	logger libLog.Logger,
) ([]*entities.Exception, libHTTP.CursorPagination, error) {
	var pagination libHTTP.CursorPagination

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.Exception, error) {
			exceptions, cursorDirection, err := queryExceptions(ctx, qe, filter, params, logger)
			if err != nil {
				return nil, err
			}

			hasPagination := len(exceptions) > params.limit
			isFirstPage := cursor.Cursor == "" ||
				(!hasPagination && cursorDirection == libHTTP.CursorDirectionPrev)

			exceptions = libHTTP.PaginateRecords(
				isFirstPage,
				hasPagination,
				cursorDirection,
				exceptions,
				params.limit,
			)

			var calcErr error

			pagination, calcErr = calculatePagination(exceptions, isFirstPage, hasPagination, params, cursorDirection)
			if calcErr != nil {
				return nil, fmt.Errorf("calculate pagination: %w", calcErr)
			}

			return exceptions, nil
		},
	)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("execute list query: %w", err)
	}

	return result, pagination, nil
}

func queryExceptions(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter repositories.ExceptionFilter,
	params listQueryParams,
	logger libLog.Logger,
) ([]*entities.Exception, string, error) {
	query, args, cursorDirection, err := buildListQuery(filter, params)
	if err != nil {
		return nil, "", fmt.Errorf("failed to build SQL: %w", err)
	}

	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to query exceptions: %w", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close rows: %v", closeErr))
		}
	}()

	exceptions, err := scanAllRows(rows, params.limit+1)
	if err != nil {
		return nil, "", err
	}

	return exceptions, cursorDirection, nil
}

func calculatePagination(
	exceptions []*entities.Exception,
	isFirstPage, hasPagination bool,
	params listQueryParams,
	cursorDirection string,
) (libHTTP.CursorPagination, error) {
	if len(exceptions) == 0 {
		return libHTTP.CursorPagination{}, nil
	}

	first, last := exceptions[0], exceptions[len(exceptions)-1]
	if err := pgcommon.ValidateSortCursorBoundaries(first, last); err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("validate pagination boundaries: %w", err)
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
			return libHTTP.CursorPagination{}, fmt.Errorf("calculate cursor: %w", err)
		}

		return pagination, nil
	}

	pointsNext := cursorDirection == libHTTP.CursorDirectionNext

	return calculateExceptionSortPagination(
		isFirstPage,
		hasPagination,
		pointsNext,
		params.sortColumn,
		exceptionSortValue(first, params.sortColumn),
		first.ID.String(),
		exceptionSortValue(last, params.sortColumn),
		last.ID.String(),
		libHTTP.CalculateSortCursorPagination,
	)
}

func calculateExceptionSortPagination(
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
		"calculate sort cursor pagination",
	)
	if err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("calculate exception sort cursor pagination: %w", err)
	}

	return pagination, nil
}

func exceptionSortValue(ex *entities.Exception, column string) string {
	if ex == nil {
		return ""
	}

	switch column {
	case "created_at":
		return ex.CreatedAt.UTC().Format(time.RFC3339Nano)
	case "updated_at":
		return ex.UpdatedAt.UTC().Format(time.RFC3339Nano)
	case "severity":
		return ex.Severity.String()
	case "status":
		return ex.Status.String()
	default:
		return ex.ID.String()
	}
}

func scanAllRows(rows *sql.Rows, capacity int) ([]*entities.Exception, error) {
	exceptions := make([]*entities.Exception, 0, capacity)

	for rows.Next() {
		entity, err := scanRows(rows)
		if err != nil {
			return nil, err
		}

		exceptions = append(exceptions, entity)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate rows: %w", err)
	}

	return exceptions, nil
}

func applyFilters(
	query squirrel.SelectBuilder,
	filter repositories.ExceptionFilter,
) squirrel.SelectBuilder {
	if filter.Status != nil {
		query = query.Where(squirrel.Eq{"status": filter.Status.String()})
	}

	if filter.Severity != nil {
		query = query.Where(squirrel.Eq{"severity": filter.Severity.String()})
	}

	if filter.AssignedTo != nil {
		query = query.Where(squirrel.Eq{"assigned_to": *filter.AssignedTo})
	}

	if filter.ExternalSystem != nil {
		query = query.Where(squirrel.Eq{"external_system": *filter.ExternalSystem})
	}

	if filter.DateFrom != nil {
		query = query.Where(squirrel.GtOrEq{"created_at": *filter.DateFrom})
	}

	if filter.DateTo != nil {
		query = query.Where(squirrel.LtOrEq{"created_at": *filter.DateTo})
	}

	return query
}

func normalizeSortColumn(column string) string {
	return libHTTP.ValidateSortColumn(column, allowedSortColumns, "id")
}

// Update updates an existing exception.
func (repo *Repository) Update(
	ctx context.Context,
	exception *entities.Exception,
) (*entities.Exception, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if exception == nil {
		return nil, entities.ErrExceptionNil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.update")

	defer span.End()

	updated, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.Exception, error) {
			return repo.executeUpdate(ctx, tx, exception)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to update exception: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update exception", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to update exception: %v", wrappedErr))

		return nil, wrappedErr
	}

	return updated, nil
}

// UpdateWithTx updates an existing exception using the provided transaction.
func (repo *Repository) UpdateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	exception *entities.Exception,
) (*entities.Exception, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	if exception == nil {
		return nil, entities.ErrExceptionNil
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.update_with_tx")

	defer span.End()

	updated, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.Exception, error) {
			return repo.executeUpdate(ctx, innerTx, exception)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to update exception: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update exception", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to update exception: %v", wrappedErr))

		return nil, wrappedErr
	}

	return updated, nil
}

// executeUpdate performs the actual update operation within a transaction.
func (repo *Repository) executeUpdate(
	ctx context.Context,
	tx *sql.Tx,
	exception *entities.Exception,
) (*entities.Exception, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE exceptions SET
			severity = $2,
			status = $3,
			external_system = $4,
			external_issue_id = $5,
			assigned_to = $6,
			due_at = $7,
			resolution_notes = $8,
			resolution_type = $9,
			resolution_reason = $10,
			reason = $11,
			version = version + 1,
			updated_at = $12
		WHERE id = $1 AND version = $13
	`,
		exception.ID.String(),
		exception.Severity.String(),
		exception.Status.String(),
		pgcommon.StringPtrToNullString(exception.ExternalSystem),
		pgcommon.StringPtrToNullString(exception.ExternalIssueID),
		pgcommon.StringPtrToNullString(exception.AssignedTo),
		pgcommon.TimePtrToNullTime(exception.DueAt),
		pgcommon.StringPtrToNullString(exception.ResolutionNotes),
		pgcommon.StringPtrToNullString(exception.ResolutionType),
		pgcommon.StringPtrToNullString(exception.ResolutionReason),
		pgcommon.StringPtrToNullString(exception.Reason),
		time.Now().UTC(),
		exception.Version,
	)
	if err != nil {
		return nil, fmt.Errorf("update exception: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)", exception.ID.String()).Scan(&exists); err != nil {
			return nil, fmt.Errorf("check exception existence: %w", err)
		}

		if exists {
			return nil, ErrConcurrentModification
		}

		return nil, entities.ErrExceptionNotFound
	}

	row := tx.QueryRowContext(ctx, `
		SELECT id, transaction_id, severity, status, external_system, external_issue_id,
		       assigned_to, due_at, resolution_notes, resolution_type, resolution_reason,
		       reason, version, created_at, updated_at
		FROM exceptions
		WHERE id = $1
	`, exception.ID.String())

	return scanException(row)
}

// ExistsForTenant checks if an exception with the given ID exists in the current tenant's schema.
// This method uses tenant-scoped read queries for schema isolation.
func (repo *Repository) ExistsForTenant(ctx context.Context, id uuid.UUID) (bool, error) {
	if repo == nil || repo.provider == nil {
		return false, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exception.exists_for_tenant")

	defer span.End()

	exists, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (bool, error) {
			var found bool

			err := qe.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM exceptions WHERE id = $1)`, id.String()).
				Scan(&found)
			if err != nil {
				return false, fmt.Errorf("check exception existence: %w", err)
			}

			return found, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to check exception existence: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to check exception existence", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to check exception existence: %v", wrappedErr))

		return false, wrappedErr
	}

	return exists, nil
}

var _ repositories.ExceptionRepository = (*Repository)(nil)
