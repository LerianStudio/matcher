// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package exception

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

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

		libLog.SafeError(logger, ctx, "failed to list exceptions", wrappedErr, runtime.IsProductionMode())

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result, pagination, nil
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
