// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dispute

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

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
