// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package report

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

func (repo *Repository) validateVarianceFilter(filter *entities.VarianceReportFilter) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return ErrContextIDRequired
	}

	if filter.Limit < 0 {
		return ErrLimitMustBePositive
	}

	if filter.Limit == 0 {
		filter.Limit = defaultLimit
	}

	if filter.Limit > maxLimit {
		return ErrLimitExceedsMaximum
	}

	return nil
}

// GetVarianceReport retrieves variance data aggregated by source, currency, and fee schedule.
func (repo *Repository) GetVarianceReport(
	ctx context.Context,
	filter entities.VarianceReportFilter,
) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error) {
	if err := repo.validateVarianceFilter(&filter); err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_variance_report")

	defer span.End()

	pagArgs, err := repo.buildVariancePaginationArgs(filter)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.VarianceReportRow, error) {
			query, err := buildVariancePaginatedQuery(filter, pagArgs)
			if err != nil {
				return nil, err
			}

			sqlStr, args, err := query.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building variance report query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlStr, args...)
			if err != nil {
				return nil, fmt.Errorf("querying variance report: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.VarianceReportRow, 0, pagArgs.limit+1)

			for rows.Next() {
				item, err := scanVarianceRow(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning variance row: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating variance rows: %w", err)
			}

			return items, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get variance report transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get variance report", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get variance report", wrappedErr, runtime.IsProductionMode())

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	items, pagination, err := paginateVarianceItems(filter, pagArgs, result)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	return items, pagination, nil
}

// buildVarianceSelectBuilder constructs the base squirrel query for fee variance
// reports. It aggregates per source, currency, and fee schedule while preserving
// the historical fee schedule name snapshot stored on each variance row.
//
// The returned builder includes GROUP BY and ORDER BY clauses but no LIMIT or
// cursor filtering -- callers add those according to their pagination strategy.
//
// Columns returned:
//
//	t.source_id, fv.currency, fv.fee_schedule_id, fee_schedule_name,
//	total_expected, total_actual, net_variance
func buildVarianceSelectBuilder(
	contextID uuid.UUID,
	dateFrom, dateTo any,
	sourceID *uuid.UUID,
	orderDirection string,
) squirrel.SelectBuilder {
	query := squirrel.Select(
		"t.source_id",
		"fv.currency",
		"fv.fee_schedule_id",
		"COALESCE(MAX(fs.name), MAX(fv.fee_schedule_name_snapshot), '') AS fee_schedule_name",
		"COALESCE(SUM(fv.expected_fee_amount), 0) AS total_expected",
		"COALESCE(SUM(fv.actual_fee_amount), 0) AS total_actual",
		"COALESCE(SUM(fv.delta), 0) AS net_variance",
	).
		From("match_fee_variances fv").
		Join("transactions t ON t.id = fv.transaction_id").
		LeftJoin("fee_schedules fs ON fs.id = fv.fee_schedule_id").
		Where(squirrel.Eq{"fv.context_id": contextID}).
		Where(squirrel.Expr("fv.created_at >= ?", dateFrom)).
		Where(squirrel.Expr("fv.created_at <= ?", dateTo)).
		GroupBy("t.source_id", "fv.currency", "fv.fee_schedule_id").
		OrderBy(
			"t.source_id "+orderDirection,
			"fv.currency "+orderDirection,
			"fv.fee_schedule_id "+orderDirection,
		).
		PlaceholderFormat(squirrel.Dollar)

	if sourceID != nil {
		query = query.Where(squirrel.Eq{"t.source_id": *sourceID})
	}

	return query
}

func buildVariancePaginatedQuery(
	filter entities.VarianceReportFilter,
	args paginationArgs,
) (squirrel.SelectBuilder, error) {
	operator, effectiveOrder, err := libHTTP.CursorDirectionRules(args.orderDirection, args.cursor.Direction)
	if err != nil {
		return squirrel.SelectBuilder{}, fmt.Errorf("cursor direction rules: %w", err)
	}

	query := buildVarianceSelectBuilder(
		filter.ContextID,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
		effectiveOrder,
	)

	if args.cursor.ID != "" {
		cursorSourceID, cursorCurrency, cursorFeeScheduleID, parseErr := parseVarianceCursorParts(args.cursor.ID)
		if parseErr != nil {
			return squirrel.SelectBuilder{}, fmt.Errorf("variance cursor: %w", parseErr)
		}

		query = query.Where(
			squirrel.Expr(
				"(t.source_id, fv.currency, fv.fee_schedule_id) "+operator+" (?, ?, ?)",
				cursorSourceID,
				cursorCurrency,
				cursorFeeScheduleID,
			),
		)
	}

	query = query.Limit(safeLimitForPage(args.limit + 1))

	return query, nil
}

func (repo *Repository) buildVariancePaginationArgs(
	filter entities.VarianceReportFilter,
) (paginationArgs, error) {
	return buildGenericPaginationArgs(filter.Cursor, filter.SortOrder, filter.Limit)
}

func paginateVarianceItems(
	filter entities.VarianceReportFilter,
	args paginationArgs,
	items []*entities.VarianceReportRow,
) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error) {
	hasPagination := len(items) > args.limit
	isFirstPage := filter.Cursor == "" || (!hasPagination && args.cursor.Direction == libHTTP.CursorDirectionPrev)

	items = libHTTP.PaginateRecords(
		isFirstPage,
		hasPagination,
		args.cursor.Direction,
		items,
		args.limit,
	)
	if len(items) == 0 {
		return items, libHTTP.CursorPagination{}, nil
	}

	getVarianceRowKey := func(row *entities.VarianceReportRow) string {
		return row.SourceID.String() + ":" + row.Currency + ":" + row.FeeScheduleID.String()
	}

	pagination, err := libHTTP.CalculateCursor(
		isFirstPage,
		hasPagination,
		args.cursor.Direction,
		getVarianceRowKey(items[0]),
		getVarianceRowKey(items[len(items)-1]),
	)
	if err != nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("failed to calculate cursor: %w", err)
	}

	return items, pagination, nil
}

func scanVarianceRow(
	scanner interface{ Scan(dest ...any) error },
) (*entities.VarianceReportRow, error) {
	var sourceID uuid.UUID

	var feeScheduleID uuid.UUID

	var currency, feeScheduleName string

	var totalExpected, totalActual, netVariance decimal.Decimal

	if err := scanner.Scan(
		&sourceID,
		&currency,
		&feeScheduleID,
		&feeScheduleName,
		&totalExpected,
		&totalActual,
		&netVariance,
	); err != nil {
		return nil, fmt.Errorf("scanning variance row: %w", err)
	}

	return entities.BuildVarianceRow(
		sourceID,
		currency,
		feeScheduleID,
		feeScheduleName,
		totalExpected,
		totalActual,
		netVariance,
	), nil
}
