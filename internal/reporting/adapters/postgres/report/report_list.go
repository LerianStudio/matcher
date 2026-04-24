// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package report

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// ListMatched retrieves matched transactions based on filter criteria.
func (repo *Repository) ListMatched(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]*entities.MatchedItem, libHTTP.CursorPagination, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_matched")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (matchedListResult, error) {
			return repo.listMatchedQuery(ctx, qe, filter)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list matched transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list matched items", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list matched items", wrappedErr, runtime.IsProductionMode())

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result.items, result.pagination, nil
}

// ListUnmatched retrieves unmatched transactions based on filter criteria.
func (repo *Repository) ListUnmatched(
	ctx context.Context,
	filter entities.ReportFilter,
) ([]*entities.UnmatchedItem, libHTTP.CursorPagination, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, libHTTP.CursorPagination{}, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_unmatched")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (unmatchedListResult, error) {
			return repo.listUnmatchedQuery(ctx, qe, filter)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list unmatched transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list unmatched items", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list unmatched items", wrappedErr, runtime.IsProductionMode())

		return nil, libHTTP.CursorPagination{}, wrappedErr
	}

	return result.items, result.pagination, nil
}

func (repo *Repository) listMatchedQuery(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter entities.ReportFilter,
) (matchedListResult, error) {
	args, err := buildPaginationArgs(filter)
	if err != nil {
		return matchedListResult{}, err
	}

	findAll := squirrel.Select(
		"t.id",
		"mg.id",
		"t.source_id",
		"mi.allocated_amount",
		"mi.allocated_currency",
		"t.date",
	).
		From("match_items mi").
		Join("match_groups mg ON mi.match_group_id = mg.id").
		Join("transactions t ON mi.transaction_id = t.id").
		Where(squirrel.Eq{"mg.context_id": filter.ContextID, "mg.status": matchGroupStatusConfirmed}).
		Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
		Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
		PlaceholderFormat(squirrel.Dollar)

	if filter.SourceID != nil {
		findAll = findAll.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
	}

	operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(args.orderDirection, args.cursor.Direction)
	if dirErr != nil {
		return matchedListResult{}, fmt.Errorf("cursor direction rules: %w", dirErr)
	}

	if args.cursor.ID != "" {
		findAll = findAll.Where(squirrel.Expr("t.id "+operator+" ?", args.cursor.ID))
	}

	findAll = findAll.
		OrderBy("t.id " + effectiveOrder).
		Limit(safeLimitForPage(args.limit) + 1)

	query, queryArgs, err := findAll.ToSql()
	if err != nil {
		return matchedListResult{}, fmt.Errorf("building matched query: %w", err)
	}

	rows, err := qe.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return matchedListResult{}, fmt.Errorf("querying matched items: %w", err)
	}

	defer rows.Close()

	items := make([]*entities.MatchedItem, 0, args.limit+1)

	for rows.Next() {
		item, err := scanMatchedItem(rows)
		if err != nil {
			return matchedListResult{}, fmt.Errorf("scanning matched item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return matchedListResult{}, fmt.Errorf("iterating matched items: %w", err)
	}

	paginatedItems, pagination, err := paginateReportItems(
		filter,
		args,
		items,
		func(item *entities.MatchedItem) string { return item.TransactionID.String() },
	)
	if err != nil {
		return matchedListResult{}, err
	}

	return matchedListResult{items: paginatedItems, pagination: pagination}, nil
}

func (repo *Repository) listUnmatchedQuery(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter entities.ReportFilter,
) (unmatchedListResult, error) {
	args, err := buildPaginationArgs(filter)
	if err != nil {
		return unmatchedListResult{}, err
	}

	findAll := squirrel.Select(
		"t.id",
		"t.source_id",
		"t.amount",
		"t.currency",
		"t.status",
		"t.date",
		"e.id",
		"e.due_at",
	).
		From("transactions t").
		Join("reconciliation_sources rs ON t.source_id = rs.id").
		LeftJoin("exceptions e ON e.transaction_id = t.id").
		Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
		Where(squirrel.NotEq{"t.status": "MATCHED"}).
		Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
		Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
		PlaceholderFormat(squirrel.Dollar)

	if filter.SourceID != nil {
		findAll = findAll.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
	}

	if filter.Status != nil {
		findAll = findAll.Where(squirrel.Eq{"t.status": *filter.Status})
	}

	operator, effectiveOrder, dirErr := libHTTP.CursorDirectionRules(args.orderDirection, args.cursor.Direction)
	if dirErr != nil {
		return unmatchedListResult{}, fmt.Errorf("cursor direction rules: %w", dirErr)
	}

	if args.cursor.ID != "" {
		findAll = findAll.Where(squirrel.Expr("t.id "+operator+" ?", args.cursor.ID))
	}

	findAll = findAll.
		OrderBy("t.id " + effectiveOrder).
		Limit(safeLimitForPage(args.limit) + 1)

	query, queryArgs, err := findAll.ToSql()
	if err != nil {
		return unmatchedListResult{}, fmt.Errorf("building unmatched query: %w", err)
	}

	rows, err := qe.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return unmatchedListResult{}, fmt.Errorf("querying unmatched items: %w", err)
	}

	defer rows.Close()

	items := make([]*entities.UnmatchedItem, 0, args.limit+1)

	for rows.Next() {
		item, err := scanUnmatchedItem(rows)
		if err != nil {
			return unmatchedListResult{}, fmt.Errorf("scanning unmatched item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return unmatchedListResult{}, fmt.Errorf("iterating unmatched items: %w", err)
	}

	paginatedItems, pagination, err := paginateReportItems(
		filter,
		args,
		items,
		func(item *entities.UnmatchedItem) string { return item.TransactionID.String() },
	)
	if err != nil {
		return unmatchedListResult{}, err
	}

	return unmatchedListResult{items: paginatedItems, pagination: pagination}, nil
}
