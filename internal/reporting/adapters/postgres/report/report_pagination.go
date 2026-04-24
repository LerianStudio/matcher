// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package report

import (
	"context"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// ListMatchedPage retrieves a page of matched transactions for streaming export.
//
//nolint:cyclop // cursor pagination with multiple filter paths
func (repo *Repository) ListMatchedPage(
	ctx context.Context,
	filter entities.ReportFilter,
	afterKey string,
	limit int,
) ([]*entities.MatchedItem, string, error) {
	if repo == nil || repo.provider == nil {
		return nil, "", ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, "", ErrContextIDRequired
	}

	limit = normalizeLimit(limit)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_matched_page")

	defer span.End()

	type pageResult struct {
		items   []*entities.MatchedItem
		nextKey string
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (pageResult, error) {
			query := squirrel.Select(
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
				OrderBy("t.id ASC").
				Limit(safeLimitForPage(limit + 1)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			if afterKey != "" {
				afterID, err := uuid.Parse(afterKey)
				if err != nil {
					return pageResult{}, fmt.Errorf("parsing after key: %w", err)
				}

				query = query.Where(squirrel.Gt{"t.id": afterID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return pageResult{}, fmt.Errorf("building page query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlQuery, args...)
			if err != nil {
				return pageResult{}, fmt.Errorf("querying matched page: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.MatchedItem, 0, limit)

			for rows.Next() {
				item, err := scanMatchedItem(rows)
				if err != nil {
					return pageResult{}, fmt.Errorf("scanning matched item: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return pageResult{}, fmt.Errorf("iterating matched items: %w", err)
			}

			var nextKey string

			if len(items) > limit {
				items = items[:limit]
				nextKey = items[limit-1].TransactionID.String()
			}

			return pageResult{items: items, nextKey: nextKey}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list matched page transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list matched page", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list matched page", wrappedErr, runtime.IsProductionMode())

		return nil, "", wrappedErr
	}

	return result.items, result.nextKey, nil
}

// ListUnmatchedPage retrieves a page of unmatched transactions for streaming export.
//
//nolint:cyclop // cursor pagination with multiple filter paths
func (repo *Repository) ListUnmatchedPage(
	ctx context.Context,
	filter entities.ReportFilter,
	afterKey string,
	limit int,
) ([]*entities.UnmatchedItem, string, error) {
	if repo == nil || repo.provider == nil {
		return nil, "", ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, "", ErrContextIDRequired
	}

	limit = normalizeLimit(limit)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_unmatched_page")

	defer span.End()

	type pageResult struct {
		items   []*entities.UnmatchedItem
		nextKey string
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (pageResult, error) {
			query := squirrel.Select(
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
				LeftJoin("exceptions e ON t.id = e.transaction_id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				Where(squirrel.NotEq{"t.status": "MATCHED"}).
				OrderBy("t.id ASC").
				Limit(safeLimitForPage(limit + 1)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			if filter.Status != nil {
				query = query.Where(squirrel.Eq{"t.status": *filter.Status})
			}

			if afterKey != "" {
				afterID, err := uuid.Parse(afterKey)
				if err != nil {
					return pageResult{}, fmt.Errorf("parsing after key: %w", err)
				}

				query = query.Where(squirrel.Gt{"t.id": afterID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return pageResult{}, fmt.Errorf("building page query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlQuery, args...)
			if err != nil {
				return pageResult{}, fmt.Errorf("querying unmatched page: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.UnmatchedItem, 0, limit)

			for rows.Next() {
				item, err := scanUnmatchedItem(rows)
				if err != nil {
					return pageResult{}, fmt.Errorf("scanning unmatched item: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return pageResult{}, fmt.Errorf("iterating unmatched items: %w", err)
			}

			var nextKey string

			if len(items) > limit {
				items = items[:limit]
				nextKey = items[limit-1].TransactionID.String()
			}

			return pageResult{items: items, nextKey: nextKey}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list unmatched page transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list unmatched page", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list unmatched page", wrappedErr, runtime.IsProductionMode())

		return nil, "", wrappedErr
	}

	return result.items, result.nextKey, nil
}

// varianceCursorFilter holds the parsed cursor state for variance page queries.
type varianceCursorFilter struct {
	query  string
	args   []any
	argIdx int
}

// applyVarianceCursor parses the composite cursor key and appends the
// appropriate WHERE clause and arguments for keyset pagination.
func applyVarianceCursor(afterKey, query string, args []any, argIdx int) (varianceCursorFilter, error) {
	if afterKey == "" {
		return varianceCursorFilter{query: query, args: args, argIdx: argIdx}, nil
	}

	sourceID, currency, feeScheduleID, err := parseVarianceCursorParts(afterKey)
	if err != nil {
		return varianceCursorFilter{}, err
	}

	p1, p2, p3 := argIdx, argIdx+1, argIdx+cursorPartCount-1
	query += fmt.Sprintf(
		" AND (t.source_id, fv.currency, fv.fee_schedule_id) > ($%d, $%d, $%d)",
		p1, p2, p3,
	)

	args = append(args, sourceID, currency, feeScheduleID)
	argIdx += cursorPartCount

	return varianceCursorFilter{query: query, args: args, argIdx: argIdx}, nil
}

func parseVarianceCursorParts(afterKey string) (uuid.UUID, string, uuid.UUID, error) {
	parts := strings.SplitN(afterKey, ":", cursorPartCount)
	if len(parts) != cursorPartCount {
		return uuid.Nil, "", uuid.Nil, fmt.Errorf("%w: expected %d parts, got %d", ErrInvalidVarianceCursor, cursorPartCount, len(parts))
	}

	sourceID, err := uuid.Parse(parts[0])
	if err != nil {
		return uuid.Nil, "", uuid.Nil, fmt.Errorf("%w: source_id is not a valid UUID: %w", ErrInvalidVarianceCursor, err)
	}

	if !currencyPattern.MatchString(parts[1]) {
		return uuid.Nil, "", uuid.Nil, fmt.Errorf("%w: currency is not a valid 3-letter ISO code", ErrInvalidVarianceCursor)
	}

	feeScheduleID, err := uuid.Parse(parts[2])
	if err != nil {
		return uuid.Nil, "", uuid.Nil, fmt.Errorf("%w: fee_schedule_id is not a valid UUID: %w", ErrInvalidVarianceCursor, err)
	}

	return sourceID, parts[1], feeScheduleID, nil
}

// ListVariancePage retrieves a page of variance rows for streaming export.
func (repo *Repository) ListVariancePage(
	ctx context.Context,
	filter entities.VarianceReportFilter,
	afterKey string,
	limit int,
) ([]*entities.VarianceReportRow, string, error) {
	if repo == nil || repo.provider == nil {
		return nil, "", ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, "", ErrContextIDRequired
	}

	limit = normalizeLimit(limit)

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_variance_page")

	defer span.End()

	type pageResult struct {
		items   []*entities.VarianceReportRow
		nextKey string
	}

	// RATIONALE: ListVariancePage uses manual SQL (varianceBaseQuery) because
	// composite ROW keyset pagination -- (source_id, currency, fs.name) > ($N, $N+1, $N+2) --
	// cannot be expressed via squirrel's Where() clause. The applyVarianceCursor
	// helper appends this comparison with validated positional args. Other variance
	// methods (GetVarianceReport, ListVarianceForExport) use squirrel fully.
	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (pageResult, error) {
			// Start from the base WHERE clause template.
			// Positional args: $1=contextID, $2=dateFrom, $3=dateTo.
			querySQL := varianceBaseQuery
			queryArgs := []any{filter.ContextID, filter.DateFrom, filter.DateTo}
			nextArgIdx := 4 // next available positional arg index

			// Optional source filter: appends the next positional arg.
			if filter.SourceID != nil {
				querySQL += fmt.Sprintf(" AND t.source_id = $%d", nextArgIdx)

				queryArgs = append(queryArgs, *filter.SourceID)
				nextArgIdx++
			}

			// Keyset cursor: appends composite ROW comparison for pagination.
			// e.g., AND (t.source_id, fv.currency, fv.fee_schedule_id) > ($N, $N+1, $N+2)
			cf, err := applyVarianceCursor(afterKey, querySQL, queryArgs, nextArgIdx)
			if err != nil {
				return pageResult{}, fmt.Errorf("applying variance cursor: %w", err)
			}

			querySQL = cf.query
			queryArgs = cf.args
			nextArgIdx = cf.argIdx

			// Finalize: GROUP BY + ORDER BY + LIMIT.
			querySQL += varianceGroupByClause
			querySQL += varianceOrderByClause
			querySQL += fmt.Sprintf(" LIMIT $%d", nextArgIdx)

			queryArgs = append(queryArgs, limit+1)

			rows, err := qe.QueryContext(ctx, querySQL, queryArgs...)
			if err != nil {
				return pageResult{}, fmt.Errorf("querying variance page: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.VarianceReportRow, 0, limit)

			for rows.Next() {
				item, err := scanVarianceRow(rows)
				if err != nil {
					return pageResult{}, fmt.Errorf("scanning variance row: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return pageResult{}, fmt.Errorf("iterating variance rows: %w", err)
			}

			var nextKey string

			if len(items) > limit {
				items = items[:limit]
				last := items[limit-1]
				nextKey = last.SourceID.String() + ":" + last.Currency + ":" + last.FeeScheduleID.String()
			}

			return pageResult{items: items, nextKey: nextKey}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list variance page transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list variance page", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to list variance page", wrappedErr, runtime.IsProductionMode())

		return nil, "", wrappedErr
	}

	return result.items, result.nextKey, nil
}
