// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package report

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/shopspring/decimal"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// GetSummary retrieves aggregated summary statistics.
func (repo *Repository) GetSummary(
	ctx context.Context,
	filter entities.ReportFilter,
) (*entities.SummaryReport, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_summary")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.SummaryReport, error) {
			// Matched summary: COUNT/SUM over distinct transactions in confirmed
			// match groups. Uses FromSelect to wrap the inner DISTINCT query as a
			// subquery -- squirrel handles positional arg indexing automatically.
			innerMatched := squirrel.Select("t.id", "t.amount").Distinct().
				From("match_items mi").
				Join("match_groups mg ON mi.match_group_id = mg.id").
				Join("transactions t ON mi.transaction_id = t.id").
				Where(squirrel.Eq{
					"mg.context_id": filter.ContextID,
					"mg.status":     matchGroupStatusConfirmed,
				}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo))

			if filter.SourceID != nil {
				innerMatched = innerMatched.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			matchedQuery := squirrel.
				Select("COUNT(*) AS cnt", "COALESCE(SUM(t.amount), 0) AS total").
				FromSelect(innerMatched, "t").
				PlaceholderFormat(squirrel.Dollar)

			matchedSQL, matchedArgs, err := matchedQuery.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building matched summary query: %w", err)
			}

			// Unmatched summary: COUNT/SUM over non-matched transactions in the
			// context's reconciliation sources, optionally filtered by source and
			// status.
			unmatchedQuery := squirrel.
				Select("COUNT(*) AS cnt", "COALESCE(SUM(t.amount), 0) AS total").
				From("transactions t").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.NotEq{"t.status": "MATCHED"}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				unmatchedQuery = unmatchedQuery.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			if filter.Status != nil {
				unmatchedQuery = unmatchedQuery.Where(squirrel.Eq{"t.status": *filter.Status})
			}

			unmatchedSQL, unmatchedArgs, err := unmatchedQuery.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building unmatched summary query: %w", err)
			}

			var matchedCount int

			var matchedTotal decimal.Decimal

			row := qe.QueryRowContext(ctx, matchedSQL, matchedArgs...)
			if err := row.Scan(&matchedCount, &matchedTotal); err != nil {
				return nil, fmt.Errorf("scanning matched summary: %w", err)
			}

			var unmatchedCount int

			var unmatchedTotal decimal.Decimal

			row = qe.QueryRowContext(ctx, unmatchedSQL, unmatchedArgs...)
			if err := row.Scan(&unmatchedCount, &unmatchedTotal); err != nil {
				return nil, fmt.Errorf("scanning unmatched summary: %w", err)
			}

			return &entities.SummaryReport{
				MatchedCount:    matchedCount,
				UnmatchedCount:  unmatchedCount,
				MatchedAmount:   matchedTotal,
				UnmatchedAmount: unmatchedTotal,
				TotalAmount:     matchedTotal.Add(unmatchedTotal),
			}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get summary transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get summary", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get summary", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}
