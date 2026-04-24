// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dashboard

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// GetSourceBreakdown retrieves per-source reconciliation statistics.
func (repo *Repository) GetSourceBreakdown(
	ctx context.Context,
	filter entities.DashboardFilter,
) ([]entities.SourceBreakdown, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_source_breakdown")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]entities.SourceBreakdown, error) {
			query := `
			WITH source_totals AS (
				SELECT
					rs.id AS source_id,
					rs.name AS source_name,
					COUNT(t.id) AS total_txns,
					COALESCE(SUM(t.amount), 0) AS total_amount,
					COALESCE(MAX(t.currency), '') AS currency
				FROM transactions t
				JOIN reconciliation_sources rs ON t.source_id = rs.id
				WHERE rs.context_id = $1 AND t.date >= $2 AND t.date <= $3
				GROUP BY rs.id, rs.name
			),
			source_matched AS (
				SELECT
					t.source_id,
					COUNT(DISTINCT t.id) AS matched_txns,
					COALESCE(SUM(t.amount), 0) AS matched_amount
				FROM match_items mi
				JOIN match_groups mg ON mi.match_group_id = mg.id
				JOIN transactions t ON mi.transaction_id = t.id
				JOIN reconciliation_sources rs ON t.source_id = rs.id
				WHERE mg.context_id = $1 AND mg.status = $4
					AND t.date >= $2 AND t.date <= $3
				GROUP BY t.source_id
			)
			SELECT
				st.source_id,
				st.source_name,
				st.total_txns,
				COALESCE(sm.matched_txns, 0) AS matched_txns,
				st.total_amount,
				COALESCE(sm.matched_amount, 0) AS matched_amount,
				st.currency
			FROM source_totals st
			LEFT JOIN source_matched sm ON st.source_id = sm.source_id
			ORDER BY st.total_txns DESC`

			args := []any{
				filter.ContextID,
				filter.DateFrom,
				filter.DateTo,
				matchGroupStatusConfirmed,
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("querying source breakdown: %w", err)
			}
			defer rows.Close()

			var breakdowns []entities.SourceBreakdown

			for rows.Next() {
				var sb entities.SourceBreakdown

				var matchedTxns int64

				var matchedAmount decimal.Decimal

				if err := rows.Scan(
					&sb.SourceID,
					&sb.SourceName,
					&sb.TotalTxns,
					&matchedTxns,
					&sb.TotalAmount,
					&matchedAmount,
					&sb.Currency,
				); err != nil {
					return nil, fmt.Errorf("scanning source breakdown row: %w", err)
				}

				sb.MatchedTxns = matchedTxns

				sb.UnmatchedTxns = max(sb.TotalTxns-matchedTxns, 0)

				if sb.TotalTxns > 0 {
					// Percentage scale (0-100), aligned with SummaryMetrics
					// and DailyTrendPoint so the dashboard is consistent.
					sb.MatchRate = float64(matchedTxns) / float64(sb.TotalTxns) * matchRatePercentageScale
					if sb.MatchRate > matchRatePercentageScale {
						sb.MatchRate = matchRatePercentageScale
					}
				}

				sb.UnmatchedAmount = sb.TotalAmount.Sub(matchedAmount)

				if sb.UnmatchedAmount.IsNegative() {
					sb.UnmatchedAmount = decimal.Zero
				}

				breakdowns = append(breakdowns, sb)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating source breakdown rows: %w", err)
			}

			if breakdowns == nil {
				breakdowns = make([]entities.SourceBreakdown, 0)
			}

			return breakdowns, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get source breakdown: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get source breakdown", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get source breakdown", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// GetCashImpactSummary retrieves unreconciled financial exposure.
func (repo *Repository) GetCashImpactSummary(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.CashImpactSummary, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_cash_impact_summary")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.CashImpactSummary, error) {
			summary := &entities.CashImpactSummary{
				TotalUnmatchedAmount: decimal.Zero,
				ByCurrency:           make([]entities.CurrencyExposure, 0),
				ByAge:                make([]entities.AgeExposure, 0),
			}

			// Query unmatched transactions grouped by currency
			currencyQuery := `
			SELECT
				COALESCE(t.currency, 'UNKNOWN') AS currency,
				COALESCE(SUM(t.amount), 0) AS amount,
				COUNT(t.id) AS txn_count
			FROM transactions t
			JOIN reconciliation_sources rs ON t.source_id = rs.id
			WHERE rs.context_id = $1 AND t.date >= $2 AND t.date <= $3
				AND NOT EXISTS (
					SELECT 1 FROM match_items mi
					JOIN match_groups mg ON mi.match_group_id = mg.id
					WHERE mg.context_id = $1 AND mg.status = $4
					AND mi.transaction_id = t.id
				)
			GROUP BY COALESCE(t.currency, 'UNKNOWN')
			ORDER BY amount DESC`

			currencyArgs := []any{
				filter.ContextID,
				filter.DateFrom,
				filter.DateTo,
				matchGroupStatusConfirmed,
			}

			rows, err := qe.QueryContext(ctx, currencyQuery, currencyArgs...)
			if err != nil {
				return nil, fmt.Errorf("querying cash impact by currency: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var ce entities.CurrencyExposure

				if err := rows.Scan(&ce.Currency, &ce.Amount, &ce.TransactionCount); err != nil {
					return nil, fmt.Errorf("scanning currency exposure row: %w", err)
				}

				summary.TotalUnmatchedAmount = summary.TotalUnmatchedAmount.Add(ce.Amount)
				summary.ByCurrency = append(summary.ByCurrency, ce)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating currency exposure rows: %w", err)
			}

			// Query unmatched transactions grouped by age bucket
			ageQuery := `
			SELECT
				CASE
					WHEN EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 < 24 THEN '0-24h'
					WHEN EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 < 72 THEN '1-3d'
					WHEN EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 < 168 THEN '3-7d'
					WHEN EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 < 720 THEN '7-30d'
					ELSE '30d+'
				END AS bucket,
				CASE
					WHEN EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 < 24 THEN 1
					WHEN EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 < 72 THEN 2
					WHEN EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 < 168 THEN 3
					WHEN EXTRACT(EPOCH FROM (NOW() - t.date)) / 3600 < 720 THEN 4
					ELSE 5
				END AS ord,
				COALESCE(SUM(t.amount), 0) AS amount,
				COUNT(t.id) AS txn_count
			FROM transactions t
			JOIN reconciliation_sources rs ON t.source_id = rs.id
			WHERE rs.context_id = $1 AND t.date >= $2 AND t.date <= $3
				AND NOT EXISTS (
					SELECT 1 FROM match_items mi
					JOIN match_groups mg ON mi.match_group_id = mg.id
					WHERE mg.context_id = $1 AND mg.status = $4
					AND mi.transaction_id = t.id
				)
			GROUP BY bucket, ord
			ORDER BY ord`

			ageArgs := []any{
				filter.ContextID,
				filter.DateFrom,
				filter.DateTo,
				matchGroupStatusConfirmed,
			}

			ageRows, err := qe.QueryContext(ctx, ageQuery, ageArgs...)
			if err != nil {
				return nil, fmt.Errorf("querying cash impact by age: %w", err)
			}
			defer ageRows.Close()

			for ageRows.Next() {
				var ae entities.AgeExposure

				var ord int

				if err := ageRows.Scan(&ae.Bucket, &ord, &ae.Amount, &ae.TransactionCount); err != nil {
					return nil, fmt.Errorf("scanning age exposure row: %w", err)
				}

				if ord <= 0 {
					return nil, fmt.Errorf("%w: %d", errInvalidAgeExposureOrder, ord)
				}

				summary.ByAge = append(summary.ByAge, ae)
			}

			if err := ageRows.Err(); err != nil {
				return nil, fmt.Errorf("iterating age exposure rows: %w", err)
			}

			return summary, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get cash impact summary: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get cash impact summary", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get cash impact summary", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}
