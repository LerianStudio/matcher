// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dashboard

import (
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// GetVolumeStats retrieves transaction volume statistics.
func (repo *Repository) GetVolumeStats(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.VolumeStats, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_volume_stats")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.VolumeStats, error) {
			matchedQuery := `
			SELECT COUNT(*) as cnt, COALESCE(SUM(t.amount), 0) as total
			FROM (
				SELECT DISTINCT t.id, t.amount
				FROM match_items mi
				JOIN match_groups mg ON mi.match_group_id = mg.id
				JOIN transactions t ON mi.transaction_id = t.id
				WHERE mg.context_id = $1 AND mg.status = $2 AND t.date >= $3 AND t.date <= $4`

			totalQuery := `
			SELECT COUNT(*) as cnt, COALESCE(SUM(t.amount), 0) as total
			FROM transactions t
			JOIN reconciliation_sources rs ON t.source_id = rs.id
			WHERE rs.context_id = $1 AND t.date >= $2 AND t.date <= $3`

			matchedArgs := []any{
				filter.ContextID,
				matchGroupStatusConfirmed,
				filter.DateFrom,
				filter.DateTo,
			}
			totalArgs := []any{filter.ContextID, filter.DateFrom, filter.DateTo}

			if filter.SourceID != nil {
				matchedQuery += " AND t.source_id = $5"
				totalQuery += " AND t.source_id = $4"

				matchedArgs = append(matchedArgs, *filter.SourceID)
				totalArgs = append(totalArgs, *filter.SourceID)
			}

			matchedQuery += "\n\t\t) t"

			var matchedCount int

			var matchedTotal decimal.Decimal

			row := qe.QueryRowContext(ctx, matchedQuery, matchedArgs...)
			if err := row.Scan(&matchedCount, &matchedTotal); err != nil {
				return nil, fmt.Errorf("scanning matched stats: %w", err)
			}

			var totalCount int

			var totalAmount decimal.Decimal

			row = qe.QueryRowContext(ctx, totalQuery, totalArgs...)
			if err := row.Scan(&totalCount, &totalAmount); err != nil {
				return nil, fmt.Errorf("scanning total stats: %w", err)
			}

			unmatchedCount := totalCount - matchedCount
			if unmatchedCount < 0 {
				logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf(
					"negative unmatched count detected: total=%d, matched=%d; clamping to 0",
					totalCount,
					matchedCount,
				))

				span.AddEvent("dashboard_data_anomaly", trace.WithAttributes(
					attribute.String("type", "negative_unmatched_count"),
					attribute.Int("total_count", totalCount),
					attribute.Int("matched_count", matchedCount),
				))

				unmatchedCount = 0
			}

			unmatchedAmount := totalAmount.Sub(matchedTotal)
			if unmatchedAmount.IsNegative() {
				logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf(
					"negative unmatched amount detected: total=%s, matched=%s; clamping to 0",
					totalAmount.String(),
					matchedTotal.String(),
				))

				span.AddEvent("dashboard_data_anomaly", trace.WithAttributes(
					attribute.String("type", "negative_unmatched_amount"),
					attribute.String("total_amount", totalAmount.String()),
					attribute.String("matched_amount", matchedTotal.String()),
				))

				unmatchedAmount = decimal.Zero
			}

			return &entities.VolumeStats{
				TotalTransactions:   totalCount,
				MatchedTransactions: matchedCount,
				UnmatchedCount:      unmatchedCount,
				TotalAmount:         totalAmount,
				MatchedAmount:       matchedTotal,
				UnmatchedAmount:     unmatchedAmount,
				PeriodStart:         filter.DateFrom,
				PeriodEnd:           filter.DateTo,
			}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get volume stats: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get volume stats", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get volume stats", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// GetSLAStats retrieves SLA compliance statistics.
func (repo *Repository) GetSLAStats(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.SLAStats, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_sla_stats")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.SLAStats, error) {
			now := time.Now().UTC()

			query := `
			SELECT 
				COUNT(*) as total_exceptions,
				COUNT(*) FILTER (WHERE e.status = 'RESOLVED' AND (e.due_at IS NULL OR e.updated_at <= e.due_at)) as resolved_on_time,
				COUNT(*) FILTER (WHERE e.status = 'RESOLVED' AND e.due_at IS NOT NULL AND e.updated_at > e.due_at) as resolved_late,
				COUNT(*) FILTER (WHERE e.status != 'RESOLVED' AND (e.due_at IS NULL OR e.due_at >= $4)) as pending_within_sla,
				COUNT(*) FILTER (WHERE e.status != 'RESOLVED' AND e.due_at IS NOT NULL AND e.due_at < $4) as pending_overdue,
				COALESCE(AVG(EXTRACT(EPOCH FROM (e.updated_at - e.created_at)) * 1000) FILTER (WHERE e.status = 'RESOLVED'), 0) as avg_resolution_ms
			FROM exceptions e
			JOIN transactions t ON e.transaction_id = t.id
			JOIN reconciliation_sources rs ON t.source_id = rs.id
			WHERE rs.context_id = $1 AND e.created_at >= $2 AND e.created_at <= $3`

			args := []any{filter.ContextID, filter.DateFrom, filter.DateTo, now}

			if filter.SourceID != nil {
				query += " AND t.source_id = $5"

				args = append(args, *filter.SourceID)
			}

			var stats entities.SLAStats

			var avgResolutionMs float64

			row := qe.QueryRowContext(ctx, query, args...)
			if err := row.Scan(
				&stats.TotalExceptions,
				&stats.ResolvedOnTime,
				&stats.ResolvedLate,
				&stats.PendingWithinSLA,
				&stats.PendingOverdue,
				&avgResolutionMs,
			); err != nil {
				return nil, fmt.Errorf("scanning sla stats: %w", err)
			}

			stats.AverageResolutionMs = int64(avgResolutionMs)
			stats.SLAComplianceRate = entities.CalculateSLACompliance(&stats)

			return &stats, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get sla stats: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get sla stats", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get sla stats", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// GetSummaryMetrics retrieves executive summary metrics for the dashboard.
func (repo *Repository) GetSummaryMetrics(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.SummaryMetrics, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_summary_metrics")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.SummaryMetrics, error) {
			// Query parameters:
			// $1 = contextID, $2 = dateFrom, $3 = dateTo
			// $4 = matchGroupStatusConfirmed, $5 = exceptionSeverityCritical
			// $6 = exceptionStatusResolved, $7 = sourceID
			//
			// IMPORTANT: All CTEs use the same base population of transactions
			// filtered by context_id, date range, and optional source_id.
			// - total_txn: Count of all transactions in the date range
			// - matched_txn: Count of DISTINCT transactions in CONFIRMED match groups
			// - pending_exc: Exceptions for transactions in the date range (status != RESOLVED)
			query := `
			WITH base_txns AS (
				SELECT t.id, t.amount
				FROM transactions t
				JOIN reconciliation_sources rs ON t.source_id = rs.id
				WHERE rs.context_id = $1 AND t.date >= $2 AND t.date <= $3
					AND ($7::uuid IS NULL OR rs.id = $7)
			),
			total_txn AS (
				SELECT COUNT(*) as cnt FROM base_txns
			),
			matched_txn AS (
				SELECT COUNT(DISTINCT mi.transaction_id) as cnt
				FROM match_items mi
				JOIN match_groups mg ON mi.match_group_id = mg.id
				JOIN base_txns bt ON bt.id = mi.transaction_id
				WHERE mg.status = $4
			),
			pending_exc AS (
				SELECT 
					COUNT(*) as cnt,
					COALESCE(SUM(CASE WHEN e.severity = $5 THEN bt.amount ELSE 0 END), 0) as critical_amount,
					EXTRACT(EPOCH FROM (NOW() - MIN(e.created_at))) / 3600 as oldest_age_hours
				FROM exceptions e
				JOIN base_txns bt ON e.transaction_id = bt.id
				WHERE e.status != $6
			)
			SELECT 
				COALESCE(total_txn.cnt, 0),
				COALESCE(matched_txn.cnt, 0),
				COALESCE(pending_exc.cnt, 0),
				COALESCE(pending_exc.critical_amount, 0),
				COALESCE(pending_exc.oldest_age_hours, 0)
			FROM total_txn, matched_txn, pending_exc`

			args := []any{
				filter.ContextID,
				filter.DateFrom,
				filter.DateTo,
				matchGroupStatusConfirmed,
				exceptionSeverityCritical,
				exceptionStatusResolved,
				filter.SourceID,
			}

			var totalTxn, matchedCount, pendingExc int

			var criticalExposure decimal.Decimal

			var oldestAge float64

			row := qe.QueryRowContext(ctx, query, args...)

			if err := row.Scan(&totalTxn, &matchedCount, &pendingExc, &criticalExposure, &oldestAge); err != nil {
				return nil, fmt.Errorf("scanning summary metrics: %w", err)
			}

			// Match rate as percentage (0-100) to match MatchRateStats and
			// the console display convention (page renders `toFixed(1)%`).
			// Computed as (matched / total) * 100.
			var matchRate float64

			if totalTxn > 0 {
				rawRate := float64(matchedCount) / float64(totalTxn) * matchRatePercentageScale
				matchRate = rawRate

				if matchRate > matchRatePercentageScale {
					logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf(
						"match rate over 100%% detected: matched=%d, total=%d, rate=%.4f; clamping to 100",
						matchedCount,
						totalTxn,
						rawRate,
					))

					span.AddEvent("dashboard_data_anomaly", trace.WithAttributes(
						attribute.String("type", "match_rate_overflow"),
						attribute.Int("matched_count", matchedCount),
						attribute.Int("total_txn", totalTxn),
						attribute.Float64("raw_rate", rawRate),
					))

					matchRate = matchRatePercentageScale
				}
			}

			return &entities.SummaryMetrics{
				TotalTransactions:  totalTxn,
				TotalMatches:       matchedCount,
				MatchRate:          matchRate,
				PendingExceptions:  pendingExc,
				CriticalExposure:   criticalExposure,
				OldestExceptionAge: oldestAge,
			}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get summary metrics: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get summary metrics", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get summary metrics", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}
