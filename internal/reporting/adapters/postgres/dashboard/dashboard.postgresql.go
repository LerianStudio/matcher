// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dashboard

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	matchGroupStatusConfirmed = "CONFIRMED"
	exceptionStatusResolved   = "RESOLVED"
	exceptionSeverityCritical = "CRITICAL"

	// matchRatePercentageScale converts a ratio (0.0-1.0) into a percentage
	// (0-100). SummaryMetrics.MatchRate, DailyTrendPoint.MatchRate, and
	// SourceBreakdown.MatchRate are all expressed on the percentage scale
	// to align with MatchRateStats and the console display.
	matchRatePercentageScale = 100.0
)

// Repository persists dashboard data in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new dashboard repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

func (repo *Repository) validateFilter(filter *entities.DashboardFilter) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return ErrContextIDRequired
	}

	return nil
}

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

// GetTrendMetrics retrieves time-series trend data for charts.
func (repo *Repository) GetTrendMetrics(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.TrendMetrics, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_trend_metrics")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.TrendMetrics, error) {
			query := `
			WITH date_series AS (
				SELECT generate_series(
					DATE_TRUNC('day', $2::timestamp),
					DATE_TRUNC('day', $3::timestamp),
					'1 day'::interval
				)::date as day
			),
			daily_ingestion AS (
				SELECT DATE_TRUNC('day', t.date)::date as day, COUNT(*) as cnt
				FROM transactions t
				JOIN reconciliation_sources rs ON t.source_id = rs.id
				WHERE rs.context_id = $1 AND t.date >= $2 AND t.date <= $3
					AND ($5::uuid IS NULL OR rs.id = $5)
				GROUP BY DATE_TRUNC('day', t.date)::date
			),
			daily_matches AS (
				SELECT DATE_TRUNC('day', mg.created_at)::date as day, 
					COUNT(DISTINCT mi.transaction_id) as cnt
				FROM match_groups mg
				JOIN match_items mi ON mi.match_group_id = mg.id
				JOIN transactions t ON mi.transaction_id = t.id
				JOIN reconciliation_sources rs ON t.source_id = rs.id
				WHERE mg.context_id = $1 AND mg.status = $4 
					AND mg.created_at >= $2 AND mg.created_at <= $3
					AND ($5::uuid IS NULL OR rs.id = $5)
				GROUP BY DATE_TRUNC('day', mg.created_at)::date
			),
			daily_exceptions AS (
				SELECT DATE_TRUNC('day', e.created_at)::date as day, COUNT(*) as cnt
				FROM exceptions e
				JOIN transactions t ON e.transaction_id = t.id
				JOIN reconciliation_sources rs ON t.source_id = rs.id
				WHERE rs.context_id = $1 AND e.created_at >= $2 AND e.created_at <= $3
					AND ($5::uuid IS NULL OR rs.id = $5)
				GROUP BY DATE_TRUNC('day', e.created_at)::date
			)
			SELECT 
				ds.day,
				COALESCE(di.cnt, 0) as ingested,
				COALESCE(dm.cnt, 0) as matched,
				COALESCE(de.cnt, 0) as exceptions
			FROM date_series ds
			LEFT JOIN daily_ingestion di ON di.day = ds.day
			LEFT JOIN daily_matches dm ON dm.day = ds.day
			LEFT JOIN daily_exceptions de ON de.day = ds.day
			ORDER BY ds.day`

			args := []any{
				filter.ContextID,
				filter.DateFrom,
				filter.DateTo,
				matchGroupStatusConfirmed,
				filter.SourceID,
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("querying trend metrics: %w", err)
			}
			defer rows.Close()

			var points []entities.DailyTrendPoint

			for rows.Next() {
				var point entities.DailyTrendPoint

				if err := rows.Scan(&point.Date, &point.Ingested, &point.Matched, &point.Exceptions); err != nil {
					return nil, fmt.Errorf("scanning trend row: %w", err)
				}

				if point.Ingested > 0 {
					// Return match rate as percentage (0-100) to match
					// SummaryMetrics.MatchRate and the console chart scale.
					point.MatchRate = float64(point.Matched) / float64(point.Ingested) * matchRatePercentageScale
					if point.MatchRate > matchRatePercentageScale {
						point.MatchRate = matchRatePercentageScale
					}
				}

				points = append(points, point)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating trend rows: %w", err)
			}

			return entities.BuildTrendMetrics(points), nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get trend metrics: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get trend metrics", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get trend metrics", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// GetBreakdownMetrics retrieves categorical aggregations for charts.
func (repo *Repository) GetBreakdownMetrics(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.BreakdownMetrics, error) {
	if err := repo.validateFilter(&filter); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_breakdown_metrics")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.BreakdownMetrics, error) {
			breakdown := entities.NewEmptyBreakdownMetrics()

			if err := repo.loadExceptionsBySeverity(ctx, qe, filter, breakdown); err != nil {
				return nil, err
			}

			if err := repo.loadExceptionsByReason(ctx, qe, filter, breakdown); err != nil {
				return nil, err
			}

			if err := repo.loadMatchesByRule(ctx, qe, filter, breakdown); err != nil {
				return nil, err
			}

			if err := repo.loadExceptionsByAge(ctx, qe, filter, breakdown); err != nil {
				return nil, err
			}

			return breakdown, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("get breakdown metrics: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get breakdown metrics", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to get breakdown metrics", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

func (repo *Repository) loadExceptionsBySeverity(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter entities.DashboardFilter,
	breakdown *entities.BreakdownMetrics,
) error {
	query := `
		SELECT e.severity, COUNT(*) as cnt
		FROM exceptions e
		JOIN transactions t ON e.transaction_id = t.id
		JOIN reconciliation_sources rs ON t.source_id = rs.id
		WHERE rs.context_id = $1 AND e.status != $2 AND e.created_at >= $3 AND e.created_at <= $4
			AND ($5::uuid IS NULL OR t.source_id = $5)
		GROUP BY e.severity`

	args := []any{filter.ContextID, exceptionStatusResolved, filter.DateFrom, filter.DateTo, filter.SourceID}

	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("querying exceptions by severity: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var severity string

		var count int

		if err := rows.Scan(&severity, &count); err != nil {
			return fmt.Errorf("scanning severity row: %w", err)
		}

		breakdown.BySeverity[severity] = count
	}

	return rows.Err()
}

func (repo *Repository) loadExceptionsByReason(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter entities.DashboardFilter,
	breakdown *entities.BreakdownMetrics,
) error {
	query := `
		SELECT COALESCE(e.resolution_notes, 'Unspecified') as reason, COUNT(*) as cnt
		FROM exceptions e
		JOIN transactions t ON e.transaction_id = t.id
		JOIN reconciliation_sources rs ON t.source_id = rs.id
		WHERE rs.context_id = $1 AND e.status != $2 AND e.created_at >= $3 AND e.created_at <= $4
			AND ($5::uuid IS NULL OR t.source_id = $5)
		GROUP BY COALESCE(e.resolution_notes, 'Unspecified')
		ORDER BY cnt DESC
		LIMIT 10`

	args := []any{filter.ContextID, exceptionStatusResolved, filter.DateFrom, filter.DateTo, filter.SourceID}

	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("querying exceptions by reason: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var reason string

		var count int

		if err := rows.Scan(&reason, &count); err != nil {
			return fmt.Errorf("scanning reason row: %w", err)
		}

		breakdown.ByReason[reason] = count
	}

	return rows.Err()
}

func (repo *Repository) loadMatchesByRule(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter entities.DashboardFilter,
	breakdown *entities.BreakdownMetrics,
) error {
	query := `
		SELECT mr.id, mr.type, COUNT(mg.id) as cnt
		FROM match_groups mg
		JOIN match_rules mr ON mg.rule_id = mr.id
		LEFT JOIN match_items mi ON mi.match_group_id = mg.id
		LEFT JOIN transactions t ON mi.transaction_id = t.id
		WHERE mg.context_id = $1 AND mg.status = $2 AND mg.created_at >= $3 AND mg.created_at <= $4
			AND ($5::uuid IS NULL OR t.source_id = $5)
		GROUP BY mr.id, mr.type
		ORDER BY cnt DESC`

	args := []any{filter.ContextID, matchGroupStatusConfirmed, filter.DateFrom, filter.DateTo, filter.SourceID}

	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("querying matches by rule: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var ruleID uuid.UUID

		var ruleName string

		var count int

		if err := rows.Scan(&ruleID, &ruleName, &count); err != nil {
			return fmt.Errorf("scanning rule row: %w", err)
		}

		breakdown.ByRule = append(breakdown.ByRule, entities.RuleMatchCount{
			ID:    ruleID,
			Name:  ruleName,
			Count: count,
		})
	}

	return rows.Err()
}

func (repo *Repository) loadExceptionsByAge(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	filter entities.DashboardFilter,
	breakdown *entities.BreakdownMetrics,
) error {
	query := `
		SELECT
			CASE
				WHEN EXTRACT(EPOCH FROM (NOW() - e.created_at)) / 3600 < 24 THEN '<24h'
				WHEN EXTRACT(EPOCH FROM (NOW() - e.created_at)) / 3600 < 72 THEN '1-3d'
				ELSE '>3d'
			END as bucket,
			CASE
				WHEN EXTRACT(EPOCH FROM (NOW() - e.created_at)) / 3600 < 24 THEN 1
				WHEN EXTRACT(EPOCH FROM (NOW() - e.created_at)) / 3600 < 72 THEN 2
				ELSE 3
			END as ord,
			COUNT(*) as cnt
		FROM exceptions e
		JOIN transactions t ON e.transaction_id = t.id
		JOIN reconciliation_sources rs ON t.source_id = rs.id
		WHERE rs.context_id = $1 AND e.status != $2 AND e.created_at >= $3 AND e.created_at <= $4
			AND ($5::uuid IS NULL OR t.source_id = $5)
		GROUP BY bucket, ord
		ORDER BY ord`

	args := []any{filter.ContextID, exceptionStatusResolved, filter.DateFrom, filter.DateTo, filter.SourceID}

	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("querying exceptions by age: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var bucket string

		var ord int

		var count int

		if err := rows.Scan(&bucket, &ord, &count); err != nil {
			return fmt.Errorf("scanning age row: %w", err)
		}

		if ord <= 0 {
			return fmt.Errorf("%w: %d", errInvalidAgeBucketOrder, ord)
		}

		breakdown.ByAge = append(breakdown.ByAge, entities.AgeBucket{
			Bucket: bucket,
			Value:  count,
		})
	}

	return rows.Err()
}

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

var _ repositories.DashboardRepository = (*Repository)(nil)
