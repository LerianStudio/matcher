// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dashboard

import (
	"context"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

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
