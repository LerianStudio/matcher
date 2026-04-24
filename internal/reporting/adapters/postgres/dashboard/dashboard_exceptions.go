// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dashboard

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

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
