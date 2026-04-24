// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package report

import (
	"context"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// applyUnmatchedExportFilters adds optional filters to the unmatched export query.
func applyUnmatchedExportFilters(
	query squirrel.SelectBuilder,
	filter entities.ReportFilter,
) squirrel.SelectBuilder {
	if filter.SourceID != nil {
		query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
	}

	if filter.Status != nil {
		query = query.Where(squirrel.Eq{"t.status": *filter.Status})
	}

	return query
}

// executeUnmatchedExportQuery executes the query and scans results.
func executeUnmatchedExportQuery(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	query squirrel.SelectBuilder,
	maxRecords int,
) ([]*entities.UnmatchedItem, error) {
	sqlQuery, args, err := query.ToSql()
	if err != nil {
		return nil, fmt.Errorf("building export query: %w", err)
	}

	rows, err := qe.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("querying unmatched items for export: %w", err)
	}

	defer rows.Close()

	items := make([]*entities.UnmatchedItem, 0, maxRecords)

	for rows.Next() {
		item, err := scanUnmatchedItem(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning unmatched item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating unmatched items: %w", err)
	}

	// Defensive: unreachable while query applies LIMIT, retained as safety net.
	if len(items) > maxRecords {
		return nil, fmt.Errorf(
			"%w: result set exceeds %d records",
			ErrExportLimitExceeded,
			maxRecords,
		)
	}

	return items, nil
}

// ListMatchedForExport retrieves matched transactions for export with a maximum record limit.
func (repo *Repository) ListMatchedForExport(
	ctx context.Context,
	filter entities.ReportFilter,
	maxRecords int,
) ([]*entities.MatchedItem, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	if maxRecords <= 0 {
		return nil, ErrMaxRecordsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_matched_for_export")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.MatchedItem, error) {
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
				Limit(safeExportLimit(maxRecords)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building export query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlQuery, args...)
			if err != nil {
				return nil, fmt.Errorf("querying matched items for export: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.MatchedItem, 0, maxRecords)

			for rows.Next() {
				item, err := scanMatchedItem(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning matched item: %w", err)
				}

				items = append(items, item)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating matched items: %w", err)
			}

			// Defensive: unreachable while query applies LIMIT, retained as safety net.
			if len(items) > maxRecords {
				return nil, fmt.Errorf(
					"%w: result set exceeds %d records",
					ErrExportLimitExceeded,
					maxRecords,
				)
			}

			return items, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list matched for export transaction: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list matched items for export",
			wrappedErr,
		)

		libLog.SafeError(logger, ctx, "failed to list matched items for export", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// ListUnmatchedForExport retrieves unmatched transactions for export with a maximum record limit.
func (repo *Repository) ListUnmatchedForExport(
	ctx context.Context,
	filter entities.ReportFilter,
	maxRecords int,
) ([]*entities.UnmatchedItem, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	if maxRecords <= 0 {
		return nil, ErrMaxRecordsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_unmatched_for_export")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.UnmatchedItem, error) {
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
				LeftJoin("exceptions e ON e.transaction_id = t.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.NotEq{"t.status": "MATCHED"}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				OrderBy("t.id ASC").
				Limit(safeExportLimit(maxRecords)).
				PlaceholderFormat(squirrel.Dollar)

			query = applyUnmatchedExportFilters(query, filter)

			return executeUnmatchedExportQuery(ctx, qe, query, maxRecords)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list unmatched for export transaction: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list unmatched items for export",
			wrappedErr,
		)

		libLog.SafeError(logger, ctx, "failed to list unmatched items for export", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// ListVarianceForExport retrieves variance rows for export with a maximum record limit.
func (repo *Repository) ListVarianceForExport(
	ctx context.Context,
	filter entities.VarianceReportFilter,
	maxRecords int,
) ([]*entities.VarianceReportRow, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	if maxRecords <= 0 {
		return nil, ErrMaxRecordsMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_variance_for_export")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.VarianceReportRow, error) {
			query := buildVarianceSelectBuilder(
				filter.ContextID,
				filter.DateFrom,
				filter.DateTo,
				filter.SourceID,
				sortOrderAsc,
			).
				Limit(safeExportLimit(maxRecords))

			sqlStr, args, err := query.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building variance export query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, sqlStr, args...)
			if err != nil {
				return nil, fmt.Errorf("querying variance report for export: %w", err)
			}

			defer rows.Close()

			items := make([]*entities.VarianceReportRow, 0, safeExportLimit(maxRecords))

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

			// Defensive: unreachable while query applies LIMIT, retained as safety net.
			if len(items) > maxRecords {
				return nil, fmt.Errorf(
					"%w: result set exceeds %d records",
					ErrExportLimitExceeded,
					maxRecords,
				)
			}

			return items, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list variance for export transaction: %w", err)
		libOpentelemetry.HandleSpanError(
			span,
			"failed to list variance rows for export",
			wrappedErr,
		)

		libLog.SafeError(logger, ctx, "failed to list variance rows for export", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}
