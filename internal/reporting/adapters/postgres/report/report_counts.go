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
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// CountMatched returns the total count of matched records for a filter.
func (repo *Repository) CountMatched(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return 0, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.count_matched")

	defer span.End()

	count, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (int64, error) {
			query := squirrel.Select("COUNT(*)").
				From("match_items mi").
				Join("match_groups mg ON mi.match_group_id = mg.id").
				Join("transactions t ON mi.transaction_id = t.id").
				Where(squirrel.Eq{"mg.context_id": filter.ContextID, "mg.status": matchGroupStatusConfirmed}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return 0, fmt.Errorf("building count query: %w", err)
			}

			var count int64
			if err := qe.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
				return 0, fmt.Errorf("counting matched: %w", err)
			}

			return count, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("count matched transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to count matched", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to count matched", wrappedErr, runtime.IsProductionMode())

		return 0, wrappedErr
	}

	return count, nil
}

// CountUnmatched returns the total count of unmatched records for a filter.
func (repo *Repository) CountUnmatched(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return 0, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.count_unmatched")

	defer span.End()

	count, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (int64, error) {
			query := squirrel.Select("COUNT(*)").
				From("transactions t").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				Where(squirrel.NotEq{"t.status": "MATCHED"}).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			if filter.Status != nil {
				query = query.Where(squirrel.Eq{"t.status": *filter.Status})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return 0, fmt.Errorf("building count query: %w", err)
			}

			var count int64
			if err := qe.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
				return 0, fmt.Errorf("counting unmatched: %w", err)
			}

			return count, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("count unmatched transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to count unmatched", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to count unmatched", wrappedErr, runtime.IsProductionMode())

		return 0, wrappedErr
	}

	return count, nil
}

// CountTransactions returns the total count of all transactions for a filter.
func (repo *Repository) CountTransactions(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return 0, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.count_transactions")

	defer span.End()

	count, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (int64, error) {
			query := squirrel.Select("COUNT(*)").
				From("transactions t").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.Expr("t.date >= ?", filter.DateFrom)).
				Where(squirrel.Expr("t.date <= ?", filter.DateTo)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return 0, fmt.Errorf("building count query: %w", err)
			}

			var count int64
			if err := qe.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
				return 0, fmt.Errorf("counting transactions: %w", err)
			}

			return count, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("count transactions: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to count transactions", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to count transactions", wrappedErr, runtime.IsProductionMode())

		return 0, wrappedErr
	}

	return count, nil
}

// CountExceptions returns the total count of exceptions for a filter.
func (repo *Repository) CountExceptions(
	ctx context.Context,
	filter entities.ReportFilter,
) (int64, error) {
	if repo == nil || repo.provider == nil {
		return 0, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return 0, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.count_exceptions")

	defer span.End()

	count, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (int64, error) {
			query := squirrel.Select("COUNT(*)").
				From("exceptions e").
				Join("transactions t ON e.transaction_id = t.id").
				Join("reconciliation_sources rs ON t.source_id = rs.id").
				Where(squirrel.Eq{"rs.context_id": filter.ContextID}).
				Where(squirrel.Expr("e.created_at >= ?", filter.DateFrom)).
				Where(squirrel.Expr("e.created_at <= ?", filter.DateTo)).
				PlaceholderFormat(squirrel.Dollar)

			if filter.SourceID != nil {
				query = query.Where(squirrel.Eq{"t.source_id": *filter.SourceID})
			}

			sqlQuery, args, err := query.ToSql()
			if err != nil {
				return 0, fmt.Errorf("building count query: %w", err)
			}

			var count int64
			if err := qe.QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
				return 0, fmt.Errorf("counting exceptions: %w", err)
			}

			return count, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("count exceptions: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to count exceptions", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to count exceptions", wrappedErr, runtime.IsProductionMode())

		return 0, wrappedErr
	}

	return count, nil
}

var _ repositories.ReportRepository = (*Repository)(nil)
