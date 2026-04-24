package report

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errIteratorNotInitialized indicates an iterator was used after its
// underlying rows were released or before they were attached. Returned
// by Scan on all three streaming iterators so production code receives a
// sentinel instead of a nil deref.
var errIteratorNotInitialized = errors.New("iterator: rows not initialized")

// matchedRowIterator wraps sql.Rows for streaming matched items.
// Implements repositories.MatchedRowIterator.
type matchedRowIterator struct {
	rows *sql.Rows
	tx   *sharedPorts.TxLease
	span trace.Span
}

func (it *matchedRowIterator) Next() bool { //nolint:revive // implements interface
	if it.rows == nil {
		return false
	}

	return it.rows.Next()
}

func (it *matchedRowIterator) Err() error { //nolint:revive // implements interface
	if it.rows == nil {
		return nil
	}

	return it.rows.Err()
}

func (it *matchedRowIterator) Close() error { //nolint:revive // implements interface
	if it.span != nil {
		defer it.span.End()
	}

	var rowsErr error
	if it.rows != nil {
		rowsErr = it.rows.Close()
	}

	var txErr error
	if it.tx != nil {
		txErr = it.tx.Commit()
	}

	return errors.Join(rowsErr, txErr)
}

func (it *matchedRowIterator) Scan() (*entities.MatchedItem, error) { //nolint:revive // implements interface
	if it.rows == nil {
		return nil, errIteratorNotInitialized
	}

	var item entities.MatchedItem
	if err := it.rows.Scan(
		&item.TransactionID,
		&item.MatchGroupID,
		&item.SourceID,
		&item.Amount,
		&item.Currency,
		&item.Date,
	); err != nil {
		return nil, fmt.Errorf("scanning matched item: %w", err)
	}

	return &item, nil
}

// unmatchedRowIterator wraps sql.Rows for streaming unmatched items.
// Implements repositories.UnmatchedRowIterator.
type unmatchedRowIterator struct {
	rows *sql.Rows
	tx   *sharedPorts.TxLease
	span trace.Span
}

func (it *unmatchedRowIterator) Next() bool { //nolint:revive // implements interface
	if it.rows == nil {
		return false
	}

	return it.rows.Next()
}

func (it *unmatchedRowIterator) Err() error { //nolint:revive // implements interface
	if it.rows == nil {
		return nil
	}

	return it.rows.Err()
}

func (it *unmatchedRowIterator) Close() error { //nolint:revive // implements interface
	if it.span != nil {
		defer it.span.End()
	}

	var rowsErr error
	if it.rows != nil {
		rowsErr = it.rows.Close()
	}

	var txErr error
	if it.tx != nil {
		txErr = it.tx.Commit()
	}

	return errors.Join(rowsErr, txErr)
}

func (it *unmatchedRowIterator) Scan() (*entities.UnmatchedItem, error) { //nolint:revive // implements interface
	if it.rows == nil {
		return nil, errIteratorNotInitialized
	}

	var item entities.UnmatchedItem
	if err := it.rows.Scan(
		&item.TransactionID,
		&item.SourceID,
		&item.Amount,
		&item.Currency,
		&item.Status,
		&item.Date,
		&item.ExceptionID,
		&item.DueAt,
	); err != nil {
		return nil, fmt.Errorf("scanning unmatched item: %w", err)
	}

	return &item, nil
}

// varianceRowIterator wraps sql.Rows for streaming variance rows.
// Implements repositories.VarianceRowIterator.
type varianceRowIterator struct {
	rows *sql.Rows
	tx   *sharedPorts.TxLease
	span trace.Span
}

func (iter *varianceRowIterator) Next() bool { //nolint:revive // implements interface
	if iter.rows == nil {
		return false
	}

	return iter.rows.Next()
}

func (iter *varianceRowIterator) Err() error { //nolint:revive // implements interface
	if iter.rows == nil {
		return nil
	}

	return iter.rows.Err()
}

func (iter *varianceRowIterator) Close() error { //nolint:revive // implements interface
	if iter.span != nil {
		defer iter.span.End()
	}

	var rowsErr error
	if iter.rows != nil {
		rowsErr = iter.rows.Close()
	}

	var txErr error
	if iter.tx != nil {
		txErr = iter.tx.Commit()
	}

	return errors.Join(rowsErr, txErr)
}

func (iter *varianceRowIterator) Scan() (*entities.VarianceReportRow, error) { //nolint:revive // implements interface
	if iter.rows == nil {
		return nil, errIteratorNotInitialized
	}

	var (
		sourceID        uuid.UUID
		feeScheduleID   uuid.UUID
		currency        string
		feeScheduleName string
		totalExpected   decimal.Decimal
		totalActual     decimal.Decimal
		netVariance     decimal.Decimal
	)

	if err := iter.rows.Scan(
		&sourceID,
		&currency,
		&feeScheduleID,
		&feeScheduleName,
		&totalExpected,
		&totalActual,
		&netVariance,
	); err != nil {
		return nil, fmt.Errorf("scanning variance row: %w", err)
	}

	return entities.BuildVarianceRow(
		sourceID,
		currency,
		feeScheduleID,
		feeScheduleName,
		totalExpected,
		totalActual,
		netVariance,
	), nil
}

// StreamMatchedForExport streams matched transactions for export.
func (repo *Repository) StreamMatchedForExport(
	ctx context.Context,
	filter entities.ReportFilter,
	maxRecords int,
) (repositories.MatchedRowIterator, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.stream_matched_for_export")

	tx, err := repo.provider.BeginTx(ctx)
	if err != nil {
		span.End()
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)

		libLog.SafeError(logger, ctx, "failed to begin transaction for streaming export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("begin streaming transaction: %w", err)
	}

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
		span.End()

		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			libLog.SafeError(logger, ctx, "failed to rollback after query build error", rollbackErr, runtime.IsProductionMode())
		}

		return nil, fmt.Errorf("building export query: %w", err)
	}

	//nolint:rowserrcheck // Iterator.Err() is called by the caller after iteration
	rows, err := tx.SQLTx().QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		span.End()

		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			libLog.SafeError(logger, ctx, "failed to rollback after query error", rollbackErr, runtime.IsProductionMode())
		}

		libOpentelemetry.HandleSpanError(span, "failed to query matched items", err)

		return nil, fmt.Errorf("querying matched items for streaming export: %w", err)
	}

	return &matchedRowIterator{rows: rows, tx: tx, span: span}, nil
}

// StreamUnmatchedForExport streams unmatched transactions for export.
func (repo *Repository) StreamUnmatchedForExport(
	ctx context.Context,
	filter entities.ReportFilter,
	maxRecords int,
) (repositories.UnmatchedRowIterator, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.stream_unmatched_for_export")

	tx, err := repo.provider.BeginTx(ctx)
	if err != nil {
		span.End()
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)

		libLog.SafeError(logger, ctx, "failed to begin transaction for streaming export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("begin streaming transaction: %w", err)
	}

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

	sqlQuery, args, err := query.ToSql()
	if err != nil {
		span.End()

		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			libLog.SafeError(logger, ctx, "failed to rollback after query build error", rollbackErr, runtime.IsProductionMode())
		}

		return nil, fmt.Errorf("building export query: %w", err)
	}

	//nolint:rowserrcheck // Iterator.Err() is called by the caller after iteration
	rows, err := tx.SQLTx().QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		span.End()

		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			libLog.SafeError(logger, ctx, "failed to rollback after query error", rollbackErr, runtime.IsProductionMode())
		}

		libOpentelemetry.HandleSpanError(span, "failed to query unmatched items", err)

		return nil, fmt.Errorf("querying unmatched items for streaming export: %w", err)
	}

	return &unmatchedRowIterator{rows: rows, tx: tx, span: span}, nil
}

// StreamVarianceForExport streams variance rows for export.
func (repo *Repository) StreamVarianceForExport(
	ctx context.Context,
	filter entities.VarianceReportFilter,
	maxRecords int,
) (repositories.VarianceRowIterator, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return nil, ErrContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.stream_variance_for_export")

	tx, err := repo.provider.BeginTx(ctx)
	if err != nil {
		span.End()
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)

		libLog.SafeError(logger, ctx, "failed to begin transaction for streaming export", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("begin streaming transaction: %w", err)
	}

	queryStr := varianceBaseQuery

	args := []any{filter.ContextID, filter.DateFrom, filter.DateTo}
	argIdx := 4

	if filter.SourceID != nil {
		queryStr += fmt.Sprintf(" AND t.source_id = $%d", argIdx)

		args = append(args, *filter.SourceID)
		argIdx++
	}

	queryStr += varianceGroupByClause
	queryStr += varianceOrderByClause
	queryStr += fmt.Sprintf(" LIMIT $%d", argIdx)

	args = append(args, safeExportLimit(maxRecords))

	//nolint:rowserrcheck // Iterator.Err() is called by the caller after iteration
	rows, err := tx.SQLTx().QueryContext(ctx, queryStr, args...)
	if err != nil {
		span.End()

		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			libLog.SafeError(logger, ctx, "failed to rollback after query error", rollbackErr, runtime.IsProductionMode())
		}

		libOpentelemetry.HandleSpanError(span, "failed to query variance rows", err)

		return nil, fmt.Errorf("querying variance rows for streaming export: %w", err)
	}

	return &varianceRowIterator{rows: rows, tx: tx, span: span}, nil
}

var _ repositories.StreamingReportRepository = (*Repository)(nil)
