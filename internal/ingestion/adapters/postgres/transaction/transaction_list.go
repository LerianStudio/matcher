// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package transaction

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	repositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// FindByJobID retrieves transactions by job ID with cursor pagination.
//
//nolint:cyclop // pagination logic is inherently complex
func (repo *Repository) FindByJobID(
	ctx context.Context,
	jobID uuid.UUID,
	filter repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, errTxRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_transactions_by_job")

	defer span.End()

	var pagination libHTTP.CursorPagination

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (transactions []*shared.Transaction, err error) {
			orderDirection := libHTTP.ValidateSortDirection(filter.SortOrder)

			sortColumn := normalizeTransactionSortColumn(filter.SortBy)

			limit := libHTTP.ValidateLimit(
				filter.Limit,
				defaultTransactionPaginationLimit,
				constants.MaximumPaginationLimit,
			)

			useIDCursor := sortColumn == "id"

			findAll := squirrel.Select(strings.Split(transactionColumns, ", ")...).
				From("transactions").
				Where(squirrel.Eq{"ingestion_job_id": jobID.String()}).
				PlaceholderFormat(squirrel.Dollar)

			var cursorDirection string

			findAll, _, cursorDirection, err = pgcommon.ApplyCursorPagination(
				findAll, filter.Cursor, sortColumn, orderDirection, limit, useIDCursor, "transactions",
			)
			if err != nil {
				return nil, fmt.Errorf("apply cursor pagination: %w", err)
			}

			query, args, err := findAll.ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build SQL: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to query transactions: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			transactions = make([]*shared.Transaction, 0, limit+1)

			for rows.Next() {
				transaction, err := scanTransaction(rows)
				if err != nil {
					return nil, err
				}

				transactions = append(transactions, transaction)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate rows: %w", err)
			}

			hasPagination := len(transactions) > limit
			isFirstPage := filter.Cursor == "" || (!hasPagination && cursorDirection == libHTTP.CursorDirectionPrev)

			transactions = libHTTP.PaginateRecords(
				isFirstPage,
				hasPagination,
				cursorDirection,
				transactions,
				limit,
			)

			pagination, err = calculateTransactionPagination(
				transactions, useIDCursor, isFirstPage, hasPagination, cursorDirection, sortColumn,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate cursor: %w", err)
			}

			return transactions, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list transactions", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list transactions")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf(
			"failed to list transactions by job: %w",
			err,
		)
	}

	return result, pagination, nil
}

// FindByJobAndContextID retrieves transactions by job ID and context ID with cursor pagination.
//
//nolint:cyclop // pagination logic is inherently complex
func (repo *Repository) FindByJobAndContextID(
	ctx context.Context,
	jobID, contextID uuid.UUID,
	filter repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	if repo == nil || repo.provider == nil {
		return nil, libHTTP.CursorPagination{}, errTxRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_transactions_by_job_context")

	defer span.End()

	var pagination libHTTP.CursorPagination

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (transactions []*shared.Transaction, err error) {
			orderDirection := libHTTP.ValidateSortDirection(filter.SortOrder)

			sortColumn := normalizeTransactionSortColumn(filter.SortBy)

			limit := libHTTP.ValidateLimit(
				filter.Limit,
				defaultTransactionPaginationLimit,
				constants.MaximumPaginationLimit,
			)

			useIDCursor := sortColumn == "id"

			findAll := squirrel.Select(strings.Split(transactionColumns, ", ")...).
				From("transactions").
				Where(squirrel.Eq{"ingestion_job_id": jobID.String()}).
				Where(squirrel.Expr("source_id IN (SELECT id FROM reconciliation_sources WHERE context_id = ?)", contextID.String())).
				PlaceholderFormat(squirrel.Dollar)

			var cursorDirection string

			findAll, _, cursorDirection, err = pgcommon.ApplyCursorPagination(
				findAll, filter.Cursor, sortColumn, orderDirection, limit, useIDCursor, "transactions",
			)
			if err != nil {
				return nil, fmt.Errorf("apply cursor pagination: %w", err)
			}

			query, args, err := findAll.ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build SQL: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to query transactions: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			transactions = make([]*shared.Transaction, 0, limit+1)

			for rows.Next() {
				transaction, err := scanTransaction(rows)
				if err != nil {
					return nil, err
				}

				transactions = append(transactions, transaction)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate rows: %w", err)
			}

			hasPagination := len(transactions) > limit
			isFirstPage := filter.Cursor == "" || (!hasPagination && cursorDirection == libHTTP.CursorDirectionPrev)

			transactions = libHTTP.PaginateRecords(
				isFirstPage,
				hasPagination,
				cursorDirection,
				transactions,
				limit,
			)

			pagination, err = calculateTransactionPagination(
				transactions, useIDCursor, isFirstPage, hasPagination, cursorDirection, sortColumn,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate cursor: %w", err)
			}

			return transactions, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list transactions", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list transactions")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf(
			"failed to list transactions by job and context: %w",
			err,
		)
	}

	return result, pagination, nil
}

// ListUnmatchedByContext retrieves unmatched transactions by context ID.
func (repo *Repository) ListUnmatchedByContext(
	ctx context.Context,
	contextID uuid.UUID,
	startInclusive, endInclusive *time.Time,
	limit, offset int,
) ([]*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if err := validateListUnmatchedParams(contextID, limit, offset); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_unmatched_transactions_by_context")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (transactions []*shared.Transaction, err error) {
			query, args, err := buildUnmatchedByContextQuery(
				contextID,
				startInclusive,
				endInclusive,
				limit,
				offset,
			)
			if err != nil {
				return nil, err
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to query transactions: %w", err)
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			return scanRowsToTransactions(rows, scanTransaction)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list unmatched transactions", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to list unmatched transactions")

		return nil, fmt.Errorf("failed to list unmatched transactions: %w", err)
	}

	return result, nil
}

// calculateTransactionPagination computes cursor pagination metadata for a transaction result set.
func calculateTransactionPagination(
	transactions []*shared.Transaction,
	useIDCursor, isFirstPage, hasPagination bool,
	cursorDirection string,
	sortColumn string,
) (libHTTP.CursorPagination, error) {
	if len(transactions) == 0 {
		return libHTTP.CursorPagination{}, nil
	}

	first, last := transactions[0], transactions[len(transactions)-1]
	if err := sharedpg.ValidateSortCursorBoundaries(first, last); err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("validate transaction pagination boundaries: %w", err)
	}

	if useIDCursor {
		pagination, err := libHTTP.CalculateCursor(
			isFirstPage, hasPagination, cursorDirection,
			first.ID.String(), last.ID.String(),
		)
		if err != nil {
			return libHTTP.CursorPagination{}, fmt.Errorf("calculate cursor: %w", err)
		}

		return pagination, nil
	}

	return calculateTransactionSortPagination(
		isFirstPage,
		hasPagination,
		cursorDirection == libHTTP.CursorDirectionNext,
		sortColumn,
		transactionSortValue(first, sortColumn),
		first.ID.String(),
		transactionSortValue(last, sortColumn),
		last.ID.String(),
		libHTTP.CalculateSortCursorPagination,
	)
}

func calculateTransactionSortPagination(
	isFirstPage, hasPagination, pointsNext bool,
	sortColumn,
	firstSortValue,
	firstID,
	lastSortValue,
	lastID string,
	calculateSortCursor sharedpg.SortCursorCalculator,
) (libHTTP.CursorPagination, error) {
	pagination, err := sharedpg.CalculateSortCursorPaginationWrapped(
		isFirstPage,
		hasPagination,
		pointsNext,
		sortColumn,
		firstSortValue,
		firstID,
		lastSortValue,
		lastID,
		calculateSortCursor,
		"calculate sort cursor pagination",
	)
	if err != nil {
		return libHTTP.CursorPagination{}, fmt.Errorf("calculate transaction sort cursor pagination: %w", err)
	}

	return pagination, nil
}

// allowedTransactionSortColumns lists columns valid for sort operations.
var allowedTransactionSortColumns = []string{"id", columnCreatedAt, columnDate, columnStatus, columnExtractionStatus}

func normalizeTransactionSortColumn(sortBy string) string {
	return libHTTP.ValidateSortColumn(strings.TrimSpace(sortBy), allowedTransactionSortColumns, "id")
}

func validateListUnmatchedParams(contextID uuid.UUID, limit, offset int) error {
	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if limit <= 0 {
		return errLimitMustBePositive
	}

	if offset < 0 {
		return errOffsetMustBeNonNegative
	}

	return nil
}

func buildUnmatchedByContextQuery(
	contextID uuid.UUID,
	startInclusive, endInclusive *time.Time,
	limit, offset int,
) (string, []any, error) {
	queryBuilder := squirrel.Select(strings.Split(transactionColumns, ", ")...).
		From("transactions").
		Where(squirrel.Expr("source_id IN (SELECT id FROM reconciliation_sources WHERE context_id = ?)", contextID.String())).
		Where(squirrel.Eq{"extraction_status": "COMPLETE"}).
		Where(squirrel.Eq{"status": "UNMATCHED"}).
		OrderBy("date ASC", "id ASC").
		Limit(sharedpg.SafeIntToUint64(limit)).
		Offset(sharedpg.SafeIntToUint64(offset)).
		PlaceholderFormat(squirrel.Dollar)

	if startInclusive != nil {
		queryBuilder = queryBuilder.Where(squirrel.GtOrEq{"date": *startInclusive})
	}

	if endInclusive != nil {
		queryBuilder = queryBuilder.Where(squirrel.LtOrEq{"date": *endInclusive})
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return "", nil, fmt.Errorf("failed to build SQL: %w", err)
	}

	return query, args, nil
}
