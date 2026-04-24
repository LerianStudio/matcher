// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package transaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	repositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// FindByContextAndIDs retrieves transactions by context ID and transaction IDs.
func (repo *Repository) FindByContextAndIDs(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) ([]*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return nil, errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return []*shared.Transaction{}, nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_transactions_by_context_and_ids")

	defer span.End()

	stringIDs := make([]string, len(transactionIDs))
	for i, id := range transactionIDs {
		stringIDs[i] = id.String()
	}

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (transactions []*shared.Transaction, err error) {
			rawCols := strings.Split(transactionColumns, ", ")

			qualifiedCols := make([]string, len(rawCols))
			for i, col := range rawCols {
				qualifiedCols[i] = "t." + strings.TrimSpace(col)
			}

			queryBuilder := squirrel.Select(qualifiedCols...).
				From("transactions t").
				Join("ingestion_jobs j ON j.id = t.ingestion_job_id").
				Where(squirrel.Eq{"j.context_id": contextID.String()}).
				Where(squirrel.Eq{"t.id": stringIDs}).
				PlaceholderFormat(squirrel.Dollar)

			query, args, err := queryBuilder.ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build query: %w", err)
			}

			rows, err := tx.QueryContext(ctx, query, args...)
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
		libOpentelemetry.HandleSpanError(
			span,
			"failed to find transactions by context and ids",
			err,
		)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find transactions by context and ids")

		return nil, fmt.Errorf("failed to find transactions by context and ids: %w", err)
	}

	return result, nil
}

// FindBySourceAndExternalID retrieves a transaction by source ID and external ID.
func (repo *Repository) FindBySourceAndExternalID(
	ctx context.Context,
	sourceID uuid.UUID,
	externalID string,
) (*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_transaction_by_external_id")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*shared.Transaction, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT "+transactionColumns+" FROM transactions WHERE source_id = $1 AND external_id = $2",
				sourceID.String(),
				externalID,
			)

			return scanTransaction(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find transaction", err)

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find transaction")
		}

		return nil, fmt.Errorf("failed to find transaction by external ID: %w", err)
	}

	return result, nil
}

// SearchTransactions retrieves transactions matching search criteria within a context.
//
//nolint:cyclop,gocyclo // dynamic query building with multiple optional filters
func (repo *Repository) SearchTransactions(
	ctx context.Context,
	contextID uuid.UUID,
	params repositories.TransactionSearchParams,
) ([]*shared.Transaction, int64, error) {
	if repo == nil || repo.provider == nil {
		return nil, 0, errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return nil, 0, errContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.search_transactions")

	defer span.End()

	limit := params.Limit
	if limit <= 0 {
		limit = constants.DefaultPaginationLimit
	}

	const maxSearchLimit = 50
	if limit > maxSearchLimit {
		limit = maxSearchLimit
	}

	offset := max(params.Offset, 0)

	type searchResult struct {
		transactions []*shared.Transaction
		total        int64
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*searchResult, error) {
			baseWhere := buildSearchBaseWhere(contextID)

			dataQuery := squirrel.Select(strings.Split(transactionColumns, ", ")...).
				From("transactions").
				PlaceholderFormat(squirrel.Dollar)

			countQuery := squirrel.Select("COUNT(*)").
				From("transactions").
				PlaceholderFormat(squirrel.Dollar)

			for _, w := range baseWhere {
				dataQuery = dataQuery.Where(w)
				countQuery = countQuery.Where(w)
			}

			var filterErr error

			dataQuery, filterErr = applySearchFilters(dataQuery, params)
			if filterErr != nil {
				return nil, fmt.Errorf("apply search filters: %w", filterErr)
			}

			countQuery, filterErr = applySearchFilters(countQuery, params)
			if filterErr != nil {
				return nil, fmt.Errorf("apply search filters: %w", filterErr)
			}

			dataQuery = dataQuery.
				OrderBy("created_at DESC").
				Limit(sharedpg.SafeIntToUint64(limit)).
				Offset(sharedpg.SafeIntToUint64(offset))

			// Execute count query
			countSQL, countArgs, err := countQuery.ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build count SQL: %w", err)
			}

			var total int64

			if err := qe.QueryRowContext(ctx, countSQL, countArgs...).Scan(&total); err != nil {
				return nil, fmt.Errorf("failed to count transactions: %w", err)
			}

			if total == 0 {
				return &searchResult{transactions: []*shared.Transaction{}, total: 0}, nil
			}

			// Execute data query
			dataSQL, dataArgs, err := dataQuery.ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build data SQL: %w", err)
			}

			rows, err := qe.QueryContext(ctx, dataSQL, dataArgs...)
			if err != nil {
				return nil, fmt.Errorf("failed to query transactions: %w", err)
			}

			defer rows.Close()

			transactions, err := scanRowsToTransactions(rows, scanTransaction)
			if err != nil {
				return nil, err
			}

			return &searchResult{transactions: transactions, total: total}, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to search transactions", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to search transactions")

		return nil, 0, fmt.Errorf("failed to search transactions: %w", err)
	}

	return result.transactions, result.total, nil
}

func buildSearchBaseWhere(contextID uuid.UUID) []squirrel.Sqlizer {
	return []squirrel.Sqlizer{
		squirrel.Expr(
			"source_id IN (SELECT id FROM reconciliation_sources WHERE context_id = ?)",
			contextID.String(),
		),
		squirrel.Eq{"extraction_status": "COMPLETE"},
	}
}

// escapeLikePattern escapes special LIKE pattern characters.
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)

	return s
}

func applySearchFilters(
	qb squirrel.SelectBuilder,
	params repositories.TransactionSearchParams,
) (squirrel.SelectBuilder, error) {
	if params.Query != "" {
		pattern := "%" + escapeLikePattern(params.Query) + "%"
		qb = qb.Where(squirrel.Or{
			squirrel.ILike{"external_id": pattern},
			squirrel.ILike{"description": pattern},
		})
	}

	if params.Reference != "" {
		qb = qb.Where(squirrel.Eq{"external_id": params.Reference})
	}

	if params.AmountMin != nil {
		qb = qb.Where(squirrel.GtOrEq{"amount": *params.AmountMin})
	}

	if params.AmountMax != nil {
		qb = qb.Where(squirrel.LtOrEq{"amount": *params.AmountMax})
	}

	if params.DateFrom != nil {
		qb = qb.Where(squirrel.GtOrEq{"date": *params.DateFrom})
	}

	if params.DateTo != nil {
		qb = qb.Where(squirrel.LtOrEq{"date": *params.DateTo})
	}

	if params.Currency != "" {
		qb = qb.Where(squirrel.Eq{"currency": strings.ToUpper(params.Currency)})
	}

	if params.SourceID != nil && *params.SourceID != uuid.Nil {
		qb = qb.Where(squirrel.Eq{"source_id": params.SourceID.String()})
	}

	if params.Status != "" {
		normalized := strings.ToUpper(params.Status)

		if _, err := shared.ParseTransactionStatus(normalized); err != nil {
			return qb, fmt.Errorf("invalid status filter %q: %w", params.Status, shared.ErrInvalidTransactionStatus)
		}

		qb = qb.Where(squirrel.Eq{"status": normalized})
	}

	return qb, nil
}
