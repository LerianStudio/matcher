package transaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	repositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Column name constants for sort operations.
const (
	columnCreatedAt        = "created_at"
	columnDate             = "date"
	columnStatus           = "status"
	columnExtractionStatus = "extraction_status"
)

func transactionSortValue(tx *shared.Transaction, column string) string {
	switch column {
	case columnCreatedAt:
		return tx.CreatedAt.UTC().Format(time.RFC3339Nano)
	case columnDate:
		return tx.Date.UTC().Format(time.RFC3339Nano)
	case columnStatus:
		return tx.Status.String()
	case columnExtractionStatus:
		return tx.ExtractionStatus.String()
	default:
		return tx.ID.String()
	}
}

const transactionColumns = "id, ingestion_job_id, source_id, external_id, amount, currency, amount_base, base_currency, fx_rate, fx_rate_source, fx_rate_effective_date, extraction_status, date, description, status, metadata, created_at, updated_at"

// Repository is a PostgreSQL implementation of TransactionRepository.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new transaction repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// Create persists a new transaction.
func (repo *Repository) Create(
	ctx context.Context,
	txEntity *shared.Transaction,
) (*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if txEntity == nil {
		return nil, errTxEntityRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_transaction")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*shared.Transaction, error) {
			return repo.executeCreate(ctx, tx, txEntity)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create transaction", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create transaction")

		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	return result, nil
}

// CreateWithTx persists a new transaction within an existing transaction.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	txEntity *shared.Transaction,
) (*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if txEntity == nil {
		return nil, errTxEntityRequired
	}

	if tx == nil {
		return nil, errTxRequired
	}

	return repo.executeCreate(ctx, tx, txEntity)
}

// executeCreate performs the actual transaction creation within a database transaction.
func (repo *Repository) executeCreate(
	ctx context.Context,
	tx *sql.Tx,
	txEntity *shared.Transaction,
) (*shared.Transaction, error) {
	model, err := NewTransactionPostgreSQLModel(txEntity)
	if err != nil {
		return nil, err
	}

	query := `INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, amount_base, base_currency, fx_rate, fx_rate_source, fx_rate_effective_date, extraction_status, date, description, status, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`

	_, err = tx.ExecContext(ctx, query,
		model.ID,
		model.IngestionJobID,
		model.SourceID,
		model.ExternalID,
		model.Amount,
		model.Currency,
		model.AmountBase,
		model.BaseCurrency,
		model.FXRate,
		model.FXRateSource,
		model.FXRateEffectiveDate,
		model.ExtractionStatus,
		model.Date,
		model.Description,
		model.Status,
		model.Metadata,
		model.CreatedAt,
		model.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert transaction: %w", err)
	}

	row := tx.QueryRowContext(
		ctx,
		"SELECT "+transactionColumns+" FROM transactions WHERE id = $1",
		model.ID,
	)

	return scanTransaction(row)
}

// CreateBatch persists multiple transactions.
func (repo *Repository) CreateBatch(
	ctx context.Context,
	txs []*shared.Transaction,
) ([]*shared.Transaction, error) {
	return repo.createBatch(ctx, nil, txs)
}

// CreateBatchWithTx persists multiple transactions within a transaction.
func (repo *Repository) CreateBatchWithTx(
	ctx context.Context,
	tx *sql.Tx,
	txs []*shared.Transaction,
) ([]*shared.Transaction, error) {
	return repo.createBatch(ctx, tx, txs)
}

func (repo *Repository) createBatch(
	ctx context.Context,
	tx *sql.Tx,
	txs []*shared.Transaction,
) ([]*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if len(txs) == 0 {
		return nil, nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_transaction_batch")

	defer span.End()

	created, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(execTx *sql.Tx) ([]*shared.Transaction, error) {
			stmt, err := execTx.PrepareContext(
				ctx,
				`INSERT INTO transactions (id, ingestion_job_id, source_id, external_id, amount, currency, amount_base, base_currency, fx_rate, fx_rate_source, fx_rate_effective_date, extraction_status, date, description, status, metadata, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to prepare statement: %w", err)
			}
			defer stmt.Close()

			insertedIDs := make([]string, 0, len(txs))

			for _, entity := range txs {
				model, err := NewTransactionPostgreSQLModel(entity)
				if err != nil {
					return nil, err
				}

				_, err = stmt.ExecContext(ctx,
					model.ID,
					model.IngestionJobID,
					model.SourceID,
					model.ExternalID,
					model.Amount,
					model.Currency,
					model.AmountBase,
					model.BaseCurrency,
					model.FXRate,
					model.FXRateSource,
					model.FXRateEffectiveDate,
					model.ExtractionStatus,
					model.Date,
					model.Description,
					model.Status,
					model.Metadata,
					model.CreatedAt,
					model.UpdatedAt,
				)
				if err != nil {
					return nil, fmt.Errorf("failed to execute statement: %w", err)
				}

				insertedIDs = append(insertedIDs, model.ID)
			}

			query, args, err := squirrel.Select(strings.Split(transactionColumns, ", ")...).
				From("transactions").
				Where(squirrel.Eq{"id": insertedIDs}).
				OrderBy("created_at ASC").
				PlaceholderFormat(squirrel.Dollar).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("failed to build batch select query: %w", err)
			}

			rows, err := execTx.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch created transactions: %w", err)
			}
			defer rows.Close()

			return scanRowsToTransactions(rows, scanTransaction)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create transaction batch", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create transaction batch")

		return nil, fmt.Errorf("failed to create batch: %w", err)
	}

	return created, nil
}

// FindByID retrieves a transaction by its ID.
func (repo *Repository) FindByID(ctx context.Context, id uuid.UUID) (*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.find_transaction_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*shared.Transaction, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+transactionColumns+" FROM transactions WHERE id = $1",
				id.String(),
			)

			return scanTransaction(row)
		},
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to find transaction", err)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find transaction")
		}

		return nil, fmt.Errorf("failed to find transaction: %w", err)
	}

	return result, nil
}

// FindByJobID retrieves transactions by job ID with cursor pagination.
//
//nolint:gocyclo,cyclop // pagination logic is inherently complex
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

			limit := filter.Limit
			if limit <= 0 {
				limit = 50
			}

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

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list transactions")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf(
			"failed to list transactions by job: %w",
			err,
		)
	}

	return result, pagination, nil
}

// FindByJobAndContextID retrieves transactions by job ID and context ID with cursor pagination.
//
//nolint:gocyclo,cyclop // pagination logic is inherently complex
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

			limit := filter.Limit
			if limit <= 0 {
				limit = 50
			}

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

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list transactions")

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

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list unmatched transactions")

		return nil, fmt.Errorf("failed to list unmatched transactions: %w", err)
	}

	return result, nil
}

// MarkMatched marks transactions as matched by their IDs.
func (repo *Repository) MarkMatched(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_matched")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeMarkMatched(ctx, tx, contextID, transactionIDs)
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark transactions matched", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to mark transactions matched")

		return fmt.Errorf("failed to mark transactions matched: %w", err)
	}

	return nil
}

// MarkMatchedWithTx marks transactions as matched within an existing transaction.
func (repo *Repository) MarkMatchedWithTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if tx == nil {
		return errTxRequired
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	return repo.executeMarkMatched(ctx, tx, contextID, transactionIDs)
}

// executeMarkMatched performs the actual mark matched operation within a database transaction.
func (repo *Repository) executeMarkMatched(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	queryBuilder, err := BuildMarkMatchedQuery(contextID, transactionIDs)
	if err != nil {
		return fmt.Errorf("build mark matched query: %w", err)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build SQL: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

// MarkPendingReview marks transactions as pending review by their IDs.
func (repo *Repository) MarkPendingReview(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_pending_review")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeMarkPendingReview(ctx, tx, contextID, transactionIDs)
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark transactions pending review", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to mark transactions pending review")

		return fmt.Errorf("failed to mark transactions pending review: %w", err)
	}

	return nil
}

// MarkPendingReviewWithTx marks transactions as pending review within an existing transaction.
func (repo *Repository) MarkPendingReviewWithTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if tx == nil {
		return errTxRequired
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	return repo.executeMarkPendingReview(ctx, tx, contextID, transactionIDs)
}

// executeMarkPendingReview performs the actual mark pending review operation within a database transaction.
func (repo *Repository) executeMarkPendingReview(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	queryBuilder, err := BuildMarkPendingReviewQuery(contextID, transactionIDs)
	if err != nil {
		return fmt.Errorf("build mark pending review query: %w", err)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build SQL: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
}

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

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find transactions by context and ids")

		return nil, fmt.Errorf("failed to find transactions by context and ids: %w", err)
	}

	return result, nil
}

// MarkUnmatched marks transactions as unmatched by their IDs.
func (repo *Repository) MarkUnmatched(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.mark_transactions_unmatched")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) (struct{}, error) {
		return struct{}{}, repo.executeMarkUnmatched(ctx, tx, contextID, transactionIDs)
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark transactions unmatched", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to mark transactions unmatched")

		return fmt.Errorf("failed to mark transactions unmatched: %w", err)
	}

	return nil
}

// MarkUnmatchedWithTx marks transactions as unmatched within an existing transaction.
func (repo *Repository) MarkUnmatchedWithTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if tx == nil {
		return errTxRequired
	}

	if contextID == uuid.Nil {
		return errContextIDRequired
	}

	if len(transactionIDs) == 0 {
		return nil
	}

	return repo.executeMarkUnmatched(ctx, tx, contextID, transactionIDs)
}

// executeMarkUnmatched performs the actual mark unmatched operation within a database transaction.
func (repo *Repository) executeMarkUnmatched(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	queryBuilder, err := BuildMarkUnmatchedQuery(contextID, transactionIDs)
	if err != nil {
		return fmt.Errorf("build mark unmatched query: %w", err)
	}

	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build SQL: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return nil
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

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find transaction")
		}

		return nil, fmt.Errorf("failed to find transaction by external ID: %w", err)
	}

	return result, nil
}

// ExistsBySourceAndExternalID checks if a transaction exists by source ID and external ID.
func (repo *Repository) ExistsBySourceAndExternalID(
	ctx context.Context,
	sourceID uuid.UUID,
	externalID string,
) (bool, error) {
	if repo == nil || repo.provider == nil {
		return false, errTxRepoNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exists_transaction_by_external_id")

	defer span.End()

	exists, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (bool, error) {
			row := tx.QueryRowContext(
				ctx,
				"SELECT EXISTS (SELECT 1 FROM transactions WHERE source_id = $1 AND external_id = $2)",
				sourceID.String(),
				externalID,
			)

			var result bool

			if err := row.Scan(&result); err != nil {
				return false, fmt.Errorf("failed to scan result: %w", err)
			}

			return result, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to check transaction existence", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to check transaction existence")

		return false, fmt.Errorf("failed to check transaction existence: %w", err)
	}

	return exists, nil
}

// ExistsBulkBySourceAndExternalID checks existence for multiple source/external ID pairs in a single query.
func (repo *Repository) ExistsBulkBySourceAndExternalID(
	ctx context.Context,
	keys []repositories.ExternalIDKey,
) (map[repositories.ExternalIDKey]bool, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if len(keys) == 0 {
		return make(map[repositories.ExternalIDKey]bool), nil
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.exists_bulk_transaction_by_external_id")

	defer span.End()

	const argsPerKey = 2

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (map[repositories.ExternalIDKey]bool, error) {
			existsMap := make(map[repositories.ExternalIDKey]bool, len(keys))

			valueStrings := make([]string, 0, len(keys))
			valueArgs := make([]any, 0, len(keys)*argsPerKey)

			for i, key := range keys {
				valueStrings = append(
					valueStrings,
					fmt.Sprintf("($%d, $%d)", i*argsPerKey+1, i*argsPerKey+argsPerKey),
				)
				valueArgs = append(valueArgs, key.SourceID.String(), key.ExternalID)
			}

			// #nosec G201 -- false positive - valueStrings only contains placeholders ($1, $2, etc.)
			query := fmt.Sprintf(`
			SELECT t.source_id, t.external_id
			FROM transactions t
			INNER JOIN (VALUES %s) AS v(source_id, external_id)
			ON t.source_id = v.source_id::uuid AND t.external_id = v.external_id
		`, strings.Join(valueStrings, ", "))

			rows, err := tx.QueryContext(ctx, query, valueArgs...)
			if err != nil {
				return nil, fmt.Errorf("failed to query existing transactions: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var sourceIDStr, externalID string
				if err := rows.Scan(&sourceIDStr, &externalID); err != nil {
					return nil, fmt.Errorf("failed to scan row: %w", err)
				}

				sourceID, err := uuid.Parse(sourceIDStr)
				if err != nil {
					return nil, fmt.Errorf("failed to parse source ID: %w", err)
				}

				existsMap[repositories.ExternalIDKey{SourceID: sourceID, ExternalID: externalID}] = true
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("failed to iterate rows: %w", err)
			}

			return existsMap, nil
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to check bulk transaction existence", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to check bulk transaction existence")

		return nil, fmt.Errorf("failed to check bulk transaction existence: %w", err)
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

	if useIDCursor {
		pagination, err := libHTTP.CalculateCursor(
			isFirstPage, hasPagination, cursorDirection,
			transactions[0].ID.String(), transactions[len(transactions)-1].ID.String(),
		)
		if err != nil {
			return libHTTP.CursorPagination{}, fmt.Errorf("calculate cursor: %w", err)
		}

		return pagination, nil
	}

	first, last := transactions[0], transactions[len(transactions)-1]

	next, prev := libHTTP.CalculateSortCursorPagination(
		isFirstPage, hasPagination, cursorDirection == libHTTP.CursorDirectionNext,
		sortColumn,
		transactionSortValue(first, sortColumn), first.ID.String(),
		transactionSortValue(last, sortColumn), last.ID.String(),
	)

	return libHTTP.CursorPagination{Next: next, Prev: prev}, nil
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

// BuildMarkMatchedQuery delegates to shared infrastructure.
func BuildMarkMatchedQuery(contextID uuid.UUID, transactionIDs []uuid.UUID) (squirrel.UpdateBuilder, error) {
	builder, err := sharedpg.BuildMarkMatchedQuery(contextID, transactionIDs)
	if err != nil {
		return builder, fmt.Errorf("build mark matched query: %w", err)
	}

	return builder, nil
}

// BuildMarkPendingReviewQuery delegates to shared infrastructure.
func BuildMarkPendingReviewQuery(
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) (squirrel.UpdateBuilder, error) {
	builder, err := sharedpg.BuildMarkPendingReviewQuery(contextID, transactionIDs)
	if err != nil {
		return builder, fmt.Errorf("build mark pending review query: %w", err)
	}

	return builder, nil
}

// BuildMarkUnmatchedQuery delegates to shared infrastructure.
func BuildMarkUnmatchedQuery(
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) (squirrel.UpdateBuilder, error) {
	builder, err := sharedpg.BuildMarkUnmatchedQuery(contextID, transactionIDs)
	if err != nil {
		return builder, fmt.Errorf("build mark unmatched query: %w", err)
	}

	return builder, nil
}

func scanRowsToTransactions(
	rows *sql.Rows,
	scanFn func(scanner interface{ Scan(dest ...any) error }) (*shared.Transaction, error),
) ([]*shared.Transaction, error) {
	const defaultBatchCapacity = 64

	transactions := make([]*shared.Transaction, 0, defaultBatchCapacity)

	for rows.Next() {
		transaction, err := scanFn(rows)
		if err != nil {
			return nil, err
		}

		transactions = append(transactions, transaction)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate rows: %w", err)
	}

	return transactions, nil
}

// UpdateStatus updates the status of a transaction within a context.
func (repo *Repository) UpdateStatus(
	ctx context.Context,
	id, contextID uuid.UUID,
	status shared.TransactionStatus,
) (*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if contextID == uuid.Nil {
		return nil, errContextIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.update_transaction_status")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*shared.Transaction, error) {
			return repo.executeUpdateStatus(ctx, tx, id, contextID, status)
		},
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update transaction status", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update transaction status")

		return nil, fmt.Errorf("failed to update transaction status: %w", err)
	}

	return result, nil
}

// UpdateStatusWithTx updates the status of a transaction within an existing transaction.
func (repo *Repository) UpdateStatusWithTx(
	ctx context.Context,
	tx *sql.Tx,
	id, contextID uuid.UUID,
	status shared.TransactionStatus,
) (*shared.Transaction, error) {
	if repo == nil || repo.provider == nil {
		return nil, errTxRepoNotInit
	}

	if tx == nil {
		return nil, errTxRequired
	}

	if contextID == uuid.Nil {
		return nil, errContextIDRequired
	}

	return repo.executeUpdateStatus(ctx, tx, id, contextID, status)
}

// executeUpdateStatus performs the actual status update within a database transaction.
func (repo *Repository) executeUpdateStatus(
	ctx context.Context,
	tx *sql.Tx,
	id, contextID uuid.UUID,
	status shared.TransactionStatus,
) (*shared.Transaction, error) {
	query := `UPDATE transactions 
		SET status = $1, updated_at = NOW() 
		WHERE id = $2 
		AND source_id IN (SELECT id FROM reconciliation_sources WHERE context_id = $3)
		RETURNING ` + transactionColumns

	row := tx.QueryRowContext(ctx, query, status.String(), id.String(), contextID.String())

	return scanTransaction(row)
}

// CleanupFailedJobTransactionsWithTx marks UNMATCHED transactions for a failed
// ingestion job as IGNORED inside an existing transaction.
func (repo *Repository) CleanupFailedJobTransactionsWithTx(
	ctx context.Context,
	tx *sql.Tx,
	jobID uuid.UUID,
) error {
	if repo == nil || repo.provider == nil {
		return errTxRepoNotInit
	}

	if tx == nil {
		return errTxRequired
	}

	if jobID == uuid.Nil {
		return errJobIDRequired
	}

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		return fmt.Errorf("apply tenant schema: %w", err)
	}

	query := `UPDATE transactions
		SET status = $1, updated_at = $2
		WHERE ingestion_job_id = $3 AND status = $4`

	if _, err := tx.ExecContext(
		ctx,
		query,
		shared.TransactionStatusIgnored.String(),
		time.Now().UTC(),
		jobID,
		shared.TransactionStatusUnmatched.String(),
	); err != nil {
		return fmt.Errorf("cleanup failed job transactions: %w", err)
	}

	return nil
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
		limit = 20
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

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to search transactions")

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

func scanTransaction(scanner interface{ Scan(dest ...any) error }) (*shared.Transaction, error) {
	var model pgcommon.TransactionPostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.IngestionJobID,
		&model.SourceID,
		&model.ExternalID,
		&model.Amount,
		&model.Currency,
		&model.AmountBase,
		&model.BaseCurrency,
		&model.FXRate,
		&model.FXRateSource,
		&model.FXRateEffectiveDate,
		&model.ExtractionStatus,
		&model.Date,
		&model.Description,
		&model.Status,
		&model.Metadata,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("failed to scan transaction: %w", err)
	}

	return transactionModelToEntity(&model)
}
