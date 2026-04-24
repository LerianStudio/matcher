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
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

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

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to create transaction")

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

				insertedIDs = append(insertedIDs, model.ID.String())
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

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to create transaction batch")

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

			logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to find transaction")
		}

		return nil, fmt.Errorf("failed to find transaction: %w", err)
	}

	return result, nil
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

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to update transaction status")

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
