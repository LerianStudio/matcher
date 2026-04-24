// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package transaction

import (
	"database/sql"
	"fmt"
	"time"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Column name constants for sort operations.
const (
	columnCreatedAt        = "created_at"
	columnDate             = "date"
	columnStatus           = "status"
	columnExtractionStatus = "extraction_status"

	// defaultTransactionPaginationLimit is intentionally higher than the generic
	// shared default because transaction listings are a high-volume read path.
	defaultTransactionPaginationLimit = 50
)

func transactionSortValue(tx *shared.Transaction, column string) string {
	if tx == nil {
		return ""
	}

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
