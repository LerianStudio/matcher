// Package transaction provides PostgreSQL repository for transactions.
package transaction

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// NewTransactionPostgreSQLModel converts an entity to a PostgreSQL model.
//
//nolint:cyclop // field mapping requires multiple nil checks
func NewTransactionPostgreSQLModel(
	entity *shared.Transaction,
) (*pgcommon.TransactionPostgreSQLModel, error) {
	if entity == nil {
		return nil, errTxEntityRequired
	}

	id := entity.ID
	if id == uuid.Nil {
		id = uuid.New()
	}

	createdAt := entity.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	updatedAt := entity.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}

	amountBase := decimal.NullDecimal{}
	if entity.AmountBase != nil {
		amountBase = decimal.NullDecimal{Decimal: *entity.AmountBase, Valid: true}
	}

	baseCurrency := sql.NullString{}
	if entity.BaseCurrency != nil {
		baseCurrency = sql.NullString{String: *entity.BaseCurrency, Valid: true}
	}

	fxRate := decimal.NullDecimal{}
	if entity.FXRate != nil {
		fxRate = decimal.NullDecimal{Decimal: *entity.FXRate, Valid: true}
	}

	fxRateSource := sql.NullString{}
	if entity.FXRateSource != nil {
		fxRateSource = sql.NullString{String: *entity.FXRateSource, Valid: true}
	}

	fxRateDate := sql.NullTime{}
	if entity.FXRateEffDate != nil {
		fxRateDate = sql.NullTime{Time: *entity.FXRateEffDate, Valid: true}
	}

	extractionStatus := entity.ExtractionStatus
	if !extractionStatus.IsValid() {
		return nil, fmt.Errorf("%w: %s", errInvalidExtractionStatus, extractionStatus)
	}

	status := entity.Status
	if !status.IsValid() {
		return nil, fmt.Errorf("%w: %s", errInvalidTxStatus, status)
	}

	metadata := entity.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize transaction metadata: %w", err)
	}

	return &pgcommon.TransactionPostgreSQLModel{
		ID:                  id,
		IngestionJobID:      entity.IngestionJobID,
		SourceID:            entity.SourceID,
		ExternalID:          entity.ExternalID,
		Amount:              entity.Amount,
		Currency:            entity.Currency,
		AmountBase:          amountBase,
		BaseCurrency:        baseCurrency,
		FXRate:              fxRate,
		FXRateSource:        fxRateSource,
		FXRateEffectiveDate: fxRateDate,
		ExtractionStatus:    extractionStatus.String(),
		Date:                entity.Date,
		Description: sql.NullString{
			String: entity.Description,
			Valid:  entity.Description != "",
		},
		Status:    status.String(),
		Metadata:  metadataJSON,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func transactionModelToEntity(
	model *pgcommon.TransactionPostgreSQLModel,
) (*shared.Transaction, error) {
	if model == nil {
		return nil, errTxModelRequired
	}

	extractionStatus, err := shared.ParseExtractionStatus(model.ExtractionStatus)
	if err != nil {
		return nil, fmt.Errorf("parsing ExtractionStatus '%s': %w", model.ExtractionStatus, err)
	}

	status, err := shared.ParseTransactionStatus(model.Status)
	if err != nil {
		return nil, fmt.Errorf("parsing Status '%s': %w", model.Status, err)
	}

	metadata := make(map[string]any)
	if len(model.Metadata) > 0 {
		if err := json.Unmarshal(model.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("parsing Metadata: %w", err)
		}
	}

	transaction := &shared.Transaction{
		ID:               model.ID,
		IngestionJobID:   model.IngestionJobID,
		SourceID:         model.SourceID,
		ExternalID:       model.ExternalID,
		Amount:           model.Amount,
		Currency:         model.Currency,
		ExtractionStatus: extractionStatus,
		Status:           status,
		Date:             model.Date,
		Description:      model.Description.String,
		Metadata:         metadata,
		CreatedAt:        model.CreatedAt,
		UpdatedAt:        model.UpdatedAt,
	}

	if model.AmountBase.Valid {
		amountBase := model.AmountBase.Decimal
		transaction.AmountBase = &amountBase
	}

	if model.BaseCurrency.Valid {
		baseCurrency := model.BaseCurrency.String
		transaction.BaseCurrency = &baseCurrency
	}

	if model.FXRate.Valid {
		fxRate := model.FXRate.Decimal
		transaction.FXRate = &fxRate
	}

	if model.FXRateSource.Valid {
		fxRateSource := model.FXRateSource.String
		transaction.FXRateSource = &fxRateSource
	}

	if model.FXRateEffectiveDate.Valid {
		fxRateDate := model.FXRateEffectiveDate.Time
		transaction.FXRateEffDate = &fxRateDate
	}

	return transaction, nil
}
