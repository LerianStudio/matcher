// Package fee_variance provides PostgreSQL persistence adapters for fee variance entities.
package fee_variance

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// PostgreSQLModel represents the match_fee_variances table mapping.
type PostgreSQLModel struct {
	ID            string
	ContextID     string
	RunID         string
	MatchGroupID  string
	TransactionID string
	RateID        string
	Currency      string
	ExpectedFee   decimal.Decimal
	ActualFee     decimal.Decimal
	Delta         decimal.Decimal
	ToleranceAbs  decimal.Decimal
	TolerancePct  decimal.Decimal
	VarianceType  string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewPostgreSQLModel converts a fee variance entity to a PostgreSQLModel.
func NewPostgreSQLModel(entity *matchingEntities.FeeVariance) (*PostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrFeeVarianceEntityNeeded
	}

	return &PostgreSQLModel{
		ID:            entity.ID.String(),
		ContextID:     entity.ContextID.String(),
		RunID:         entity.RunID.String(),
		MatchGroupID:  entity.MatchGroupID.String(),
		TransactionID: entity.TransactionID.String(),
		RateID:        entity.RateID.String(),
		Currency:      entity.Currency,
		ExpectedFee:   entity.ExpectedFee,
		ActualFee:     entity.ActualFee,
		Delta:         entity.Delta,
		ToleranceAbs:  entity.ToleranceAbs,
		TolerancePct:  entity.TolerancePct,
		VarianceType:  entity.VarianceType,
		CreatedAt:     entity.CreatedAt,
		UpdatedAt:     entity.UpdatedAt,
	}, nil
}

// ToEntity converts a PostgreSQLModel to a fee variance entity.
func (model *PostgreSQLModel) ToEntity() (*matchingEntities.FeeVariance, error) {
	if model == nil {
		return nil, ErrFeeVarianceModelNeeded
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}

	contextID, err := uuid.Parse(model.ContextID)
	if err != nil {
		return nil, fmt.Errorf("parse context id: %w", err)
	}

	runID, err := uuid.Parse(model.RunID)
	if err != nil {
		return nil, fmt.Errorf("parse run id: %w", err)
	}

	matchGroupID, err := uuid.Parse(model.MatchGroupID)
	if err != nil {
		return nil, fmt.Errorf("parse match group id: %w", err)
	}

	transactionID, err := uuid.Parse(model.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("parse transaction id: %w", err)
	}

	rateID, err := uuid.Parse(model.RateID)
	if err != nil {
		return nil, fmt.Errorf("parse rate id: %w", err)
	}

	return &matchingEntities.FeeVariance{
		ID:            id,
		ContextID:     contextID,
		RunID:         runID,
		MatchGroupID:  matchGroupID,
		TransactionID: transactionID,
		RateID:        rateID,
		Currency:      model.Currency,
		ExpectedFee:   model.ExpectedFee,
		ActualFee:     model.ActualFee,
		Delta:         model.Delta,
		ToleranceAbs:  model.ToleranceAbs,
		TolerancePct:  model.TolerancePct,
		VarianceType:  model.VarianceType,
		CreatedAt:     model.CreatedAt,
		UpdatedAt:     model.UpdatedAt,
	}, nil
}
