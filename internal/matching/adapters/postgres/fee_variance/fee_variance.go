// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package fee_variance provides PostgreSQL persistence adapters for fee variance entities.
package fee_variance

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// PostgreSQLModel represents the match_fee_variances table mapping.
type PostgreSQLModel struct {
	ID                      uuid.UUID
	ContextID               uuid.UUID
	RunID                   uuid.UUID
	MatchGroupID            uuid.UUID
	TransactionID           uuid.UUID
	FeeScheduleID           uuid.UUID
	FeeScheduleNameSnapshot string
	Currency                string
	ExpectedFee             decimal.Decimal
	ActualFee               decimal.Decimal
	Delta                   decimal.Decimal
	ToleranceAbs            decimal.Decimal
	TolerancePct            decimal.Decimal
	VarianceType            string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

// NewPostgreSQLModel converts a fee variance entity to a PostgreSQLModel.
func NewPostgreSQLModel(entity *matchingEntities.FeeVariance) (*PostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrFeeVarianceEntityNeeded
	}

	return &PostgreSQLModel{
		ID:                      entity.ID,
		ContextID:               entity.ContextID,
		RunID:                   entity.RunID,
		MatchGroupID:            entity.MatchGroupID,
		TransactionID:           entity.TransactionID,
		FeeScheduleID:           entity.FeeScheduleID,
		FeeScheduleNameSnapshot: entity.FeeScheduleNameSnapshot,
		Currency:                entity.Currency,
		ExpectedFee:             entity.ExpectedFee,
		ActualFee:               entity.ActualFee,
		Delta:                   entity.Delta,
		ToleranceAbs:            entity.ToleranceAbs,
		TolerancePct:            entity.TolerancePct,
		VarianceType:            entity.VarianceType,
		CreatedAt:               entity.CreatedAt,
		UpdatedAt:               entity.UpdatedAt,
	}, nil
}

// ToEntity converts a PostgreSQLModel to a fee variance entity.
func (model *PostgreSQLModel) ToEntity() (*matchingEntities.FeeVariance, error) {
	if model == nil {
		return nil, ErrFeeVarianceModelNeeded
	}

	return &matchingEntities.FeeVariance{
		ID:                      model.ID,
		ContextID:               model.ContextID,
		RunID:                   model.RunID,
		MatchGroupID:            model.MatchGroupID,
		TransactionID:           model.TransactionID,
		FeeScheduleID:           model.FeeScheduleID,
		FeeScheduleNameSnapshot: model.FeeScheduleNameSnapshot,
		Currency:                model.Currency,
		ExpectedFee:             model.ExpectedFee,
		ActualFee:               model.ActualFee,
		Delta:                   model.Delta,
		ToleranceAbs:            model.ToleranceAbs,
		TolerancePct:            model.TolerancePct,
		VarianceType:            model.VarianceType,
		CreatedAt:               model.CreatedAt,
		UpdatedAt:               model.UpdatedAt,
	}, nil
}
