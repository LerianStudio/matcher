// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package adjustment provides PostgreSQL persistence adapters for adjustment entities.
package adjustment

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// PostgreSQLModel represents the adjustments table mapping.
type PostgreSQLModel struct {
	ID            uuid.UUID
	ContextID     uuid.UUID
	MatchGroupID  uuid.NullUUID
	TransactionID uuid.NullUUID
	Type          string
	Direction     string
	Amount        decimal.Decimal
	Currency      string
	Description   string
	Reason        string
	CreatedBy     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewPostgreSQLModel converts an adjustment entity to a PostgreSQLModel.
func NewPostgreSQLModel(entity *matchingEntities.Adjustment) (*PostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrAdjustmentEntityNeeded
	}

	model := &PostgreSQLModel{
		ID:          entity.ID,
		ContextID:   entity.ContextID,
		Type:        string(entity.Type),
		Direction:   string(entity.Direction),
		Amount:      entity.Amount,
		Currency:    entity.Currency,
		Description: entity.Description,
		Reason:      entity.Reason,
		CreatedBy:   entity.CreatedBy,
		CreatedAt:   entity.CreatedAt,
		UpdatedAt:   entity.UpdatedAt,
	}

	if entity.MatchGroupID != nil {
		model.MatchGroupID = uuid.NullUUID{UUID: *entity.MatchGroupID, Valid: true}
	}

	if entity.TransactionID != nil {
		model.TransactionID = uuid.NullUUID{UUID: *entity.TransactionID, Valid: true}
	}

	return model, nil
}

// ToEntity converts a PostgreSQLModel to an adjustment entity.
func (model *PostgreSQLModel) ToEntity() (*matchingEntities.Adjustment, error) {
	if model == nil {
		return nil, ErrAdjustmentModelNeeded
	}

	adjType := matchingEntities.AdjustmentType(model.Type)
	if !adjType.IsValid() {
		return nil, fmt.Errorf("parse adjustment type %q: %w", model.Type, ErrInvalidAdjustmentType)
	}

	adjDirection := matchingEntities.AdjustmentDirection(model.Direction)
	if !adjDirection.IsValid() {
		return nil, fmt.Errorf("parse adjustment direction %q: %w", model.Direction, ErrInvalidAdjustmentDirection)
	}

	entity := &matchingEntities.Adjustment{
		ID:          model.ID,
		ContextID:   model.ContextID,
		Type:        adjType,
		Direction:   adjDirection,
		Amount:      model.Amount,
		Currency:    model.Currency,
		Description: model.Description,
		Reason:      model.Reason,
		CreatedBy:   model.CreatedBy,
		CreatedAt:   model.CreatedAt,
		UpdatedAt:   model.UpdatedAt,
	}

	if model.MatchGroupID.Valid {
		matchGroupID := model.MatchGroupID.UUID
		entity.MatchGroupID = &matchGroupID
	}

	if model.TransactionID.Valid {
		transactionID := model.TransactionID.UUID
		entity.TransactionID = &transactionID
	}

	return entity, nil
}
