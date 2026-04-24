// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package match_item provides PostgreSQL adapter models for match items.
package match_item

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// PostgreSQLModel represents the match_items table mapping.
// It stores persisted IDs, allocation amounts, currency, and timestamps for a match item row.
type PostgreSQLModel struct {
	ID            uuid.UUID
	MatchGroupID  uuid.UUID
	TransactionID uuid.UUID

	AllocatedAmount   decimal.Decimal
	AllocatedCurrency string
	ExpectedAmount    decimal.Decimal
	AllowPartial      bool

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewPostgreSQLModel converts a match item entity into its PostgreSQL persistence model.
// It returns ErrMatchItemEntityNeeded when the input is nil and maps IDs, amounts, and timestamps.
func NewPostgreSQLModel(entity *matchingEntities.MatchItem) (*PostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrMatchItemEntityNeeded
	}

	return &PostgreSQLModel{
		ID:                entity.ID,
		MatchGroupID:      entity.MatchGroupID,
		TransactionID:     entity.TransactionID,
		AllocatedAmount:   entity.AllocatedAmount,
		AllocatedCurrency: entity.AllocatedCurrency,
		ExpectedAmount:    entity.ExpectedAmount,
		AllowPartial:      entity.AllowPartial,
		CreatedAt:         entity.CreatedAt,
		UpdatedAt:         entity.UpdatedAt,
	}, nil
}

// ToEntity converts the PostgreSQL model into a domain match item entity.
func (model *PostgreSQLModel) ToEntity() (*matchingEntities.MatchItem, error) {
	if model == nil {
		return nil, ErrMatchItemModelNeeded
	}

	return &matchingEntities.MatchItem{
		ID:                model.ID,
		MatchGroupID:      model.MatchGroupID,
		TransactionID:     model.TransactionID,
		AllocatedAmount:   model.AllocatedAmount,
		AllocatedCurrency: model.AllocatedCurrency,
		ExpectedAmount:    model.ExpectedAmount,
		AllowPartial:      model.AllowPartial,
		CreatedAt:         model.CreatedAt,
		UpdatedAt:         model.UpdatedAt,
	}, nil
}
