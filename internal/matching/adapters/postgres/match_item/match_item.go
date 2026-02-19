// Package match_item provides PostgreSQL adapter models for match items.
package match_item

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// PostgreSQLModel represents the match_items table mapping.
// It stores persisted IDs, allocation amounts, currency, and timestamps for a match item row.
type PostgreSQLModel struct {
	ID            string
	MatchGroupID  string
	TransactionID string

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
		ID:                entity.ID.String(),
		MatchGroupID:      entity.MatchGroupID.String(),
		TransactionID:     entity.TransactionID.String(),
		AllocatedAmount:   entity.AllocatedAmount,
		AllocatedCurrency: entity.AllocatedCurrency,
		ExpectedAmount:    entity.ExpectedAmount,
		AllowPartial:      entity.AllowPartial,
		CreatedAt:         entity.CreatedAt,
		UpdatedAt:         entity.UpdatedAt,
	}, nil
}

// ToEntity converts the PostgreSQL model into a domain match item entity.
// It parses UUIDs for ID, match group, and transaction fields, returning wrapped errors on failure.
func (model *PostgreSQLModel) ToEntity() (*matchingEntities.MatchItem, error) {
	if model == nil {
		return nil, ErrMatchItemModelNeeded
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}

	groupID, err := uuid.Parse(model.MatchGroupID)
	if err != nil {
		return nil, fmt.Errorf("parse match group id: %w", err)
	}

	txID, err := uuid.Parse(model.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("parse transaction id: %w", err)
	}

	return &matchingEntities.MatchItem{
		ID:                id,
		MatchGroupID:      groupID,
		TransactionID:     txID,
		AllocatedAmount:   model.AllocatedAmount,
		AllocatedCurrency: model.AllocatedCurrency,
		ExpectedAmount:    model.ExpectedAmount,
		AllowPartial:      model.AllowPartial,
		CreatedAt:         model.CreatedAt,
		UpdatedAt:         model.UpdatedAt,
	}, nil
}
