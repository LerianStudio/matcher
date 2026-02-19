// Package adjustment provides PostgreSQL persistence adapters for adjustment entities.
package adjustment

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

// PostgreSQLModel represents the adjustments table mapping.
type PostgreSQLModel struct {
	ID            string
	ContextID     string
	MatchGroupID  sql.NullString
	TransactionID sql.NullString
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
		ID:          entity.ID.String(),
		ContextID:   entity.ContextID.String(),
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
		model.MatchGroupID = sql.NullString{String: entity.MatchGroupID.String(), Valid: true}
	}

	if entity.TransactionID != nil {
		model.TransactionID = sql.NullString{String: entity.TransactionID.String(), Valid: true}
	}

	return model, nil
}

// ToEntity converts a PostgreSQLModel to an adjustment entity.
func (model *PostgreSQLModel) ToEntity() (*matchingEntities.Adjustment, error) {
	if model == nil {
		return nil, ErrAdjustmentModelNeeded
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}

	contextID, err := uuid.Parse(model.ContextID)
	if err != nil {
		return nil, fmt.Errorf("parse context id: %w", err)
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
		ID:          id,
		ContextID:   contextID,
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
		matchGroupID, parseErr := uuid.Parse(model.MatchGroupID.String)
		if parseErr != nil {
			return nil, fmt.Errorf("parse match group id: %w", parseErr)
		}

		entity.MatchGroupID = &matchGroupID
	}

	if model.TransactionID.Valid {
		transactionID, parseErr := uuid.Parse(model.TransactionID.String)
		if parseErr != nil {
			return nil, fmt.Errorf("parse transaction id: %w", parseErr)
		}

		entity.TransactionID = &transactionID
	}

	return entity, nil
}
