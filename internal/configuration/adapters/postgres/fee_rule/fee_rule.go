package fee_rule

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// PostgreSQLModel represents the fee_rules table mapping.
type PostgreSQLModel struct {
	ID            string
	ContextID     string
	Side          string
	FeeScheduleID string
	Name          string
	Priority      int
	Predicates    []byte
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewPostgreSQLModel creates a new PostgreSQL model from a fee rule entity.
func NewPostgreSQLModel(entity *fee.FeeRule) (*PostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrFeeRuleEntityNil
	}

	if entity.ID == uuid.Nil {
		return nil, ErrFeeRuleEntityIDNil
	}

	predicatesJSON, err := json.Marshal(entity.Predicates)
	if err != nil {
		return nil, fmt.Errorf("marshal predicates: %w", err)
	}

	return &PostgreSQLModel{
		ID:            entity.ID.String(),
		ContextID:     entity.ContextID.String(),
		Side:          string(entity.Side),
		FeeScheduleID: entity.FeeScheduleID.String(),
		Name:          entity.Name,
		Priority:      entity.Priority,
		Predicates:    predicatesJSON,
		CreatedAt:     entity.CreatedAt,
		UpdatedAt:     entity.UpdatedAt,
	}, nil
}

// ToEntity converts the PostgreSQL model to a domain fee.FeeRule entity.
func (model *PostgreSQLModel) ToEntity() (*fee.FeeRule, error) {
	if model == nil {
		return nil, ErrFeeRuleModelNeeded
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("parsing ID: %w", err)
	}

	contextID, err := uuid.Parse(model.ContextID)
	if err != nil {
		return nil, fmt.Errorf("parsing ContextID: %w", err)
	}

	feeScheduleID, err := uuid.Parse(model.FeeScheduleID)
	if err != nil {
		return nil, fmt.Errorf("parsing FeeScheduleID: %w", err)
	}

	var predicates []fee.FieldPredicate
	if len(model.Predicates) > 0 {
		if err := json.Unmarshal(model.Predicates, &predicates); err != nil {
			return nil, fmt.Errorf("unmarshal predicates: %w", err)
		}
	}

	return &fee.FeeRule{
		ID:            id,
		ContextID:     contextID,
		Side:          fee.MatchingSide(model.Side),
		FeeScheduleID: feeScheduleID,
		Name:          model.Name,
		Priority:      model.Priority,
		Predicates:    predicates,
		CreatedAt:     model.CreatedAt,
		UpdatedAt:     model.UpdatedAt,
	}, nil
}

// FromEntity converts a domain fee.FeeRule entity to a PostgreSQL model.
// This is a convenience function that delegates to NewPostgreSQLModel.
func FromEntity(rule *fee.FeeRule) (*PostgreSQLModel, error) {
	return NewPostgreSQLModel(rule)
}
