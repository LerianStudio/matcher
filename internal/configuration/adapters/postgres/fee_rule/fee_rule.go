// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	ID            uuid.UUID
	ContextID     uuid.UUID
	Side          string
	FeeScheduleID uuid.UUID
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
		ID:            entity.ID,
		ContextID:     entity.ContextID,
		Side:          string(entity.Side),
		FeeScheduleID: entity.FeeScheduleID,
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

	var predicates []fee.FieldPredicate
	if len(model.Predicates) > 0 {
		if err := json.Unmarshal(model.Predicates, &predicates); err != nil {
			return nil, fmt.Errorf("unmarshal predicates: %w", err)
		}
	}

	return &fee.FeeRule{
		ID:            model.ID,
		ContextID:     model.ContextID,
		Side:          fee.MatchingSide(model.Side),
		FeeScheduleID: model.FeeScheduleID,
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
