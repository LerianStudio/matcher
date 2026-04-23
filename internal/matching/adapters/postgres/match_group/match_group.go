// Package match_group provides PostgreSQL persistence adapters for match group entities.
package match_group

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

// PostgreSQLModel represents the match_groups table mapping.
type PostgreSQLModel struct {
	ID         uuid.UUID
	ContextID  uuid.UUID
	RunID      uuid.UUID
	RuleID     uuid.NullUUID // nullable: NULL for manual matches
	Confidence int
	Status     string

	RejectedReason *string
	ConfirmedAt    *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewPostgreSQLModel converts a match group entity to a PostgreSQLModel.
func NewPostgreSQLModel(entity *matchingEntities.MatchGroup) (*PostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrMatchGroupEntityNeeded
	}

	var ruleID uuid.NullUUID

	if entity.RuleID != uuid.Nil {
		ruleID = uuid.NullUUID{UUID: entity.RuleID, Valid: true}
	}

	return &PostgreSQLModel{
		ID:             entity.ID,
		ContextID:      entity.ContextID,
		RunID:          entity.RunID,
		RuleID:         ruleID,
		Confidence:     entity.Confidence.Value(),
		Status:         entity.Status.String(),
		RejectedReason: entity.RejectedReason,
		ConfirmedAt:    entity.ConfirmedAt,
		CreatedAt:      entity.CreatedAt,
		UpdatedAt:      entity.UpdatedAt,
	}, nil
}

// ToEntity converts a PostgreSQLModel to a match group entity.
func (model *PostgreSQLModel) ToEntity() (*matchingEntities.MatchGroup, error) {
	if model == nil {
		return nil, ErrMatchGroupModelNeeded
	}

	var ruleID uuid.UUID
	if model.RuleID.Valid {
		ruleID = model.RuleID.UUID
	}

	conf, err := matchingVO.ParseConfidenceScore(model.Confidence)
	if err != nil {
		return nil, fmt.Errorf("parse confidence: %w", err)
	}

	status, err := matchingVO.ParseMatchGroupStatus(model.Status)
	if err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}

	return &matchingEntities.MatchGroup{
		ID:             model.ID,
		ContextID:      model.ContextID,
		RunID:          model.RunID,
		RuleID:         ruleID,
		Confidence:     conf,
		Status:         status,
		Items:          []*matchingEntities.MatchItem{},
		CreatedAt:      model.CreatedAt,
		UpdatedAt:      model.UpdatedAt,
		RejectedReason: model.RejectedReason,
		ConfirmedAt:    model.ConfirmedAt,
	}, nil
}
