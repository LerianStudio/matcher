// Package match_group provides PostgreSQL persistence adapters for match group entities.
package match_group

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/pointers"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

// PostgreSQLModel represents the match_groups table mapping.
type PostgreSQLModel struct {
	ID         string
	ContextID  string
	RunID      string
	RuleID     *string // nullable: NULL for manual matches
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

	var ruleID *string

	if entity.RuleID != uuid.Nil {
		ruleID = pointers.String(entity.RuleID.String())
	}

	return &PostgreSQLModel{
		ID:             entity.ID.String(),
		ContextID:      entity.ContextID.String(),
		RunID:          entity.RunID.String(),
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

	var ruleID uuid.UUID
	if model.RuleID != nil {
		ruleID, err = uuid.Parse(*model.RuleID)
		if err != nil {
			return nil, fmt.Errorf("parse rule id: %w", err)
		}
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
		ID:             id,
		ContextID:      contextID,
		RunID:          runID,
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
