// Package match_rule provides PostgreSQL repository implementation for match rules.
package match_rule

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// MatchRulePostgreSQLModel represents the database model for match rules.
type MatchRulePostgreSQLModel struct {
	ID        string
	ContextID string
	Priority  int
	Type      string
	Config    []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewMatchRulePostgreSQLModel creates a new PostgreSQL model from a match rule entity.
func NewMatchRulePostgreSQLModel(entity *entities.MatchRule) (*MatchRulePostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrMatchRuleEntityRequired
	}

	id := entity.ID
	if id == uuid.Nil {
		id = uuid.New()
	}

	if entity.ContextID == uuid.Nil {
		return nil, ErrMatchRuleContextIDRequired
	}

	configJSON, err := json.Marshal(entity.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}

	createdAt := entity.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	updatedAt := entity.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}

	return &MatchRulePostgreSQLModel{
		ID:        id.String(),
		ContextID: entity.ContextID.String(),
		Priority:  entity.Priority,
		Type:      entity.Type.String(),
		Config:    configJSON,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// ToEntity converts the PostgreSQL model to a domain entity.
func (model *MatchRulePostgreSQLModel) ToEntity() (*entities.MatchRule, error) {
	if model == nil {
		return nil, ErrMatchRuleModelRequired
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ID: %w", err)
	}

	contextID, err := uuid.Parse(model.ContextID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse context ID: %w", err)
	}

	ruleType, err := shared.ParseRuleType(model.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rule type: %w", err)
	}

	config := make(map[string]any)
	if len(model.Config) > 0 {
		if err := json.Unmarshal(model.Config, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}
	}

	if config == nil {
		config = make(map[string]any)
	}

	return &entities.MatchRule{
		ID:        id,
		ContextID: contextID,
		Priority:  model.Priority,
		Type:      ruleType,
		Config:    config,
		CreatedAt: model.CreatedAt,
		UpdatedAt: model.UpdatedAt,
	}, nil
}
