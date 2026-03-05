// Package source provides PostgreSQL repository implementation for reconciliation sources.
package source

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

// SourcePostgreSQLModel represents the database model for reconciliation sources.
type SourcePostgreSQLModel struct {
	ID            string
	ContextID     string
	Name          string
	Type          string
	Config        []byte
	FeeScheduleID *string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewSourcePostgreSQLModel creates a new PostgreSQL model from a source entity.
func NewSourcePostgreSQLModel(
	entity *entities.ReconciliationSource,
) (*SourcePostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrSourceEntityRequired
	}

	if entity.ID == uuid.Nil {
		return nil, ErrSourceEntityIDRequired
	}

	id := entity.ID

	configToMarshal := entity.Config
	if configToMarshal == nil {
		configToMarshal = make(map[string]any)
	}

	configJSON, err := json.Marshal(configToMarshal)
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

	var feeScheduleID *string

	if entity.FeeScheduleID != nil {
		s := entity.FeeScheduleID.String()
		feeScheduleID = &s
	}

	return &SourcePostgreSQLModel{
		ID:            id.String(),
		ContextID:     entity.ContextID.String(),
		Name:          entity.Name,
		Type:          entity.Type.String(),
		Config:        configJSON,
		FeeScheduleID: feeScheduleID,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}, nil
}

// ToEntity converts the PostgreSQL model to a domain entity.
func (model *SourcePostgreSQLModel) ToEntity() (*entities.ReconciliationSource, error) {
	if model == nil {
		return nil, ErrSourceModelRequired
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ID: %w", err)
	}

	contextID, err := uuid.Parse(model.ContextID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse context ID: %w", err)
	}

	sourceType, err := value_objects.ParseSourceType(model.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source type: %w", err)
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

	var feeScheduleID *uuid.UUID

	if model.FeeScheduleID != nil && *model.FeeScheduleID != "" {
		parsed, err := uuid.Parse(*model.FeeScheduleID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse fee schedule ID: %w", err)
		}

		feeScheduleID = &parsed
	}

	return &entities.ReconciliationSource{
		ID:            id,
		ContextID:     contextID,
		Name:          model.Name,
		Type:          sourceType,
		Config:        config,
		FeeScheduleID: feeScheduleID,
		CreatedAt:     model.CreatedAt,
		UpdatedAt:     model.UpdatedAt,
	}, nil
}
