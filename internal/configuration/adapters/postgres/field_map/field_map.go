// Package field_map provides PostgreSQL repository implementation for field maps.
package field_map

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// FieldMapPostgreSQLModel represents the database model for field maps.
type FieldMapPostgreSQLModel struct {
	ID        string
	ContextID string
	SourceID  string
	Mapping   []byte
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewFieldMapPostgreSQLModel creates a new PostgreSQL model from a field map entity.
// The entity must have a valid ID set by the NewFieldMap constructor.
func NewFieldMapPostgreSQLModel(entity *shared.FieldMap) (*FieldMapPostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrFieldMapEntityRequired
	}

	if entity.ID == uuid.Nil {
		return nil, ErrFieldMapEntityIDRequired
	}

	mappingJSON, err := json.Marshal(entity.Mapping)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal mapping: %w", err)
	}

	createdAt := entity.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	updatedAt := entity.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}

	return &FieldMapPostgreSQLModel{
		ID:        entity.ID.String(),
		ContextID: entity.ContextID.String(),
		SourceID:  entity.SourceID.String(),
		Mapping:   mappingJSON,
		Version:   entity.Version,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// ToEntity converts the PostgreSQL model to a domain entity.
func (model *FieldMapPostgreSQLModel) ToEntity() (*shared.FieldMap, error) {
	if model == nil {
		return nil, ErrFieldMapModelRequired
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("parsing ID: %w", err)
	}

	contextID, err := uuid.Parse(model.ContextID)
	if err != nil {
		return nil, fmt.Errorf("parsing ContextID: %w", err)
	}

	sourceID, err := uuid.Parse(model.SourceID)
	if err != nil {
		return nil, fmt.Errorf("parsing SourceID: %w", err)
	}

	mapping := make(map[string]any)
	if len(model.Mapping) > 0 {
		if err := json.Unmarshal(model.Mapping, &mapping); err != nil {
			return nil, fmt.Errorf("failed to unmarshal mapping: %w", err)
		}
	}

	if mapping == nil {
		mapping = make(map[string]any)
	}

	return &shared.FieldMap{
		ID:        id,
		ContextID: contextID,
		SourceID:  sourceID,
		Mapping:   mapping,
		Version:   model.Version,
		CreatedAt: model.CreatedAt,
		UpdatedAt: model.UpdatedAt,
	}, nil
}
