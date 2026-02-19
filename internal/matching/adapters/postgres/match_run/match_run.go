// Package match_run provides PostgreSQL persistence for match run entities.
package match_run

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

// PostgreSQLModel represents the match_runs table mapping.
type PostgreSQLModel struct {
	ID        string
	ContextID string
	Mode      string
	Status    string

	StartedAt     time.Time
	CompletedAt   *time.Time
	Stats         []byte
	FailureReason *string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewPostgreSQLModel converts a match run entity to a PostgreSQLModel.
func NewPostgreSQLModel(entity *matchingEntities.MatchRun) (*PostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrMatchRunEntityNeeded
	}

	statsJSON, err := json.Marshal(entity.Stats)
	if err != nil {
		return nil, fmt.Errorf("marshal stats: %w", err)
	}

	return &PostgreSQLModel{
		ID:            entity.ID.String(),
		ContextID:     entity.ContextID.String(),
		Mode:          entity.Mode.String(),
		Status:        entity.Status.String(),
		StartedAt:     entity.StartedAt,
		CompletedAt:   entity.CompletedAt,
		Stats:         statsJSON,
		FailureReason: entity.FailureReason,
		CreatedAt:     entity.CreatedAt,
		UpdatedAt:     entity.UpdatedAt,
	}, nil
}

// ToEntity converts a PostgreSQLModel to a match run entity.
func (model *PostgreSQLModel) ToEntity() (*matchingEntities.MatchRun, error) {
	if model == nil {
		return nil, ErrMatchRunModelNeeded
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}

	contextID, err := uuid.Parse(model.ContextID)
	if err != nil {
		return nil, fmt.Errorf("parse context id: %w", err)
	}

	mode, err := matchingVO.ParseMatchRunMode(model.Mode)
	if err != nil {
		return nil, fmt.Errorf("parse mode: %w", err)
	}

	status, err := matchingVO.ParseMatchRunStatus(model.Status)
	if err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}

	stats := map[string]int{}
	if len(model.Stats) > 0 {
		if err := json.Unmarshal(model.Stats, &stats); err != nil {
			return nil, fmt.Errorf("unmarshal stats: %w", err)
		}
	}

	return &matchingEntities.MatchRun{
		ID:            id,
		ContextID:     contextID,
		Mode:          mode,
		Status:        status,
		StartedAt:     model.StartedAt,
		CompletedAt:   model.CompletedAt,
		Stats:         stats,
		FailureReason: model.FailureReason,
		CreatedAt:     model.CreatedAt,
		UpdatedAt:     model.UpdatedAt,
	}, nil
}
