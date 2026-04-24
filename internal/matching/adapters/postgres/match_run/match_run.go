// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	ID        uuid.UUID
	ContextID uuid.UUID
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
		ID:            entity.ID,
		ContextID:     entity.ContextID,
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
		ID:            model.ID,
		ContextID:     model.ContextID,
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
