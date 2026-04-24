// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package job provides PostgreSQL repository implementation for ingestion jobs.
package job

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

// NewJobPostgreSQLModel converts an entity to a PostgreSQL model.
func NewJobPostgreSQLModel(entity *entities.IngestionJob) (*pgcommon.JobPostgreSQLModel, error) {
	if entity == nil {
		return nil, errJobEntityRequired
	}

	id := entity.ID
	if id == uuid.Nil {
		id = uuid.New()
	}

	createdAt := entity.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	updatedAt := entity.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}

	startedAt := entity.StartedAt
	if startedAt.IsZero() {
		startedAt = createdAt
	}

	metadataJSON, err := entity.MetadataJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize job metadata: %w", err)
	}

	completedAt := sql.NullTime{}
	if entity.CompletedAt != nil {
		completedAt = sql.NullTime{Time: *entity.CompletedAt, Valid: true}
	}

	status := entity.Status
	if !status.IsValid() {
		return nil, fmt.Errorf("%w: %s", errInvalidJobStatus, status)
	}

	return &pgcommon.JobPostgreSQLModel{
		ID:          id,
		ContextID:   entity.ContextID,
		SourceID:    entity.SourceID,
		Status:      status.String(),
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		Metadata:    metadataJSON,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

func jobModelToEntity(model *pgcommon.JobPostgreSQLModel) (*entities.IngestionJob, error) {
	if model == nil {
		return nil, errJobModelRequired
	}

	status, err := value_objects.ParseJobStatus(model.Status)
	if err != nil {
		return nil, fmt.Errorf("parsing Status '%s': %w", model.Status, err)
	}

	metadata := entities.JobMetadata{}
	if len(model.Metadata) > 0 {
		if err := json.Unmarshal(model.Metadata, &metadata); err != nil {
			return nil, fmt.Errorf("parsing Metadata: %w", err)
		}
	}

	job := &entities.IngestionJob{
		ID:        model.ID,
		ContextID: model.ContextID,
		SourceID:  model.SourceID,
		Status:    status,
		StartedAt: model.StartedAt,
		Metadata:  metadata,
		CreatedAt: model.CreatedAt,
		UpdatedAt: model.UpdatedAt,
	}

	if model.CompletedAt.Valid {
		job.CompletedAt = &model.CompletedAt.Time
	}

	return job, nil
}
