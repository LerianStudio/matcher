// Package job provides PostgreSQL repository implementation for ingestion jobs.
package job

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

var (
	errJobEntityRequired = errors.New("ingestion job entity is required")
	errJobModelRequired  = errors.New("ingestion job model is required")
	errInvalidJobStatus  = errors.New("invalid job status")
	errRepoNotInit       = errors.New("job repository not initialized")
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
		ID:          id.String(),
		ContextID:   entity.ContextID.String(),
		SourceID:    entity.SourceID.String(),
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
		ID:        id,
		ContextID: contextID,
		SourceID:  sourceID,
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
