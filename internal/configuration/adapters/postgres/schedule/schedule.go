// Package schedule provides PostgreSQL repository implementation for reconciliation schedules.
package schedule

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// SchedulePostgreSQLModel represents the database model for reconciliation schedules.
type SchedulePostgreSQLModel struct {
	ID             string
	ContextID      string
	CronExpression string
	Enabled        bool
	LastRunAt      *time.Time
	NextRunAt      *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewSchedulePostgreSQLModel creates a new PostgreSQL model from a schedule entity.
func NewSchedulePostgreSQLModel(entity *entities.ReconciliationSchedule) (*SchedulePostgreSQLModel, error) {
	if entity == nil {
		return nil, ErrScheduleEntityRequired
	}

	if entity.ContextID == uuid.Nil {
		return nil, ErrScheduleContextIDRequired
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

	return &SchedulePostgreSQLModel{
		ID:             id.String(),
		ContextID:      entity.ContextID.String(),
		CronExpression: entity.CronExpression,
		Enabled:        entity.Enabled,
		LastRunAt:      entity.LastRunAt,
		NextRunAt:      entity.NextRunAt,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}, nil
}

// ToEntity converts the PostgreSQL model to a domain entity.
func (model *SchedulePostgreSQLModel) ToEntity() (*entities.ReconciliationSchedule, error) {
	if model == nil {
		return nil, ErrScheduleModelRequired
	}

	id, err := uuid.Parse(model.ID)
	if err != nil {
		return nil, fmt.Errorf("parsing ID: %w", err)
	}

	contextID, err := uuid.Parse(model.ContextID)
	if err != nil {
		return nil, fmt.Errorf("parsing ContextID: %w", err)
	}

	return &entities.ReconciliationSchedule{
		ID:             id,
		ContextID:      contextID,
		CronExpression: model.CronExpression,
		Enabled:        model.Enabled,
		LastRunAt:      model.LastRunAt,
		NextRunAt:      model.NextRunAt,
		CreatedAt:      model.CreatedAt,
		UpdatedAt:      model.UpdatedAt,
	}, nil
}
