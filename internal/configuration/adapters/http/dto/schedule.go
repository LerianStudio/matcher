package dto

import (
	"time"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// ScheduleResponse represents a reconciliation schedule in API responses.
// @Description Cron-based reconciliation schedule
type ScheduleResponse struct {
	// Unique identifier for the schedule
	ID string `json:"id"             example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this schedule belongs to
	ContextID string `json:"contextId"      example:"550e8400-e29b-41d4-a716-446655440000"`
	// Cron expression defining the schedule
	CronExpression string `json:"cronExpression" example:"0 0 * * *"`
	// Whether the schedule is enabled
	Enabled bool `json:"enabled"        example:"true"`
	// Last successful run time
	LastRunAt *string `json:"lastRunAt,omitempty" example:"2025-01-15T10:30:00Z"`
	// Next scheduled run time
	NextRunAt *string `json:"nextRunAt,omitempty" example:"2025-01-16T00:00:00Z"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"      example:"2025-01-15T10:30:00Z"`
	// Last update timestamp in RFC3339 format
	UpdatedAt string `json:"updatedAt"      example:"2025-01-15T10:30:00Z"`
}

// ScheduleToResponse converts a domain schedule entity to a response DTO.
func ScheduleToResponse(schedule *entities.ReconciliationSchedule) ScheduleResponse {
	if schedule == nil {
		return ScheduleResponse{}
	}

	var lastRunAt, nextRunAt *string

	if schedule.LastRunAt != nil {
		formatted := schedule.LastRunAt.Format(time.RFC3339)
		lastRunAt = &formatted
	}

	if schedule.NextRunAt != nil {
		formatted := schedule.NextRunAt.Format(time.RFC3339)
		nextRunAt = &formatted
	}

	return ScheduleResponse{
		ID:             schedule.ID.String(),
		ContextID:      schedule.ContextID.String(),
		CronExpression: schedule.CronExpression,
		Enabled:        schedule.Enabled,
		LastRunAt:      lastRunAt,
		NextRunAt:      nextRunAt,
		CreatedAt:      schedule.CreatedAt.Format(time.RFC3339),
		UpdatedAt:      schedule.UpdatedAt.Format(time.RFC3339),
	}
}

// SchedulesToResponse converts a slice of schedules to response DTOs.
func SchedulesToResponse(schedules []*entities.ReconciliationSchedule) []ScheduleResponse {
	result := make([]ScheduleResponse, 0, len(schedules))

	for _, s := range schedules {
		if s != nil {
			result = append(result, ScheduleToResponse(s))
		}
	}

	return result
}
