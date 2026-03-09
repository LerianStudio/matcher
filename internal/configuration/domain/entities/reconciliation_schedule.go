package entities

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"
	"github.com/LerianStudio/lib-commons/v4/commons/cron"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Reconciliation schedule errors.
var (
	// ErrScheduleContextIDRequired is returned when the context ID is not provided.
	ErrScheduleContextIDRequired = errors.New("context id is required for schedule")
	// ErrScheduleCronExpressionRequired is returned when the cron expression is not provided.
	ErrScheduleCronExpressionRequired = errors.New("cron expression is required")
	// ErrScheduleCronExpressionInvalid is returned when the cron expression cannot be parsed.
	ErrScheduleCronExpressionInvalid = errors.New("invalid cron expression")
	// ErrScheduleNil is returned when the schedule is nil.
	ErrScheduleNil = errors.New("schedule is nil")
)

// ReconciliationSchedule represents a cron-based schedule for automated matching.
type ReconciliationSchedule struct {
	ID             uuid.UUID
	ContextID      uuid.UUID
	CronExpression string
	Enabled        bool
	LastRunAt      *time.Time
	NextRunAt      *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	// TenantID is populated by FindDueSchedules via a JOIN with reconciliation_contexts.
	// It is not persisted in the reconciliation_schedules table; it is a read-only
	// projection used by the scheduler worker to pass the correct tenant when triggering matches.
	TenantID uuid.UUID
}

// CreateScheduleInput defines the input required to create a schedule.
type CreateScheduleInput struct {
	CronExpression string `json:"cronExpression" validate:"required,max=100" example:"0 0 * * *"  minLength:"1" maxLength:"100"`
	Enabled        *bool  `json:"enabled,omitempty"                          example:"true"`
}

// UpdateScheduleInput defines fields that can be updated on a schedule.
type UpdateScheduleInput struct {
	CronExpression *string `json:"cronExpression,omitempty" validate:"omitempty,max=100" example:"0 6 * * *"  maxLength:"100"`
	Enabled        *bool   `json:"enabled,omitempty"                                     example:"false"`
}

// NewReconciliationSchedule validates input and returns a new schedule entity.
func NewReconciliationSchedule(
	ctx context.Context,
	contextID uuid.UUID,
	input CreateScheduleInput,
) (*ReconciliationSchedule, error) {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_schedule.new",
	)

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, ErrScheduleContextIDRequired
	}

	if err := asserter.That(ctx, input.CronExpression != "", "cron expression is required"); err != nil {
		return nil, ErrScheduleCronExpressionRequired
	}

	if _, err := cron.Parse(input.CronExpression); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrScheduleCronExpressionInvalid, err)
	}

	now := time.Now().UTC()

	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}

	schedule := &ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      contextID,
		CronExpression: input.CronExpression,
		Enabled:        enabled,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if enabled {
		nextRun := schedule.CalculateNextRun(now)
		schedule.NextRunAt = nextRun
	}

	return schedule, nil
}

// Update applies changes to a reconciliation schedule.
func (schedule *ReconciliationSchedule) Update(
	ctx context.Context,
	input UpdateScheduleInput,
) error {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"configuration.reconciliation_schedule.update",
	)

	if err := asserter.NotNil(ctx, schedule, "schedule is required"); err != nil {
		return ErrScheduleNil
	}

	if input.CronExpression != nil && *input.CronExpression != "" {
		if _, err := cron.Parse(*input.CronExpression); err != nil {
			return fmt.Errorf("%w: %w", ErrScheduleCronExpressionInvalid, err)
		}

		schedule.CronExpression = *input.CronExpression
	}

	if input.Enabled != nil {
		schedule.Enabled = *input.Enabled
	}

	now := time.Now().UTC()

	if schedule.Enabled {
		nextRun := schedule.CalculateNextRun(now)
		schedule.NextRunAt = nextRun
	} else {
		schedule.NextRunAt = nil
	}

	schedule.UpdatedAt = now

	return nil
}

// CalculateNextRun computes the next run time based on the cron expression.
// Returns nil if the expression is invalid, empty, or no matching time is found.
func (schedule *ReconciliationSchedule) CalculateNextRun(from time.Time) *time.Time {
	if schedule == nil || schedule.CronExpression == "" {
		return nil
	}

	parsed, err := cron.Parse(schedule.CronExpression)
	if err != nil {
		return nil
	}

	next, err := parsed.Next(from)
	if err != nil {
		return nil
	}

	return &next
}

// MarkRun updates the schedule after a successful run. The now parameter is normalized to UTC.
func (schedule *ReconciliationSchedule) MarkRun(now time.Time) {
	if schedule == nil {
		return
	}

	now = now.UTC()

	schedule.LastRunAt = &now
	nextRun := schedule.CalculateNextRun(now)
	schedule.NextRunAt = nextRun
	schedule.UpdatedAt = now
}
