// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package entities

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/cron"
	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

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
	// ErrScheduleCronExpressionTooFrequent is returned when the cron expression
	// fires more often than the minimum allowed interval. Rejecting these at
	// construction time prevents a single tenant from scheduling per-minute
	// (or sub-minute) reconciliation runs and starving every other tenant
	// of worker time.
	ErrScheduleCronExpressionTooFrequent = errors.New("cron expression fires more frequently than the minimum allowed interval")
	// ErrScheduleNil is returned when the schedule is nil.
	ErrScheduleNil = errors.New("schedule is nil")
)

// MinScheduleInterval is the shortest interval permitted between two
// consecutive firings of a reconciliation schedule. A single reconciliation
// run is expensive (ingestion + matching across every source for a
// context), and matcher is multi-tenant — `* * * * *` on a handful of
// contexts would saturate worker pools within minutes. Pick a 5-minute
// floor as a sensible default; operators with a genuine need for tighter
// cadences can raise this in code as part of a deliberate review.
const MinScheduleInterval = 5 * time.Minute

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

	parsed, err := cron.Parse(input.CronExpression)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrScheduleCronExpressionInvalid, err)
	}

	if err := enforceMinimumScheduleInterval(parsed); err != nil {
		return nil, err
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
		parsed, err := cron.Parse(*input.CronExpression)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrScheduleCronExpressionInvalid, err)
		}

		if err := enforceMinimumScheduleInterval(parsed); err != nil {
			return err
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

// enforceMinimumScheduleInterval rejects cron expressions whose two
// consecutive firings fall less than MinScheduleInterval apart. It uses
// a fixed reference time (2000-01-01T00:00:00Z) so the check is
// deterministic and independent of when the schedule is created; the
// smallest gap that a 5-field cron expression can produce is a function
// of the expression itself, not wall-clock time.
func enforceMinimumScheduleInterval(schedule cron.Schedule) error {
	if schedule == nil {
		return fmt.Errorf("%w: nil schedule", ErrScheduleCronExpressionInvalid)
	}

	reference := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

	first, err := schedule.Next(reference)
	if err != nil {
		return fmt.Errorf("%w: cannot compute first firing: %w", ErrScheduleCronExpressionInvalid, err)
	}

	second, err := schedule.Next(first)
	if err != nil {
		return fmt.Errorf("%w: cannot compute second firing: %w", ErrScheduleCronExpressionInvalid, err)
	}

	if second.Sub(first) < MinScheduleInterval {
		return fmt.Errorf("%w: minimum %s between firings", ErrScheduleCronExpressionTooFrequent, MinScheduleInterval)
	}

	return nil
}

// MarkRun updates the schedule after a successful run. The now parameter is normalized to UTC.
func (schedule *ReconciliationSchedule) MarkRun(now time.Time) {
	if schedule == nil {
		return
	}

	now = now.UTC()

	schedule.LastRunAt = pointers.Time(now)
	nextRun := schedule.CalculateNextRun(now)
	schedule.NextRunAt = nextRun
	schedule.UpdatedAt = now
}
