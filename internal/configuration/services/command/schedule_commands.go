package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
)

// Schedule sentinel errors.
var (
	ErrNilScheduleRepository   = errors.New("schedule repository is required")
	ErrScheduleNotFound        = errors.New("schedule not found")
	ErrScheduleContextMismatch = errors.New("schedule does not belong to the specified context")
)

// WithScheduleRepository sets the schedule repository on the use case.
func WithScheduleRepository(repo configPorts.ScheduleRepository) UseCaseOption {
	return func(uc *UseCase) {
		if repo != nil {
			uc.scheduleRepo = repo
		}
	}
}

// CreateSchedule creates a new reconciliation schedule for a context.
func (uc *UseCase) CreateSchedule(
	ctx context.Context,
	contextID uuid.UUID,
	input entities.CreateScheduleInput,
) (*entities.ReconciliationSchedule, error) {
	if uc.scheduleRepo == nil {
		return nil, ErrNilScheduleRepository
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed

	ctx, span := tracer.Start(ctx, "command.configuration.create_schedule")
	defer span.End()

	// Verify context exists
	_, err := uc.contextRepo.FindByID(ctx, contextID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("create schedule: context not found: %w", err)
		}

		return nil, fmt.Errorf("create schedule: verify context: %w", err)
	}

	schedule, err := entities.NewReconciliationSchedule(ctx, contextID, input)
	if err != nil {
		return nil, fmt.Errorf("create schedule entity: %w", err)
	}

	result, err := uc.scheduleRepo.Create(ctx, schedule)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create schedule", err)
		return nil, fmt.Errorf("persist schedule: %w", err)
	}

	return result, nil
}

// UpdateSchedule updates an existing reconciliation schedule.
// contextID is used to verify that the schedule belongs to the specified context.
func (uc *UseCase) UpdateSchedule(
	ctx context.Context,
	contextID uuid.UUID,
	scheduleID uuid.UUID,
	input entities.UpdateScheduleInput,
) (*entities.ReconciliationSchedule, error) {
	if uc.scheduleRepo == nil {
		return nil, ErrNilScheduleRepository
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed

	ctx, span := tracer.Start(ctx, "command.configuration.update_schedule")
	defer span.End()

	existing, err := uc.scheduleRepo.FindByID(ctx, scheduleID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrScheduleNotFound
		}

		return nil, fmt.Errorf("find schedule: %w", err)
	}

	if existing.ContextID != contextID {
		return nil, ErrScheduleContextMismatch
	}

	if err := existing.Update(ctx, input); err != nil {
		return nil, fmt.Errorf("update schedule entity: %w", err)
	}

	result, err := uc.scheduleRepo.Update(ctx, existing)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update schedule", err)
		return nil, fmt.Errorf("persist schedule update: %w", err)
	}

	return result, nil
}

// DeleteSchedule removes a reconciliation schedule.
// contextID is used to verify that the schedule belongs to the specified context.
func (uc *UseCase) DeleteSchedule(
	ctx context.Context,
	contextID uuid.UUID,
	scheduleID uuid.UUID,
) error {
	if uc.scheduleRepo == nil {
		return ErrNilScheduleRepository
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed

	ctx, span := tracer.Start(ctx, "command.configuration.delete_schedule")
	defer span.End()

	existing, err := uc.scheduleRepo.FindByID(ctx, scheduleID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrScheduleNotFound
		}

		return fmt.Errorf("find schedule for delete: %w", err)
	}

	if existing.ContextID != contextID {
		return ErrScheduleContextMismatch
	}

	if err := uc.scheduleRepo.Delete(ctx, scheduleID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrScheduleNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to delete schedule", err)

		return fmt.Errorf("delete schedule: %w", err)
	}

	return nil
}
