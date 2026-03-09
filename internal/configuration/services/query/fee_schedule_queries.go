package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// ErrNilFeeScheduleRepository is returned when the fee schedule repository is nil.
// Same sentinel as command package; separate package requires own definition.
var ErrNilFeeScheduleRepository = errors.New("fee schedule repository is required for queries")

// GetFeeSchedule retrieves a fee schedule by ID.
func (uc *UseCase) GetFeeSchedule(
	ctx context.Context,
	scheduleID uuid.UUID,
) (*fee.FeeSchedule, error) {
	if uc.feeScheduleRepo == nil {
		return nil, ErrNilFeeScheduleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.get_fee_schedule")
	defer span.End()

	result, err := uc.feeScheduleRepo.GetByID(ctx, scheduleID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get fee schedule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to get fee schedule")

		return nil, fmt.Errorf("finding fee schedule: %w", err)
	}

	return result, nil
}

// ListFeeSchedules retrieves fee schedules with an optional limit.
// A limit of 0 means no limit (returns all).
func (uc *UseCase) ListFeeSchedules(ctx context.Context, limit int) ([]*fee.FeeSchedule, error) {
	if uc.feeScheduleRepo == nil {
		return nil, ErrNilFeeScheduleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.list_fee_schedules")
	defer span.End()

	result, err := uc.feeScheduleRepo.List(ctx, limit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list fee schedules", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list fee schedules")

		return nil, fmt.Errorf("listing fee schedules: %w", err)
	}

	return result, nil
}
