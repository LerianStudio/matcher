package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// ErrNilScheduleRepository is returned when the schedule repository is nil.
var ErrNilScheduleRepository = errors.New("schedule repository is required for queries")

// ErrScheduleNotFound is returned when a schedule is not found.
var ErrScheduleNotFound = errors.New("schedule not found")

// GetSchedule retrieves a single schedule by ID.
func (uc *UseCase) GetSchedule(
	ctx context.Context,
	scheduleID uuid.UUID,
) (*entities.ReconciliationSchedule, error) {
	if uc.scheduleRepo == nil {
		return nil, ErrNilScheduleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.get_schedule")
	defer span.End()

	result, err := uc.scheduleRepo.FindByID(ctx, scheduleID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrScheduleNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to get schedule", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to get schedule")

		return nil, fmt.Errorf("finding schedule: %w", err)
	}

	return result, nil
}
