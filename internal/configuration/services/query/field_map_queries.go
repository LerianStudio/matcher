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

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// GetFieldMap retrieves a field map by ID.
func (uc *UseCase) GetFieldMap(
	ctx context.Context,
	fieldMapID uuid.UUID,
) (*shared.FieldMap, error) {
	if uc == nil || uc.fieldMapRepo == nil {
		return nil, ErrNilFieldMapRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.get_field_map")
	defer span.End()

	result, err := uc.fieldMapRepo.FindByID(ctx, fieldMapID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to get field map", err)

			logger.With(
				libLog.Any("field_map.id", fieldMapID.String()),
				libLog.Err(err),
			).Log(ctx, libLog.LevelError, "failed to get field map")
		}

		return nil, fmt.Errorf("finding field map: %w", err)
	}

	return result, nil
}

// GetFieldMapBySource retrieves a field map by source ID.
func (uc *UseCase) GetFieldMapBySource(
	ctx context.Context,
	sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	if uc == nil || uc.fieldMapRepo == nil {
		return nil, ErrNilFieldMapRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.get_field_map_by_source")
	defer span.End()

	result, err := uc.fieldMapRepo.FindBySourceID(ctx, sourceID)
	if err != nil {
		// sql.ErrNoRows is a normal "not found" case, not an error worth logging.
		// The handler returns 404 for this, so only log actual failures at ERROR.
		if !errors.Is(err, sql.ErrNoRows) {
			libOpentelemetry.HandleSpanError(span, "failed to get field map by source", err)

			logger.With(
				libLog.Any("source.id", sourceID.String()),
				libLog.Err(err),
			).Log(ctx, libLog.LevelError, "failed to get field map by source")
		}

		return nil, fmt.Errorf("finding field map by source: %w", err)
	}

	return result, nil
}
