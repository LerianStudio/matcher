package command

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// CreateFieldMap creates a new field map for a source.
func (uc *UseCase) CreateFieldMap(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
	input entities.CreateFieldMapInput,
) (*entities.FieldMap, error) {
	if uc == nil || uc.fieldMapRepo == nil {
		return nil, ErrNilFieldMapRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.create_field_map")
	defer span.End()

	entity, err := entities.NewFieldMap(ctx, contextID, sourceID, input)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid field map input", err)
		return nil, err
	}

	created, err := uc.fieldMapRepo.Create(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create field map", err)

		logger.With(
			libLog.Any("context.id", contextID.String()),
			libLog.Any("source.id", sourceID.String()),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to create field map")

		return nil, fmt.Errorf("creating field map: %w", err)
	}

	uc.publishAudit(ctx, "field_map", created.ID, "create", map[string]any{
		"source_id":  created.SourceID.String(),
		"context_id": created.ContextID.String(),
	})

	return created, nil
}

// UpdateFieldMap modifies an existing field map.
func (uc *UseCase) UpdateFieldMap(
	ctx context.Context,
	fieldMapID uuid.UUID,
	input entities.UpdateFieldMapInput,
) (*entities.FieldMap, error) {
	if uc == nil || uc.fieldMapRepo == nil {
		return nil, ErrNilFieldMapRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.update_field_map")
	defer span.End()

	entity, err := uc.fieldMapRepo.FindByID(ctx, fieldMapID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load field map", err)

		logger.With(
			libLog.Any("field_map.id", fieldMapID.String()),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to load field map")

		return nil, fmt.Errorf("finding field map: %w", err)
	}

	if err := entity.Update(ctx, input); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid field map update", err)

		logger.With(
			libLog.Any("field_map.id", fieldMapID.String()),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "invalid field map update")

		return nil, err
	}

	updated, err := uc.fieldMapRepo.Update(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update field map", err)

		logger.With(
			libLog.Any("field_map.id", fieldMapID.String()),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to update field map")

		return nil, fmt.Errorf("updating field map: %w", err)
	}

	uc.publishAudit(ctx, "field_map", updated.ID, "update", map[string]any{
		"context_id": updated.ContextID.String(),
		"source_id":  updated.SourceID.String(),
	})

	return updated, nil
}

// DeleteFieldMap removes a field map.
func (uc *UseCase) DeleteFieldMap(ctx context.Context, fieldMapID uuid.UUID) error {
	if uc == nil || uc.fieldMapRepo == nil {
		return ErrNilFieldMapRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.delete_field_map")
	defer span.End()

	if err := uc.fieldMapRepo.Delete(ctx, fieldMapID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to delete field map", err)

		logger.With(
			libLog.Any("field_map.id", fieldMapID.String()),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to delete field map")

		return fmt.Errorf("deleting field map: %w", err)
	}

	uc.publishAudit(ctx, "field_map", fieldMapID, "delete", nil)

	return nil
}
