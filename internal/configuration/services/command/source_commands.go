package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// CreateSource creates a new reconciliation source.
func (uc *UseCase) CreateSource(
	ctx context.Context,
	contextID uuid.UUID,
	input entities.CreateReconciliationSourceInput,
) (*entities.ReconciliationSource, error) {
	if uc == nil || uc.sourceRepo == nil {
		return nil, ErrNilSourceRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.create_reconciliation_source")
	defer span.End()

	entity, err := entities.NewReconciliationSource(ctx, contextID, input)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid reconciliation source input", err)
		return nil, err
	}

	created, err := uc.sourceRepo.Create(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create reconciliation source", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to create reconciliation source")

		return nil, fmt.Errorf("creating reconciliation source: %w", err)
	}

	uc.publishAudit(ctx, "source", created.ID, "create", map[string]any{
		"name":       created.Name,
		"type":       created.Type,
		"side":       created.Side,
		"context_id": created.ContextID.String(),
	})

	return created, nil
}

// UpdateSource modifies an existing reconciliation source.
func (uc *UseCase) UpdateSource(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
	input entities.UpdateReconciliationSourceInput,
) (*entities.ReconciliationSource, error) {
	if uc == nil || uc.sourceRepo == nil {
		return nil, ErrNilSourceRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.update_reconciliation_source")
	defer span.End()

	entity, err := uc.sourceRepo.FindByID(ctx, contextID, sourceID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load reconciliation source", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to load reconciliation source")

		return nil, fmt.Errorf("finding reconciliation source: %w", err)
	}

	if err := entity.Update(ctx, input); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid reconciliation source update", err)
		return nil, err
	}

	updated, err := uc.sourceRepo.Update(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update reconciliation source", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to update reconciliation source")

		return nil, fmt.Errorf("updating reconciliation source: %w", err)
	}

	uc.publishAudit(ctx, "source", updated.ID, "update", map[string]any{
		"name":       updated.Name,
		"type":       updated.Type,
		"side":       updated.Side,
		"context_id": updated.ContextID.String(),
	})

	return updated, nil
}

// DeleteSource removes a reconciliation source.
//
// Before deleting, this method checks whether the source has an associated
// field map. If one exists, the deletion is rejected with ErrSourceHasFieldMap
// to prevent orphan field map records. In production, FK constraints on the
// ingestion transaction table provide an additional safety net.
func (uc *UseCase) DeleteSource(ctx context.Context, contextID, sourceID uuid.UUID) error {
	if uc == nil || uc.sourceRepo == nil {
		return ErrNilSourceRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.delete_reconciliation_source")
	defer span.End()

	// Guard: check for associated field map before deletion.
	if uc.fieldMapRepo != nil {
		if err := uc.checkSourceHasNoFieldMap(ctx, span, logger, sourceID); err != nil {
			return err
		}
	}

	if err := uc.sourceRepo.Delete(ctx, contextID, sourceID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to delete reconciliation source", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to delete reconciliation source")

		return fmt.Errorf("deleting reconciliation source: %w", err)
	}

	uc.publishAudit(ctx, "source", sourceID, "delete", map[string]any{
		"context_id": contextID.String(),
	})

	return nil
}

// checkSourceHasNoFieldMap verifies the source has no associated field map.
// Returns ErrSourceHasFieldMap if a field map exists, nil otherwise.
func (uc *UseCase) checkSourceHasNoFieldMap(ctx context.Context, span trace.Span, logger libLog.Logger, sourceID uuid.UUID) error {
	fieldMap, err := uc.fieldMapRepo.FindBySourceID(ctx, sourceID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		libOpentelemetry.HandleSpanError(span, "failed to check source field map", err)

		logger.With(
			libLog.String("source.id", sourceID.String()),
			libLog.Err(err),
		).Log(ctx, libLog.LevelError, "failed to check field map for source")

		return fmt.Errorf("checking source field map: %w", err)
	}

	if fieldMap != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "source has associated field map", ErrSourceHasFieldMap)

		logger.With(
			libLog.String("source.id", sourceID.String()),
			libLog.String("field_map.id", fieldMap.ID.String()),
		).Log(ctx, libLog.LevelError, "cannot delete source: has associated field map")

		return ErrSourceHasFieldMap
	}

	return nil
}
