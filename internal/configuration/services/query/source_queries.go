package query

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

// GetSource retrieves a reconciliation source by ID.
func (uc *UseCase) GetSource(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
) (*entities.ReconciliationSource, error) {
	if uc.sourceRepo == nil {
		return nil, ErrNilSourceRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.get_reconciliation_source")
	defer span.End()

	result, err := uc.sourceRepo.FindByID(ctx, contextID, sourceID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get reconciliation source", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to get reconciliation source")

		return nil, fmt.Errorf("finding reconciliation source: %w", err)
	}

	return result, nil
}

// ListSources retrieves all reconciliation sources with optional type filter using cursor-based pagination.
func (uc *UseCase) ListSources(
	ctx context.Context,
	contextID uuid.UUID,
	cursor string,
	limit int,
	sourceType *value_objects.SourceType,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if uc.sourceRepo == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilSourceRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.list_reconciliation_sources")
	defer span.End()

	var (
		result     []*entities.ReconciliationSource
		pagination libHTTP.CursorPagination
		err        error
	)
	if sourceType != nil {
		result, pagination, err = uc.sourceRepo.FindByContextIDAndType(
			ctx,
			contextID,
			*sourceType,
			cursor,
			limit,
		)
	} else {
		result, pagination, err = uc.sourceRepo.FindByContextID(ctx, contextID, cursor, limit)
	}

	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation sources", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list reconciliation sources")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing reconciliation sources: %w", err)
	}

	return result, pagination, nil
}
