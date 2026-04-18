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

// GetContext retrieves a reconciliation context by ID.
func (uc *UseCase) GetContext(
	ctx context.Context,
	contextID uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if uc.contextRepo == nil {
		return nil, ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.get_reconciliation_context")
	defer span.End()

	result, err := uc.contextRepo.FindByID(ctx, contextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get reconciliation context", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to get reconciliation context")

		return nil, fmt.Errorf("finding reconciliation context: %w", err)
	}

	return result, nil
}

// ListContexts retrieves all reconciliation contexts with optional filters using cursor-based pagination.
func (uc *UseCase) ListContexts(
	ctx context.Context,
	cursor string,
	limit int,
	contextType *value_objects.ContextType,
	status *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	if uc.contextRepo == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.list_reconciliation_contexts")
	defer span.End()

	result, pagination, err := uc.contextRepo.FindAll(ctx, cursor, limit, contextType, status)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list reconciliation contexts", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list reconciliation contexts")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing reconciliation contexts: %w", err)
	}

	return result, pagination, nil
}

// CountContexts returns the total number of reconciliation contexts.
func (uc *UseCase) CountContexts(ctx context.Context) (int64, error) {
	if uc == nil || uc.contextRepo == nil {
		return 0, ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.count_reconciliation_contexts")
	defer span.End()

	result, err := uc.contextRepo.Count(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to count reconciliation contexts", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to count reconciliation contexts")

		return 0, fmt.Errorf("counting reconciliation contexts: %w", err)
	}

	return result, nil
}
