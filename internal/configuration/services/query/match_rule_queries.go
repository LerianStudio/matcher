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
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ListMatchRules retrieves all match rules with optional type filter using cursor-based pagination.
func (uc *UseCase) ListMatchRules(
	ctx context.Context,
	contextID uuid.UUID,
	cursor string,
	limit int,
	ruleType *shared.RuleType,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if uc == nil || uc.matchRuleRepo == nil {
		return nil, libHTTP.CursorPagination{}, fmt.Errorf("nil matchRuleRepo: %w", ErrNilMatchRuleRepository)
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.list_match_rules")
	defer span.End()

	var (
		result     entities.MatchRules
		pagination libHTTP.CursorPagination
		err        error
	)
	if ruleType != nil {
		result, pagination, err = uc.matchRuleRepo.FindByContextIDAndType(
			ctx,
			contextID,
			*ruleType,
			cursor,
			limit,
		)
	} else {
		result, pagination, err = uc.matchRuleRepo.FindByContextID(ctx, contextID, cursor, limit)
	}

	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list match rules", err)

		logger.With(
			libLog.Any("context.id", contextID.String()),
			libLog.Err(err),
		).Log(ctx, libLog.LevelError, "failed to list match rules")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing match rules: %w", err)
	}

	return result, pagination, nil
}
