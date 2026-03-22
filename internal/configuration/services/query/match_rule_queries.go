package query

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

// GetMatchRule retrieves a match rule by ID.
func (uc *UseCase) GetMatchRule(
	ctx context.Context,
	contextID, ruleID uuid.UUID,
) (*entities.MatchRule, error) {
	if uc == nil || uc.matchRuleRepo == nil {
		return nil, ErrNilMatchRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.get_match_rule")
	defer span.End()

	result, err := uc.matchRuleRepo.FindByID(ctx, contextID, ruleID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get match rule", err)

		logger.With(
			libLog.Any("context.id", contextID.String()),
			libLog.Any("rule.id", ruleID.String()),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to get match rule")

		return nil, fmt.Errorf("finding match rule: %w", err)
	}

	return result, nil
}

// ListMatchRules retrieves all match rules with optional type filter using cursor-based pagination.
func (uc *UseCase) ListMatchRules(
	ctx context.Context,
	contextID uuid.UUID,
	cursor string,
	limit int,
	ruleType *value_objects.RuleType,
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
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to list match rules")

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("listing match rules: %w", err)
	}

	return result, pagination, nil
}
