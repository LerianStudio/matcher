package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// ErrNilFeeRuleRepository is returned when the fee rule repository is nil.
// Same sentinel as command package; separate package requires own definition.
var ErrNilFeeRuleRepository = errors.New("fee rule repository is required for queries")

// GetFeeRule retrieves a fee rule by ID.
func (uc *UseCase) GetFeeRule(
	ctx context.Context,
	feeRuleID uuid.UUID,
) (*fee.FeeRule, error) {
	if uc.feeRuleRepo == nil {
		return nil, ErrNilFeeRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.get_fee_rule")
	defer span.End()

	result, err := uc.feeRuleRepo.FindByID(ctx, feeRuleID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get fee rule", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to get fee rule")

		return nil, fmt.Errorf("finding fee rule: %w", err)
	}

	return result, nil
}

// ListFeeRules retrieves all fee rules for a context.
func (uc *UseCase) ListFeeRules(
	ctx context.Context,
	contextID uuid.UUID,
) ([]*fee.FeeRule, error) {
	if uc.feeRuleRepo == nil {
		return nil, ErrNilFeeRuleRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.list_fee_rules")
	defer span.End()

	result, err := uc.feeRuleRepo.FindByContextID(ctx, contextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to list fee rules", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list fee rules")

		return nil, fmt.Errorf("listing fee rules: %w", err)
	}

	return result, nil
}
