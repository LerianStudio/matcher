package command

import (
	"context"
	"errors"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// Sentinel errors for execute rules operations.
var (
	ErrMatchRuleProviderRequired = errors.New("match rule provider is required")
	ErrContextIDRequired         = errors.New("context id is required")
	ErrEngineIsNil               = errors.New("engine is nil")
	ErrUnsupportedContextType    = errors.New("unsupported context type")
)

// ExecuteRulesInput contains the input parameters for executing match rules.
type ExecuteRulesInput struct {
	ContextID        uuid.UUID
	ContextType      shared.ContextType
	Left             []*shared.Transaction
	Right            []*shared.Transaction
	LeftRules        []*fee.FeeRule
	RightRules       []*fee.FeeRule
	AllSchedules     map[uuid.UUID]*fee.FeeSchedule
	FeeNormalization fee.NormalizationMode
}

// ExecuteRules executes match rules for the given input transactions. The
// public entry point creates its own span so the simplified-result call path
// is visible in traces distinct from ExecuteRulesDetailed's full-result
// path. ExecuteRulesDetailed's span nests under this one.
func (uc *UseCase) ExecuteRules(
	ctx context.Context,
	in ExecuteRulesInput,
) ([]matching.MatchProposal, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed here; ExecuteRulesDetailed re-extracts logger

	ctx, span := tracer.Start(ctx, "command.matching.execute_rules_summary")
	defer span.End()

	result, err := uc.ExecuteRulesDetailed(ctx, in)
	if err != nil {
		return nil, err
	}

	return result.Proposals, nil
}

// ExecuteRulesDetailed executes match rules and returns structured failure information.
func (uc *UseCase) ExecuteRulesDetailed(
	ctx context.Context,
	in ExecuteRulesInput,
) (*ExecuteRulesResult, error) {
	if uc == nil || uc.ruleProvider == nil {
		return nil, ErrMatchRuleProviderRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.matching.execute_rules")
	defer span.End()

	if err := validateExecuteRulesInput(ctx, logger, span, in.ContextID); err != nil {
		return nil, err
	}

	defs, err := loadRuleDefinitions(ctx, span, logger, uc.ruleProvider, in.ContextID)
	if err != nil {
		return nil, err
	}

	var left, right []matching.CandidateTransaction

	if in.FeeNormalization != fee.NormalizationModeNone && (len(in.LeftRules) > 0 || len(in.RightRules) > 0) {
		left = mapTransactionsWithFeeRules(ctx, in.Left, in.LeftRules, in.AllSchedules, in.FeeNormalization, logger)
		right = mapTransactionsWithFeeRules(ctx, in.Right, in.RightRules, in.AllSchedules, in.FeeNormalization, logger)
	} else {
		left = mapTransactions(in.Left)
		right = mapTransactions(in.Right)
	}

	missingBaseAmountTotal, missingBaseCurrencyTotal, err := recordBaseFieldMetrics(
		span,
		left,
		right,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to set base field attributes", err)
	}

	validateBaseMatchingAvailability(
		ctx,
		logger,
		span,
		in.ContextID,
		defs,
		missingBaseAmountTotal,
		missingBaseCurrencyTotal,
	)

	return executeRulesEngineDetailed(ctx, span, logger, in.ContextID, defs, left, right, in.ContextType)
}

// ExecuteRulesResult contains the result of executing match rules including any allocation failures.
type ExecuteRulesResult struct {
	Proposals     []matching.MatchProposal
	AllocFailures map[uuid.UUID]*matching.AllocationFailure
}
