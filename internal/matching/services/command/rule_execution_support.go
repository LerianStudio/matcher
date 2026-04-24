// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedObservability "github.com/LerianStudio/matcher/internal/shared/observability"
)

func validateExecuteRulesInput(ctx context.Context, logger libLog.Logger, span trace.Span, contextID uuid.UUID) error {
	if contextID != uuid.Nil {
		return nil
	}

	libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid context id", ErrContextIDRequired)

	logger.With(libLog.Any("context.id", contextID.String())).Log(ctx, libLog.LevelError, "invalid context id")

	return ErrContextIDRequired
}

func loadRuleDefinitions(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	provider ports.MatchRuleProvider,
	contextID uuid.UUID,
) ([]matching.RuleDefinition, error) {
	rules, err := provider.ListByContextID(ctx, contextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load match rules", err)

		logger.With(libLog.Any("context.id", contextID.String()), libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to load match rules")

		return nil, fmt.Errorf("failed to load match rules: %w", err)
	}

	defs := make([]matching.RuleDefinition, 0, len(rules))
	for _, rule := range rules {
		def, err := matching.DecodeRuleDefinition(rule)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to decode match rule", err)

			logger.With(libLog.Any("context.id", contextID.String()), libLog.Any("rule.id", safeRuleID(rule)), libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to decode match rule")

			return nil, fmt.Errorf("failed to decode match rule: %w", err)
		}

		defs = append(defs, def)
	}

	return defs, nil
}

func recordBaseFieldMetrics(
	span trace.Span,
	left, right []matching.CandidateTransaction,
) (int, int, error) {
	missingBaseAmountLeft, missingBaseAmountRight, missingBaseCurrencyLeft, missingBaseCurrencyRight := countMissingBaseFields(
		left,
		right,
	)
	missingBaseAmountTotal := missingBaseAmountLeft + missingBaseAmountRight
	missingBaseCurrencyTotal := missingBaseCurrencyLeft + missingBaseCurrencyRight

	err := libOpentelemetry.SetSpanAttributesFromValue(
		span,
		"matching.base_fields",
		struct {
			MissingBaseAmountCount   int `json:"missingBaseAmountCount"`
			MissingBaseCurrencyCount int `json:"missingBaseCurrencyCount"`
		}{
			MissingBaseAmountCount:   missingBaseAmountTotal,
			MissingBaseCurrencyCount: missingBaseCurrencyTotal,
		},
		sharedObservability.NewMatcherRedactor(),
	)
	if err != nil {
		return missingBaseAmountTotal, missingBaseCurrencyTotal, fmt.Errorf(
			"failed to set base field attributes: %w",
			err,
		)
	}

	return missingBaseAmountTotal, missingBaseCurrencyTotal, nil
}

func executeRulesEngineDetailed(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	contextID uuid.UUID,
	defs []matching.RuleDefinition,
	left, right []matching.CandidateTransaction,
	contextType shared.ContextType,
) (*ExecuteRulesResult, error) {
	engine := matching.NewEngine()

	engineResult, err := executeByContextTypeDetailed(engine, defs, left, right, contextType)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "rule engine failed", err)

		logger.With(libLog.Any("context.id", contextID.String()), libLog.Err(err)).Log(ctx, libLog.LevelError, "rule engine failed")

		return nil, err
	}

	return &ExecuteRulesResult{
		Proposals:     engineResult.Proposals,
		AllocFailures: engineResult.AllocFailures,
	}, nil
}

func executeByContextTypeDetailed(
	engine *matching.Engine,
	rules []matching.RuleDefinition,
	left, right []matching.CandidateTransaction,
	contextType shared.ContextType,
) (*matching.EngineResult, error) {
	if engine == nil {
		return nil, ErrEngineIsNil
	}

	var result *matching.EngineResult

	var err error

	switch contextType {
	case shared.ContextTypeOneToMany:
		result, err = engine.Execute1vNDetailed(rules, left, right)
	case shared.ContextTypeOneToOne, "":
		proposals, execErr := engine.Execute1v1(rules, left, right)
		if execErr != nil {
			return nil, fmt.Errorf("engine execution failed: %w", execErr)
		}

		result = &matching.EngineResult{
			Proposals:     proposals,
			AllocFailures: make(map[uuid.UUID]*matching.AllocationFailure),
		}
	case shared.ContextTypeManyToMany:
		return nil, fmt.Errorf("%w: %s (M:N matching is not yet implemented)", ErrUnsupportedContextType, contextType)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedContextType, contextType)
	}

	if err != nil {
		return nil, fmt.Errorf("engine execution failed: %w", err)
	}

	return result, nil
}

func validateBaseMatchingAvailability(
	ctx context.Context,
	logger libLog.Logger,
	span trace.Span,
	contextID uuid.UUID,
	defs []matching.RuleDefinition,
	missingBaseAmountTotal, missingBaseCurrencyTotal int,
) {
	if !requiresBaseMatching(defs) {
		return
	}

	if missingBaseAmountTotal == 0 && missingBaseCurrencyTotal == 0 {
		return
	}

	attrErr := libOpentelemetry.SetSpanAttributesFromValue(
		span,
		"matching.fx_rate",
		struct {
			Unavailable bool `json:"unavailable"`
		}{
			Unavailable: true,
		},
		sharedObservability.NewMatcherRedactor(),
	)
	if attrErr != nil {
		libOpentelemetry.HandleSpanError(
			span,
			"failed to set fx rate availability attribute",
			attrErr,
		)
	}

	logger.With(libLog.Any("context.id", contextID.String()), libLog.Any("missing.base_amount_count", missingBaseAmountTotal), libLog.Any("missing.base_currency_count", missingBaseCurrencyTotal), libLog.Any("exception.reason", "FX_RATE_UNAVAILABLE")).Log(ctx, libLog.LevelWarn, "fx rate unavailable for base matching")
}

func safeRuleID(r *shared.MatchRule) string {
	if r == nil {
		return ""
	}

	return r.ID.String()
}

func countMissingBaseFields(left, right []matching.CandidateTransaction) (int, int, int, int) {
	missingBaseAmountLeft := 0
	missingBaseAmountRight := 0
	missingBaseCurrencyLeft := 0
	missingBaseCurrencyRight := 0

	for _, tx := range left {
		if tx.AmountBase == nil {
			missingBaseAmountLeft++
		}

		if tx.CurrencyBase == "" {
			missingBaseCurrencyLeft++
		}
	}

	for _, tx := range right {
		if tx.AmountBase == nil {
			missingBaseAmountRight++
		}

		if tx.CurrencyBase == "" {
			missingBaseCurrencyRight++
		}
	}

	return missingBaseAmountLeft, missingBaseAmountRight, missingBaseCurrencyLeft, missingBaseCurrencyRight
}

func requiresBaseMatching(defs []matching.RuleDefinition) bool {
	for _, def := range defs {
		if def.Tolerance != nil &&
			(def.Tolerance.MatchBaseAmount || def.Tolerance.MatchBaseCurrency) {
			return true
		}

		if def.Exact != nil && (def.Exact.MatchBaseAmount || def.Exact.MatchBaseCurrency) {
			return true
		}

		if def.Allocation != nil && def.Allocation.UseBaseAmount {
			return true
		}
	}

	return false
}
