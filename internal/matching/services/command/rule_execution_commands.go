package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	"github.com/LerianStudio/matcher/internal/matching/ports"
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
	LeftSchedules    map[uuid.UUID]*fee.FeeSchedule // sourceID → schedule
	RightSchedules   map[uuid.UUID]*fee.FeeSchedule // sourceID → schedule
	FeeNormalization fee.NormalizationMode
}

// ExecuteRules executes match rules for the given input transactions.
func (uc *UseCase) ExecuteRules(
	ctx context.Context,
	in ExecuteRulesInput,
) ([]matching.MatchProposal, error) {
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

	if in.FeeNormalization != fee.NormalizationModeNone && (in.LeftSchedules != nil || in.RightSchedules != nil) {
		left = mapTransactionsWithFees(ctx, in.Left, in.LeftSchedules, in.FeeNormalization, logger)
		right = mapTransactionsWithFees(ctx, in.Right, in.RightSchedules, in.FeeNormalization, logger)
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

func validateExecuteRulesInput(ctx context.Context, logger libLog.Logger, span trace.Span, contextID uuid.UUID) error {
	if contextID != uuid.Nil {
		return nil
	}

	libOpentelemetry.HandleSpanError(span, "invalid context id", ErrContextIDRequired)

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

		logger.With(libLog.Any("context.id", contextID.String()), libLog.Any("error.message", err.Error())).Log(ctx, libLog.LevelError, "failed to load match rules")

		return nil, fmt.Errorf("failed to load match rules: %w", err)
	}

	defs := make([]matching.RuleDefinition, 0, len(rules))
	for _, rule := range rules {
		def, err := matching.DecodeRuleDefinition(rule)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to decode match rule", err)

			logger.With(libLog.Any("context.id", contextID.String()), libLog.Any("rule.id", safeRuleID(rule)), libLog.Any("error.message", err.Error())).Log(ctx, libLog.LevelError, "failed to decode match rule")

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
		nil,
	)
	if err != nil {
		return missingBaseAmountTotal, missingBaseCurrencyTotal, fmt.Errorf(
			"failed to set base field attributes: %w",
			err,
		)
	}

	return missingBaseAmountTotal, missingBaseCurrencyTotal, nil
}

// ExecuteRulesResult contains the result of executing match rules including any allocation failures.
type ExecuteRulesResult struct {
	Proposals     []matching.MatchProposal
	AllocFailures map[uuid.UUID]*matching.AllocationFailure
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

		logger.With(libLog.Any("context.id", contextID.String()), libLog.Any("error.message", err.Error())).Log(ctx, libLog.LevelError, "rule engine failed")

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
		nil,
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

func mapTransactions(in []*shared.Transaction) []matching.CandidateTransaction {
	out := make([]matching.CandidateTransaction, 0, len(in))
	for _, txn := range in {
		if txn == nil {
			continue
		}

		currencyBase := ""
		if txn.BaseCurrency != nil {
			currencyBase = *txn.BaseCurrency
		}

		out = append(out, matching.CandidateTransaction{
			ID:             txn.ID,
			SourceID:       txn.SourceID,
			Amount:         txn.Amount,
			Currency:       txn.Currency,
			AmountBase:     txn.AmountBase,
			CurrencyBase:   currencyBase,
			Date:           txn.Date,
			Reference:      txn.ExternalID,
			OriginalAmount: txn.Amount,
		})
	}

	return out
}

func mapTransactionsWithFees(
	ctx context.Context,
	in []*shared.Transaction,
	scheduleBySourceID map[uuid.UUID]*fee.FeeSchedule,
	mode fee.NormalizationMode,
	logger libLog.Logger,
) []matching.CandidateTransaction {
	out := make([]matching.CandidateTransaction, 0, len(in))

	for _, txn := range in {
		if txn == nil {
			continue
		}

		currencyBase := ""
		if txn.BaseCurrency != nil {
			currencyBase = *txn.BaseCurrency
		}

		candidate := matching.CandidateTransaction{
			ID:             txn.ID,
			SourceID:       txn.SourceID,
			Amount:         txn.Amount,
			Currency:       txn.Currency,
			AmountBase:     txn.AmountBase,
			CurrencyBase:   currencyBase,
			Date:           txn.Date,
			Reference:      txn.ExternalID,
			OriginalAmount: txn.Amount,
		}

		if scheduleBySourceID != nil {
			if schedule, ok := scheduleBySourceID[txn.SourceID]; ok {
				applyFeeNormalization(ctx, &candidate, txn, schedule, mode, logger)
			}
		}

		out = append(out, candidate)
	}

	return out
}

func validateFeeCurrencies(
	ctx context.Context,
	txn *shared.Transaction,
	schedule *fee.FeeSchedule,
	logger libLog.Logger,
) bool {
	if txn == nil || schedule == nil {
		logger.Log(ctx, libLog.LevelWarn, "fee normalization: nil transaction or schedule")

		return false
	}

	normalizedCurrency, normErr := fee.NormalizeCurrency(txn.Currency)
	scheduleCurrency, schedErr := fee.NormalizeCurrency(schedule.Currency)

	if normErr != nil || schedErr != nil {
		logger.With(libLog.Any("tx.id", txn.ID.String()), libLog.Any("tx.currency", txn.Currency), libLog.Any("schedule.currency", schedule.Currency), libLog.Any("normErr", fmt.Sprint(normErr)), libLog.Any("schedErr", fmt.Sprint(schedErr))).Log(ctx, libLog.LevelWarn, "fee normalization: failed to normalize currency")

		return false
	}

	if normalizedCurrency != scheduleCurrency {
		logger.With(libLog.Any("tx.id", txn.ID.String()), libLog.Any("tx.currency", normalizedCurrency), libLog.Any("schedule.currency", scheduleCurrency)).Log(ctx, libLog.LevelWarn, "fee normalization: currency mismatch between transaction and schedule")

		return false
	}

	return true
}

// applyFeeNormalization adjusts a candidate's amount based on the fee schedule and mode.
// Returns without modification if schedule is nil, currency mismatches, or mode is NormalizationModeNone.
func applyFeeNormalization(
	ctx context.Context,
	candidate *matching.CandidateTransaction,
	txn *shared.Transaction,
	schedule *fee.FeeSchedule,
	mode fee.NormalizationMode,
	logger libLog.Logger,
) {
	if schedule == nil || mode == fee.NormalizationModeNone {
		return
	}

	if !validateFeeCurrencies(ctx, txn, schedule, logger) {
		return
	}

	gross, err := fee.NewMoney(txn.Amount, txn.Currency)
	if err != nil {
		logger.With(libLog.Any("tx.id", txn.ID.String()), libLog.Any("error", err.Error())).Log(ctx, libLog.LevelWarn, "fee normalization: failed to create money from transaction amount")

		return
	}

	applyFeeMode(ctx, candidate, txn, gross, schedule, mode, logger)
}

func applyFeeMode(
	ctx context.Context,
	candidate *matching.CandidateTransaction,
	txn *shared.Transaction,
	gross fee.Money,
	schedule *fee.FeeSchedule,
	mode fee.NormalizationMode,
	logger libLog.Logger,
) {
	switch mode {
	case fee.NormalizationModeNet:
		breakdown, calcErr := fee.CalculateSchedule(ctx, gross, schedule)
		if calcErr != nil {
			logger.With(libLog.Any("tx.id", txn.ID.String()), libLog.Any("error", calcErr.Error())).Log(ctx, libLog.LevelWarn, "fee normalization: failed to calculate net from gross")

			return
		}

		if breakdown != nil {
			candidate.FeeBreakdown = breakdown
			candidate.Amount = breakdown.NetAmount.Amount
		}
	case fee.NormalizationModeGross:
		grossMoney, grossBreakdown, grossErr := fee.CalculateGrossFromNet(ctx, gross, schedule)
		if grossErr != nil {
			logger.With(libLog.Any("tx.id", txn.ID.String()), libLog.Any("error", grossErr.Error())).Log(ctx, libLog.LevelWarn, "fee normalization: failed to calculate gross from net")

			return
		}

		if grossBreakdown != nil {
			candidate.FeeBreakdown = grossBreakdown
			candidate.Amount = grossMoney.Amount
		}
	case fee.NormalizationModeNone:
		// No-op: outer guard prevents reaching here, but included for switch exhaustiveness.
	}
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
