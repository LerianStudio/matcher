package command

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func (uc *UseCase) completeEmptyRun(
	ctx context.Context,
	in RunMatchInput,
	stats map[string]int,
	leftCandidates, rightCandidates []*shared.Transaction,
	externalUnmatched []uuid.UUID,
	sourceTypeByID map[uuid.UUID]string,
) (*matchingEntities.MatchRun, []*matchingEntities.MatchGroup, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.matching.complete_empty_run")
	defer span.End()

	stats["matches"] = 0
	stats["unmatched_left"] = len(leftCandidates)
	stats["unmatched_right"] = len(rightCandidates)
	stats["unmatched_external"] = len(externalUnmatched)
	stats["auto_matched_left"] = 0
	stats["auto_matched_right"] = 0
	stats["pending_review_left"] = 0
	stats["pending_review_right"] = 0
	stats["proposed_left"] = 0
	stats["proposed_right"] = 0

	txByID := mergeTransactionMaps(
		indexTransactions(leftCandidates),
		indexTransactions(rightCandidates),
	)

	run, err := matchingEntities.NewMatchRun(ctx, in.ContextID, in.Mode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create match run entity: %w", err)
	}

	var (
		created *matchingEntities.MatchRun
		updated *matchingEntities.MatchRun
	)

	commitErr := uc.matchRunRepo.WithTx(ctx, func(tx repositories.Tx) error {
		persisted, err := uc.matchRunRepo.CreateWithTx(ctx, tx, run)
		if err != nil {
			return err
		}

		if persisted == nil {
			return ErrMatchRunPersistedNil
		}

		created = persisted

		if err := created.Complete(ctx, stats); err != nil {
			return fmt.Errorf("failed to complete match run: %w", err)
		}

		updatedRun, err := uc.matchRunRepo.UpdateWithTx(ctx, tx, created)
		if err != nil {
			return err
		}

		updated = updatedRun

		if in.Mode == matchingVO.MatchRunModeCommit {
			unmatchedIDs := collectUnmatched(leftCandidates, map[uuid.UUID]struct{}{})

			unmatchedIDs = append(
				unmatchedIDs,
				collectUnmatched(rightCandidates, map[uuid.UUID]struct{}{})...,
			)
			if len(externalUnmatched) > 0 {
				unmatchedIDs = append(unmatchedIDs, externalUnmatched...)
			}

			exceptionInputs := buildExceptionInputs(unmatchedIDs, txByID, sourceTypeByID, nil)
			if err := uc.exceptionCreator.CreateExceptionsWithTx(ctx, tx, in.ContextID, created.ID, exceptionInputs, nil); err != nil {
				return err
			}
		}

		return nil
	})
	if commitErr != nil {
		libOpentelemetry.HandleSpanError(span, "failed to complete match run", commitErr)
		return nil, nil, commitErr
	}

	if updated == nil {
		return nil, nil, ErrMatchRunPersistedNil
	}

	return updated, []*matchingEntities.MatchGroup{}, nil
}

func (uc *UseCase) loadFeeRulesAndSchedules(
	ctx context.Context,
	contextID uuid.UUID,
) ([]*fee.FeeRule, []*fee.FeeRule, map[uuid.UUID]*fee.FeeSchedule, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Bool("fee_rules_configured", true))

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed here

	rules, err := uc.feeRuleProvider.FindByContextID(ctx, contextID)
	if err != nil {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))
		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load fee rules")

		return nil, nil, nil, fmt.Errorf("loading fee rules: %w", err)
	}

	if len(rules) == 0 {
		span.SetAttributes(attribute.Bool("fee_rules_configured", false))
		return nil, nil, nil, nil
	}

	if len(rules) > fee.MaxFeeRulesPerContext {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))
		logger.With(libLog.Any("fee_rule_count", len(rules))).Log(ctx, libLog.LevelError, "fee rule count exceeds maximum allowed per context")

		return nil, nil, nil, fee.ErrFeeRuleCountLimitExceeded
	}

	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i] == nil {
			return false
		}

		if rules[j] == nil {
			return true
		}

		return rules[i].Priority < rules[j].Priority
	})

	scheduleIDSet := make(map[uuid.UUID]struct{})

	for _, rule := range rules {
		if rule != nil {
			scheduleIDSet[rule.FeeScheduleID] = struct{}{}
		}
	}

	scheduleIDs := make([]uuid.UUID, 0, len(scheduleIDSet))
	for id := range scheduleIDSet {
		scheduleIDs = append(scheduleIDs, id)
	}

	if uc.feeScheduleRepo == nil || len(scheduleIDs) == 0 {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))
		return nil, nil, nil, ErrNilFeeScheduleRepository
	}

	schedules, err := uc.feeScheduleRepo.GetByIDs(ctx, scheduleIDs)
	if err != nil {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))
		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load fee schedules")

		return nil, nil, nil, fmt.Errorf("loading fee schedules: %w", err)
	}

	if len(schedules) != len(scheduleIDs) {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))

		missingCount := len(scheduleIDs) - len(schedules)
		logger.With(libLog.Any("missing_count", missingCount)).Log(ctx, libLog.LevelError, "fee rules reference missing fee schedules")

		return nil, nil, nil, fmt.Errorf(
			"loading fee schedules: %w: %d missing references",
			ErrFeeRulesReferenceMissingSchedules,
			missingCount,
		)
	}

	span.SetAttributes(
		attribute.Bool("fee_rules_configured", true),
		attribute.Int("fee_rules_count", len(rules)),
	)

	leftRules, rightRules := fee.SplitRulesBySide(rules)

	return leftRules, rightRules, schedules, nil
}

func buildSourceTypeMap(sources []*ports.SourceInfo) map[uuid.UUID]string {
	if len(sources) == 0 {
		return nil
	}

	sourceTypes := make(map[uuid.UUID]string, len(sources))
	for _, src := range sources {
		if src == nil {
			continue
		}

		sourceTypes[src.ID] = string(src.Type)
	}

	return sourceTypes
}
