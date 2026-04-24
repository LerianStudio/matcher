package command

import (
	"context"
	"fmt"
	"maps"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	sharedObservability "github.com/LerianStudio/matcher/internal/shared/observability"
)

func (uc *UseCase) executeMatchRules(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	mrc *matchRunContext,
	createdRun *matchingEntities.MatchRun,
) (*matchExecutionResult, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, execSpan := tracer.Start(ctx, "command.matching.execute_match_rules")
	defer execSpan.End()

	executeRulesDetailedFn := uc.executeRulesDetailed
	if executeRulesDetailedFn == nil {
		executeRulesDetailedFn = uc.ExecuteRulesDetailed
	}

	var feeNorm fee.NormalizationMode
	if mrc.ctxInfo.FeeNormalization != nil {
		feeNorm = fee.NormalizationMode(*mrc.ctxInfo.FeeNormalization)
	}

	rulesResult, err := executeRulesDetailedFn(ctx, ExecuteRulesInput{
		ContextID:        mrc.input.ContextID,
		ContextType:      mrc.ctxInfo.Type,
		Left:             mrc.leftCandidates,
		Right:            mrc.rightCandidates,
		LeftRules:        mrc.leftRules,
		RightRules:       mrc.rightRules,
		AllSchedules:     mrc.allSchedules,
		FeeNormalization: feeNorm,
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to execute match rules", err)
		return nil, err
	}

	proposals := rulesResult.Proposals
	leftMatched := make(map[uuid.UUID]struct{})
	rightMatched := make(map[uuid.UUID]struct{})
	leftConfirmed := make(map[uuid.UUID]struct{})
	rightConfirmed := make(map[uuid.UUID]struct{})
	leftPending := make(map[uuid.UUID]struct{})
	rightPending := make(map[uuid.UUID]struct{})
	autoMatchedIDs := make([]uuid.UUID, 0, len(proposals)*sliceCapMultiplier)
	pendingReviewIDs := make([]uuid.UUID, 0, len(proposals)*sliceCapMultiplier)
	groups := make([]*matchingEntities.MatchGroup, 0, len(proposals))
	items := make([]*matchingEntities.MatchItem, 0, len(proposals)*sliceCapMultiplier)

	leftByID := indexTransactions(mrc.leftCandidates)
	rightByID := indexTransactions(mrc.rightCandidates)

	processOutput, proposalErr := uc.processProposals(
		ctx,
		span,
		logger,
		mrc.input.ContextID,
		createdRun.ID,
		proposals,
		leftByID,
		rightByID,
	)
	if proposalErr != nil {
		libOpentelemetry.HandleSpanError(span, "proposal processing failed", proposalErr)
		return nil, fmt.Errorf("proposal processing: %w", proposalErr)
	}

	groups = append(groups, processOutput.groups...)
	items = append(items, processOutput.items...)
	autoMatchedIDs = append(autoMatchedIDs, processOutput.autoMatchedIDs...)
	pendingReviewIDs = append(pendingReviewIDs, processOutput.pendingReviewIDs...)
	mergeMatched(leftMatched, processOutput.leftMatched)
	mergeMatched(rightMatched, processOutput.rightMatched)
	mergeMatched(leftConfirmed, processOutput.leftConfirmed)
	mergeMatched(rightConfirmed, processOutput.rightConfirmed)
	mergeMatched(leftPending, processOutput.leftPending)
	mergeMatched(rightPending, processOutput.rightPending)
	unmatchedReasons := processOutput.unmatchedReasons

	for txID, failure := range rulesResult.AllocFailures {
		if _, alreadySet := unmatchedReasons[txID]; !alreadySet {
			unmatchedReasons[txID] = string(failure.Code)
		}
	}

	unmatchedLeft := collectUnmatched(mrc.leftCandidates, leftMatched)
	unmatchedRight := collectUnmatched(mrc.rightCandidates, rightMatched)
	externalUnmatchedCount := len(mrc.unmatchedIDs)

	allUnmatchedIDs := make(
		[]uuid.UUID,
		0,
		len(mrc.unmatchedIDs)+len(unmatchedLeft)+len(unmatchedRight),
	)
	allUnmatchedIDs = append(allUnmatchedIDs, mrc.unmatchedIDs...)
	allUnmatchedIDs = append(allUnmatchedIDs, unmatchedLeft...)
	allUnmatchedIDs = append(allUnmatchedIDs, unmatchedRight...)

	if len(unmatchedReasons) == 0 {
		unmatchedReasons = nil
	}

	stats := make(map[string]int, len(mrc.stats)+statsFieldCount)
	maps.Copy(stats, mrc.stats)
	stats["matches"] = len(groups)
	stats["unmatched_left"] = len(unmatchedLeft)
	stats["unmatched_right"] = len(unmatchedRight)
	stats["unmatched_external"] = externalUnmatchedCount
	stats["auto_matched_left"] = len(leftConfirmed)
	stats["auto_matched_right"] = len(rightConfirmed)
	stats["pending_review_left"] = len(leftPending)
	stats["pending_review_right"] = len(rightPending)
	stats["proposed_left"] = len(leftMatched) - len(leftConfirmed)
	stats["proposed_right"] = len(rightMatched) - len(rightConfirmed)

	if span != nil {
		matchedCount := len(leftConfirmed) + len(rightConfirmed) + len(leftPending) + len(rightPending)
		unmatchedCount := len(unmatchedLeft) + len(unmatchedRight) + externalUnmatchedCount
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"matcher",
			struct {
				GroupsCreated  int `json:"groupsCreated"`
				MatchedCount   int `json:"matchedCount"`
				UnmatchedCount int `json:"unmatchedCount"`
			}{
				GroupsCreated:  len(groups),
				MatchedCount:   matchedCount,
				UnmatchedCount: unmatchedCount,
			},
			sharedObservability.NewMatcherRedactor(),
		)
	}

	allTxByID := mergeTransactionMaps(leftByID, rightByID, mrc.externalTxByID)

	return &matchExecutionResult{
		groups:           groups,
		items:            items,
		autoMatchedIDs:   autoMatchedIDs,
		pendingReviewIDs: pendingReviewIDs,
		unmatchedIDs:     allUnmatchedIDs,
		unmatchedReasons: unmatchedReasons,
		allTxByID:        allTxByID,
		stats:            stats,
	}, nil
}

func indexTransactions(transactions []*shared.Transaction) map[uuid.UUID]*shared.Transaction {
	indexed := make(map[uuid.UUID]*shared.Transaction, len(transactions))
	for _, tx := range transactions {
		if tx == nil {
			continue
		}

		indexed[tx.ID] = tx
	}

	return indexed
}

func mergeTransactionMaps(txMaps ...map[uuid.UUID]*shared.Transaction) map[uuid.UUID]*shared.Transaction {
	totalSize := 0
	for _, m := range txMaps {
		totalSize += len(m)
	}

	merged := make(map[uuid.UUID]*shared.Transaction, totalSize)
	for _, m := range txMaps {
		maps.Copy(merged, m)
	}

	return merged
}

func collectUnmatched(
	transactions []*shared.Transaction,
	matched map[uuid.UUID]struct{},
) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(transactions))
	for _, tx := range transactions {
		if tx == nil {
			continue
		}

		if _, ok := matched[tx.ID]; ok {
			continue
		}

		out = append(out, tx.ID)
	}

	return out
}

func mergeMatched(dest, src map[uuid.UUID]struct{}) {
	if dest == nil || src == nil {
		return
	}

	for id := range src {
		dest[id] = struct{}{}
	}
}
