package command

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/enums"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func allocationMap(allocations []matching.Allocation) map[uuid.UUID]decimal.Decimal {
	out := make(map[uuid.UUID]decimal.Decimal, len(allocations))
	for _, allocation := range allocations {
		out[allocation.TransactionID] = allocation.AllocatedAmount
	}

	return out
}

func allocationCurrencyMap(allocations []matching.Allocation) map[uuid.UUID]string {
	out := make(map[uuid.UUID]string, len(allocations))
	for _, allocation := range allocations {
		out[allocation.TransactionID] = allocation.Currency
	}

	return out
}

func allocationUseBaseMap(allocations []matching.Allocation) map[uuid.UUID]bool {
	out := make(map[uuid.UUID]bool, len(allocations))
	for _, allocation := range allocations {
		out[allocation.TransactionID] = allocation.UseBaseAmount
	}

	return out
}

func allocationFields(
	tx *shared.Transaction,
	allocations map[uuid.UUID]decimal.Decimal,
	allocationCurrencies map[uuid.UUID]string,
	allocationUseBase map[uuid.UUID]bool,
) (decimal.Decimal, string, decimal.Decimal, *allocationErrorInfo) {
	allocated := tx.Amount
	currency := tx.Currency
	useBase := false

	if value, ok := allocations[tx.ID]; ok {
		allocated = value

		if allocationCurrency, ok := allocationCurrencies[tx.ID]; ok && allocationCurrency != "" {
			currency = allocationCurrency
		}

		if allocationUsesBase, ok := allocationUseBase[tx.ID]; ok {
			useBase = allocationUsesBase
		}
	}

	expected := tx.Amount
	if !useBase {
		return allocated, currency, expected, nil
	}

	if tx.AmountBase == nil {
		return decimal.Zero, "", decimal.Zero, &allocationErrorInfo{
			logMessage: invalidAllocationMissingBase,
			reason:     enums.ReasonMissingBaseAmount,
			spanErr:    ErrMissingBaseAmountForAllocation,
		}
	}

	if tx.BaseCurrency == nil || strings.TrimSpace(*tx.BaseCurrency) == "" {
		return decimal.Zero, "", decimal.Zero, &allocationErrorInfo{
			logMessage: invalidAllocationMissingBaseCurrency,
			reason:     enums.ReasonMissingBaseCurrency,
			spanErr:    ErrMissingBaseCurrencyForAllocation,
		}
	}

	expected = *tx.AmountBase
	currency = *tx.BaseCurrency

	return allocated, currency, expected, nil
}

func buildProposalItems(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	ids []uuid.UUID,
	pic *proposalItemsContext,
	unmatchedReasons map[uuid.UUID]string,
) ([]*matchingEntities.MatchItem, bool) {
	items := make([]*matchingEntities.MatchItem, 0, len(ids))
	for _, id := range ids {
		tx, ok := pic.txByID[id]
		if !ok {
			logProposalError(ctx, logger, span, id, "proposal transaction not found", pic.notFoundErr)
			return nil, true
		}

		allocated, currency, expected, baseErr := allocationFields(
			tx,
			pic.allocations,
			pic.allocationCurrencies,
			pic.allocationUseBase,
		)
		if baseErr != nil {
			logProposalError(ctx, logger, span, id, baseErr.logMessage, baseErr.spanErr)
			unmatchedReasons[tx.ID] = baseErr.reason

			return nil, true
		}

		item, err := matchingEntities.NewMatchItemWithPolicy(
			ctx,
			tx.ID,
			allocated,
			currency,
			expected,
			allocated.LessThan(expected),
		)
		if err != nil {
			logProposalError(ctx, logger, span, id, "match proposal processing failed", err)
			return nil, true
		}

		items = append(items, item)
	}

	return items, false
}

func logProposalError(
	ctx context.Context,
	logger libLog.Logger,
	span trace.Span,
	txID uuid.UUID,
	message string,
	err error,
) {
	libOpentelemetry.HandleSpanBusinessErrorEvent(span, message, err)
	logger.With(libLog.Any("transaction.id", txID.String())).Log(ctx, libLog.LevelError, message)
}

func recordGroupResults(
	result *proposalProcessingResult,
	group *matchingEntities.MatchGroup,
	leftByID map[uuid.UUID]*shared.Transaction,
	rightByID map[uuid.UUID]*shared.Transaction,
) {
	result.groups = append(result.groups, group)
	result.items = append(result.items, group.Items...)

	canAutoConfirm := group.CanAutoConfirm()
	for _, item := range group.Items {
		if canAutoConfirm {
			result.autoMatchedIDs = append(result.autoMatchedIDs, item.TransactionID)
		} else {
			result.pendingReviewIDs = append(result.pendingReviewIDs, item.TransactionID)
		}

		if _, ok := leftByID[item.TransactionID]; ok {
			result.leftMatched[item.TransactionID] = struct{}{}
			if canAutoConfirm {
				result.leftConfirmed[item.TransactionID] = struct{}{}
			} else {
				result.leftPending[item.TransactionID] = struct{}{}
			}

			continue
		}

		if _, ok := rightByID[item.TransactionID]; ok {
			result.rightMatched[item.TransactionID] = struct{}{}
			if canAutoConfirm {
				result.rightConfirmed[item.TransactionID] = struct{}{}
			} else {
				result.rightPending[item.TransactionID] = struct{}{}
			}
		}
	}
}

func (uc *UseCase) processProposals(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	contextID uuid.UUID,
	runID uuid.UUID,
	proposals []matching.MatchProposal,
	leftByID map[uuid.UUID]*shared.Transaction,
	rightByID map[uuid.UUID]*shared.Transaction,
) (proposalProcessingResult, error) {
	result := proposalProcessingResult{
		groups:           make([]*matchingEntities.MatchGroup, 0, len(proposals)),
		items:            make([]*matchingEntities.MatchItem, 0, len(proposals)*sliceCapMultiplier),
		autoMatchedIDs:   make([]uuid.UUID, 0, len(proposals)*sliceCapMultiplier),
		pendingReviewIDs: make([]uuid.UUID, 0, len(proposals)*sliceCapMultiplier),
		leftMatched:      make(map[uuid.UUID]struct{}),
		rightMatched:     make(map[uuid.UUID]struct{}),
		leftConfirmed:    make(map[uuid.UUID]struct{}),
		rightConfirmed:   make(map[uuid.UUID]struct{}),
		leftPending:      make(map[uuid.UUID]struct{}),
		rightPending:     make(map[uuid.UUID]struct{}),
		unmatchedReasons: make(map[uuid.UUID]string),
	}

	for _, proposal := range proposals {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("process proposals: %w", err)
		}

		group, err := uc.processSingleProposal(
			ctx,
			span,
			logger,
			contextID,
			runID,
			proposal,
			leftByID,
			rightByID,
			result.unmatchedReasons,
		)
		if err != nil {
			return result, fmt.Errorf("process proposal: %w", err)
		}

		if group == nil {
			continue
		}

		recordGroupResults(&result, group, leftByID, rightByID)
	}

	return result, nil
}

func (uc *UseCase) processSingleProposal(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	contextID uuid.UUID,
	runID uuid.UUID,
	proposal matching.MatchProposal,
	leftByID map[uuid.UUID]*shared.Transaction,
	rightByID map[uuid.UUID]*shared.Transaction,
	unmatchedReasons map[uuid.UUID]string,
) (*matchingEntities.MatchGroup, error) {
	confidence, err := matchingVO.ParseConfidenceScore(proposal.Score)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "invalid proposal confidence score", err)
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "invalid proposal confidence score")

		return nil, fmt.Errorf("invalid proposal score %d: %w", proposal.Score, err)
	}

	leftItems, invalid := buildProposalItems(
		ctx,
		span,
		logger,
		proposal.LeftIDs,
		&proposalItemsContext{
			txByID:               leftByID,
			allocations:          allocationMap(proposal.LeftAllocations),
			allocationCurrencies: allocationCurrencyMap(proposal.LeftAllocations),
			allocationUseBase:    allocationUseBaseMap(proposal.LeftAllocations),
			notFoundErr:          ErrProposalLeftTransactionNotFound,
		},
		unmatchedReasons,
	)
	if invalid || len(leftItems) == 0 {
		return nil, nil
	}

	rightItems, invalid := buildProposalItems(
		ctx,
		span,
		logger,
		proposal.RightIDs,
		&proposalItemsContext{
			txByID:               rightByID,
			allocations:          allocationMap(proposal.RightAllocations),
			allocationCurrencies: allocationCurrencyMap(proposal.RightAllocations),
			allocationUseBase:    allocationUseBaseMap(proposal.RightAllocations),
			notFoundErr:          ErrProposalRightTransactionNotFound,
		},
		unmatchedReasons,
	)
	if invalid || len(rightItems) == 0 {
		return nil, nil
	}

	proposalItems := make([]*matchingEntities.MatchItem, 0, len(leftItems)+len(rightItems))
	proposalItems = append(proposalItems, leftItems...)
	proposalItems = append(proposalItems, rightItems...)

	if len(proposalItems) < minMatchedItemsCount {
		return nil, nil
	}

	group, err := matchingEntities.NewMatchGroup(
		ctx,
		contextID,
		runID,
		proposal.RuleID,
		confidence,
		proposalItems,
	)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "match proposal processing failed", err)
		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "match proposal processing failed")

		return nil, nil
	}

	if group.CanAutoConfirm() {
		if confirmErr := group.Confirm(ctx); confirmErr != nil {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "match proposal confirm failed", confirmErr)
			logger.With(libLog.Err(confirmErr)).Log(ctx, libLog.LevelError, "match proposal confirm failed")
		}
	}

	return group, nil
}

func buildExceptionInputs(
	txIDs []uuid.UUID,
	txByID map[uuid.UUID]*shared.Transaction,
	sourceTypeByID map[uuid.UUID]string,
	reasons map[uuid.UUID]string,
) []ports.ExceptionTransactionInput {
	if len(txIDs) == 0 {
		return nil
	}

	var (
		seen   = make(map[uuid.UUID]bool, len(txIDs))
		inputs = make([]ports.ExceptionTransactionInput, 0, len(txIDs))
	)

	for _, txID := range txIDs {
		if seen[txID] {
			continue
		}

		seen[txID] = true

		reason := ""
		if reasons != nil {
			reason = reasons[txID]
		}

		txn, ok := txByID[txID]
		if !ok || txn == nil {
			inputs = append(inputs, ports.ExceptionTransactionInput{
				TransactionID: txID,
				Reason:        reason,
			})

			continue
		}

		input := buildExceptionInputFromTx(txn, sourceTypeByID, reason)
		if input != nil {
			inputs = append(inputs, *input)
		}
	}

	return inputs
}

func buildExceptionInputFromTx(
	txn *shared.Transaction,
	sourceTypeByID map[uuid.UUID]string,
	reason string,
) *ports.ExceptionTransactionInput {
	if txn == nil {
		return nil
	}

	var amountAbsBase decimal.Decimal
	if txn.AmountBase != nil {
		amountAbsBase = txn.AmountBase.Abs()
	} else {
		amountAbsBase = txn.Amount.Abs()
	}

	sourceType := ""
	if sourceTypeByID != nil {
		sourceType = sourceTypeByID[txn.SourceID]
	}

	fxMissing := txn.AmountBase == nil && txn.BaseCurrency != nil

	return &ports.ExceptionTransactionInput{
		TransactionID:   txn.ID,
		AmountAbsBase:   amountAbsBase,
		TransactionDate: txn.Date,
		SourceType:      sourceType,
		FXMissing:       fxMissing,
		Reason:          reason,
	}
}
