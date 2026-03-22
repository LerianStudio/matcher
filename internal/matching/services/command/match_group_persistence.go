package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/enums"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func (uc *UseCase) commitMatchResults(
	ctx context.Context,
	_ trace.Span,
	createdRun *matchingEntities.MatchRun,
	groups []*matchingEntities.MatchGroup,
	items []*matchingEntities.MatchItem,
	autoMatchedIDs, pendingReviewIDs, unmatchedIDs []uuid.UUID,
	unmatchedReasons map[uuid.UUID]string,
	refreshFailed *atomic.Bool,
	stats map[string]int,
	feeInput *feeVerificationInput,
) (*matchingEntities.MatchRun, error) {
	var updatedRun *matchingEntities.MatchRun

	if refreshFailed != nil && refreshFailed.Load() {
		return nil, finalizeRunFailure(ctx, uc, createdRun, ErrLockRefreshFailed)
	}

	commitErr := uc.matchRunRepo.WithTx(ctx, func(tx repositories.Tx) error {
		if refreshFailed != nil && refreshFailed.Load() {
			return ErrLockRefreshFailed
		}

		if err := uc.persistMatchArtifacts(ctx, tx, createdRun, groups, items, autoMatchedIDs, pendingReviewIDs, unmatchedIDs, unmatchedReasons, feeInput); err != nil {
			return err
		}

		if err := createdRun.Complete(ctx, stats); err != nil {
			return fmt.Errorf("failed to complete match run: %w", err)
		}

		updated, err := uc.matchRunRepo.UpdateWithTx(ctx, tx, createdRun)
		if err != nil {
			return err
		}

		updatedRun = updated

		return nil
	})
	if commitErr != nil {
		return nil, finalizeRunFailure(ctx, uc, createdRun, commitErr)
	}

	if updatedRun == nil {
		return nil, ErrMatchRunPersistedNil
	}

	return updatedRun, nil
}

func (uc *UseCase) completeDryRun(
	ctx context.Context,
	span trace.Span,
	createdRun *matchingEntities.MatchRun,
	stats map[string]int,
	groups []*matchingEntities.MatchGroup,
	refreshFailed *atomic.Bool,
) (*matchingEntities.MatchRun, []*matchingEntities.MatchGroup, error) {
	if refreshFailed != nil && refreshFailed.Load() {
		return nil, nil, finalizeRunFailure(ctx, uc, createdRun, ErrLockRefreshFailed)
	}

	if err := createdRun.Complete(ctx, stats); err != nil {
		return nil, nil, fmt.Errorf("failed to complete match run: %w", err)
	}

	updatedRun, err := uc.matchRunRepo.Update(ctx, createdRun)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to complete match run", err)
		return nil, nil, fmt.Errorf("failed to update match run: %w", err)
	}

	if updatedRun == nil {
		return nil, nil, ErrMatchRunPersistedNil
	}

	return updatedRun, groups, nil
}

func (uc *UseCase) persistMatchArtifacts(
	ctx context.Context,
	tx repositories.Tx,
	createdRun *matchingEntities.MatchRun,
	groups []*matchingEntities.MatchGroup,
	items []*matchingEntities.MatchItem,
	autoMatchedIDs []uuid.UUID,
	pendingReviewIDs []uuid.UUID,
	unmatchedIDs []uuid.UUID,
	unmatchedReasons map[uuid.UUID]string,
	feeInput *feeVerificationInput,
) error {
	if len(groups) > 0 {
		if _, err := uc.matchGroupRepo.CreateBatchWithTx(ctx, tx, groups); err != nil {
			return err
		}

		if _, err := uc.matchItemRepo.CreateBatchWithTx(ctx, tx, items); err != nil {
			return err
		}
	}

	if len(autoMatchedIDs) > 0 {
		if err := uc.txRepo.MarkMatchedWithTx(ctx, tx, createdRun.ContextID, autoMatchedIDs); err != nil {
			return err
		}
	}

	if len(pendingReviewIDs) > 0 {
		if err := uc.txRepo.MarkPendingReviewWithTx(ctx, tx, createdRun.ContextID, pendingReviewIDs); err != nil {
			return err
		}
	}

	if len(unmatchedReasons) == 0 {
		unmatchedReasons = nil
	}

	var (
		txByID         map[uuid.UUID]*shared.Transaction
		sourceTypeByID map[uuid.UUID]string
	)

	if feeInput != nil {
		txByID = feeInput.txByID
		sourceTypeByID = feeInput.sourceTypeByID
	}

	exceptionInputs := buildExceptionInputs(
		unmatchedIDs,
		txByID,
		sourceTypeByID,
		unmatchedReasons,
	)
	if err := uc.exceptionCreator.CreateExceptionsWithTx(ctx, tx, createdRun.ContextID, createdRun.ID, exceptionInputs, nil); err != nil {
		return err
	}

	if err := uc.performFeeVerification(ctx, tx, createdRun, groups, feeInput); err != nil {
		return err
	}

	return uc.enqueueMatchConfirmedEvents(ctx, tx, groups)
}

func (uc *UseCase) performFeeVerification(
	ctx context.Context,
	tx repositories.Tx,
	createdRun *matchingEntities.MatchRun,
	groups []*matchingEntities.MatchGroup,
	feeInput *feeVerificationInput,
) error {
	if feeInput == nil || feeInput.ctxInfo == nil || feeInput.ctxInfo.RateID == nil {
		return nil
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.matching.fee_verification")
	defer span.End()

	rate, err := uc.rateRepo.GetByID(ctx, *feeInput.ctxInfo.RateID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load rate for fee verification", err)
		return fmt.Errorf("load rate for fee verification: %w", err)
	}

	tolerance := fee.Tolerance{
		Abs:     feeInput.ctxInfo.FeeToleranceAbs,
		Percent: feeInput.ctxInfo.FeeTolerancePct,
	}

	findings := collectFeeFindings(ctx, span, groups, createdRun, feeInput, rate, tolerance)

	span.SetAttributes(
		attribute.String("fee.currency", rate.Currency),
		attribute.String("fee.structure_type", string(rate.Structure.Type())),
		attribute.Int("fee.items_checked", len(feeInput.txByID)),
		attribute.Int("fee.variances_found", len(findings.variances)),
		attribute.Int("fee.exceptions_created", len(findings.exceptionInputs)),
	)

	return uc.persistFeeFindings(ctx, tx, span, createdRun, findings)
}

func collectFeeFindings(
	ctx context.Context,
	span trace.Span,
	groups []*matchingEntities.MatchGroup,
	createdRun *matchingEntities.MatchRun,
	feeInput *feeVerificationInput,
	rate *fee.Rate,
	tolerance fee.Tolerance,
) *feeFindings {
	findings := &feeFindings{}

	for _, group := range groups {
		if group == nil || group.Status != matchingVO.MatchGroupStatusConfirmed {
			continue
		}

		for _, item := range group.Items {
			result := processFeeForItem(ctx, span, item, group, createdRun, feeInput, rate, tolerance)
			if result == nil {
				continue
			}

			if result.variance != nil {
				findings.variances = append(findings.variances, result.variance)
			}

			if result.exceptionInput != nil {
				findings.exceptionInputs = append(findings.exceptionInputs, *result.exceptionInput)
			}
		}
	}

	return findings
}

func (uc *UseCase) persistFeeFindings(
	ctx context.Context,
	tx repositories.Tx,
	span trace.Span,
	createdRun *matchingEntities.MatchRun,
	findings *feeFindings,
) error {
	if len(findings.variances) > 0 {
		if _, err := uc.feeVarianceRepo.CreateBatchWithTx(ctx, tx, findings.variances); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to persist fee variances", err)
			return fmt.Errorf("persist fee variances: %w", err)
		}
	}

	if len(findings.exceptionInputs) > 0 {
		if err := uc.exceptionCreator.CreateExceptionsWithTx(ctx, tx, createdRun.ContextID, createdRun.ID, findings.exceptionInputs, nil); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to create fee exceptions", err)
			return fmt.Errorf("create fee exceptions: %w", err)
		}
	}

	return nil
}

func processFeeForItem(
	ctx context.Context,
	span trace.Span,
	item *matchingEntities.MatchItem,
	group *matchingEntities.MatchGroup,
	createdRun *matchingEntities.MatchRun,
	feeInput *feeVerificationInput,
	rate *fee.Rate,
	tolerance fee.Tolerance,
) *feeItemResult {
	if item == nil {
		return nil
	}

	txn, ok := feeInput.txByID[item.TransactionID]
	if !ok || txn == nil {
		return nil
	}

	actualFee, feeErr := extractActualFee(txn, rate.Currency)
	if feeErr != nil {
		return &feeItemResult{
			exceptionInput: buildExceptionInputFromTx(txn, feeInput.sourceTypeByID, feeErr.reason),
		}
	}

	txForFee := &fee.TransactionForFee{
		Amount:    fee.Money{Amount: txn.Amount.Abs(), Currency: txn.Currency},
		ActualFee: &actualFee,
	}

	expectedFee, calcErr := fee.CalculateExpectedFee(ctx, txForFee, rate)
	if calcErr != nil {
		if errors.Is(calcErr, fee.ErrCurrencyMismatch) {
			return &feeItemResult{
				exceptionInput: buildExceptionInputFromTx(
					txn,
					feeInput.sourceTypeByID,
					enums.ReasonFeeCurrencyMismatch,
				),
			}
		}

		return nil
	}

	varianceResult, verifyErr := fee.VerifyFee(actualFee, expectedFee, tolerance)
	if verifyErr != nil {
		if errors.Is(verifyErr, fee.ErrCurrencyMismatch) {
			return &feeItemResult{
				exceptionInput: buildExceptionInputFromTx(
					txn,
					feeInput.sourceTypeByID,
					enums.ReasonFeeCurrencyMismatch,
				),
			}
		}

		return nil
	}

	if varianceResult.Type != fee.VarianceMatch {
		fv, fvErr := matchingEntities.NewFeeVariance(
			ctx,
			createdRun.ContextID,
			createdRun.ID,
			group.ID,
			item.TransactionID,
			*feeInput.ctxInfo.RateID,
			rate.Currency,
			expectedFee.Amount,
			actualFee.Amount,
			tolerance.Abs,
			tolerance.Percent,
			string(varianceResult.Type),
		)
		if fvErr != nil {
			libOpentelemetry.HandleSpanError(span, "failed to create fee variance entity", fvErr)
			return nil
		}

		return &feeItemResult{
			variance: fv,
			exceptionInput: buildExceptionInputFromTx(
				txn,
				feeInput.sourceTypeByID,
				enums.ReasonFeeVariance,
			),
		}
	}

	return nil
}

func parseAmount(amountRaw any) (decimal.Decimal, *feeExtractionError) {
	switch amountValue := amountRaw.(type) {
	case string:
		parsed, err := decimal.NewFromString(amountValue)
		if err != nil {
			return decimal.Decimal{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
		}

		return parsed, nil
	case float64:
		return decimal.NewFromFloat(amountValue), nil
	case int:
		return decimal.NewFromInt(int64(amountValue)), nil
	case int64:
		return decimal.NewFromInt(amountValue), nil
	default:
		return decimal.Decimal{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}
}

func extractActualFee(
	txn *shared.Transaction,
	expectedCurrency string,
) (fee.Money, *feeExtractionError) {
	if txn.Metadata == nil {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}

	feeData, ok := txn.Metadata["fee"]
	if !ok {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}

	feeMap, ok := feeData.(map[string]any)
	if !ok {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}

	amountRaw, ok := feeMap["amount"]
	if !ok {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}

	amount, parseErr := parseAmount(amountRaw)
	if parseErr != nil {
		return fee.Money{}, parseErr
	}

	currency := expectedCurrency

	if currencyRaw, ok := feeMap["currency"]; ok {
		if currencyStr, ok := currencyRaw.(string); ok && strings.TrimSpace(currencyStr) != "" {
			currency = strings.ToUpper(strings.TrimSpace(currencyStr))
		}
	}

	if currency != expectedCurrency {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeCurrencyMismatch}
	}

	return fee.Money{Amount: amount, Currency: currency}, nil
}
