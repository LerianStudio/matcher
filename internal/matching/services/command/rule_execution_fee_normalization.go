// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

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

// mapTransactionsWithFeeRules resolves fee schedules per transaction using predicate rules
// and applies fee normalization (net/gross adjustment) to candidate amounts.
//
// Fee normalization only adjusts Amount (the original-currency transaction value).
// AmountBase and CurrencyBase represent FX-converted values and are intentionally not
// modified by fee normalization — they are an orthogonal concern (cross-currency conversion)
// that is computed independently upstream.
func mapTransactionsWithFeeRules(
	ctx context.Context,
	in []*shared.Transaction,
	rules []*fee.FeeRule,
	schedules map[uuid.UUID]*fee.FeeSchedule,
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

		schedule := fee.ResolveFeeSchedule(txn.Metadata, rules, schedules)
		if schedule != nil {
			applyFeeNormalization(ctx, &candidate, txn, schedule, mode, logger)
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
		logger.With(libLog.Any("tx.id", txn.ID.String()), libLog.Err(err)).Log(ctx, libLog.LevelWarn, "fee normalization: failed to create money from transaction amount")

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
			logger.With(libLog.Any("tx.id", txn.ID.String()), libLog.Err(calcErr)).Log(ctx, libLog.LevelWarn, "fee normalization: failed to calculate net from gross")

			return
		}

		if breakdown != nil {
			candidate.FeeBreakdown = breakdown
			candidate.Amount = breakdown.NetAmount.Amount
		}
	case fee.NormalizationModeGross:
		grossMoney, grossBreakdown, grossErr := fee.CalculateGrossFromNet(ctx, gross, schedule)
		if grossErr != nil {
			logger.With(libLog.Any("tx.id", txn.ID.String()), libLog.Err(grossErr)).Log(ctx, libLog.LevelWarn, "fee normalization: failed to calculate gross from net")

			return
		}

		if grossBreakdown != nil {
			candidate.FeeBreakdown = grossBreakdown
			candidate.Amount = grossMoney.Amount
		}
	case fee.NormalizationModeNone:
	}
}
