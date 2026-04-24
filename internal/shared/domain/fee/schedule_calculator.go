// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// convergencePrecision is the number of decimal places used for convergence checks
// in the iterative gross-from-net calculation.
const convergencePrecision = 10

// FeeBreakdown contains the result of applying a fee schedule to a gross amount.
//
// Note: When cascading fees exceed the gross amount (e.g., flat fees totaling more
// than the input), NetAmount is clamped to zero and TotalFee may exceed the gross.
// In this case, TotalFee + NetAmount != gross. Consumers should not assume this
// invariant holds for all inputs.
type FeeBreakdown struct {
	TotalFee  Money
	NetAmount Money
	ItemFees  []ItemFee
}

// ItemFee contains the fee calculated for a single schedule item.
type ItemFee struct {
	ItemID   uuid.UUID
	ItemName string
	Fee      Money
	BaseUsed Money
}

// CalculateSchedule applies all fee schedule items to the gross amount and
// returns a breakdown of fees, the total fee, and the net amount.
func CalculateSchedule(ctx context.Context, gross Money, schedule *FeeSchedule) (*FeeBreakdown, error) {
	if schedule == nil {
		return nil, ErrNilSchedule
	}

	grossCurrency, err := NormalizeCurrency(gross.Currency)
	if err != nil {
		return nil, fmt.Errorf("gross currency: %w", err)
	}

	scheduleCurrency, err := NormalizeCurrency(schedule.Currency)
	if err != nil {
		return nil, fmt.Errorf("schedule currency: %w", err)
	}

	if grossCurrency != scheduleCurrency {
		return nil, fmt.Errorf(
			"gross currency %s vs schedule currency %s: %w",
			grossCurrency, scheduleCurrency, ErrCurrencyMismatch,
		)
	}

	sorted := sortItemsByPriority(schedule.Items)

	switch schedule.ApplicationOrder {
	case ApplicationOrderParallel:
		return calculateParallel(ctx, gross, sorted, schedule.RoundingScale, schedule.RoundingMode, grossCurrency)
	case ApplicationOrderCascading:
		return calculateCascading(ctx, gross, sorted, schedule.RoundingScale, schedule.RoundingMode, grossCurrency)
	default:
		return nil, fmt.Errorf("application order %s: %w", schedule.ApplicationOrder, ErrInvalidApplicationOrder)
	}
}

// sortItemsByPriority returns a copy of items sorted by priority ascending.
func sortItemsByPriority(items []FeeScheduleItem) []FeeScheduleItem {
	sorted := make([]FeeScheduleItem, len(items))
	copy(sorted, items)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})

	return sorted
}

// calculateParallel computes fees where each item is applied independently to the gross amount.
func calculateParallel(ctx context.Context, gross Money, items []FeeScheduleItem, scale int, mode RoundingMode, currency string) (*FeeBreakdown, error) {
	totalFee := decimal.Zero
	itemFees := make([]ItemFee, 0, len(items))

	for _, item := range items {
		if item.Structure == nil {
			return nil, ErrNilFeeStructure
		}

		itemFee, err := item.Structure.Calculate(ctx, gross.Amount)
		if err != nil {
			return nil, fmt.Errorf("calculate item %q (id=%s): %w", item.Name, item.ID, err)
		}

		roundedFee := roundAmount(itemFee, scale, mode)
		totalFee = totalFee.Add(roundedFee)

		itemFees = append(itemFees, ItemFee{
			ItemID:   item.ID,
			ItemName: item.Name,
			Fee:      Money{Amount: roundedFee, Currency: currency},
			BaseUsed: Money{Amount: gross.Amount, Currency: currency},
		})
	}

	return &FeeBreakdown{
		TotalFee:  Money{Amount: totalFee, Currency: currency},
		NetAmount: Money{Amount: gross.Amount.Sub(totalFee), Currency: currency},
		ItemFees:  itemFees,
	}, nil
}

// calculateCascading computes fees where each item is applied to the net amount remaining
// after previous items have been deducted.
func calculateCascading(
	ctx context.Context,
	gross Money,
	items []FeeScheduleItem,
	scale int,
	mode RoundingMode,
	currency string,
) (*FeeBreakdown, error) {
	currentBase := gross.Amount
	totalFee := decimal.Zero
	itemFees := make([]ItemFee, 0, len(items))

	for _, item := range items {
		if item.Structure == nil {
			return nil, ErrNilFeeStructure
		}

		fee, err := item.Structure.Calculate(ctx, currentBase)
		if err != nil {
			return nil, fmt.Errorf("calculate item %q (id=%s): %w", item.Name, item.ID, err)
		}

		roundedFee := roundAmount(fee, scale, mode)
		totalFee = totalFee.Add(roundedFee)

		itemFees = append(itemFees, ItemFee{
			ItemID:   item.ID,
			ItemName: item.Name,
			Fee:      Money{Amount: roundedFee, Currency: currency},
			BaseUsed: Money{Amount: currentBase, Currency: currency},
		})

		currentBase = currentBase.Sub(roundedFee)

		if currentBase.IsNegative() {
			// Clamp to zero: fees are contractual obligations even if they exceed gross.
			// This breaks the invariant totalFee + netAmount == gross for this edge case.
			currentBase = decimal.Zero
		}
	}

	return &FeeBreakdown{
		TotalFee:  Money{Amount: totalFee, Currency: currency},
		NetAmount: Money{Amount: currentBase, Currency: currency},
		ItemFees:  itemFees,
	}, nil
}

// CalculateGrossFromNet finds the gross amount that, after applying the fee schedule,
// produces the given net amount. Uses iterative fixed-point convergence.
func CalculateGrossFromNet(ctx context.Context, net Money, schedule *FeeSchedule) (Money, *FeeBreakdown, error) {
	if schedule == nil {
		return Money{}, nil, ErrNilSchedule
	}

	normNet, err := NormalizeCurrency(net.Currency)
	if err != nil {
		return Money{}, nil, err
	}

	normSched, err := NormalizeCurrency(schedule.Currency)
	if err != nil {
		return Money{}, nil, err
	}

	if normNet != normSched {
		return Money{}, nil, fmt.Errorf("%w: net=%s schedule=%s", ErrCurrencyMismatch, normNet, normSched)
	}

	const maxIterations = 100

	epsilon := decimal.New(1, -convergencePrecision)

	grossEstimate := net.Amount

	var lastDiff decimal.Decimal

	for i := range maxIterations {
		grossMoney, newMoneyErr := NewMoney(grossEstimate, net.Currency)
		if newMoneyErr != nil {
			return Money{}, nil, fmt.Errorf("create gross money iteration %d: %w", i, newMoneyErr)
		}

		breakdown, calcErr := CalculateSchedule(ctx, grossMoney, schedule)
		if calcErr != nil {
			return Money{}, nil, fmt.Errorf("calculate gross iteration %d: %w", i, calcErr)
		}

		impliedNet := grossEstimate.Sub(breakdown.TotalFee.Amount)
		diff := impliedNet.Sub(net.Amount).Abs()
		lastDiff = diff

		if diff.LessThanOrEqual(epsilon) {
			return grossMoney, breakdown, nil
		}

		// Fixed-point iteration: adjust gross by the difference
		grossEstimate = net.Amount.Add(breakdown.TotalFee.Amount)
	}

	return Money{}, nil, fmt.Errorf("%w: after %d iterations (remaining diff: %s)", ErrGrossConvergenceFailed, maxIterations, lastDiff.StringFixed(convergencePrecision))
}

// roundAmount rounds a decimal amount to the given scale using the specified rounding mode.
func roundAmount(amount decimal.Decimal, scale int, mode RoundingMode) decimal.Decimal {
	if scale < 0 {
		scale = 0
	}

	if scale > maxRoundingScale {
		scale = maxRoundingScale
	}

	decimalScale := int32(scale)

	switch mode {
	case RoundingModeHalfUp:
		return amount.Round(decimalScale)
	case RoundingModeBankers:
		return amount.RoundBank(decimalScale)
	case RoundingModeFloor:
		return amount.RoundFloor(decimalScale)
	case RoundingModeCeil:
		return amount.RoundCeil(decimalScale)
	case RoundingModeTruncate:
		return amount.Truncate(decimalScale)
	default:
		return amount.Round(decimalScale)
	}
}
