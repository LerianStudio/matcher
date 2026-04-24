// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// FeeStructureType identifies the kind of fee calculation strategy.
type FeeStructureType string

// Fee structure type constants.
const (
	FeeStructureFlat       FeeStructureType = "FLAT"
	FeeStructurePercentage FeeStructureType = "PERCENTAGE"
	FeeStructureTiered     FeeStructureType = "TIERED"
)

// FeeStructure defines the interface for fee calculation strategies.
type FeeStructure interface {
	Type() FeeStructureType
	Calculate(ctx context.Context, baseAmount decimal.Decimal) (decimal.Decimal, error)
	Validate(ctx context.Context) error
}

// FlatFee represents a fixed fee amount regardless of transaction value.
type FlatFee struct {
	Amount decimal.Decimal
}

// Type returns the fee structure type identifier.
func (ff FlatFee) Type() FeeStructureType { return FeeStructureFlat }

// Validate checks that the flat fee amount is non-negative.
func (ff FlatFee) Validate(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "shared.fee.flat.validate")
	if err := asserter.That(ctx, !ff.Amount.IsNegative(), "flat fee must be non-negative", "amount", ff.Amount.String()); err != nil {
		return fmt.Errorf("flat fee: %w", err)
	}

	return nil
}

// Calculate returns the fixed fee amount regardless of the base amount.
func (ff FlatFee) Calculate(ctx context.Context, _ decimal.Decimal) (decimal.Decimal, error) {
	if err := ff.Validate(ctx); err != nil {
		return decimal.Decimal{}, err
	}

	return ff.Amount, nil
}

// PercentageFee represents a fee calculated as a fraction of the base amount.
// Rate is a fraction (0.015 == 1.5%).
type PercentageFee struct {
	Rate decimal.Decimal
}

// Type returns the fee structure type identifier.
func (pf PercentageFee) Type() FeeStructureType { return FeeStructurePercentage }

// Validate checks that the percentage rate is between 0 and 1 inclusive.
func (pf PercentageFee) Validate(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "shared.fee.percentage.validate")

	if err := asserter.That(ctx, !pf.Rate.IsNegative(), "percentage rate must be >= 0", "rate", pf.Rate.String()); err != nil {
		return fmt.Errorf("percentage fee: %w", ErrInvalidPercentageRate)
	}

	if err := asserter.That(ctx, !pf.Rate.GreaterThan(decimal.NewFromInt(1)), "percentage rate must be <= 1", "rate", pf.Rate.String()); err != nil {
		return fmt.Errorf("percentage fee: %w", ErrInvalidPercentageRate)
	}

	return nil
}

// Calculate returns the fee as a percentage of the base amount.
func (pf PercentageFee) Calculate(
	ctx context.Context,
	baseAmount decimal.Decimal,
) (decimal.Decimal, error) {
	if baseAmount.IsNegative() {
		return decimal.Decimal{}, ErrNegativeAmount
	}

	if err := pf.Validate(ctx); err != nil {
		return decimal.Decimal{}, err
	}

	return baseAmount.Mul(pf.Rate), nil
}

// Tier represents a single tier in a tiered fee structure.
type Tier struct {
	UpTo *decimal.Decimal
	Rate decimal.Decimal
}

// TieredFee represents a marginal/progressive fee structure.
type TieredFee struct {
	Tiers []Tier
}

// calculateTierCap calculates the applicable amount for a tier.
func calculateTierCap(tier Tier, lower, remaining decimal.Decimal) decimal.Decimal {
	if tier.UpTo == nil {
		return remaining
	}

	maxInBracket := tier.UpTo.Sub(lower)
	if maxInBracket.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}

	if remaining.GreaterThan(maxInBracket) {
		return maxInBracket
	}

	return remaining
}

// Type returns the fee structure type identifier.
func (tf TieredFee) Type() FeeStructureType { return FeeStructureTiered }

// Validate checks that tiers are properly ordered with valid rates.
func (tf TieredFee) Validate(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "shared.fee.tiered.validate")
	if err := asserter.That(ctx, len(tf.Tiers) > 0, "tiered fee must have tiers"); err != nil {
		return fmt.Errorf("tiered fee: %w", ErrInvalidTieredDefinition)
	}

	prevUpper := decimal.Zero
	seenInfinite := false

	for index, tier := range tf.Tiers {
		if seenInfinite {
			return fmt.Errorf(
				"%w: tier after infinite upper bound (index=%d)",
				ErrInvalidTieredDefinition,
				index,
			)
		}

		if tier.Rate.IsNegative() || tier.Rate.GreaterThan(decimal.NewFromInt(1)) {
			return fmt.Errorf("%w: invalid rate at index=%d", ErrInvalidTieredDefinition, index)
		}

		if tier.UpTo == nil {
			seenInfinite = true
			continue
		}

		upper := tier.UpTo
		if upper.LessThanOrEqual(decimal.Zero) {
			return fmt.Errorf(
				"%w: upper bound must be > 0 (index=%d)",
				ErrInvalidTieredDefinition,
				index,
			)
		}

		if upper.LessThanOrEqual(prevUpper) {
			return fmt.Errorf(
				"%w: upper bounds must be strictly increasing (index=%d)",
				ErrInvalidTieredDefinition,
				index,
			)
		}

		prevUpper = *upper
	}

	return nil
}

// Calculate computes the fee by applying marginal rates across tiers.
func (tf TieredFee) Calculate(
	ctx context.Context,
	baseAmount decimal.Decimal,
) (decimal.Decimal, error) {
	if baseAmount.IsNegative() {
		return decimal.Decimal{}, ErrNegativeAmount
	}

	if err := tf.Validate(ctx); err != nil {
		return decimal.Decimal{}, err
	}

	remaining := baseAmount
	total := decimal.Zero
	lower := decimal.Zero

	for _, tier := range tf.Tiers {
		if remaining.LessThanOrEqual(decimal.Zero) {
			break
		}

		tierCap := calculateTierCap(tier, lower, remaining)
		if tierCap.LessThanOrEqual(decimal.Zero) {
			continue
		}

		total = total.Add(tierCap.Mul(tier.Rate))
		remaining = remaining.Sub(tierCap)

		if tier.UpTo != nil {
			lower = *tier.UpTo
		}
	}

	return total, nil
}
