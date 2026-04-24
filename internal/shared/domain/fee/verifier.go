// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fee

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// VarianceType categorizes the result of fee verification.
type VarianceType string

// Variance type constants.
const (
	VarianceMatch       VarianceType = "MATCH"
	VarianceUndercharge VarianceType = "UNDERCHARGE"
	VarianceOvercharge  VarianceType = "OVERCHARGE"
)

// Tolerance defines acceptable variance thresholds for fee verification.
type Tolerance struct {
	Abs     decimal.Decimal // absolute money tolerance
	Percent decimal.Decimal // fraction (0.02 == 2%)
}

// VarianceResult contains the outcome of comparing actual vs expected fees.
type VarianceResult struct {
	Type      VarianceType
	Expected  Money
	Actual    Money
	Delta     decimal.Decimal // Actual - Expected
	Threshold decimal.Decimal
}

// VerifyFee compares actual and expected fees and returns the variance result.
func VerifyFee(actual, expected Money, tolerance Tolerance) (VarianceResult, error) {
	if actual.Currency != expected.Currency {
		return VarianceResult{}, fmt.Errorf(
			"%w: actual=%s expected=%s",
			ErrCurrencyMismatch,
			actual.Currency,
			expected.Currency,
		)
	}

	if tolerance.Abs.IsNegative() || tolerance.Percent.IsNegative() {
		return VarianceResult{}, ErrToleranceNegative
	}

	maxAbs := expected.Amount.Abs()
	if actual.Amount.Abs().GreaterThan(maxAbs) {
		maxAbs = actual.Amount.Abs()
	}

	percentThreshold := tolerance.Percent.Mul(maxAbs)
	threshold := tolerance.Abs

	if percentThreshold.GreaterThan(threshold) {
		threshold = percentThreshold
	}

	delta := actual.Amount.Sub(expected.Amount)

	if delta.Abs().LessThanOrEqual(threshold) {
		return VarianceResult{
			Type:      VarianceMatch,
			Expected:  expected,
			Actual:    actual,
			Delta:     delta,
			Threshold: threshold,
		}, nil
	}

	if actual.Amount.LessThan(expected.Amount) {
		return VarianceResult{
			Type:      VarianceUndercharge,
			Expected:  expected,
			Actual:    actual,
			Delta:     delta,
			Threshold: threshold,
		}, nil
	}

	return VarianceResult{
		Type:      VarianceOvercharge,
		Expected:  expected,
		Actual:    actual,
		Delta:     delta,
		Threshold: threshold,
	}, nil
}
