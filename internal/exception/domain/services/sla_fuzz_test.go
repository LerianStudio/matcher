// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func FuzzComputeSLADueAt_NoPanic(f *testing.F) {
	baseTime := int64(1704067200) // 2024-01-01 00:00:00 UTC

	f.Add(int64(1000), 24, baseTime)
	f.Add(int64(-5000), -12, baseTime)

	f.Add(int64(0), 0, baseTime)
	f.Add(int64(math.MaxInt64), 0, baseTime)
	f.Add(int64(math.MinInt64), 0, baseTime)

	f.Add(int64(1000), -1, baseTime)
	f.Add(int64(1000), math.MaxInt32, baseTime)
	f.Add(int64(1000), 24, baseTime)  // slaMediumAgeHours
	f.Add(int64(1000), 72, baseTime)  // slaHighAgeHours
	f.Add(int64(1000), 120, baseTime) // slaCriticalAgeHours
	f.Add(int64(1000), 168, baseTime) // slaLowDueHours

	f.Add(int64(1000), 24, int64(0))               // epoch
	f.Add(int64(1000), 24, int64(4102444800))      // 2100-01-01 (far future)
	f.Add(int64(1000), 24, int64(946684800))       // 2000-01-01 (year boundary)
	f.Add(int64(1000), 24, int64(1735689600))      // 2025-01-01 (year boundary)
	f.Add(int64(1000), 24, int64(math.MaxInt32)+1) // beyond 32-bit range

	f.Fuzz(func(t *testing.T, amount int64, ageHours int, referenceUnix int64) {
		reference := time.Unix(referenceUnix, 0).UTC()
		if reference.IsZero() {
			reference = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		}

		input := SLAInput{
			AmountAbsBase: decimal.NewFromInt(amount),
			AgeHours:      ageHours,
			ReferenceTime: reference,
		}

		result, err := ComputeSLADueAt(input, DefaultSLARules())
		if err == nil {
			require.False(t, result.DueAt.Before(reference))
		}
	})
}

func FuzzComputeSLADueAt_RuleSelection(f *testing.F) {
	baseTime := int64(1704067200)

	f.Add(int64(500), 12, baseTime)
	f.Add(int64(1000), 24, baseTime)
	f.Add(int64(10000), 72, baseTime)
	f.Add(int64(100000), 120, baseTime)
	f.Add(int64(50000), 50, baseTime)
	f.Add(int64(5000), 100, baseTime)

	f.Fuzz(func(t *testing.T, amount int64, ageHours int, referenceUnix int64) {
		reference := time.Unix(referenceUnix, 0).UTC()
		if reference.IsZero() {
			reference = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		}

		input := SLAInput{
			AmountAbsBase: decimal.NewFromInt(amount),
			AgeHours:      ageHours,
			ReferenceTime: reference,
		}

		rules := DefaultSLARules()

		result1, err1 := ComputeSLADueAt(input, rules)
		result2, err2 := ComputeSLADueAt(input, rules)

		require.Equal(t, err1, err2, "same input should produce same error")

		if err1 == nil {
			require.Equal(
				t,
				result1.RuleName,
				result2.RuleName,
				"same input should select same rule",
			)
			require.Equal(
				t,
				result1.RuleIndex,
				result2.RuleIndex,
				"same input should select same rule index",
			)
			require.Equal(
				t,
				result1.DueAt,
				result2.DueAt,
				"same input should produce same due date",
			)
		}
	})
}

func FuzzComputeSLADueAt_BoundaryAmounts(f *testing.F) {
	baseTime := int64(1704067200)

	f.Add(int64(999), 0, baseTime)
	f.Add(int64(1000), 0, baseTime)
	f.Add(int64(1001), 0, baseTime)

	f.Add(int64(9999), 0, baseTime)
	f.Add(int64(10000), 0, baseTime)
	f.Add(int64(10001), 0, baseTime)

	f.Add(int64(99999), 0, baseTime)
	f.Add(int64(100000), 0, baseTime)
	f.Add(int64(100001), 0, baseTime)

	f.Add(int64(0), 23, baseTime)
	f.Add(int64(0), 24, baseTime)
	f.Add(int64(0), 25, baseTime)
	f.Add(int64(0), 71, baseTime)
	f.Add(int64(0), 72, baseTime)
	f.Add(int64(0), 73, baseTime)
	f.Add(int64(0), 119, baseTime)
	f.Add(int64(0), 120, baseTime)
	f.Add(int64(0), 121, baseTime)

	f.Fuzz(func(t *testing.T, amount int64, ageHours int, referenceUnix int64) {
		reference := time.Unix(referenceUnix, 0).UTC()
		if reference.IsZero() {
			reference = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		}

		input := SLAInput{
			AmountAbsBase: decimal.NewFromInt(amount),
			AgeHours:      ageHours,
			ReferenceTime: reference,
		}

		result, err := ComputeSLADueAt(input, DefaultSLARules())
		require.NoError(t, err, "default rules should always match (LOW is catch-all)")

		absAmount := decimal.NewFromInt(amount).Abs()

		normalizedAge := ageHours
		if normalizedAge < 0 {
			normalizedAge = 0
		}

		criticalAmount := decimal.NewFromInt(slaCriticalAmountThreshold)
		highAmount := decimal.NewFromInt(slaHighAmountThreshold)
		mediumAmount := decimal.NewFromInt(slaMediumAmountThreshold)

		isCritical := !absAmount.LessThan(criticalAmount) || normalizedAge >= slaCriticalAgeHours
		isHigh := !absAmount.LessThan(highAmount) || normalizedAge >= slaHighAgeHours
		isMedium := !absAmount.LessThan(mediumAmount) || normalizedAge >= slaMediumAgeHours

		switch {
		case isCritical:
			require.Equal(t, "CRITICAL", result.RuleName, "should match CRITICAL rule")
		case isHigh:
			require.Equal(t, "HIGH", result.RuleName, "should match HIGH rule")
		case isMedium:
			require.Equal(t, "MEDIUM", result.RuleName, "should match MEDIUM rule")
		default:
			require.Equal(t, "LOW", result.RuleName, "should match LOW rule")
		}
	})
}
