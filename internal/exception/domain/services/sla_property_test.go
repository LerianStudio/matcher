// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"math"
	"math/rand"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProperty_SLA_Deterministic(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(42)),
	}

	property := func(amount int64, age uint16) bool {
		reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		input := SLAInput{
			AmountAbsBase: decimal.NewFromInt(amount),
			AgeHours:      int(age),
			ReferenceTime: reference,
		}

		first, err1 := ComputeSLADueAt(input, DefaultSLARules())
		second, err2 := ComputeSLADueAt(input, DefaultSLARules())

		if (err1 == nil) != (err2 == nil) {
			return false
		}

		if err1 != nil {
			return true
		}

		return first.DueAt.Equal(second.DueAt) && first.RuleName == second.RuleName
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_SLA_DueAtNotBeforeReference(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(7)),
	}

	property := func(amount int64, age int16) bool {
		reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		input := SLAInput{
			AmountAbsBase: decimal.NewFromInt(amount),
			AgeHours:      int(age),
			ReferenceTime: reference,
		}

		result, err := ComputeSLADueAt(input, DefaultSLARules())
		if err != nil {
			return true
		}

		return !result.DueAt.Before(reference)
	}

	require.NoError(t, quick.Check(property, &cfg))
}

// --- Currency / decimal edge-case properties ---

func TestProperty_SLA_ExtremeDecimalAmounts(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 200,
		Rand:     rand.New(rand.NewSource(99)),
	}

	property := func(mantissa int64, exponent int8) bool {
		// Clamp exponent to avoid astronomically large or small decimals
		// that would slow decimal arithmetic without adding test value.
		exp := int32(exponent) % 19 //nolint:mnd // keep exponent within int64 range
		amount := decimal.New(mantissa, exp)

		reference := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
		input := SLAInput{
			AmountAbsBase: amount,
			AgeHours:      1,
			ReferenceTime: reference,
		}

		result, err := ComputeSLADueAt(input, DefaultSLARules())
		if err != nil {
			return true // rule validation errors are fine
		}

		// Core invariant: due date is always in the future relative to reference.
		return !result.DueAt.Before(reference)
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_SLA_VeryLargeAmounts(t *testing.T) {
	t.Parallel()

	// Amounts at int64 boundaries should always classify as CRITICAL
	// (they far exceed the 100,000 threshold).
	extremes := []int64{
		math.MaxInt64,
		math.MaxInt64 - 1,
		math.MinInt64,     // abs() -> still enormous
		math.MinInt64 + 1, // abs() -> still enormous
	}

	reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rules := DefaultSLARules()

	for _, amount := range extremes {
		input := SLAInput{
			AmountAbsBase: decimal.NewFromInt(amount),
			AgeHours:      0,
			ReferenceTime: reference,
		}

		result, err := ComputeSLADueAt(input, rules)
		require.NoError(t, err)
		assert.Equal(t, "CRITICAL", result.RuleName,
			"int64 boundary amount %d should always be CRITICAL", amount)
	}
}

func TestProperty_SLA_ZeroAndSmallAmounts(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(13)),
	}

	// Amounts below the MEDIUM threshold (1000) with low age should classify as LOW.
	property := func(cents uint16) bool {
		// Keep amount in [0, 999].
		amount := decimal.NewFromInt(int64(cents % 1000)) //nolint:mnd // keep below medium threshold

		reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		input := SLAInput{
			AmountAbsBase: amount,
			AgeHours:      0,
			ReferenceTime: reference,
		}

		result, err := ComputeSLADueAt(input, DefaultSLARules())
		if err != nil {
			return false // default rules should always match
		}

		return result.RuleName == "LOW"
	}

	require.NoError(t, quick.Check(property, &cfg))
}

// --- Timezone / UTC normalization properties ---

func TestProperty_SLA_TimezoneNormalization(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(55)),
	}

	// Fixed set of timezone offsets (hours east of UTC) covering the
	// full range: UTC-12 through UTC+14.
	offsets := []int{-12, -5, -1, 0, 1, 3, 5, 8, 9, 12, 14}

	// The same wall-clock instant expressed in different timezones must
	// produce the same SLA result, because normalizeSLAInput converts
	// to UTC.
	property := func(amount int64, age uint16) bool {
		// Use a fixed UTC instant.
		utcRef := time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)
		rules := DefaultSLARules()

		baseInput := SLAInput{
			AmountAbsBase: decimal.NewFromInt(amount),
			AgeHours:      int(age),
			ReferenceTime: utcRef,
		}

		baseResult, baseErr := ComputeSLADueAt(baseInput, rules)

		for _, offset := range offsets {
			tz := time.FixedZone("test", offset*3600) //nolint:mnd // seconds per hour
			localRef := utcRef.In(tz)

			localInput := SLAInput{
				AmountAbsBase: decimal.NewFromInt(amount),
				AgeHours:      int(age),
				ReferenceTime: localRef,
			}

			localResult, localErr := ComputeSLADueAt(localInput, rules)

			if (baseErr == nil) != (localErr == nil) {
				return false
			}

			if baseErr != nil {
				continue
			}

			// DueAt must represent the same instant (Equal compares
			// instants, not wall-clock representations).
			if !baseResult.DueAt.Equal(localResult.DueAt) {
				return false
			}

			if baseResult.RuleName != localResult.RuleName {
				return false
			}
		}

		return true
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_SLA_ZeroReferenceTimeFallsBackToUTC(t *testing.T) {
	t.Parallel()

	// When ReferenceTime is zero-value, normalizeSLAInput replaces it
	// with time.Now().UTC(). The result should still be in UTC and the
	// due date should be after the (approximate) current time.
	before := time.Now().UTC()

	input := SLAInput{
		AmountAbsBase: decimal.NewFromInt(500),
		AgeHours:      0,
		ReferenceTime: time.Time{}, // zero value
	}

	result, err := ComputeSLADueAt(input, DefaultSLARules())
	require.NoError(t, err)

	// DueAt zone must be UTC.
	_, offset := result.DueAt.Zone()
	assert.Equal(t, 0, offset, "DueAt should be in UTC")

	// DueAt should be at least `slaLowDueHours` after `before` (minus
	// a small margin for test execution time).
	earliest := before.Add(time.Duration(slaLowDueHours)*time.Hour - time.Second)
	assert.False(t, result.DueAt.Before(earliest),
		"DueAt %v should not be before %v", result.DueAt, earliest)
}

// --- Concurrent SLA recalculation (race-detector coverage) ---

func TestProperty_SLA_ConcurrentRecalculation(t *testing.T) {
	t.Parallel()

	const goroutines = 50
	const iterations = 20

	reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rules := DefaultSLARules()

	input := SLAInput{
		AmountAbsBase: decimal.NewFromInt(50000),
		AgeHours:      80,
		ReferenceTime: reference,
	}

	// Compute the expected result once (single-threaded).
	expected, err := ComputeSLADueAt(input, rules)
	require.NoError(t, err)

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			for range iterations {
				result, computeErr := ComputeSLADueAt(input, rules)
				assert.NoError(t, computeErr)
				assert.Equal(t, expected.RuleName, result.RuleName)
				assert.True(t, expected.DueAt.Equal(result.DueAt))
			}
		}()
	}

	wg.Wait()
}

func TestProperty_SLA_ConcurrentDifferentInputs(t *testing.T) {
	t.Parallel()

	const goroutines = 30

	reference := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rules := DefaultSLARules()

	// Each goroutine uses a unique input; verify no cross-contamination.
	var wg sync.WaitGroup

	wg.Add(goroutines)

	for i := range goroutines {
		go func() {
			defer wg.Done()

			amount := int64(i) * 5000 //nolint:mnd // spread across severity buckets
			input := SLAInput{
				AmountAbsBase: decimal.NewFromInt(amount),
				AgeHours:      i * 5, //nolint:mnd // vary age too
				ReferenceTime: reference,
			}

			r1, err1 := ComputeSLADueAt(input, rules)
			r2, err2 := ComputeSLADueAt(input, rules)

			assert.Equal(t, err1, err2)
			if err1 == nil {
				assert.Equal(t, r1.RuleName, r2.RuleName)
				assert.True(t, r1.DueAt.Equal(r2.DueAt))
			}
		}()
	}

	wg.Wait()
}
