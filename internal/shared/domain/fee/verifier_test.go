// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fee

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMoney(amount decimal.Decimal, currency string) Money {
	return Money{Amount: amount, Currency: currency}
}

func newTolerance(abs, percent decimal.Decimal) Tolerance {
	return Tolerance{Abs: abs, Percent: percent}
}

func TestVerifyFee_ExactMatch(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.NewFromInt(10), "USD")
	expected := newMoney(decimal.NewFromInt(10), "USD")
	tolerance := newTolerance(decimal.Zero, decimal.Zero)

	result, err := VerifyFee(actual, expected, tolerance)
	require.NoError(t, err)
	assert.Equal(t, VarianceMatch, result.Type)
	assert.True(t, result.Delta.IsZero())
}

func TestVerifyFee_MatchWithinAbsoluteTolerance(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.RequireFromString("10.05"), "USD")
	expected := newMoney(decimal.NewFromInt(10), "USD")
	tolerance := newTolerance(decimal.RequireFromString("0.10"), decimal.Zero)

	result, err := VerifyFee(actual, expected, tolerance)
	require.NoError(t, err)
	assert.Equal(t, VarianceMatch, result.Type)
}

func TestVerifyFee_MatchAtToleranceBoundary(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.RequireFromString("10.10"), "USD")
	expected := newMoney(decimal.NewFromInt(10), "USD")
	tolerance := newTolerance(decimal.RequireFromString("0.10"), decimal.Zero)

	result, err := VerifyFee(actual, expected, tolerance)
	require.NoError(t, err)
	assert.Equal(t, VarianceMatch, result.Type)
}

func TestVerifyFee_OverchargeOutsideTolerance(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.RequireFromString("10.11"), "USD")
	expected := newMoney(decimal.NewFromInt(10), "USD")
	tolerance := newTolerance(decimal.RequireFromString("0.10"), decimal.Zero)

	result, err := VerifyFee(actual, expected, tolerance)
	require.NoError(t, err)
	assert.Equal(t, VarianceOvercharge, result.Type)
	assert.True(t, result.Delta.Equal(decimal.RequireFromString("0.11")))
}

func TestVerifyFee_UnderchargeDetected(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.NewFromInt(9), "USD")
	expected := newMoney(decimal.NewFromInt(10), "USD")
	tolerance := newTolerance(decimal.RequireFromString("0.50"), decimal.Zero)

	result, err := VerifyFee(actual, expected, tolerance)
	require.NoError(t, err)
	assert.Equal(t, VarianceUndercharge, result.Type)
	assert.True(t, result.Delta.Equal(decimal.NewFromInt(-1)))
}

func TestVerifyFee_PercentageToleranceWhenLarger(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.NewFromInt(102), "USD")
	expected := newMoney(decimal.NewFromInt(100), "USD")
	// 5% of 102 = 5.1, which is larger than abs tolerance of 1
	tolerance := newTolerance(decimal.NewFromInt(1), decimal.RequireFromString("0.05"))

	result, err := VerifyFee(actual, expected, tolerance)
	require.NoError(t, err)
	assert.Equal(t, VarianceMatch, result.Type)
	assert.True(t, result.Threshold.Equal(decimal.RequireFromString("5.1")))
}

func TestVerifyFee_AbsoluteToleranceWhenLarger(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.RequireFromString("10.50"), "USD")
	expected := newMoney(decimal.NewFromInt(10), "USD")
	// 1% of 10.50 = 0.105, abs tolerance of 1 is larger
	tolerance := newTolerance(decimal.NewFromInt(1), decimal.RequireFromString("0.01"))

	result, err := VerifyFee(actual, expected, tolerance)
	require.NoError(t, err)
	assert.Equal(t, VarianceMatch, result.Type)
	assert.True(t, result.Threshold.Equal(decimal.NewFromInt(1)))
}

func TestVerifyFee_CurrencyMismatchError(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.NewFromInt(10), "USD")
	expected := newMoney(decimal.NewFromInt(10), "EUR")
	tolerance := newTolerance(decimal.NewFromInt(1), decimal.Zero)

	_, err := VerifyFee(actual, expected, tolerance)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCurrencyMismatch)
}

func TestVerifyFee_NegativeAbsoluteToleranceError(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.NewFromInt(10), "USD")
	expected := newMoney(decimal.NewFromInt(10), "USD")
	tolerance := newTolerance(decimal.NewFromInt(-1), decimal.Zero)

	_, err := VerifyFee(actual, expected, tolerance)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrToleranceNegative)
}

func TestVerifyFee_NegativePercentToleranceError(t *testing.T) {
	t.Parallel()

	actual := newMoney(decimal.NewFromInt(10), "USD")
	expected := newMoney(decimal.NewFromInt(10), "USD")
	tolerance := newTolerance(decimal.Zero, decimal.NewFromInt(-1))

	_, err := VerifyFee(actual, expected, tolerance)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrToleranceNegative)
}

func TestVerifyFee_ThresholdUsesMaxForPercentage(t *testing.T) {
	t.Parallel()

	// actual is larger, so percentage should be based on actual
	actual := newMoney(decimal.NewFromInt(200), "USD")
	expected := newMoney(decimal.NewFromInt(100), "USD")
	// 50% of 200 = 100
	tolerance := newTolerance(decimal.Zero, decimal.RequireFromString("0.50"))

	result, err := VerifyFee(actual, expected, tolerance)
	require.NoError(t, err)
	assert.Equal(t, VarianceMatch, result.Type)
	assert.True(t, result.Threshold.Equal(decimal.NewFromInt(100)))
}
