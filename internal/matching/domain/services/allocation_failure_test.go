// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocationFailureCode_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     AllocationFailureCode
		expected string
	}{
		{
			name:     "FX rate unavailable code",
			code:     AllocationFailureFXRateUnavailable,
			expected: "FX_RATE_UNAVAILABLE",
		},
		{
			name:     "split incomplete code",
			code:     AllocationFailureSplitIncomplete,
			expected: "SPLIT_INCOMPLETE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.code))
		})
	}
}

func TestAllocationFailure_MetaConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "meta missing key",
			constant: AllocationFailureMetaMissingKey,
			expected: "missing",
		},
		{
			name:     "missing amount base",
			constant: AllocationFailureMissingAmountBase,
			expected: "amount_base",
		},
		{
			name:     "missing currency base",
			constant: AllocationFailureMissingCurrencyBase,
			expected: "currency_base",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.constant)
		})
	}
}

func TestNewFXRateUnavailableFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		targetID     uuid.UUID
		missing      string
		expectedCode AllocationFailureCode
	}{
		{
			name:         "missing amount base",
			targetID:     uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			missing:      AllocationFailureMissingAmountBase,
			expectedCode: AllocationFailureFXRateUnavailable,
		},
		{
			name:         "missing currency base",
			targetID:     uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			missing:      AllocationFailureMissingCurrencyBase,
			expectedCode: AllocationFailureFXRateUnavailable,
		},
		{
			name:         "nil target ID",
			targetID:     uuid.Nil,
			missing:      AllocationFailureMissingAmountBase,
			expectedCode: AllocationFailureFXRateUnavailable,
		},
		{
			name:         "empty missing field",
			targetID:     uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			missing:      "",
			expectedCode: AllocationFailureFXRateUnavailable,
		},
		{
			name:         "custom missing field",
			targetID:     uuid.MustParse("44444444-4444-4444-4444-444444444444"),
			missing:      "custom_field",
			expectedCode: AllocationFailureFXRateUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := NewFXRateUnavailableFailure(tt.targetID, tt.missing)

			require.NotNil(t, failure)
			assert.Equal(t, tt.expectedCode, failure.Code)
			assert.Equal(t, tt.targetID, failure.TargetID)
			require.NotNil(t, failure.Meta)
			assert.Equal(t, tt.missing, failure.Meta[AllocationFailureMetaMissingKey])
			assert.Len(t, failure.Meta, 1)
		})
	}
}

func validateSplitIncompleteFailure(
	t *testing.T,
	failure *AllocationFailure,
	targetID uuid.UUID,
	expectedTotal, allocatedTotal, gap, currency string,
	useBaseAmount, allowPartial bool,
) {
	t.Helper()
	require.NotNil(t, failure)
	assert.Equal(t, AllocationFailureSplitIncomplete, failure.Code)
	assert.Equal(t, targetID, failure.TargetID)
	require.NotNil(t, failure.Meta)
	assert.Len(t, failure.Meta, 6)
	assert.Equal(t, expectedTotal, failure.Meta["expected_total"])
	assert.Equal(t, allocatedTotal, failure.Meta["allocated_total"])
	assert.Equal(t, gap, failure.Meta["gap"])
	assert.Equal(t, currency, failure.Meta["currency"])
	assert.Equal(t, boolToString(useBaseAmount), failure.Meta["use_base_amount"])
	assert.Equal(t, boolToString(allowPartial), failure.Meta["allow_partial"])
}

func TestNewSplitIncompleteFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		targetID       uuid.UUID
		expectedTotal  string
		allocatedTotal string
		gap            string
		currency       string
		useBaseAmount  bool
		allowPartial   bool
	}{
		{
			name:           "basic split incomplete with base amount",
			targetID:       uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			expectedTotal:  "1000.00",
			allocatedTotal: "750.00",
			gap:            "250.00",
			currency:       "USD",
			useBaseAmount:  true,
			allowPartial:   false,
		},
		{
			name:           "split incomplete without base amount",
			targetID:       uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			expectedTotal:  "500.00",
			allocatedTotal: "300.00",
			gap:            "200.00",
			currency:       "EUR",
			useBaseAmount:  false,
			allowPartial:   false,
		},
		{
			name:           "split incomplete with partial allowed",
			targetID:       uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			expectedTotal:  "2000.00",
			allocatedTotal: "1500.00",
			gap:            "500.00",
			currency:       "BRL",
			useBaseAmount:  false,
			allowPartial:   true,
		},
		{
			name:           "both flags true",
			targetID:       uuid.MustParse("44444444-4444-4444-4444-444444444444"),
			expectedTotal:  "100.00",
			allocatedTotal: "50.00",
			gap:            "50.00",
			currency:       "GBP",
			useBaseAmount:  true,
			allowPartial:   true,
		},
		{
			name:           "nil target ID",
			targetID:       uuid.Nil,
			expectedTotal:  "100.00",
			allocatedTotal: "0.00",
			gap:            "100.00",
			currency:       "USD",
			useBaseAmount:  false,
			allowPartial:   false,
		},
		{
			name:           "empty values",
			targetID:       uuid.MustParse("55555555-5555-5555-5555-555555555555"),
			expectedTotal:  "",
			allocatedTotal: "",
			gap:            "",
			currency:       "",
			useBaseAmount:  false,
			allowPartial:   false,
		},
		{
			name:           "zero gap",
			targetID:       uuid.MustParse("66666666-6666-6666-6666-666666666666"),
			expectedTotal:  "1000.00",
			allocatedTotal: "1000.00",
			gap:            "0.00",
			currency:       "USD",
			useBaseAmount:  true,
			allowPartial:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := NewSplitIncompleteFailure(
				tt.targetID,
				tt.expectedTotal,
				tt.allocatedTotal,
				tt.gap,
				tt.currency,
				tt.useBaseAmount,
				tt.allowPartial,
			)

			validateSplitIncompleteFailure(
				t,
				failure,
				tt.targetID,
				tt.expectedTotal,
				tt.allocatedTotal,
				tt.gap,
				tt.currency,
				tt.useBaseAmount,
				tt.allowPartial,
			)
		})
	}
}

func TestBoolToString_ViaConstructors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		useBaseAmount bool
		allowPartial  bool
		expectedBase  string
		expectedAllow string
	}{
		{
			name:          "both false",
			useBaseAmount: false,
			allowPartial:  false,
			expectedBase:  "false",
			expectedAllow: "false",
		},
		{
			name:          "both true",
			useBaseAmount: true,
			allowPartial:  true,
			expectedBase:  "true",
			expectedAllow: "true",
		},
		{
			name:          "base true partial false",
			useBaseAmount: true,
			allowPartial:  false,
			expectedBase:  "true",
			expectedAllow: "false",
		},
		{
			name:          "base false partial true",
			useBaseAmount: false,
			allowPartial:  true,
			expectedBase:  "false",
			expectedAllow: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := NewSplitIncompleteFailure(
				uuid.New(),
				"100.00",
				"50.00",
				"50.00",
				"USD",
				tt.useBaseAmount,
				tt.allowPartial,
			)

			assert.Equal(t, tt.expectedBase, failure.Meta["use_base_amount"])
			assert.Equal(t, tt.expectedAllow, failure.Meta["allow_partial"])
		})
	}
}

func TestAllocationFailure_StructFields(t *testing.T) {
	t.Parallel()

	targetID := uuid.New()
	failure := NewFXRateUnavailableFailure(targetID, AllocationFailureMissingAmountBase)

	t.Run("code is set correctly", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, AllocationFailureFXRateUnavailable, failure.Code)
	})

	t.Run("target ID is set correctly", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, targetID, failure.TargetID)
	})

	t.Run("meta map is initialized", func(t *testing.T) {
		t.Parallel()
		assert.NotNil(t, failure.Meta)
	})
}
