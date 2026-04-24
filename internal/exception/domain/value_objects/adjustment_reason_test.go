// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdjustmentReasonCode_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    AdjustmentReasonCode
		expected string
	}{
		{name: "amount_correction", input: AdjustmentReasonAmountCorrection, expected: "AMOUNT_CORRECTION"},
		{name: "currency_correction", input: AdjustmentReasonCurrencyCorrection, expected: "CURRENCY_CORRECTION"},
		{name: "date_correction", input: AdjustmentReasonDateCorrection, expected: "DATE_CORRECTION"},
		{name: "other", input: AdjustmentReasonOther, expected: "OTHER"},
		{name: "empty", input: AdjustmentReasonCode(""), expected: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, tc.input.String())
		})
	}
}

func TestAdjustmentReason_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    AdjustmentReasonCode
		expected bool
	}{
		{name: "amount_correction is valid", value: AdjustmentReasonAmountCorrection, expected: true},
		{name: "currency_correction is valid", value: AdjustmentReasonCurrencyCorrection, expected: true},
		{name: "date_correction is valid", value: AdjustmentReasonDateCorrection, expected: true},
		{name: "other is valid", value: AdjustmentReasonOther, expected: true},
		{name: "BAD is invalid", value: AdjustmentReasonCode("BAD"), expected: false},
		{name: "empty is invalid", value: AdjustmentReasonCode(""), expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expected, tc.value.IsValid())
		})
	}
}

func TestParseAdjustmentReason(t *testing.T) {
	t.Parallel()

	reason, err := ParseAdjustmentReason(" amount_correction ")
	require.NoError(t, err)
	require.Equal(t, AdjustmentReasonAmountCorrection, reason)

	reason, err = ParseAdjustmentReason("CURRENCY_CORRECTION")
	require.NoError(t, err)
	require.Equal(t, AdjustmentReasonCurrencyCorrection, reason)

	_, err = ParseAdjustmentReason(" ")
	require.ErrorIs(t, err, ErrInvalidAdjustmentReason)

	_, err = ParseAdjustmentReason("")
	require.ErrorIs(t, err, ErrInvalidAdjustmentReason)

	_, err = ParseAdjustmentReason("INVALID")
	require.ErrorIs(t, err, ErrInvalidAdjustmentReason)
}
