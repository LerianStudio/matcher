// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package parsers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsValidCurrencyCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		code  string
		valid bool
	}{
		{"USD is valid", "USD", true},
		{"EUR is valid", "EUR", true},
		{"GBP is valid", "GBP", true},
		{"JPY is valid", "JPY", true},
		{"CNY is valid", "CNY", true},
		{"CHF is valid", "CHF", true},
		{"AUD is valid", "AUD", true},
		{"CAD is valid", "CAD", true},
		{"BRL is valid", "BRL", true},
		{"INR is valid", "INR", true},
		{"MXN is valid", "MXN", true},
		{"KRW is valid", "KRW", true},
		{"RUB is valid", "RUB", true},
		{"ZAR is valid", "ZAR", true},
		{"XXX is valid (no currency)", "XXX", true},
		{"XAU is valid (gold)", "XAU", true},
		{"XAG is valid (silver)", "XAG", true},

		{"empty string is invalid", "", false},
		{"lowercase usd is invalid", "usd", false},
		{"USDD is invalid (too long)", "USDD", false},
		{"US is invalid (too short)", "US", false},
		{"123 is invalid (numeric)", "123", false},
		{"ABC is invalid (not ISO 4217)", "ABC", false},
		{"XYZ is invalid (not ISO 4217)", "XYZ", false},
		{"FOO is invalid", "FOO", false},
		{"BAR is invalid", "BAR", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := isValidCurrencyCode(tc.code)
			require.Equal(t, tc.valid, result)
		})
	}
}

func TestValidCurrencyCodesContainsExpected(t *testing.T) {
	t.Parallel()

	expectedCodes := []string{
		"USD", "EUR", "GBP", "JPY", "CNY", "CHF", "AUD", "CAD",
		"NZD", "HKD", "SGD", "SEK", "NOK", "DKK", "PLN", "CZK",
		"HUF", "TRY", "THB", "MYR", "IDR", "PHP", "VND", "PKR",
		"BDT", "NGN", "EGP", "KES", "TZS", "GHS", "ZMW", "UGX",
	}

	for _, code := range expectedCodes {
		require.Contains(t, validCurrencyCodes, code, "Expected %s to be in valid currency codes", code)
	}
}

func TestValidCurrencyCodesCount(t *testing.T) {
	t.Parallel()

	count := len(validCurrencyCodes)
	require.GreaterOrEqual(t, count, 150, "Expected at least 150 currency codes")
	require.LessOrEqual(t, count, 200, "Expected at most 200 currency codes")
}
