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

func TestNormalizeCurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       string
		expected    string
		expectedErr error
	}{
		{
			name:        "valid uppercase currency",
			input:       "USD",
			expected:    "USD",
			expectedErr: nil,
		},
		{
			name:        "valid lowercase currency",
			input:       "eur",
			expected:    "EUR",
			expectedErr: nil,
		},
		{
			name:        "mixed case currency",
			input:       "bRl",
			expected:    "BRL",
			expectedErr: nil,
		},
		{
			name:        "currency with leading whitespace",
			input:       "  USD",
			expected:    "USD",
			expectedErr: nil,
		},
		{
			name:        "currency with trailing whitespace",
			input:       "USD  ",
			expected:    "USD",
			expectedErr: nil,
		},
		{
			name:        "currency with both leading and trailing whitespace",
			input:       "  GBP  ",
			expected:    "GBP",
			expectedErr: nil,
		},
		{
			name:        "lowercase with whitespace",
			input:       "  jpy  ",
			expected:    "JPY",
			expectedErr: nil,
		},
		{
			name:        "empty string",
			input:       "",
			expected:    "",
			expectedErr: ErrInvalidCurrency,
		},
		{
			name:        "whitespace only single space",
			input:       " ",
			expected:    "",
			expectedErr: ErrInvalidCurrency,
		},
		{
			name:        "whitespace only multiple spaces",
			input:       "   ",
			expected:    "",
			expectedErr: ErrInvalidCurrency,
		},
		{
			name:        "whitespace only tab",
			input:       "\t",
			expected:    "",
			expectedErr: ErrInvalidCurrency,
		},
		{
			name:        "whitespace only mixed",
			input:       " \t\n ",
			expected:    "",
			expectedErr: ErrInvalidCurrency,
		},
		{
			name:        "non-ISO 4217 currency code",
			input:       "BTC",
			expected:    "",
			expectedErr: ErrInvalidCurrency,
		},
		{
			name:        "obsolete currency HRK rejected",
			input:       "HRK",
			expected:    "",
			expectedErr: ErrInvalidCurrency,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := NormalizeCurrency(tc.input)

			if tc.expectedErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tc.expectedErr)
				assert.Equal(t, tc.expected, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestNewMoney(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		amount           decimal.Decimal
		currency         string
		expectedCurrency string
		expectedErr      error
	}{
		{
			name:             "valid positive amount with uppercase currency",
			amount:           decimal.NewFromFloat(100.50),
			currency:         "USD",
			expectedCurrency: "USD",
			expectedErr:      nil,
		},
		{
			name:             "valid zero amount",
			amount:           decimal.Zero,
			currency:         "EUR",
			expectedCurrency: "EUR",
			expectedErr:      nil,
		},
		{
			name:             "valid negative amount",
			amount:           decimal.NewFromFloat(-50.25),
			currency:         "BRL",
			expectedCurrency: "BRL",
			expectedErr:      nil,
		},
		{
			name:             "lowercase currency gets normalized",
			amount:           decimal.NewFromFloat(200.00),
			currency:         "gbp",
			expectedCurrency: "GBP",
			expectedErr:      nil,
		},
		{
			name:             "mixed case currency gets normalized",
			amount:           decimal.NewFromFloat(300.00),
			currency:         "jPy",
			expectedCurrency: "JPY",
			expectedErr:      nil,
		},
		{
			name:             "currency with whitespace gets normalized",
			amount:           decimal.NewFromFloat(400.00),
			currency:         "  CHF  ",
			expectedCurrency: "CHF",
			expectedErr:      nil,
		},
		{
			name:             "large amount",
			amount:           decimal.NewFromFloat(9999999999.999999),
			currency:         "USD",
			expectedCurrency: "USD",
			expectedErr:      nil,
		},
		{
			name:             "small decimal amount",
			amount:           decimal.NewFromFloat(0.0001),
			currency:         "JPY",
			expectedCurrency: "JPY",
			expectedErr:      nil,
		},
		{
			name:             "unknown currency returns error",
			amount:           decimal.NewFromFloat(100.00),
			currency:         "XYZ",
			expectedCurrency: "",
			expectedErr:      ErrInvalidCurrency,
		},
		{
			name:             "empty currency returns error",
			amount:           decimal.NewFromFloat(100.00),
			currency:         "",
			expectedCurrency: "",
			expectedErr:      ErrInvalidCurrency,
		},
		{
			name:             "whitespace only currency returns error",
			amount:           decimal.NewFromFloat(100.00),
			currency:         "   ",
			expectedCurrency: "",
			expectedErr:      ErrInvalidCurrency,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			money, err := NewMoney(tc.amount, tc.currency)

			if tc.expectedErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tc.expectedErr)
				assert.Equal(t, Money{}, money)
			} else {
				require.NoError(t, err)
				assert.True(t, tc.amount.Equal(money.Amount))
				assert.Equal(t, tc.expectedCurrency, money.Currency)
			}
		})
	}
}

func TestMoneyStruct(t *testing.T) {
	t.Parallel()

	t.Run("zero value Money has zero amount and empty currency", func(t *testing.T) {
		t.Parallel()

		var m Money

		assert.True(t, m.Amount.IsZero())
		assert.Empty(t, m.Currency)
	})

	t.Run("Money fields are accessible", func(t *testing.T) {
		t.Parallel()

		money, err := NewMoney(decimal.NewFromFloat(123.45), "USD")
		require.NoError(t, err)

		assert.True(t, decimal.NewFromFloat(123.45).Equal(money.Amount))
		assert.Equal(t, "USD", money.Currency)
	})
}
