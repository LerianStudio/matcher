//go:build unit

package fee

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRateWithFlatFee(currency string, amount decimal.Decimal) *Rate {
	return &Rate{
		ID:        uuid.New(),
		Currency:  currency,
		Structure: FlatFee{Amount: amount},
	}
}

func newRateWithPercentage(currency string, rate decimal.Decimal) *Rate {
	return &Rate{
		ID:        uuid.New(),
		Currency:  currency,
		Structure: PercentageFee{Rate: rate},
	}
}

func newRateWithTiers(currency string, tiers []Tier) *Rate {
	return &Rate{
		ID:        uuid.New(),
		Currency:  currency,
		Structure: TieredFee{Tiers: tiers},
	}
}

func newTransactionForFee(amount decimal.Decimal, currency string) *TransactionForFee {
	return &TransactionForFee{
		Amount: Money{Amount: amount, Currency: currency},
	}
}

func TestCalculateExpectedFee(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("calculates flat fee correctly", func(t *testing.T) {
		t.Parallel()

		rate := newRateWithFlatFee("USD", decimal.RequireFromString("2.50"))
		tx := newTransactionForFee(decimal.NewFromInt(100), "USD")

		result, err := CalculateExpectedFee(ctx, tx, rate)
		require.NoError(t, err)
		assert.Equal(t, "USD", result.Currency)
		assert.True(t, result.Amount.Equal(decimal.RequireFromString("2.50")))
	})

	t.Run("calculates percentage fee correctly", func(t *testing.T) {
		t.Parallel()

		rate := newRateWithPercentage("USD", decimal.RequireFromString("0.015"))
		tx := newTransactionForFee(decimal.NewFromInt(1000), "USD")

		result, err := CalculateExpectedFee(ctx, tx, rate)
		require.NoError(t, err)
		assert.True(t, result.Amount.Equal(decimal.NewFromInt(15)))
	})

	t.Run("calculates tiered fee correctly", func(t *testing.T) {
		t.Parallel()

		upTo100 := decimal.NewFromInt(100)
		rate := newRateWithTiers("USD", []Tier{
			{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")},
			{UpTo: nil, Rate: decimal.RequireFromString("0.02")},
		})
		tx := newTransactionForFee(decimal.NewFromInt(200), "USD")

		result, err := CalculateExpectedFee(ctx, tx, rate)
		require.NoError(t, err)
		// First 100 * 0.01 = 1, next 100 * 0.02 = 2
		assert.True(t, result.Amount.Equal(decimal.NewFromInt(3)))
	})

	t.Run("uses absolute value of transaction amount", func(t *testing.T) {
		t.Parallel()

		rate := newRateWithPercentage("USD", decimal.RequireFromString("0.01"))
		tx := newTransactionForFee(decimal.NewFromInt(-1000), "USD")

		result, err := CalculateExpectedFee(ctx, tx, rate)
		require.NoError(t, err)
		assert.True(t, result.Amount.Equal(decimal.NewFromInt(10)))
	})

	t.Run("returns error for nil transaction", func(t *testing.T) {
		t.Parallel()

		rate := newRateWithFlatFee("USD", decimal.NewFromInt(1))

		_, err := CalculateExpectedFee(ctx, nil, rate)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNilTransaction)
	})

	t.Run("returns error for nil rate", func(t *testing.T) {
		t.Parallel()

		tx := newTransactionForFee(decimal.NewFromInt(100), "USD")

		_, err := CalculateExpectedFee(ctx, tx, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNilRate)
	})

	t.Run("returns error for currency mismatch", func(t *testing.T) {
		t.Parallel()

		rate := newRateWithFlatFee("EUR", decimal.NewFromInt(1))
		tx := newTransactionForFee(decimal.NewFromInt(100), "USD")

		_, err := CalculateExpectedFee(ctx, tx, rate)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrCurrencyMismatch)
	})

	t.Run("calculates fee correctly for non-USD currency", func(t *testing.T) {
		t.Parallel()

		rate := newRateWithPercentage("EUR", decimal.RequireFromString("0.02"))
		tx := newTransactionForFee(decimal.NewFromInt(500), "EUR")

		result, err := CalculateExpectedFee(ctx, tx, rate)
		require.NoError(t, err)
		assert.Equal(t, "EUR", result.Currency)
		assert.True(t, result.Amount.Equal(decimal.NewFromInt(10)))
	})

	t.Run("exact arithmetic - no floating point errors", func(t *testing.T) {
		t.Parallel()

		rate := newRateWithPercentage("USD", decimal.RequireFromString("0.029"))
		tx := newTransactionForFee(decimal.RequireFromString("99.99"), "USD")

		result, err := CalculateExpectedFee(ctx, tx, rate)
		require.NoError(t, err)

		expected := decimal.RequireFromString("2.89971")
		assert.True(
			t,
			result.Amount.Equal(expected),
			"expected %s, got %s",
			expected,
			result.Amount,
		)
	})
}

func TestCalculateExpectedFee_TierBoundaries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	upTo100 := decimal.NewFromInt(100)
	upTo500 := decimal.NewFromInt(500)
	upTo1000 := decimal.NewFromInt(1000)

	rate := &Rate{
		ID:       uuid.New(),
		Currency: "USD",
		Structure: TieredFee{Tiers: []Tier{
			{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")},
			{UpTo: &upTo500, Rate: decimal.RequireFromString("0.02")},
			{UpTo: &upTo1000, Rate: decimal.RequireFromString("0.025")},
			{UpTo: nil, Rate: decimal.RequireFromString("0.03")},
		}},
	}

	tests := []struct {
		name     string
		amount   decimal.Decimal
		expected decimal.Decimal
	}{
		{"zero amount", decimal.Zero, decimal.Zero},
		{"exactly at first tier", decimal.NewFromInt(100), decimal.NewFromInt(1)},
		{
			"one cent over first tier",
			decimal.RequireFromString("100.01"),
			decimal.RequireFromString("1.0002"),
		},
		{"exactly at second tier", decimal.NewFromInt(500), decimal.NewFromInt(9)},
		{"exactly at third tier", decimal.NewFromInt(1000), decimal.RequireFromString("21.5")},
		{"into infinite tier", decimal.NewFromInt(1500), decimal.RequireFromString("36.5")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tx := &TransactionForFee{
				Amount: Money{Amount: tt.amount, Currency: "USD"},
			}
			result, err := CalculateExpectedFee(ctx, tx, rate)
			require.NoError(t, err)
			assert.True(
				t,
				result.Amount.Equal(tt.expected),
				"for amount %s: expected %s, got %s",
				tt.amount,
				tt.expected,
				result.Amount,
			)
		})
	}
}
