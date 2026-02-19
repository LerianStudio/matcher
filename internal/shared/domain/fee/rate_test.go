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

func TestNewRate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("creates rate with flat fee", func(t *testing.T) {
		t.Parallel()

		id := uuid.New()
		input := NewRateInput{
			ID:        id,
			Currency:  "usd",
			Structure: FlatFee{Amount: decimal.NewFromInt(5)},
		}

		rate, err := NewRate(ctx, input)
		require.NoError(t, err)
		assert.Equal(t, id, rate.ID)
		assert.Equal(t, "USD", rate.Currency)
		assert.Equal(t, FeeStructureFlat, rate.Structure.Type())
		assert.False(t, rate.CreatedAt.IsZero())
		assert.False(t, rate.UpdatedAt.IsZero())
	})

	t.Run("creates rate with percentage fee", func(t *testing.T) {
		t.Parallel()

		input := NewRateInput{
			Currency:  "EUR",
			Structure: PercentageFee{Rate: decimal.RequireFromString("0.025")},
		}

		rate, err := NewRate(ctx, input)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, rate.ID)
		assert.Equal(t, "EUR", rate.Currency)
		assert.Equal(t, FeeStructurePercentage, rate.Structure.Type())
	})

	t.Run("creates rate with tiered fee", func(t *testing.T) {
		t.Parallel()

		upTo1000 := decimal.NewFromInt(1000)
		input := NewRateInput{
			Currency: "BRL",
			Structure: TieredFee{Tiers: []Tier{
				{UpTo: &upTo1000, Rate: decimal.RequireFromString("0.01")},
				{UpTo: nil, Rate: decimal.RequireFromString("0.005")},
			}},
		}

		rate, err := NewRate(ctx, input)
		require.NoError(t, err)
		assert.Equal(t, "BRL", rate.Currency)
		assert.Equal(t, FeeStructureTiered, rate.Structure.Type())
	})

	t.Run("generates ID when not provided", func(t *testing.T) {
		t.Parallel()

		input := NewRateInput{
			Currency:  "USD",
			Structure: FlatFee{Amount: decimal.NewFromInt(1)},
		}

		rate, err := NewRate(ctx, input)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, rate.ID)
	})

	t.Run("fails with nil structure", func(t *testing.T) {
		t.Parallel()

		input := NewRateInput{
			Currency:  "USD",
			Structure: nil,
		}

		_, err := NewRate(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNilFeeStructure)
	})

	t.Run("fails with empty currency", func(t *testing.T) {
		t.Parallel()

		input := NewRateInput{
			Currency:  "",
			Structure: FlatFee{Amount: decimal.NewFromInt(5)},
		}

		_, err := NewRate(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidCurrency)
	})

	t.Run("fails with invalid fee structure", func(t *testing.T) {
		t.Parallel()

		input := NewRateInput{
			Currency:  "USD",
			Structure: FlatFee{Amount: decimal.NewFromInt(-5)},
		}

		_, err := NewRate(ctx, input)
		require.Error(t, err)
	})

	t.Run("normalizes currency to uppercase", func(t *testing.T) {
		t.Parallel()

		input := NewRateInput{
			Currency:  "  gbp  ",
			Structure: FlatFee{Amount: decimal.NewFromInt(1)},
		}

		rate, err := NewRate(ctx, input)
		require.NoError(t, err)
		assert.Equal(t, "GBP", rate.Currency)
	})
}
