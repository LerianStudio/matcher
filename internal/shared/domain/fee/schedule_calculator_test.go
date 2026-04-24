// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

func makeSchedule(order ApplicationOrder, scale int, mode RoundingMode, currency string, items []FeeScheduleItem) *FeeSchedule {
	return &FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Test Schedule",
		Currency:         currency,
		ApplicationOrder: order,
		RoundingScale:    scale,
		RoundingMode:     mode,
		Items:            items,
	}
}

func makeItem(name string, priority int, structure FeeStructure) FeeScheduleItem {
	return FeeScheduleItem{
		ID:        uuid.New(),
		Name:      name,
		Priority:  priority,
		Structure: structure,
	}
}

func TestCalculateSchedule(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("parallel single percentage", func(t *testing.T) {
		t.Parallel()

		items := []FeeScheduleItem{
			makeItem("Interchange", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
		}
		schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("1.50").Equal(breakdown.TotalFee.Amount),
			"expected total fee 1.50, got %s", breakdown.TotalFee.Amount)
		assert.True(t, decimal.RequireFromString("98.50").Equal(breakdown.NetAmount.Amount),
			"expected net 98.50, got %s", breakdown.NetAmount.Amount)
		assert.Len(t, breakdown.ItemFees, 1)
		assert.Equal(t, "USD", breakdown.TotalFee.Currency)
		assert.Equal(t, "USD", breakdown.NetAmount.Currency)
	})

	t.Run("parallel multiple items", func(t *testing.T) {
		t.Parallel()

		items := []FeeScheduleItem{
			makeItem("Interchange", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
			makeItem("Scheme Fee", 2, PercentageFee{Rate: decimal.RequireFromString("0.001")}),
			makeItem("Processing", 3, FlatFee{Amount: decimal.RequireFromString("0.30")}),
		}
		schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)

		// 1.50 + 0.10 + 0.30 = 1.90
		assert.True(t, decimal.RequireFromString("1.90").Equal(breakdown.TotalFee.Amount),
			"expected total fee 1.90, got %s", breakdown.TotalFee.Amount)
		assert.True(t, decimal.RequireFromString("98.10").Equal(breakdown.NetAmount.Amount),
			"expected net 98.10, got %s", breakdown.NetAmount.Amount)
		assert.Len(t, breakdown.ItemFees, 3)

		// All items should use gross as base in parallel mode
		for _, itemFee := range breakdown.ItemFees {
			assert.True(t, decimal.RequireFromString("100").Equal(itemFee.BaseUsed.Amount),
				"expected base 100, got %s for item %s", itemFee.BaseUsed.Amount, itemFee.ItemName)
		}
	})

	t.Run("parallel tiered schedule", func(t *testing.T) {
		t.Parallel()

		upTo100 := decimal.NewFromInt(100)
		items := []FeeScheduleItem{
			makeItem("Tiered", 1, TieredFee{Tiers: []Tier{
				{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")},
				{UpTo: nil, Rate: decimal.RequireFromString("0.02")},
			}}),
		}
		schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("150.00"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("2.00").Equal(breakdown.TotalFee.Amount),
			"expected total fee 2.00, got %s", breakdown.TotalFee.Amount)
		assert.True(t, decimal.RequireFromString("148.00").Equal(breakdown.NetAmount.Amount),
			"expected net 148.00, got %s", breakdown.NetAmount.Amount)
	})

	t.Run("cascading single item same as parallel", func(t *testing.T) {
		t.Parallel()

		items := []FeeScheduleItem{
			makeItem("Interchange", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("1.50").Equal(breakdown.TotalFee.Amount),
			"expected total fee 1.50, got %s", breakdown.TotalFee.Amount)
		assert.True(t, decimal.RequireFromString("98.50").Equal(breakdown.NetAmount.Amount),
			"expected net 98.50, got %s", breakdown.NetAmount.Amount)
	})

	t.Run("cascading multiple percentage items", func(t *testing.T) {
		t.Parallel()

		// fee1 = 100 * 0.015 = 1.50 (rounded to 2dp)
		// net1 = 100 - 1.50 = 98.50
		// fee2 = 98.50 * 0.005 = 0.4925 → rounded to 0.49 (HALF_UP at 2dp)
		// net2 = 98.50 - 0.49 = 98.01
		// totalFee = 1.50 + 0.49 = 1.99

		items := []FeeScheduleItem{
			makeItem("Interchange", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
			makeItem("Scheme Fee", 2, PercentageFee{Rate: decimal.RequireFromString("0.005")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("1.99").Equal(breakdown.TotalFee.Amount),
			"expected total fee 1.99, got %s", breakdown.TotalFee.Amount)
		assert.True(t, decimal.RequireFromString("98.01").Equal(breakdown.NetAmount.Amount),
			"expected net 98.01, got %s", breakdown.NetAmount.Amount)

		// Verify bases used
		require.Len(t, breakdown.ItemFees, 2)
		assert.True(t, decimal.RequireFromString("100").Equal(breakdown.ItemFees[0].BaseUsed.Amount),
			"item 0 base should be 100, got %s", breakdown.ItemFees[0].BaseUsed.Amount)
		assert.True(t, decimal.RequireFromString("98.50").Equal(breakdown.ItemFees[1].BaseUsed.Amount),
			"item 1 base should be 98.50, got %s", breakdown.ItemFees[1].BaseUsed.Amount)
	})

	t.Run("cascading with rounding mode HALF_UP", func(t *testing.T) {
		t.Parallel()

		// 100 * 0.015 = 1.5 → 1.50
		// 98.50 * 0.003 = 0.2955 → 0.30 (HALF_UP rounds 5 up)
		items := []FeeScheduleItem{
			makeItem("Fee A", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
			makeItem("Fee B", 2, PercentageFee{Rate: decimal.RequireFromString("0.003")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("0.30").Equal(breakdown.ItemFees[1].Fee.Amount),
			"HALF_UP: expected fee B = 0.30, got %s", breakdown.ItemFees[1].Fee.Amount)
	})

	t.Run("cascading with rounding mode FLOOR", func(t *testing.T) {
		t.Parallel()

		// 100 * 0.015 = 1.5 → 1.50
		// 98.50 * 0.003 = 0.2955 → 0.29 (FLOOR always rounds down)
		items := []FeeScheduleItem{
			makeItem("Fee A", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
			makeItem("Fee B", 2, PercentageFee{Rate: decimal.RequireFromString("0.003")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeFloor, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("0.29").Equal(breakdown.ItemFees[1].Fee.Amount),
			"FLOOR: expected fee B = 0.29, got %s", breakdown.ItemFees[1].Fee.Amount)
	})

	t.Run("cascading with rounding mode CEIL", func(t *testing.T) {
		t.Parallel()

		// 100 * 0.015 = 1.5 → 1.50
		// 98.50 * 0.003 = 0.2955 → 0.30 (CEIL always rounds up)
		items := []FeeScheduleItem{
			makeItem("Fee A", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
			makeItem("Fee B", 2, PercentageFee{Rate: decimal.RequireFromString("0.003")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeCeil, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("0.30").Equal(breakdown.ItemFees[1].Fee.Amount),
			"CEIL: expected fee B = 0.30, got %s", breakdown.ItemFees[1].Fee.Amount)
	})

	t.Run("cascading with rounding mode TRUNCATE", func(t *testing.T) {
		t.Parallel()

		// 100 * 0.015 = 1.5 → 1.50
		// 98.50 * 0.003 = 0.2955 → 0.29 (TRUNCATE drops digits)
		items := []FeeScheduleItem{
			makeItem("Fee A", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
			makeItem("Fee B", 2, PercentageFee{Rate: decimal.RequireFromString("0.003")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeTruncate, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("0.29").Equal(breakdown.ItemFees[1].Fee.Amount),
			"TRUNCATE: expected fee B = 0.29, got %s", breakdown.ItemFees[1].Fee.Amount)
	})

	t.Run("cascading with rounding mode BANKERS", func(t *testing.T) {
		t.Parallel()

		// Banker's rounding: rounds to even when exactly at 0.5
		// 0.2955 at 2dp → 0.30 (rounds 5 toward even: 29->30 because 30 is even)
		// Actually 0.2955 at 2dp: third digit is 5, so banker's rounds to even second digit
		// 0.29|55 → 0.30 (since 9 is odd, round up)
		items := []FeeScheduleItem{
			makeItem("Fee A", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
			makeItem("Fee B", 2, PercentageFee{Rate: decimal.RequireFromString("0.003")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeBankers, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		// 0.2955 bank-rounded at 2dp: digit after is 5 and there are more digits (5), so round up
		assert.True(t, decimal.RequireFromString("0.30").Equal(breakdown.ItemFees[1].Fee.Amount),
			"BANKERS: expected fee B = 0.30, got %s", breakdown.ItemFees[1].Fee.Amount)
	})

	t.Run("cascading mixed flat and percentage", func(t *testing.T) {
		t.Parallel()

		// flat $1 first, then 2% on remainder
		// fee1 = $1.00 (flat, rounded = 1.00)
		// net1 = 100 - 1.00 = 99.00
		// fee2 = 99.00 * 0.02 = 1.98
		// net2 = 99.00 - 1.98 = 97.02
		// totalFee = 1.00 + 1.98 = 2.98
		items := []FeeScheduleItem{
			makeItem("Processing", 1, FlatFee{Amount: decimal.RequireFromString("1.00")}),
			makeItem("Interchange", 2, PercentageFee{Rate: decimal.RequireFromString("0.02")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.RequireFromString("2.98").Equal(breakdown.TotalFee.Amount),
			"expected total fee 2.98, got %s", breakdown.TotalFee.Amount)
		assert.True(t, decimal.RequireFromString("97.02").Equal(breakdown.NetAmount.Amount),
			"expected net 97.02, got %s", breakdown.NetAmount.Amount)

		// Verify the second item used the net amount as base
		assert.True(t, decimal.RequireFromString("99.00").Equal(breakdown.ItemFees[1].BaseUsed.Amount),
			"expected base 99.00 for item 1, got %s", breakdown.ItemFees[1].BaseUsed.Amount)
	})

	t.Run("zero gross amount", func(t *testing.T) {
		t.Parallel()

		items := []FeeScheduleItem{
			makeItem("Interchange", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
			makeItem("Processing", 2, FlatFee{Amount: decimal.RequireFromString("0.30")}),
		}
		schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.Zero, Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		// 0 * 0.015 = 0, flat = 0.30 → total = 0.30
		// For zero gross in parallel: percentage gives 0, flat gives 0.30
		assert.True(t, decimal.RequireFromString("0.30").Equal(breakdown.TotalFee.Amount),
			"expected total fee 0.30, got %s", breakdown.TotalFee.Amount)
	})

	t.Run("nil schedule returns error", func(t *testing.T) {
		t.Parallel()

		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		_, err := CalculateSchedule(ctx, gross, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNilSchedule)
	})

	t.Run("currency mismatch returns error", func(t *testing.T) {
		t.Parallel()

		items := []FeeScheduleItem{
			makeItem("Fee", 1, FlatFee{Amount: decimal.NewFromInt(1)}),
		}
		schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "EUR", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		_, err := CalculateSchedule(ctx, gross, schedule)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrCurrencyMismatch)
	})

	t.Run("items processed in priority order regardless of input order", func(t *testing.T) {
		t.Parallel()

		// Items given out of order: priority 3, 1, 2
		// In cascading mode, order matters for the result
		items := []FeeScheduleItem{
			makeItem("Third", 3, PercentageFee{Rate: decimal.RequireFromString("0.001")}),
			makeItem("First", 1, FlatFee{Amount: decimal.RequireFromString("2.00")}),
			makeItem("Second", 2, PercentageFee{Rate: decimal.RequireFromString("0.01")}),
		}
		schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeHalfUp, "USD", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		require.Len(t, breakdown.ItemFees, 3)

		// Verify processing order by checking item names
		assert.Equal(t, "First", breakdown.ItemFees[0].ItemName)
		assert.Equal(t, "Second", breakdown.ItemFees[1].ItemName)
		assert.Equal(t, "Third", breakdown.ItemFees[2].ItemName)

		// First: flat $2.00 on $100 → fee=2.00, net=98.00
		assert.True(t, decimal.RequireFromString("2.00").Equal(breakdown.ItemFees[0].Fee.Amount),
			"expected first fee 2.00, got %s", breakdown.ItemFees[0].Fee.Amount)
		assert.True(t, decimal.RequireFromString("100").Equal(breakdown.ItemFees[0].BaseUsed.Amount))

		// Second: 1% on 98.00 → fee=0.98, net=97.02
		assert.True(t, decimal.RequireFromString("0.98").Equal(breakdown.ItemFees[1].Fee.Amount),
			"expected second fee 0.98, got %s", breakdown.ItemFees[1].Fee.Amount)
		assert.True(t, decimal.RequireFromString("98.00").Equal(breakdown.ItemFees[1].BaseUsed.Amount),
			"expected second base 98.00, got %s", breakdown.ItemFees[1].BaseUsed.Amount)

		// Third: 0.1% on 97.02 → fee=0.09702 → rounded to 0.10
		assert.True(t, decimal.RequireFromString("97.02").Equal(breakdown.ItemFees[2].BaseUsed.Amount),
			"expected third base 97.02, got %s", breakdown.ItemFees[2].BaseUsed.Amount)
	})

	t.Run("currency normalization matches case-insensitively", func(t *testing.T) {
		t.Parallel()

		items := []FeeScheduleItem{
			makeItem("Fee", 1, FlatFee{Amount: decimal.NewFromInt(1)}),
		}
		schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "usd", items)
		gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

		breakdown, err := CalculateSchedule(ctx, gross, schedule)
		require.NoError(t, err)
		assert.Equal(t, "USD", breakdown.TotalFee.Currency)
	})
}

func TestRoundAmount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		amount   string
		scale    int
		mode     RoundingMode
		expected string
	}{
		{
			name:     "HALF_UP rounds 0.125 at 2dp to 0.13",
			amount:   "0.125",
			scale:    2,
			mode:     RoundingModeHalfUp,
			expected: "0.13",
		},
		{
			name:     "FLOOR rounds 0.129 at 2dp to 0.12",
			amount:   "0.129",
			scale:    2,
			mode:     RoundingModeFloor,
			expected: "0.12",
		},
		{
			name:     "CEIL rounds 0.121 at 2dp to 0.13",
			amount:   "0.121",
			scale:    2,
			mode:     RoundingModeCeil,
			expected: "0.13",
		},
		{
			name:     "TRUNCATE drops digits 0.129 at 2dp to 0.12",
			amount:   "0.129",
			scale:    2,
			mode:     RoundingModeTruncate,
			expected: "0.12",
		},
		{
			name:     "BANKERS rounds 0.125 at 2dp to 0.12 (round to even)",
			amount:   "0.125",
			scale:    2,
			mode:     RoundingModeBankers,
			expected: "0.12",
		},
		{
			name:     "BANKERS rounds 0.135 at 2dp to 0.14 (round to even)",
			amount:   "0.135",
			scale:    2,
			mode:     RoundingModeBankers,
			expected: "0.14",
		},
		{
			name:     "scale 0 rounds to integer",
			amount:   "1.567",
			scale:    0,
			mode:     RoundingModeHalfUp,
			expected: "2",
		},
		{
			name:     "scale 4 preserves precision",
			amount:   "1.23456",
			scale:    4,
			mode:     RoundingModeHalfUp,
			expected: "1.2346",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			amount := decimal.RequireFromString(tt.amount)
			expected := decimal.RequireFromString(tt.expected)
			result := roundAmount(amount, tt.scale, tt.mode)
			assert.True(t, expected.Equal(result),
				"expected %s, got %s", expected, result)
		})
	}
}

func TestCalculateSchedule_NilStructureParallel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	items := []FeeScheduleItem{
		{ID: uuid.New(), Name: "Bad", Priority: 1, Structure: nil},
	}
	schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
	gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

	_, err := CalculateSchedule(ctx, gross, schedule)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilFeeStructure)
}

func TestCalculateSchedule_NilStructureCascading(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	items := []FeeScheduleItem{
		{ID: uuid.New(), Name: "Bad", Priority: 1, Structure: nil},
	}
	schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeHalfUp, "USD", items)
	gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

	_, err := CalculateSchedule(ctx, gross, schedule)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilFeeStructure)
}

func TestCalculateSchedule_CascadingNegativeBaseClampedToZero(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Flat fee of $60 + flat fee of $60 on gross of $100
	// After first: base = 100 - 60 = 40
	// After second: base = 40 - 60 = -20 → clamped to 0
	// Total fee = 120, but net = 0 (clamped)
	items := []FeeScheduleItem{
		makeItem("Big Fee 1", 1, FlatFee{Amount: decimal.RequireFromString("60")}),
		makeItem("Big Fee 2", 2, FlatFee{Amount: decimal.RequireFromString("60")}),
		makeItem("Percentage After", 3, PercentageFee{Rate: decimal.RequireFromString("0.10")}),
	}
	schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeHalfUp, "USD", items)
	gross := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

	breakdown, err := CalculateSchedule(ctx, gross, schedule)
	require.NoError(t, err)

	// Third item should get base 0 (clamped), so fee = 0
	require.Len(t, breakdown.ItemFees, 3)
	assert.True(t, decimal.Zero.Equal(breakdown.ItemFees[2].BaseUsed.Amount),
		"expected base 0 after clamping, got %s", breakdown.ItemFees[2].BaseUsed.Amount)
	assert.True(t, decimal.Zero.Equal(breakdown.ItemFees[2].Fee.Amount),
		"expected fee 0 on clamped base, got %s", breakdown.ItemFees[2].Fee.Amount)
}

func TestCalculateGrossFromNet_SinglePercentage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// net = $98.50, 1.5% fee → gross = 98.50 / (1 - 0.015) = 100.0
	items := []FeeScheduleItem{
		makeItem("Interchange", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
	}
	schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
	net := Money{Amount: decimal.RequireFromString("98.50"), Currency: "USD"}

	gross, breakdown, err := CalculateGrossFromNet(ctx, net, schedule)
	require.NoError(t, err)

	// gross should be 100.00
	assert.True(t, decimal.RequireFromString("100").Equal(gross.Amount.Round(2)),
		"expected gross ~100.00, got %s", gross.Amount)
	assert.NotNil(t, breakdown)
}

func TestCalculateGrossFromNet_MultipleItems(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Interchange 1.5% + Scheme 0.1% + flat $0.30
	items := []FeeScheduleItem{
		makeItem("Interchange", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
		makeItem("Scheme Fee", 2, PercentageFee{Rate: decimal.RequireFromString("0.001")}),
		makeItem("Processing", 3, FlatFee{Amount: decimal.RequireFromString("0.30")}),
	}
	schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
	net := Money{Amount: decimal.RequireFromString("97.70"), Currency: "USD"}

	gross, breakdown, err := CalculateGrossFromNet(ctx, net, schedule)
	require.NoError(t, err)
	require.NotNil(t, breakdown)

	// Verify the gross produces close to the expected net
	assert.True(t, gross.Amount.GreaterThan(net.Amount),
		"gross should be greater than net")
}

func TestCalculateGrossFromNet_FlatOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// net = $99.70, flat $0.30 → gross = $100.00
	items := []FeeScheduleItem{
		makeItem("Processing", 1, FlatFee{Amount: decimal.RequireFromString("0.30")}),
	}
	schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
	net := Money{Amount: decimal.RequireFromString("99.70"), Currency: "USD"}

	gross, breakdown, err := CalculateGrossFromNet(ctx, net, schedule)
	require.NoError(t, err)

	assert.True(t, decimal.RequireFromString("100").Equal(gross.Amount.Round(2)),
		"expected gross 100.00, got %s", gross.Amount)
	assert.NotNil(t, breakdown)
}

func TestCalculateGrossFromNet_NilSchedule(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	net := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

	_, _, err := CalculateGrossFromNet(ctx, net, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilSchedule)
}

func TestCalculateGrossFromNet_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	items := []FeeScheduleItem{
		makeItem("Fee", 1, FlatFee{Amount: decimal.NewFromInt(1)}),
	}
	schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "EUR", items)
	net := Money{Amount: decimal.RequireFromString("100"), Currency: "USD"}

	_, _, err := CalculateGrossFromNet(ctx, net, schedule)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCurrencyMismatch)
}

func TestCalculateGrossFromNet_ZeroNet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("zero net with percentage only", func(t *testing.T) {
		t.Parallel()

		items := []FeeScheduleItem{
			makeItem("Fee", 1, PercentageFee{Rate: decimal.RequireFromString("0.015")}),
		}
		schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
		net := Money{Amount: decimal.Zero, Currency: "USD"}

		gross, _, err := CalculateGrossFromNet(ctx, net, schedule)
		require.NoError(t, err)
		assert.True(t, decimal.Zero.Equal(gross.Amount),
			"expected gross 0, got %s", gross.Amount)
	})

	t.Run("zero net with flat fee", func(t *testing.T) {
		t.Parallel()

		items := []FeeScheduleItem{
			makeItem("Flat", 1, FlatFee{Amount: decimal.RequireFromString("0.30")}),
		}
		schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
		net := Money{Amount: decimal.Zero, Currency: "USD"}

		gross, _, err := CalculateGrossFromNet(ctx, net, schedule)
		require.NoError(t, err)
		// With flat fee of 0.30, gross should be 0.30 so that net = 0.30 - 0.30 = 0
		assert.True(t, decimal.RequireFromString("0.30").Equal(gross.Amount.Round(2)),
			"expected gross 0.30, got %s", gross.Amount)
	})
}

func TestCalculateGrossFromNet_ExtremeRate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// 50% fee rate — should converge with 100 iterations
	schedule := &FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Extreme Rate Schedule",
		Currency:         "USD",
		ApplicationOrder: ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     RoundingModeHalfUp,
		Items: []FeeScheduleItem{
			{ID: uuid.New(), Name: "high_fee", Priority: 1, Structure: PercentageFee{Rate: decimal.RequireFromString("0.50")}},
		},
	}

	net, err := NewMoney(decimal.RequireFromString("50.00"), "USD")
	require.NoError(t, err)

	gross, breakdown, err := CalculateGrossFromNet(ctx, net, schedule)
	require.NoError(t, err)
	// gross should be 100.00 (50 / (1 - 0.5) = 100)
	assert.True(t, gross.Amount.Equal(decimal.RequireFromString("100.00")),
		"expected gross 100.00, got %s", gross.Amount)
	assert.True(t, breakdown.TotalFee.Amount.Equal(decimal.RequireFromString("50.00")),
		"expected fee 50.00, got %s", breakdown.TotalFee.Amount)
}

func TestCalculateGrossFromNet_Cascading(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Cascading: flat $1 then 2% on remainder
	items := []FeeScheduleItem{
		makeItem("Processing", 1, FlatFee{Amount: decimal.RequireFromString("1.00")}),
		makeItem("Interchange", 2, PercentageFee{Rate: decimal.RequireFromString("0.02")}),
	}
	schedule := makeSchedule(ApplicationOrderCascading, 2, RoundingModeHalfUp, "USD", items)
	net := Money{Amount: decimal.RequireFromString("97.02"), Currency: "USD"}

	gross, breakdown, err := CalculateGrossFromNet(ctx, net, schedule)
	require.NoError(t, err)
	require.NotNil(t, breakdown)

	// Forward: gross=$100 → flat $1 on $100 → net1=$99 → 2% of $99=$1.98 → net=$97.02
	// So inverse from net=$97.02 should converge to gross=$100.00
	assert.True(t, decimal.RequireFromString("100.00").Equal(gross.Amount.Round(2)),
		"expected gross 100.00, got %s", gross.Amount)
	assert.True(t, decimal.RequireFromString("2.98").Equal(breakdown.TotalFee.Amount),
		"expected total fee 2.98, got %s", breakdown.TotalFee.Amount)
}

func TestCalculateGrossFromNet_TieredSchedule(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	upTo100 := decimal.NewFromInt(100)
	items := []FeeScheduleItem{
		makeItem("Tiered", 1, TieredFee{Tiers: []Tier{
			{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")},
			{UpTo: nil, Rate: decimal.RequireFromString("0.02")},
		}}),
	}
	schedule := makeSchedule(ApplicationOrderParallel, 2, RoundingModeHalfUp, "USD", items)
	net := Money{Amount: decimal.RequireFromString("148.00"), Currency: "USD"}

	gross, breakdown, err := CalculateGrossFromNet(ctx, net, schedule)
	require.NoError(t, err)
	require.NotNil(t, breakdown)
	assert.True(t, decimal.RequireFromString("150.00").Equal(gross.Amount.Round(2)),
		"expected gross 150.00, got %s", gross.Amount)
	assert.True(t, decimal.RequireFromString("2.00").Equal(breakdown.TotalFee.Amount),
		"expected total fee 2.00, got %s", breakdown.TotalFee.Amount)
}
