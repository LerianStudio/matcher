// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fee

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlatFee_Type(t *testing.T) {
	t.Parallel()

	f := FlatFee{Amount: decimal.NewFromInt(100)}
	assert.Equal(t, FeeStructureFlat, f.Type())
}

func TestFlatFee_Validate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name    string
		amount  decimal.Decimal
		wantErr bool
	}{
		{"zero is valid", decimal.Zero, false},
		{"positive is valid", decimal.NewFromInt(100), false},
		{"negative is invalid", decimal.NewFromInt(-1), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := FlatFee{Amount: tt.amount}

			err := f.Validate(ctx)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFlatFee_Calculate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("returns fixed amount regardless of base", func(t *testing.T) {
		t.Parallel()

		flatFee := FlatFee{Amount: decimal.RequireFromString("5.00")}

		result, err := flatFee.Calculate(ctx, decimal.NewFromInt(100))
		require.NoError(t, err)
		assert.True(t, result.Equal(decimal.RequireFromString("5.00")))

		result, err = flatFee.Calculate(ctx, decimal.NewFromInt(1000))
		require.NoError(t, err)
		assert.True(t, result.Equal(decimal.RequireFromString("5.00")))
	})

	t.Run("negative flat fee fails validation", func(t *testing.T) {
		t.Parallel()

		f := FlatFee{Amount: decimal.NewFromInt(-5)}
		_, err := f.Calculate(ctx, decimal.NewFromInt(100))
		require.Error(t, err)
	})
}

func TestPercentageFee_Type(t *testing.T) {
	t.Parallel()

	p := PercentageFee{Rate: decimal.RequireFromString("0.015")}
	assert.Equal(t, FeeStructurePercentage, p.Type())
}

func TestPercentageFee_Validate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name    string
		rate    decimal.Decimal
		wantErr bool
	}{
		{"zero is valid", decimal.Zero, false},
		{"0.5 is valid", decimal.RequireFromString("0.5"), false},
		{"1.0 is valid", decimal.NewFromInt(1), false},
		{"negative is invalid", decimal.NewFromInt(-1), true},
		{"greater than 1 is invalid", decimal.RequireFromString("1.01"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := PercentageFee{Rate: tt.rate}

			err := p.Validate(ctx)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidPercentageRate)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPercentageFee_Calculate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("calculates correct percentage", func(t *testing.T) {
		t.Parallel()

		p := PercentageFee{Rate: decimal.RequireFromString("0.015")} // 1.5%

		result, err := p.Calculate(ctx, decimal.NewFromInt(1000))
		require.NoError(t, err)
		assert.True(t, result.Equal(decimal.RequireFromString("15")))
	})

	t.Run("negative base amount fails", func(t *testing.T) {
		t.Parallel()

		p := PercentageFee{Rate: decimal.RequireFromString("0.01")}
		_, err := p.Calculate(ctx, decimal.NewFromInt(-100))
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNegativeAmount)
	})

	t.Run("zero base returns zero fee", func(t *testing.T) {
		t.Parallel()

		p := PercentageFee{Rate: decimal.RequireFromString("0.05")}
		result, err := p.Calculate(ctx, decimal.Zero)
		require.NoError(t, err)
		assert.True(t, result.IsZero())
	})

	t.Run("exact arithmetic no float errors", func(t *testing.T) {
		t.Parallel()

		p := PercentageFee{Rate: decimal.RequireFromString("0.03")} // 3%
		result, err := p.Calculate(ctx, decimal.RequireFromString("333.33"))
		require.NoError(t, err)

		expected := decimal.RequireFromString("9.9999")
		assert.True(t, result.Equal(expected), "expected %s, got %s", expected, result)
	})
}

func TestTieredFee_Type(t *testing.T) {
	t.Parallel()

	tf := TieredFee{Tiers: []Tier{{UpTo: nil, Rate: decimal.RequireFromString("0.01")}}}
	assert.Equal(t, FeeStructureTiered, tf.Type())
}

func TestTieredFee_Validate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	upTo100 := decimal.NewFromInt(100)
	upTo200 := decimal.NewFromInt(200)
	upTo50 := decimal.NewFromInt(50)
	upToZero := decimal.Zero
	upToNeg := decimal.NewFromInt(-10)

	tests := []struct {
		name    string
		tiers   []Tier
		wantErr bool
	}{
		{
			name:    "empty tiers invalid",
			tiers:   []Tier{},
			wantErr: true,
		},
		{
			name: "single infinite tier valid",
			tiers: []Tier{
				{UpTo: nil, Rate: decimal.RequireFromString("0.01")},
			},
			wantErr: false,
		},
		{
			name: "valid progressive tiers",
			tiers: []Tier{
				{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")},
				{UpTo: &upTo200, Rate: decimal.RequireFromString("0.02")},
				{UpTo: nil, Rate: decimal.RequireFromString("0.03")},
			},
			wantErr: false,
		},
		{
			name: "tier after infinite invalid",
			tiers: []Tier{
				{UpTo: nil, Rate: decimal.RequireFromString("0.01")},
				{UpTo: &upTo100, Rate: decimal.RequireFromString("0.02")},
			},
			wantErr: true,
		},
		{
			name: "non-increasing upper bounds invalid",
			tiers: []Tier{
				{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")},
				{UpTo: &upTo50, Rate: decimal.RequireFromString("0.02")},
			},
			wantErr: true,
		},
		{
			name: "zero upper bound invalid",
			tiers: []Tier{
				{UpTo: &upToZero, Rate: decimal.RequireFromString("0.01")},
			},
			wantErr: true,
		},
		{
			name: "negative upper bound invalid",
			tiers: []Tier{
				{UpTo: &upToNeg, Rate: decimal.RequireFromString("0.01")},
			},
			wantErr: true,
		},
		{
			name: "negative rate invalid",
			tiers: []Tier{
				{UpTo: nil, Rate: decimal.NewFromInt(-1)},
			},
			wantErr: true,
		},
		{
			name: "rate greater than 1 invalid",
			tiers: []Tier{
				{UpTo: nil, Rate: decimal.RequireFromString("1.5")},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tf := TieredFee{Tiers: tt.tiers}

			err := tf.Validate(ctx)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTieredFee_Calculate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	upTo100 := decimal.NewFromInt(100)
	upTo500 := decimal.NewFromInt(500)

	t.Run("single tier no cap", func(t *testing.T) {
		t.Parallel()

		tf := TieredFee{Tiers: []Tier{
			{UpTo: nil, Rate: decimal.RequireFromString("0.02")},
		}}
		result, err := tf.Calculate(ctx, decimal.NewFromInt(1000))
		require.NoError(t, err)
		assert.True(t, result.Equal(decimal.NewFromInt(20)), "expected 20, got %s", result)
	})

	t.Run("progressive tiers", func(t *testing.T) {
		t.Parallel()

		tf := TieredFee{Tiers: []Tier{
			{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")}, // 1% on first 100
			{UpTo: &upTo500, Rate: decimal.RequireFromString("0.02")}, // 2% on 100-500
			{UpTo: nil, Rate: decimal.RequireFromString("0.03")},      // 3% on 500+
		}}

		// Test amount within first tier
		result, err := tf.Calculate(ctx, decimal.NewFromInt(50))
		require.NoError(t, err)

		expected := decimal.RequireFromString("0.5") // 50 * 0.01
		assert.True(t, result.Equal(expected), "expected %s, got %s", expected, result)

		// Test amount spanning two tiers
		result, err = tf.Calculate(ctx, decimal.NewFromInt(200))
		require.NoError(t, err)
		// First 100: 100 * 0.01 = 1
		// Next 100: 100 * 0.02 = 2
		// Total: 3
		expected = decimal.NewFromInt(3)
		assert.True(t, result.Equal(expected), "expected %s, got %s", expected, result)

		// Test amount spanning all tiers
		result, err = tf.Calculate(ctx, decimal.NewFromInt(700))
		require.NoError(t, err)
		// First 100: 100 * 0.01 = 1
		// 100-500: 400 * 0.02 = 8
		// 500+: 200 * 0.03 = 6
		// Total: 15
		expected = decimal.NewFromInt(15)
		assert.True(t, result.Equal(expected), "expected %s, got %s", expected, result)
	})

	t.Run("zero base amount", func(t *testing.T) {
		t.Parallel()

		tf := TieredFee{Tiers: []Tier{
			{UpTo: nil, Rate: decimal.RequireFromString("0.05")},
		}}
		result, err := tf.Calculate(ctx, decimal.Zero)
		require.NoError(t, err)
		assert.True(t, result.IsZero())
	})

	t.Run("negative base amount fails", func(t *testing.T) {
		t.Parallel()

		tf := TieredFee{Tiers: []Tier{
			{UpTo: nil, Rate: decimal.RequireFromString("0.01")},
		}}
		_, err := tf.Calculate(ctx, decimal.NewFromInt(-100))
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNegativeAmount)
	})

	t.Run("tier boundary exact", func(t *testing.T) {
		t.Parallel()

		tf := TieredFee{Tiers: []Tier{
			{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")},
			{UpTo: nil, Rate: decimal.RequireFromString("0.02")},
		}}
		result, err := tf.Calculate(ctx, decimal.NewFromInt(100))
		require.NoError(t, err)

		expected := decimal.NewFromInt(1) // exactly 100 * 0.01
		assert.True(t, result.Equal(expected), "expected %s, got %s", expected, result)
	})
}
