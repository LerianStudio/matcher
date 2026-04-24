// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFXRate_NewFXRate(t *testing.T) {
	t.Parallel()

	validTime := time.Now()

	tests := []struct {
		name        string
		rate        decimal.Decimal
		source      string
		effectiveAt time.Time
		wantErr     error
	}{
		{"valid", decimal.NewFromFloat(1.5), "ECB", validTime, nil},
		{"zero rate", decimal.Zero, "ECB", validTime, ErrFXRateNotPositive},
		{"negative rate", decimal.NewFromFloat(-1.0), "ECB", validTime, ErrFXRateNotPositive},
		{"empty source", decimal.NewFromFloat(1.5), "", validTime, ErrFXRateSourceRequired},
		{"whitespace source", decimal.NewFromFloat(1.5), "   ", validTime, ErrFXRateSourceRequired},
		{
			"zero time",
			decimal.NewFromFloat(1.5),
			"ECB",
			time.Time{},
			ErrFXRateEffectiveDateRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx, err := NewFXRate(tt.rate, tt.source, tt.effectiveAt)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.True(t, fx.Rate().Equal(tt.rate))
				assert.Equal(t, tt.source, fx.Source())
				assert.Equal(t, tt.effectiveAt.UTC(), fx.EffectiveAt(), "effectiveAt should be normalized to UTC")
			}
		})
	}
}

func TestFXRate_IsValid(t *testing.T) {
	t.Parallel()

	validTime := time.Now()

	tests := []struct {
		name string
		fx   FXRate
		want bool
	}{
		{
			"valid FXRate",
			FXRate{rate: decimal.NewFromFloat(1.5), source: "ECB", effectiveAt: validTime},
			true,
		},
		{
			"zero-value struct is invalid",
			FXRate{},
			false,
		},
		{
			"zero rate is invalid",
			FXRate{rate: decimal.Zero, source: "ECB", effectiveAt: validTime},
			false,
		},
		{
			"empty source is invalid",
			FXRate{rate: decimal.NewFromFloat(1.5), source: "", effectiveAt: validTime},
			false,
		},
		{
			"zero time is invalid",
			FXRate{rate: decimal.NewFromFloat(1.5), source: "ECB", effectiveAt: time.Time{}},
			false,
		},
		{
			"negative rate is invalid",
			FXRate{rate: decimal.NewFromFloat(-1.0), source: "ECB", effectiveAt: validTime},
			false,
		},
		{
			"whitespace-only source is invalid",
			FXRate{rate: decimal.NewFromFloat(1.5), source: "   ", effectiveAt: validTime},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.fx.IsValid())
		})
	}
}
