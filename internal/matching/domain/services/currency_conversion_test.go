//go:build unit

package services

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestCurrencyConversion_Accuracy(t *testing.T) {
	t.Parallel()

	amount := decimal.RequireFromString("10.00")
	rate, err := matchingVO.NewFXRate(
		decimal.RequireFromString("1.2345"),
		"ecb",
		time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)

	got, err := ConvertToBaseAmount(amount, rate, 4, RoundingHalfUp)
	require.NoError(t, err)

	require.Equal(t, decimal.RequireFromString("12.3450"), got)
}

func TestCurrencyConversion_RoundingModes(t *testing.T) {
	t.Parallel()

	amount := decimal.RequireFromString("10.00")
	rate, err := matchingVO.NewFXRate(
		decimal.RequireFromString("1.0005"),
		"ecb",
		time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)

	cases := []struct {
		name string
		mode RoundingMode
		want string
	}{
		{name: "half-up", mode: RoundingHalfUp, want: "10.01"},
		{name: "bankers", mode: RoundingBankers, want: "10.00"},
		{name: "floor", mode: RoundingFloor, want: "10.00"},
		{name: "ceil", mode: RoundingCeil, want: "10.01"},
		{name: "truncate", mode: RoundingTruncate, want: "10.00"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ConvertToBaseAmount(amount, rate, 2, tt.mode)
			require.NoError(t, err)
			require.Equal(t, decimal.RequireFromString(tt.want), got)
		})
	}
}

func TestCurrencyConversion_InvalidRate(t *testing.T) {
	t.Parallel()

	_, err := matchingVO.NewFXRate(
		decimal.Zero,
		"ecb",
		time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
	)
	require.ErrorIs(t, err, matchingVO.ErrFXRateNotPositive)

	_, err = matchingVO.NewFXRate(
		decimal.RequireFromString("-1.00"),
		"ecb",
		time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
	)
	require.ErrorIs(t, err, matchingVO.ErrFXRateNotPositive)
}

func TestCurrencyConversion_InvalidRounding(t *testing.T) {
	t.Parallel()

	amount := decimal.RequireFromString("10.00")
	rate, err := matchingVO.NewFXRate(
		decimal.RequireFromString("1.01"),
		"ecb",
		time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)

	_, err = ConvertToBaseAmount(amount, rate, -1, RoundingHalfUp)
	require.ErrorIs(t, err, ErrInvalidRoundingScale)

	_, err = ConvertToBaseAmount(amount, rate, 2, RoundingMode("BAD"))
	require.ErrorIs(t, err, ErrInvalidRoundingMode)
}
