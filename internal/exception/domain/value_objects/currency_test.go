//go:build unit

package value_objects

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCurrencyCode_IsValid(t *testing.T) {
	t.Parallel()

	require.True(t, CurrencyCode("USD").IsValid())
	require.True(t, CurrencyCode("EUR").IsValid())
	require.True(t, CurrencyCode("BRL").IsValid())
	require.True(t, CurrencyCode("JPY").IsValid())
	require.False(t, CurrencyCode("XXX").IsValid())
	require.False(t, CurrencyCode("").IsValid())
	require.False(t, CurrencyCode("INVALID").IsValid())
}

func TestCurrencyCode_ObsoleteCurrenciesRejected(t *testing.T) {
	t.Parallel()

	// HRK (Croatian Kuna) was replaced by EUR on 2023-01-01.
	// Only active ISO 4217 codes are accepted.
	require.False(t, CurrencyCode("HRK").IsValid(), "HRK is obsolete since 2023-01-01; use EUR")

	_, err := ParseCurrencyCode("HRK")
	require.ErrorIs(t, err, ErrInvalidCurrencyCode)
}

func TestParseCurrencyCode(t *testing.T) {
	t.Parallel()

	code, err := ParseCurrencyCode(" usd ")
	require.NoError(t, err)
	require.Equal(t, CurrencyCode("USD"), code)

	code, err = ParseCurrencyCode("eur")
	require.NoError(t, err)
	require.Equal(t, CurrencyCode("EUR"), code)

	_, err = ParseCurrencyCode("")
	require.ErrorIs(t, err, ErrInvalidCurrencyCode)

	_, err = ParseCurrencyCode("   ")
	require.ErrorIs(t, err, ErrInvalidCurrencyCode)

	_, err = ParseCurrencyCode("INVALID")
	require.ErrorIs(t, err, ErrInvalidCurrencyCode)

	_, err = ParseCurrencyCode("ABC")
	require.ErrorIs(t, err, ErrInvalidCurrencyCode)
}

func TestCurrencyCode_String(t *testing.T) {
	t.Parallel()

	require.Equal(t, "USD", CurrencyCode("USD").String())
	require.Equal(t, "EUR", CurrencyCode("EUR").String())
}
