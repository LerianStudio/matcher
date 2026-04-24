// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestExactRule_Table(t *testing.T) {
	t.Parallel()

	cfg := &ExactConfig{
		MatchAmount:     true,
		MatchCurrency:   true,
		MatchDate:       true,
		DatePrecision:   DatePrecisionDay,
		MatchReference:  true,
		CaseInsensitive: true,
	}

	tests := []struct {
		name string
		l    CandidateTransaction
		r    CandidateTransaction
		want bool
	}{
		{
			name: "same amount currency day reference",
			l: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000001"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
				Reference:      "abc",
			},
			r: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000002"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 1, 23, 59, 0, 0, time.UTC),
				Reference:      "ABC",
			},
			want: true,
		},
		{
			name: "different amount fails",
			l: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000003"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
				Reference:      "abc",
			},
			r: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000004"),
				Amount:         decimal.RequireFromString("10.01"),
				OriginalAmount: decimal.RequireFromString("10.01"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
				Reference:      "abc",
			},
			want: false,
		},
		{
			name: "different day fails (day precision)",
			l: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000005"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 1, 23, 59, 0, 0, time.UTC),
				Reference:      "abc",
			},
			r: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000006"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
				Reference:      "abc",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ExactMatch(tt.l, tt.r, cfg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExactRule_InvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := ExactMatch(CandidateTransaction{}, CandidateTransaction{}, nil)
	require.Error(t, err)
}

func TestExactRule_InvalidDatePrecision(t *testing.T) {
	t.Parallel()

	cfg := &ExactConfig{MatchDate: true, DatePrecision: DatePrecision("BAD")}
	_, err := ExactMatch(CandidateTransaction{}, CandidateTransaction{}, cfg)
	require.ErrorContains(t, err, "invalid date precision")
}

func TestExactRule_ReferenceMustSet(t *testing.T) {
	t.Parallel()

	cfg := &ExactConfig{MatchReference: true, ReferenceMustSet: true, CaseInsensitive: true}
	matched, err := ExactMatch(
		CandidateTransaction{Reference: ""},
		CandidateTransaction{Reference: "ABC"},
		cfg,
	)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestExactRule_ReferenceTrimmedComparison(t *testing.T) {
	t.Parallel()

	cfg := &ExactConfig{MatchReference: true, CaseInsensitive: false}
	matched, err := ExactMatch(
		CandidateTransaction{Reference: " REF "},
		CandidateTransaction{Reference: "REF"},
		cfg,
	)
	require.NoError(t, err)
	require.True(t, matched)
}

func TestExactRule_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	cfg := &ExactConfig{MatchCurrency: true}
	matched, err := ExactMatch(
		CandidateTransaction{Currency: "USD"},
		CandidateTransaction{Currency: "EUR"},
		cfg,
	)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestExactRule_BaseCurrencyMatching(t *testing.T) {
	t.Parallel()

	baseAmount := decimal.RequireFromString("10.00")
	cfg := &ExactConfig{MatchBaseAmount: true, MatchBaseCurrency: true}
	matched, err := ExactMatch(
		CandidateTransaction{AmountBase: &baseAmount, CurrencyBase: "USD"},
		CandidateTransaction{AmountBase: &baseAmount, CurrencyBase: "USD"},
		cfg,
	)
	require.NoError(t, err)
	require.True(t, matched)

	matched, err = ExactMatch(
		CandidateTransaction{AmountBase: &baseAmount, CurrencyBase: "USD"},
		CandidateTransaction{AmountBase: &baseAmount, CurrencyBase: "EUR"},
		cfg,
	)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestExactRule_Determinism_PropertyLike(t *testing.T) {
	t.Parallel()

	cfg := &ExactConfig{
		MatchAmount:     true,
		MatchCurrency:   true,
		MatchDate:       true,
		DatePrecision:   DatePrecisionTimestamp,
		MatchReference:  true,
		CaseInsensitive: false,
	}

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		amt := decimal.NewFromInt(int64(rng.Intn(10000)))
		ts := time.Unix(int64(rng.Intn(2000000)), 0).UTC()
		ref := "ref-constant"

		left := CandidateTransaction{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000007"),
			Amount:         amt,
			OriginalAmount: amt,
			Currency:       "USD",
			Date:           ts,
			Reference:      ref,
		}
		right := CandidateTransaction{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000008"),
			Amount:         amt,
			OriginalAmount: amt,
			Currency:       "USD",
			Date:           ts,
			Reference:      ref,
		}

		a1, err := ExactMatch(left, right, cfg)
		require.NoError(t, err)
		a2, err := ExactMatch(left, right, cfg)
		require.NoError(t, err)
		require.Equal(t, a1, a2)

		b1, err := ExactMatch(right, left, cfg)
		require.NoError(t, err)
		require.Equal(t, a1, b1)
	}
}
