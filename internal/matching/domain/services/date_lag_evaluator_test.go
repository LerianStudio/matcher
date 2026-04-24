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

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestDateLagRule_Table(t *testing.T) {
	t.Parallel()

	cfg := &DateLagConfig{
		MinDays:       1,
		MaxDays:       3,
		Inclusive:     true,
		Direction:     DateLagDirectionAbs,
		FeeTolerance:  decimal.RequireFromString("0.30"),
		MatchCurrency: true,
		MatchScore:    80,
	}

	left := CandidateTransaction{
		ID:             uuid.MustParse("00000000-0000-0000-0000-000000000101"),
		Amount:         decimal.RequireFromString("10.00"),
		OriginalAmount: decimal.RequireFromString("10.00"),
		Currency:       "USD",
		Date:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name        string
		right       CandidateTransaction
		cfgOverride func(*DateLagConfig)
		want        bool
	}{
		{
			name: "within abs bounds",
			right: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000102"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "outside abs bounds",
			right: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000103"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "currency mismatch",
			right: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000104"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "EUR",
				Date:           time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "currency mismatch ignored",
			right: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000107"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "EUR",
				Date:           time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			cfgOverride: func(c *DateLagConfig) {
				c.MatchCurrency = false
			},
			want: true,
		},
		{
			name: "missing currency even when ignored",
			right: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000109"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "",
				Date:           time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			cfgOverride: func(c *DateLagConfig) {
				c.MatchCurrency = false
			},
			want: false,
		},
		{
			name: "amount mismatch",
			right: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000105"),
				Amount:         decimal.RequireFromString("11.00"),
				OriginalAmount: decimal.RequireFromString("11.00"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "amount within fee tolerance",
			right: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000106"),
				Amount:         decimal.RequireFromString("10.20"),
				OriginalAmount: decimal.RequireFromString("10.20"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "amount exceeds fee tolerance",
			right: CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000108"),
				Amount:         decimal.RequireFromString("10.50"),
				OriginalAmount: decimal.RequireFromString("10.50"),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			localCfg := *cfg
			if tt.cfgOverride != nil {
				tt.cfgOverride(&localCfg)
			}

			got, err := DateLagMatch(left, tt.right, &localCfg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDateLagRule_Direction(t *testing.T) {
	t.Parallel()

	left := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000104"),
		Date: time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC),
	}
	rBefore := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000105"),
		Date: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC),
	}
	rAfter := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000106"),
		Date: time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC),
	}

	cfgLeftBefore := &DateLagConfig{
		MinDays:       1,
		MaxDays:       3,
		Inclusive:     true,
		Direction:     DateLagDirectionLeftBeforeRight,
		FeeTolerance:  decimal.Zero,
		MatchCurrency: true,
		MatchScore:    80,
	}
	cfgRightBefore := &DateLagConfig{
		MinDays:       1,
		MaxDays:       3,
		Inclusive:     true,
		Direction:     DateLagDirectionRightBeforeLeft,
		FeeTolerance:  decimal.Zero,
		MatchCurrency: true,
		MatchScore:    80,
	}

	match, err := DateLagMatch(left, rAfter, cfgLeftBefore)
	require.NoError(t, err)
	require.True(t, match)

	match, err = DateLagMatch(left, rBefore, cfgLeftBefore)
	require.NoError(t, err)
	require.False(t, match)

	match, err = DateLagMatch(left, rBefore, cfgRightBefore)
	require.NoError(t, err)
	require.True(t, match)

	match, err = DateLagMatch(left, rAfter, cfgRightBefore)
	require.NoError(t, err)
	require.False(t, match)
}

func TestDateLagRule_InclusiveExclusive(t *testing.T) {
	t.Parallel()

	left := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000107"),
		Date: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	right := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000108"),
		Date: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	cfgInclusive := &DateLagConfig{
		MinDays:       1,
		MaxDays:       1,
		Inclusive:     true,
		Direction:     DateLagDirectionAbs,
		FeeTolerance:  decimal.Zero,
		MatchCurrency: true,
		MatchScore:    80,
	}
	cfgExclusive := &DateLagConfig{
		MinDays:       1,
		MaxDays:       1,
		Inclusive:     false,
		Direction:     DateLagDirectionAbs,
		FeeTolerance:  decimal.Zero,
		MatchCurrency: true,
		MatchScore:    80,
	}

	match, err := DateLagMatch(left, right, cfgInclusive)
	require.NoError(t, err)
	require.True(t, match)

	match, err = DateLagMatch(left, right, cfgExclusive)
	require.NoError(t, err)
	require.False(t, match)
}

func TestDateLagRule_InvalidConfig(t *testing.T) {
	t.Parallel()

	left := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000120"),
		Date: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	right := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000121"),
		Date: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	_, err := DateLagMatch(left, right, nil)
	require.ErrorIs(t, err, ErrDateLagConfigRequired)

	_, err = DateLagMatch(
		left,
		right,
		&DateLagConfig{
			MinDays:       -1,
			MaxDays:       1,
			Inclusive:     true,
			Direction:     DateLagDirectionAbs,
			FeeTolerance:  decimal.Zero,
			MatchCurrency: true,
			MatchScore:    80,
		},
	)
	require.ErrorContains(t, err, "minDays/maxDays must be >= 0")

	_, err = DateLagMatch(
		left,
		right,
		&DateLagConfig{
			MinDays:       5,
			MaxDays:       2,
			Inclusive:     true,
			Direction:     DateLagDirectionAbs,
			FeeTolerance:  decimal.Zero,
			MatchCurrency: true,
			MatchScore:    80,
		},
	)
	require.ErrorContains(t, err, "maxDays must be >= minDays")

	_, err = DateLagMatch(
		left,
		right,
		&DateLagConfig{
			MinDays:       0,
			MaxDays:       1,
			Inclusive:     true,
			Direction:     DateLagDirection("BAD"),
			FeeTolerance:  decimal.Zero,
			MatchCurrency: true,
			MatchScore:    80,
		},
	)
	require.ErrorContains(t, err, "invalid date lag direction")
}

func TestDateLagRule_SameDayFailsWhenMinPositive(t *testing.T) {
	t.Parallel()

	l := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000124"),
		Date: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
	}
	r := CandidateTransaction{
		ID:   uuid.MustParse("00000000-0000-0000-0000-000000000125"),
		Date: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
	}

	cfg := &DateLagConfig{
		MinDays:       1,
		MaxDays:       3,
		Inclusive:     true,
		Direction:     DateLagDirectionLeftBeforeRight,
		FeeTolerance:  decimal.Zero,
		MatchCurrency: true,
		MatchScore:    80,
	}
	match, err := DateLagMatch(l, r, cfg)
	require.NoError(t, err)
	require.False(t, match)
}

func TestDateLagRule_Determinism_PropertyLike(t *testing.T) {
	t.Parallel()

	cfg := &DateLagConfig{
		MinDays:       0,
		MaxDays:       5,
		Inclusive:     true,
		Direction:     DateLagDirectionAbs,
		FeeTolerance:  decimal.Zero,
		MatchCurrency: true,
		MatchScore:    80,
	}

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 500; i++ {
		tsLeft := time.Unix(int64(rng.Intn(2000000)), 0).UTC()
		tsRight := time.Unix(int64(rng.Intn(2000000)), 0).UTC()
		leftID := testutil.MustDeterministicUUID("left-" + tsLeft.Format(time.RFC3339Nano))
		rightID := testutil.MustDeterministicUUID("right-" + tsRight.Format(time.RFC3339Nano))

		left := CandidateTransaction{ID: leftID, Date: tsLeft}
		right := CandidateTransaction{ID: rightID, Date: tsRight}

		a1, err := DateLagMatch(left, right, cfg)
		require.NoError(t, err)
		a2, err := DateLagMatch(left, right, cfg)
		require.NoError(t, err)
		require.Equal(t, a1, a2)
	}
}
