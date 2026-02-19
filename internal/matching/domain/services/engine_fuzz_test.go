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

func uuidFromRand(r *rand.Rand) uuid.UUID {
	var uuidBytes [16]byte

	if _, err := r.Read(uuidBytes[:]); err != nil {
		return uuid.New()
	}

	id, err := uuid.FromBytes(uuidBytes[:])
	if err != nil {
		return uuid.New()
	}

	return id
}

func FuzzEngine_Execute1v1_NoPanic(f *testing.F) {
	f.Add(int64(42), 5, 5)
	f.Add(int64(123), 10, 10)
	f.Add(int64(999), 3, 7)

	f.Fuzz(func(t *testing.T, seed int64, leftCount, rightCount int) {
		if leftCount < 0 || leftCount > 50 || rightCount < 0 || rightCount > 50 {
			t.Skip("bounded inputs")
		}

		rng := rand.New(rand.NewSource(seed))
		engine := NewEngine()

		rules := []RuleDefinition{
			{
				ID:       uuidFromRand(rng),
				Priority: 1,
				Type:     "EXACT",
				Exact: &ExactConfig{
					MatchAmount:     true,
					MatchCurrency:   true,
					MatchDate:       true,
					DatePrecision:   DatePrecisionDay,
					MatchReference:  true,
					CaseInsensitive: true,
					MatchScore:      100,
				},
			},
		}

		baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
		currencies := []string{"USD", "EUR", "GBP", "BRL"}

		left := make([]CandidateTransaction, leftCount)
		for i := range left {
			left[i] = CandidateTransaction{
				ID:             uuidFromRand(rng),
				Amount:         decimal.NewFromInt(int64(rng.Intn(10000) + 1)),
				OriginalAmount: decimal.NewFromInt(int64(rng.Intn(10000) + 1)),
				Currency:       currencies[rng.Intn(len(currencies))],
				Date:           baseTime.Add(time.Duration(rng.Intn(30)) * 24 * time.Hour),
				Reference:      string(rune('A' + rng.Intn(26))),
			}
		}

		right := make([]CandidateTransaction, rightCount)
		for i := range right {
			right[i] = CandidateTransaction{
				ID:             uuidFromRand(rng),
				Amount:         decimal.NewFromInt(int64(rng.Intn(10000) + 1)),
				OriginalAmount: decimal.NewFromInt(int64(rng.Intn(10000) + 1)),
				Currency:       currencies[rng.Intn(len(currencies))],
				Date:           baseTime.Add(time.Duration(rng.Intn(30)) * 24 * time.Hour),
				Reference:      string(rune('A' + rng.Intn(26))),
			}
		}

		results, err := engine.Execute1v1(rules, left, right)
		if err == nil {
			for _, r := range results {
				require.GreaterOrEqual(t, r.Score, 0, "score must be non-negative")
				require.LessOrEqual(t, r.Score, 100, "score must not exceed 100")
			}
		}
	})
}

func FuzzEngine_Execute1v1_Determinism(f *testing.F) {
	f.Add(int64(42), 5, 5)
	f.Add(int64(12345), 8, 8)

	f.Fuzz(func(t *testing.T, seed int64, leftCount, rightCount int) {
		if leftCount < 1 || leftCount > 20 || rightCount < 1 || rightCount > 20 {
			t.Skip("bounded inputs")
		}

		rng := rand.New(rand.NewSource(seed))
		engine := NewEngine()

		ruleID := uuidFromRand(rng)
		rules := []RuleDefinition{
			{
				ID:       ruleID,
				Priority: 1,
				Type:     "EXACT",
				Exact: &ExactConfig{
					MatchAmount:     true,
					MatchCurrency:   true,
					MatchDate:       true,
					DatePrecision:   DatePrecisionDay,
					MatchReference:  true,
					CaseInsensitive: true,
					MatchScore:      100,
				},
			},
		}

		baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

		left := make([]CandidateTransaction, leftCount)
		for i := range left {
			left[i] = CandidateTransaction{
				ID:             uuidFromRand(rng),
				Amount:         decimal.NewFromInt(int64(rng.Intn(100) + 1)),
				OriginalAmount: decimal.NewFromInt(int64(rng.Intn(100) + 1)),
				Currency:       "USD",
				Date:           baseTime.Add(time.Duration(i) * 24 * time.Hour),
				Reference:      string(rune('A' + i%26)),
			}
		}

		right := make([]CandidateTransaction, rightCount)
		for i := range right {
			right[i] = CandidateTransaction{
				ID:             uuidFromRand(rng),
				Amount:         decimal.NewFromInt(int64(rng.Intn(100) + 1)),
				OriginalAmount: decimal.NewFromInt(int64(rng.Intn(100) + 1)),
				Currency:       "USD",
				Date:           baseTime.Add(time.Duration(i) * 24 * time.Hour),
				Reference:      string(rune('A' + i%26)),
			}
		}

		first, err1 := engine.Execute1v1(
			testutil.CloneRules(rules),
			testutil.CloneTransactions(left),
			testutil.CloneTransactions(right),
		)
		second, err2 := engine.Execute1v1(
			testutil.CloneRules(rules),
			testutil.CloneTransactions(left),
			testutil.CloneTransactions(right),
		)

		require.Equal(t, err1 == nil, err2 == nil, "errors must be consistent")
		require.Len(t, second, len(first), "result count must be deterministic")

		for i := range first {
			require.Equal(t, first[i].LeftIDs, second[i].LeftIDs, "left IDs must match")
			require.Equal(t, first[i].RightIDs, second[i].RightIDs, "right IDs must match")
			require.Equal(t, first[i].Score, second[i].Score, "scores must match")
		}
	})
}

func FuzzEngine_ScoreBounds(f *testing.F) {
	f.Add(int64(42), 3, 3)
	f.Add(int64(999), 5, 5)

	f.Fuzz(func(t *testing.T, seed int64, leftCount, rightCount int) {
		if leftCount < 1 || leftCount > 20 || rightCount < 1 || rightCount > 20 {
			t.Skip("bounded inputs")
		}

		rng := rand.New(rand.NewSource(seed))
		engine := NewEngine()

		rules := []RuleDefinition{
			{
				ID:       uuidFromRand(rng),
				Priority: 1,
				Type:     "EXACT",
				Exact: &ExactConfig{
					MatchAmount:     true,
					MatchCurrency:   true,
					MatchDate:       true,
					DatePrecision:   DatePrecisionDay,
					MatchReference:  true,
					CaseInsensitive: true,
					MatchScore:      100,
				},
			},
			{
				ID:       uuidFromRand(rng),
				Priority: 2,
				Type:     "TOLERANCE",
				Tolerance: &ToleranceConfig{
					MatchCurrency:      true,
					DateWindowDays:     1,
					AbsAmountTolerance: decimal.RequireFromString("1.00"),
					PercentTolerance:   decimal.RequireFromString("0.01"),
					RoundingScale:      2,
					RoundingMode:       RoundingHalfUp,
					MatchScore:         80,
				},
			},
		}

		baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

		left := make([]CandidateTransaction, leftCount)
		for i := range left {
			left[i] = CandidateTransaction{
				ID:             uuidFromRand(rng),
				Amount:         decimal.NewFromInt(int64(rng.Intn(100) + 10)),
				OriginalAmount: decimal.NewFromInt(int64(rng.Intn(100) + 10)),
				Currency:       "USD",
				Date:           baseTime.Add(time.Duration(rng.Intn(10)) * 24 * time.Hour),
				Reference:      string(rune('A' + rng.Intn(10))),
			}
		}

		right := make([]CandidateTransaction, rightCount)
		for i := range right {
			right[i] = CandidateTransaction{
				ID:             uuidFromRand(rng),
				Amount:         decimal.NewFromInt(int64(rng.Intn(100) + 10)),
				OriginalAmount: decimal.NewFromInt(int64(rng.Intn(100) + 10)),
				Currency:       "USD",
				Date:           baseTime.Add(time.Duration(rng.Intn(10)) * 24 * time.Hour),
				Reference:      string(rune('A' + rng.Intn(10))),
			}
		}

		results, err := engine.Execute1v1(rules, left, right)
		if err != nil {
			return
		}

		for _, r := range results {
			require.GreaterOrEqual(t, r.Score, 0, "score must be >= 0")
			require.LessOrEqual(t, r.Score, 100, "score must be <= 100")
		}
	})
}
