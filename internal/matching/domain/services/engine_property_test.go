// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestProperty_Engine_NoMatchTwice(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 50,
		Rand:     rand.New(rand.NewSource(42)),
	}

	property := func(seed int64, transactionCount uint8) bool {
		if transactionCount == 0 || transactionCount > 15 {
			return true
		}

		rng := rand.New(rand.NewSource(seed))
		engine := NewEngine()

		rules := []RuleDefinition{
			{
				ID:       uuid.New(),
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
		left := make([]CandidateTransaction, transactionCount)
		right := make([]CandidateTransaction, transactionCount)

		for idx := range left {
			amt := decimal.NewFromInt(int64(rng.Intn(100) + 10))
			ref := string(rune('A' + rng.Intn(26)))
			date := baseTime.Add(time.Duration(rng.Intn(10)) * 24 * time.Hour)

			left[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         amt,
				OriginalAmount: amt,
				Currency:       "USD",
				Date:           date,
				Reference:      ref,
			}
			right[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         amt,
				OriginalAmount: amt,
				Currency:       "USD",
				Date:           date,
				Reference:      ref,
			}
		}

		results, err := engine.Execute1v1(rules, left, right)
		if err != nil {
			return true
		}

		if !verifyNoDoubleMatching(results) {
			return false
		}

		for _, r := range results {
			if r.Score < 0 || r.Score > 100 {
				return false
			}
		}

		return true
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func verifyNoDoubleMatching(results []MatchProposal) bool {
	leftMatched := make(map[uuid.UUID]bool)
	rightMatched := make(map[uuid.UUID]bool)

	for _, proposal := range results {
		for _, leftID := range proposal.LeftIDs {
			if leftMatched[leftID] {
				return false
			}

			leftMatched[leftID] = true
		}

		for _, rightID := range proposal.RightIDs {
			if rightMatched[rightID] {
				return false
			}

			rightMatched[rightID] = true
		}
	}

	return true
}

func TestProperty_Engine_EmptyInputsProduceNoMatches(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 20,
		Rand:     rand.New(rand.NewSource(456)),
	}

	engine := NewEngine()
	rules := []RuleDefinition{
		{
			ID:       uuid.New(),
			Priority: 1,
			Type:     "EXACT",
			Exact: &ExactConfig{
				MatchAmount:   true,
				MatchCurrency: true,
				MatchScore:    100,
			},
		},
	}

	property := func(leftCount, rightCount uint8) bool {
		if leftCount > 10 || rightCount > 10 {
			return true
		}

		baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

		left := make([]CandidateTransaction, leftCount)
		for idx := range left {
			left[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         decimal.NewFromInt(int64((idx + 1) * 10)),
				OriginalAmount: decimal.NewFromInt(int64((idx + 1) * 10)),
				Currency:       "USD",
				Date:           baseTime,
			}
		}

		right := make([]CandidateTransaction, rightCount)
		for idx := range right {
			right[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         decimal.NewFromInt(int64((idx + 1) * 10)),
				OriginalAmount: decimal.NewFromInt(int64((idx + 1) * 10)),
				Currency:       "USD",
				Date:           baseTime,
			}
		}

		results, err := engine.Execute1v1(rules, left, right)
		if err != nil {
			return true
		}

		if leftCount == 0 || rightCount == 0 {
			return len(results) == 0
		}

		return true
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Engine_ScoresInValidRange(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 50,
		Rand:     rand.New(rand.NewSource(789)),
	}

	property := func(seed int64, transactionCount uint8) bool {
		if transactionCount == 0 || transactionCount > 15 {
			return true
		}

		rng := rand.New(rand.NewSource(seed))
		engine := NewEngine()

		rules := []RuleDefinition{
			{
				ID:       uuid.New(),
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
				ID:       uuid.New(),
				Priority: 2,
				Type:     "TOLERANCE",
				Tolerance: &ToleranceConfig{
					MatchCurrency:      true,
					DateWindowDays:     3,
					AbsAmountTolerance: decimal.RequireFromString("5.00"),
					PercentTolerance:   decimal.RequireFromString("0.05"),
					RoundingScale:      2,
					RoundingMode:       RoundingHalfUp,
					MatchScore:         75,
				},
			},
		}

		baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
		left := make([]CandidateTransaction, transactionCount)
		right := make([]CandidateTransaction, transactionCount)

		for idx := range left {
			amt := decimal.NewFromInt(int64(rng.Intn(100) + 10))
			date := baseTime.Add(time.Duration(rng.Intn(5)) * 24 * time.Hour)

			left[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         amt,
				OriginalAmount: amt,
				Currency:       "USD",
				Date:           date,
				Reference:      string(rune('A' + rng.Intn(5))),
			}
			delta := decimal.NewFromInt(int64(rng.Intn(5) - 2))
			adjustedAmt := amt.Add(delta)
			right[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         adjustedAmt,
				OriginalAmount: adjustedAmt,
				Currency:       "USD",
				Date:           date.Add(time.Duration(rng.Intn(2)) * 24 * time.Hour),
				Reference:      string(rune('A' + rng.Intn(5))),
			}
		}

		results, err := engine.Execute1v1(rules, left, right)
		if err != nil {
			return true
		}

		for _, proposal := range results {
			if proposal.Score < 0 || proposal.Score > 100 {
				return false
			}
		}

		return true
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Engine_OrderIndependence(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 30,
		Rand:     rand.New(rand.NewSource(123)),
	}

	property := func(seed int64, transactionCount uint8) bool {
		if transactionCount < 2 || transactionCount > 10 {
			return true
		}

		return verifyOrderIndependence(seed, int(transactionCount))
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func verifyOrderIndependence(seed int64, count int) bool {
	rng := rand.New(rand.NewSource(seed))
	engine := NewEngine()

	rules := []RuleDefinition{
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000001"),
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
	left := make([]CandidateTransaction, count)
	right := make([]CandidateTransaction, count)

	for idx := range left {
		amt := decimal.NewFromInt(int64((idx + 1) * 10))
		ref := string(rune('A' + idx))
		date := baseTime.Add(time.Duration(idx) * 24 * time.Hour)

		left[idx] = CandidateTransaction{
			ID: uuid.MustParse(
				"10000000-0000-0000-0000-00000000000" + string(rune('0'+idx%10)),
			),
			Amount:         amt,
			OriginalAmount: amt,
			Currency:       "USD",
			Date:           date,
			Reference:      ref,
		}
		right[idx] = CandidateTransaction{
			ID: uuid.MustParse(
				"20000000-0000-0000-0000-00000000000" + string(rune('0'+idx%10)),
			),
			Amount:         amt,
			OriginalAmount: amt,
			Currency:       "USD",
			Date:           date,
			Reference:      ref,
		}
	}

	original, err := engine.Execute1v1(
		testutil.CloneRules(rules),
		testutil.CloneTransactions(left),
		testutil.CloneTransactions(right),
	)
	if err != nil {
		return true
	}

	for _, r := range original {
		if r.Score < 0 || r.Score > 100 {
			return false
		}
	}

	shuffledLeft := testutil.PermuteTransactions(left, rng)
	shuffledRight := testutil.PermuteTransactions(right, rng)

	shuffled, err := engine.Execute1v1(
		testutil.CloneRules(rules),
		shuffledLeft,
		shuffledRight,
	)
	if err != nil {
		return true
	}

	for _, r := range shuffled {
		if r.Score < 0 || r.Score > 100 {
			return false
		}
	}

	return comparePairSets(original, shuffled)
}

func comparePairSets(original, shuffled []MatchProposal) bool {
	if len(original) != len(shuffled) {
		return false
	}

	originalPairs := make(map[string]bool)

	for _, proposal := range original {
		for _, leftID := range proposal.LeftIDs {
			for _, rightID := range proposal.RightIDs {
				key := leftID.String() + "-" + rightID.String()
				originalPairs[key] = true
			}
		}
	}

	for _, proposal := range shuffled {
		for _, leftID := range proposal.LeftIDs {
			for _, rightID := range proposal.RightIDs {
				key := leftID.String() + "-" + rightID.String()
				if !originalPairs[key] {
					return false
				}
			}
		}
	}

	return true
}
