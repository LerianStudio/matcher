//go:build unit

package entities

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func FuzzBuildSummaryReport(f *testing.F) {
	f.Add(int64(100), int64(50), int64(200), int64(75), 3, 2)
	f.Add(int64(0), int64(0), int64(0), int64(0), 0, 0)
	f.Add(int64(-100), int64(100), int64(-50), int64(50), 1, 1)
	f.Add(int64(9223372036854775807), int64(1), int64(9223372036854775807), int64(1), 10, 10)
	f.Add(int64(-9223372036854775808), int64(-9223372036854775808), int64(1), int64(1), 5, 5)

	f.Fuzz(
		func(t *testing.T, matchedAmt1, matchedAmt2, unmatchedAmt1, unmatchedAmt2 int64, matchedCount, unmatchedCount int) {
			matchedCount = abs(matchedCount) % 100
			unmatchedCount = abs(unmatchedCount) % 100

			matched := make([]*MatchedItem, matchedCount)
			for i := 0; i < matchedCount; i++ {
				var amt int64
				if i%2 == 0 {
					amt = matchedAmt1
				} else {
					amt = matchedAmt2
				}

				matched[i] = &MatchedItem{
					TransactionID: uuid.New(),
					MatchGroupID:  uuid.New(),
					SourceID:      uuid.New(),
					Amount:        decimal.NewFromInt(amt),
					Currency:      "USD",
					Date:          time.Now().UTC(),
				}
			}

			unmatched := make([]*UnmatchedItem, unmatchedCount)
			for i := 0; i < unmatchedCount; i++ {
				var amt int64
				if i%2 == 0 {
					amt = unmatchedAmt1
				} else {
					amt = unmatchedAmt2
				}

				unmatched[i] = &UnmatchedItem{
					TransactionID: uuid.New(),
					SourceID:      uuid.New(),
					Amount:        decimal.NewFromInt(amt),
					Currency:      "USD",
					Status:        "UNMATCHED",
					Date:          time.Now().UTC(),
				}
			}

			summary := BuildSummaryReport(matched, unmatched)

			require.Equal(
				t,
				matchedCount,
				summary.MatchedCount,
				"MatchedCount must equal input count",
			)
			require.Equal(
				t,
				unmatchedCount,
				summary.UnmatchedCount,
				"UnmatchedCount must equal input count",
			)

			expectedTotal := summary.MatchedAmount.Add(summary.UnmatchedAmount)
			require.True(t, summary.TotalAmount.Equal(expectedTotal),
				"TotalAmount (%s) must equal MatchedAmount (%s) + UnmatchedAmount (%s)",
				summary.TotalAmount, summary.MatchedAmount, summary.UnmatchedAmount)
		},
	)
}

func FuzzBuildSummaryReport_NilItems(f *testing.F) {
	f.Add(5, 3, 2, 1)
	f.Add(10, 10, 5, 5)
	f.Add(0, 0, 0, 0)
	f.Add(100, 50, 50, 25)

	f.Fuzz(func(t *testing.T, totalMatched, nilMatched, totalUnmatched, nilUnmatched int) {
		totalMatched = abs(totalMatched) % 50
		nilMatched = abs(nilMatched) % (totalMatched + 1)
		totalUnmatched = abs(totalUnmatched) % 50
		nilUnmatched = abs(nilUnmatched) % (totalUnmatched + 1)

		matched := make([]*MatchedItem, totalMatched)
		for i := 0; i < totalMatched; i++ {
			if i < nilMatched {
				matched[i] = nil
			} else {
				matched[i] = &MatchedItem{
					TransactionID: uuid.New(),
					MatchGroupID:  uuid.New(),
					SourceID:      uuid.New(),
					Amount:        decimal.NewFromInt(100),
					Currency:      "USD",
					Date:          time.Now().UTC(),
				}
			}
		}

		unmatched := make([]*UnmatchedItem, totalUnmatched)
		for i := 0; i < totalUnmatched; i++ {
			if i < nilUnmatched {
				unmatched[i] = nil
			} else {
				unmatched[i] = &UnmatchedItem{
					TransactionID: uuid.New(),
					SourceID:      uuid.New(),
					Amount:        decimal.NewFromInt(50),
					Currency:      "USD",
					Status:        "UNMATCHED",
					Date:          time.Now().UTC(),
				}
			}
		}

		summary := BuildSummaryReport(matched, unmatched)

		expectedMatchedCount := totalMatched - nilMatched
		expectedUnmatchedCount := totalUnmatched - nilUnmatched

		require.Equal(
			t,
			expectedMatchedCount,
			summary.MatchedCount,
			"MatchedCount should exclude nil items: expected %d, got %d",
			expectedMatchedCount,
			summary.MatchedCount,
		)
		require.Equal(
			t,
			expectedUnmatchedCount,
			summary.UnmatchedCount,
			"UnmatchedCount should exclude nil items: expected %d, got %d",
			expectedUnmatchedCount,
			summary.UnmatchedCount,
		)

		expectedMatchedAmount := decimal.NewFromInt(int64(expectedMatchedCount * 100))
		expectedUnmatchedAmount := decimal.NewFromInt(int64(expectedUnmatchedCount * 50))

		require.True(
			t,
			summary.MatchedAmount.Equal(expectedMatchedAmount),
			"MatchedAmount mismatch: expected %s, got %s",
			expectedMatchedAmount,
			summary.MatchedAmount,
		)
		require.True(
			t,
			summary.UnmatchedAmount.Equal(expectedUnmatchedAmount),
			"UnmatchedAmount mismatch: expected %s, got %s",
			expectedUnmatchedAmount,
			summary.UnmatchedAmount,
		)
	})
}

func abs(value int) int {
	if value == math.MinInt {
		return math.MaxInt
	}

	if value < 0 {
		return -value
	}

	return value
}
