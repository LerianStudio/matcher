//go:build unit

package entities

import (
	"math/rand"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPropertyVolumeStats_TotalEqualsMatchedPlusUnmatched(t *testing.T) {
	t.Parallel()

	seed := time.Now().UTC().UnixNano()
	t.Logf("seed=%d", seed)

	rng := rand.New(rand.NewSource(seed))

	for i := 0; i < 100; i++ {
		matchedTx := rng.Intn(1000)
		unmatchedTx := rng.Intn(1000)
		totalTx := matchedTx + unmatchedTx

		matchedAmt := decimal.NewFromInt(int64(rng.Intn(1000000)))
		unmatchedAmt := decimal.NewFromInt(int64(rng.Intn(1000000)))
		totalAmt := matchedAmt.Add(unmatchedAmt)

		volume := &VolumeStats{
			TotalTransactions:   totalTx,
			MatchedTransactions: matchedTx,
			UnmatchedCount:      unmatchedTx,
			TotalAmount:         totalAmt,
			MatchedAmount:       matchedAmt,
			UnmatchedAmount:     unmatchedAmt,
		}

		assert.Equal(t, volume.MatchedTransactions+volume.UnmatchedCount, volume.TotalTransactions,
			"Property: TotalTransactions = MatchedTransactions + UnmatchedCount (iteration %d)", i)

		expectedTotal := volume.MatchedAmount.Add(volume.UnmatchedAmount)
		assert.True(t, volume.TotalAmount.Equal(expectedTotal),
			"Property: TotalAmount = MatchedAmount + UnmatchedAmount (iteration %d)", i)
	}
}

func TestPropertyCalculateMatchRate_ResultInValidRange(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 100; i++ {
		totalTx := rng.Intn(10000) + 1
		matchedTx := rng.Intn(totalTx + 1)
		unmatchedTx := totalTx - matchedTx

		totalAmt := decimal.NewFromInt(int64(rng.Intn(10000000) + 1))
		matchedAmt := totalAmt.Mul(decimal.NewFromFloat(float64(matchedTx) / float64(totalTx)))
		unmatchedAmt := totalAmt.Sub(matchedAmt)

		volume := &VolumeStats{
			TotalTransactions:   totalTx,
			MatchedTransactions: matchedTx,
			UnmatchedCount:      unmatchedTx,
			TotalAmount:         totalAmt,
			MatchedAmount:       matchedAmt,
			UnmatchedAmount:     unmatchedAmt,
		}

		result := CalculateMatchRate(volume)

		require.NotNil(t, result)
		assert.GreaterOrEqual(t, result.MatchRate, 0.0,
			"Property: MatchRate >= 0 (iteration %d)", i)
		assert.LessOrEqual(t, result.MatchRate, 100.0,
			"Property: MatchRate <= 100 (iteration %d)", i)
		assert.GreaterOrEqual(t, result.MatchRateAmount, 0.0,
			"Property: MatchRateAmount >= 0 (iteration %d)", i)
		assert.LessOrEqual(t, result.MatchRateAmount, 100.0,
			"Property: MatchRateAmount <= 100 (iteration %d)", i)
	}
}

func TestPropertyCalculateMatchRate_CountsPreserved(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 100; i++ {
		totalTx := rng.Intn(10000) + 1
		matchedTx := rng.Intn(totalTx + 1)
		unmatchedTx := totalTx - matchedTx

		volume := &VolumeStats{
			TotalTransactions:   totalTx,
			MatchedTransactions: matchedTx,
			UnmatchedCount:      unmatchedTx,
			TotalAmount:         decimal.NewFromInt(int64(totalTx * 100)),
			MatchedAmount:       decimal.NewFromInt(int64(matchedTx * 100)),
			UnmatchedAmount:     decimal.NewFromInt(int64(unmatchedTx * 100)),
		}

		result := CalculateMatchRate(volume)

		require.NotNil(t, result)
		assert.Equal(t, totalTx, result.TotalCount,
			"Property: TotalCount preserved (iteration %d)", i)
		assert.Equal(t, matchedTx, result.MatchedCount,
			"Property: MatchedCount preserved (iteration %d)", i)
		assert.Equal(t, unmatchedTx, result.UnmatchedCount,
			"Property: UnmatchedCount preserved (iteration %d)", i)
	}
}

func TestPropertyCalculateSLACompliance_ResultInValidRange(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 100; i++ {
		resolvedOnTime := rng.Intn(1000)
		resolvedLate := rng.Intn(1000)
		pendingWithinSLA := rng.Intn(500)
		pendingOverdue := rng.Intn(500)
		totalExceptions := resolvedOnTime + resolvedLate + pendingWithinSLA + pendingOverdue

		stats := &SLAStats{
			TotalExceptions:     totalExceptions,
			ResolvedOnTime:      resolvedOnTime,
			ResolvedLate:        resolvedLate,
			PendingWithinSLA:    pendingWithinSLA,
			PendingOverdue:      pendingOverdue,
			AverageResolutionMs: int64(rng.Intn(86400000)),
		}

		result := CalculateSLACompliance(stats)

		assert.GreaterOrEqual(t, result, 0.0,
			"Property: SLACompliance >= 0 (iteration %d)", i)
		assert.LessOrEqual(t, result, 100.0,
			"Property: SLACompliance <= 100 (iteration %d)", i)
	}
}

func TestPropertyCalculateSLACompliance_AllOnTimeEquals100Percent(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 50; i++ {
		resolvedOnTime := rng.Intn(1000) + 1

		stats := &SLAStats{
			TotalExceptions:  resolvedOnTime,
			ResolvedOnTime:   resolvedOnTime,
			ResolvedLate:     0,
			PendingWithinSLA: 0,
			PendingOverdue:   0,
		}

		result := CalculateSLACompliance(stats)

		assert.InDelta(t, 100.0, result, 0.01,
			"Property: 100%% on-time = 100%% compliance (iteration %d)", i)
	}
}

func TestPropertyCalculateSLACompliance_AllLateEquals0Percent(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 50; i++ {
		resolvedLate := rng.Intn(1000) + 1

		stats := &SLAStats{
			TotalExceptions:  resolvedLate,
			ResolvedOnTime:   0,
			ResolvedLate:     resolvedLate,
			PendingWithinSLA: 0,
			PendingOverdue:   0,
		}

		result := CalculateSLACompliance(stats)

		assert.InDelta(t, 0.0, result, 0.01,
			"Property: 0%% on-time = 0%% compliance (iteration %d)", i)
	}
}

func TestPropertyMatchRateStats_CountInvariant(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 100; i++ {
		totalTx := rng.Intn(10000) + 1
		matchedTx := rng.Intn(totalTx + 1)
		unmatchedTx := totalTx - matchedTx

		volume := &VolumeStats{
			TotalTransactions:   totalTx,
			MatchedTransactions: matchedTx,
			UnmatchedCount:      unmatchedTx,
			TotalAmount:         decimal.NewFromInt(int64(totalTx * 100)),
			MatchedAmount:       decimal.NewFromInt(int64(matchedTx * 100)),
			UnmatchedAmount:     decimal.NewFromInt(int64(unmatchedTx * 100)),
		}

		result := CalculateMatchRate(volume)

		require.NotNil(t, result)
		assert.Equal(t, result.MatchedCount+result.UnmatchedCount, result.TotalCount,
			"Property: MatchedCount + UnmatchedCount = TotalCount (iteration %d)", i)
	}
}

func TestPropertyDashboardAggregates_VolumeMatchRateConsistency(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 50; i++ {
		totalTx := rng.Intn(10000) + 1
		matchedTx := rng.Intn(totalTx + 1)
		unmatchedTx := totalTx - matchedTx

		volume := &VolumeStats{
			TotalTransactions:   totalTx,
			MatchedTransactions: matchedTx,
			UnmatchedCount:      unmatchedTx,
			TotalAmount:         decimal.NewFromInt(int64(totalTx * 100)),
			MatchedAmount:       decimal.NewFromInt(int64(matchedTx * 100)),
			UnmatchedAmount:     decimal.NewFromInt(int64(unmatchedTx * 100)),
			PeriodStart:         time.Now().UTC().AddDate(0, 0, -30),
			PeriodEnd:           time.Now().UTC(),
		}

		matchRate := CalculateMatchRate(volume)

		require.NotNil(t, matchRate)

		assert.Equal(t, volume.TotalTransactions, matchRate.TotalCount,
			"Property: Volume.TotalTransactions = MatchRate.TotalCount (iteration %d)", i)
		assert.Equal(t, volume.MatchedTransactions, matchRate.MatchedCount,
			"Property: Volume.MatchedTransactions = MatchRate.MatchedCount (iteration %d)", i)
		assert.Equal(t, volume.UnmatchedCount, matchRate.UnmatchedCount,
			"Property: Volume.UnmatchedCount = MatchRate.UnmatchedCount (iteration %d)", i)
	}
}

func TestPropertyCalculateMatchRate_ZeroTotalReturnsEmptyStats(t *testing.T) {
	t.Parallel()

	testCases := []*VolumeStats{
		nil,
		{TotalTransactions: 0},
		{TotalTransactions: 0, MatchedTransactions: 0, UnmatchedCount: 0},
	}

	for i, tc := range testCases {
		result := CalculateMatchRate(tc)

		require.NotNil(t, result, "Property: result never nil (case %d)", i)
		assert.InDelta(
			t,
			0.0,
			result.MatchRate,
			0.0001,
			"Property: zero total -> 0 rate (case %d)",
			i,
		)
		assert.InDelta(
			t,
			0.0,
			result.MatchRateAmount,
			0.0001,
			"Property: zero total -> 0 amount rate (case %d)",
			i,
		)
	}
}

func TestPropertyCalculateSLACompliance_NilStatsReturnsZero(t *testing.T) {
	t.Parallel()

	result := CalculateSLACompliance(nil)

	assert.InDelta(t, 0.0, result, 0.0001, "Property: nil stats -> 0 compliance")
}

func TestPropertyCalculateSLACompliance_NoResolvedWithoutOverdueReturns100(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 50; i++ {
		pendingWithinSLA := rng.Intn(500)

		stats := &SLAStats{
			TotalExceptions:  pendingWithinSLA,
			ResolvedOnTime:   0,
			ResolvedLate:     0,
			PendingWithinSLA: pendingWithinSLA,
			PendingOverdue:   0,
		}

		result := CalculateSLACompliance(stats)

		assert.InDelta(t, 100.0, result, 0.01,
			"Property: no resolved items and no overdue = 100%% compliance (iteration %d)", i)
	}
}

func TestPropertyCalculateSLACompliance_NoResolvedWithOverdueReturnsZero(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

	for i := 0; i < 50; i++ {
		pendingOverdue := rng.Intn(500) + 1

		stats := &SLAStats{
			TotalExceptions:  pendingOverdue,
			ResolvedOnTime:   0,
			ResolvedLate:     0,
			PendingWithinSLA: 0,
			PendingOverdue:   pendingOverdue,
		}

		result := CalculateSLACompliance(stats)

		assert.InDelta(t, 0.0, result, 0.01,
			"Property: no resolved items with overdue = 0%% compliance (iteration %d)", i)
	}
}
