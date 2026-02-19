//go:build e2e

package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoOracleCalculate_SimpleFlatParallel(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		Name:             "Flat $5",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "flat-fee", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "5.00"}},
		},
	}

	result, err := GoOracleCalculate(spec, "100.00")
	require.NoError(t, err)
	assert.Equal(t, "5.00", result.TotalFee)
	assert.Equal(t, "95.00", result.NetAmount)
	require.Len(t, result.ItemFees, 1)
	assert.Equal(t, "5.00", result.ItemFees[0].Fee)
	assert.Equal(t, "100.00", result.ItemFees[0].BaseUsed)
}

func TestGoOracleCalculate_SimplePercentageParallel(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		Name:             "1.5% fee",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "pct-fee", Priority: 1, StructureType: "PERCENTAGE", Structure: map[string]any{"rate": "0.015"}},
		},
	}

	result, err := GoOracleCalculate(spec, "1000.00")
	require.NoError(t, err)
	assert.Equal(t, "15.00", result.TotalFee)
	assert.Equal(t, "985.00", result.NetAmount)
	require.Len(t, result.ItemFees, 1)
	assert.Equal(t, "15.00", result.ItemFees[0].Fee)
}

func TestGoOracleCalculate_CascadingTwoPercentages(t *testing.T) {
	t.Parallel()

	// $1000 with 2% then 1% cascading:
	//   fee1 = 1000 * 0.02 = 20.00, remaining = 980.00
	//   fee2 = 980 * 0.01  = 9.80,  remaining = 970.20
	//   totalFee = 29.80, net = 970.20
	spec := FuzzScheduleSpec{
		Name:             "Cascading 2% + 1%",
		Currency:         "USD",
		ApplicationOrder: "CASCADING",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "fee-a", Priority: 1, StructureType: "PERCENTAGE", Structure: map[string]any{"rate": "0.02"}},
			{Name: "fee-b", Priority: 2, StructureType: "PERCENTAGE", Structure: map[string]any{"rate": "0.01"}},
		},
	}

	result, err := GoOracleCalculate(spec, "1000.00")
	require.NoError(t, err)
	assert.Equal(t, "29.80", result.TotalFee)
	assert.Equal(t, "970.20", result.NetAmount)
	require.Len(t, result.ItemFees, 2)
	assert.Equal(t, "20.00", result.ItemFees[0].Fee)
	assert.Equal(t, "1000.00", result.ItemFees[0].BaseUsed)
	assert.Equal(t, "9.80", result.ItemFees[1].Fee)
	assert.Equal(t, "980.00", result.ItemFees[1].BaseUsed)
}

func TestGoOracleCalculate_TieredMarginal(t *testing.T) {
	t.Parallel()

	// $1500 with tiers [0-1000: 1%, 1000+: 2%]
	//   tier1: 1000 * 0.01 = 10.00
	//   tier2: 500  * 0.02 = 10.00
	//   totalFee = 20.00, net = 1480.00
	spec := FuzzScheduleSpec{
		Name:             "Two-tier marginal",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []FuzzItemSpec{
			{
				Name: "tiered-fee", Priority: 1, StructureType: "TIERED",
				Structure: map[string]any{
					"tiers": []any{
						map[string]any{"rate": "0.01", "upTo": "1000"},
						map[string]any{"rate": "0.02"},
					},
				},
			},
		},
	}

	result, err := GoOracleCalculate(spec, "1500.00")
	require.NoError(t, err)
	assert.Equal(t, "20.00", result.TotalFee)
	assert.Equal(t, "1480.00", result.NetAmount)
}

func TestGoOracleCalculate_RoundingHalfUp(t *testing.T) {
	t.Parallel()

	// 1.555% of $100 = 1.555; HALF_UP at scale=2 → 1.56
	spec := FuzzScheduleSpec{
		Name:             "HALF_UP rounding",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "pct", Priority: 1, StructureType: "PERCENTAGE", Structure: map[string]any{"rate": "0.01555"}},
		},
	}

	result, err := GoOracleCalculate(spec, "100.00")
	require.NoError(t, err)
	assert.Equal(t, "1.56", result.TotalFee)
	assert.Equal(t, "98.44", result.NetAmount)
}

func TestGoOracleCalculate_RoundingBankers(t *testing.T) {
	t.Parallel()

	// 1.555% of $100 = 1.555; BANKERS at scale=2 → 1.56 (round to even: 6 is even)
	spec := FuzzScheduleSpec{
		Name:             "BANKERS rounding",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "BANKERS",
		Items: []FuzzItemSpec{
			{Name: "pct", Priority: 1, StructureType: "PERCENTAGE", Structure: map[string]any{"rate": "0.01555"}},
		},
	}

	result, err := GoOracleCalculate(spec, "100.00")
	require.NoError(t, err)
	assert.Equal(t, "1.56", result.TotalFee)
}

func TestGoOracleCalculate_RoundingDivergence_HALF_UP_vs_BANKERS(t *testing.T) {
	t.Parallel()

	// 33.345 as flat fee:
	//   HALF_UP at scale=2 → 33.35 (5 rounds up)
	//   BANKERS at scale=2 → 33.34 (round to even: 4 is even)
	specHalfUp := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "flat", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "33.345"}},
		},
	}

	specBankers := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "BANKERS",
		Items: []FuzzItemSpec{
			{Name: "flat", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "33.345"}},
		},
	}

	halfUpResult, err := GoOracleCalculate(specHalfUp, "100.00")
	require.NoError(t, err)
	assert.Equal(t, "33.35", halfUpResult.TotalFee)

	bankersResult, err := GoOracleCalculate(specBankers, "100.00")
	require.NoError(t, err)
	assert.Equal(t, "33.34", bankersResult.TotalFee)

	// Confirm they actually differ.
	assert.NotEqual(t, halfUpResult.TotalFee, bankersResult.TotalFee,
		"HALF_UP and BANKERS should diverge on 33.345")
}

func TestGoOracleCalculate_Error_InvalidStructureType(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "bad", Priority: 1, StructureType: "INVALID", Structure: map[string]any{}},
		},
	}

	_, err := GoOracleCalculate(spec, "100.00")
	assert.ErrorIs(t, err, errOracleUnknownStructure)
}

func TestGoOracleCalculate_Error_MissingRateInPercentage(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "no-rate", Priority: 1, StructureType: "PERCENTAGE", Structure: map[string]any{}},
		},
	}

	_, err := GoOracleCalculate(spec, "100.00")
	assert.ErrorIs(t, err, errOracleMissingKey)
}

func TestGoOracleCalculate_Error_UnknownApplicationOrder(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		ApplicationOrder: "UNKNOWN", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "fee", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "1.00"}},
		},
	}

	_, err := GoOracleCalculate(spec, "100.00")
	assert.ErrorIs(t, err, errOracleUnknownOrder)
}

func TestGoOracleCalculate_Error_NegativeGrossAmount(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "fee", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "1.00"}},
		},
	}

	_, err := GoOracleCalculate(spec, "-50.00")
	assert.ErrorIs(t, err, errOracleNegativeGross)
}

func TestGoOracleCalculate_Error_NoItems(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{},
	}

	_, err := GoOracleCalculate(spec, "100.00")
	assert.ErrorIs(t, err, errOracleNoItems)
}

func TestGoOracleCalculate_Error_InvalidGrossAmount(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "fee", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "1.00"}},
		},
	}

	_, err := GoOracleCalculate(spec, "not-a-number")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse gross amount")
}

func TestGoOracleCalculate_ZeroGrossAmount(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "pct", Priority: 1, StructureType: "PERCENTAGE", Structure: map[string]any{"rate": "0.10"}},
		},
	}

	result, err := GoOracleCalculate(spec, "0.00")
	require.NoError(t, err)
	assert.Equal(t, "0.00", result.TotalFee)
	assert.Equal(t, "0.00", result.NetAmount)
}

func TestGoOracleCalculate_PrioritySortingIsApplied(t *testing.T) {
	t.Parallel()

	// Items given out of priority order; cascading result must reflect sorted order.
	// Priority 2 (5%) should go first after sort? No — priority 1 should go first.
	// Items: priority 2 first in slice, priority 1 second → oracle sorts by priority asc.
	// Cascading: fee1 on prio1 (flat $10), fee2 on prio2 (5% of remaining $90) = $4.50
	spec := FuzzScheduleSpec{
		ApplicationOrder: "CASCADING", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "pct-fee", Priority: 2, StructureType: "PERCENTAGE", Structure: map[string]any{"rate": "0.05"}},
			{Name: "flat-fee", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "10.00"}},
		},
	}

	result, err := GoOracleCalculate(spec, "100.00")
	require.NoError(t, err)
	// After sorting: flat-fee (prio 1) → $10, then pct-fee (prio 2) on $90 → $4.50
	assert.Equal(t, "14.50", result.TotalFee)
	require.Len(t, result.ItemFees, 2)
	assert.Equal(t, "flat-fee", result.ItemFees[0].Name)
	assert.Equal(t, "10.00", result.ItemFees[0].Fee)
	assert.Equal(t, "pct-fee", result.ItemFees[1].Name)
	assert.Equal(t, "4.50", result.ItemFees[1].Fee)
}

func TestGoOracleCalculate_ReasoningNotEmpty(t *testing.T) {
	t.Parallel()

	spec := FuzzScheduleSpec{
		ApplicationOrder: "PARALLEL", RoundingScale: 2, RoundingMode: "HALF_UP",
		Items: []FuzzItemSpec{
			{Name: "fee", Priority: 1, StructureType: "FLAT", Structure: map[string]any{"amount": "1.00"}},
		},
	}

	result, err := GoOracleCalculate(spec, "100.00")
	require.NoError(t, err)
	assert.NotEmpty(t, result.Reasoning)
	assert.Contains(t, result.Reasoning, "Step 1")
	assert.Contains(t, result.Reasoning, "Total fee")
}
