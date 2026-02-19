//go:build e2e

package e2e

import "fmt"

// Difficulty levels for fuzz scenarios.
const (
	difficultyTrivial      = 1
	difficultySimple       = 2
	difficultyModerate     = 3
	difficultyHard         = 4
	difficultyPathological = 5
)

// Common rounding scales used across scenarios.
const (
	scaleWholeNumber  = 0
	scaleCents        = 2
	scaleSubPenny     = 4
	scaleMaxPrecision = 10
)

// tierDef defines a single tier in a tiered fee structure.
type tierDef struct {
	upTo *string // nil means infinite (catch-all).
	rate string
}

// tier creates a bounded tier definition.
func tier(upTo, rate string) tierDef {
	return tierDef{upTo: &upTo, rate: rate}
}

// infiniteTier creates an unbounded (catch-all) tier definition.
func infiniteTier(rate string) tierDef {
	return tierDef{upTo: nil, rate: rate}
}

// tieredStructure builds the structure map for a TIERED fee item from tier definitions.
func tieredStructure(tiers ...tierDef) map[string]any {
	raw := make([]any, 0, len(tiers))

	for _, td := range tiers {
		entry := map[string]any{
			"rate": td.rate,
		}

		if td.upTo != nil {
			entry["upTo"] = *td.upTo
		}

		raw = append(raw, entry)
	}

	return map[string]any{"tiers": raw}
}

// flatStructure builds the structure map for a FLAT fee item.
func flatStructure(amount string) map[string]any {
	return map[string]any{"amount": amount}
}

// pctStructure builds the structure map for a PERCENTAGE fee item.
func pctStructure(rate string) map[string]any {
	return map[string]any{"rate": rate}
}

// idSequencer returns a function that generates sequential IDs with the given prefix.
func idSequencer(prefix string) func() string {
	counter := 0

	return func() string {
		counter++

		return fmt.Sprintf("%s_%03d", prefix, counter)
	}
}

// Rounding torture generators.

// GenerateRoundingTortureScenarios produces deterministic scenarios that
// stress-test rounding logic across all five modes and multiple scales.
func GenerateRoundingTortureScenarios() []FuzzScenario {
	next := idSequencer("rounding")
	scenarios := make([]FuzzScenario, 0, 17) //nolint:mnd // pre-calculated: 4 tiebreakers + 5 modes + 2 scale extremes + 1 repeating + 5 mode divergence

	scenarios = append(scenarios, roundingTieBreakers(next)...)
	scenarios = append(scenarios, roundingFiveModes(next)...)
	scenarios = append(scenarios, roundingScaleExtremes(next)...)
	scenarios = append(scenarios, roundingRepeatingDecimals(next)...)
	scenarios = append(scenarios, roundingModeDivergence(next)...)

	return scenarios
}

// roundingTieBreakers produces scenarios where .5 at the rounding digit tests HALF_UP vs BANKERS.
func roundingTieBreakers(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			// $33.335 with scale=2: HALF_UP rounds 5 up to 33.34; BANKERS also 33.34 (4 is even).
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: "Tie-breaker .5 at scale=2 with HALF_UP: 33.335 should round to 33.34",
			Difficulty:   difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Tie-breaker HALF_UP", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "flat-tiebreak", Priority: 1,
					StructureType: "FLAT", Structure: flatStructure("33.335"),
				}},
			},
			GrossAmount: "100.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: "Tie-breaker .5 at scale=2 with BANKERS: 33.335 rounds to 33.34 (4 is even digit)",
			Difficulty:   difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Tie-breaker BANKERS even", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "BANKERS",
				Items: []FuzzItemSpec{{
					Name: "flat-tiebreak", Priority: 1,
					StructureType: "FLAT", Structure: flatStructure("33.335"),
				}},
			},
			GrossAmount: "100.00",
		},
		{
			// $33.345: HALF_UP rounds up to 33.35; BANKERS rounds down to 33.34 (4 is even).
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: "HALF_UP vs BANKERS divergence: 33.345 -> HALF_UP=33.35, BANKERS=33.34 (round to even 4)",
			Difficulty:   difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Tie-breaker HALF_UP 33.345", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "flat-tiebreak", Priority: 1,
					StructureType: "FLAT", Structure: flatStructure("33.345"),
				}},
			},
			GrossAmount: "100.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: "BANKERS rounds 33.345 to 33.34 (4 is already even), diverges from HALF_UP which gives 33.35",
			Difficulty:   difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Tie-breaker BANKERS 33.345", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "BANKERS",
				Items: []FuzzItemSpec{{
					Name: "flat-tiebreak", Priority: 1,
					StructureType: "FLAT", Structure: flatStructure("33.345"),
				}},
			},
			GrossAmount: "100.00",
		},
	}
}

// roundingFiveModes produces scenarios testing all 5 rounding modes on an amount that hits .5 exactly.
func roundingFiveModes(next func() string) []FuzzScenario {
	// 1.5% of $3333.30 = 49.9995 — the .5 hits exactly at the rounding digit for scale=2.
	modes := []string{"HALF_UP", "BANKERS", "FLOOR", "CEIL", "TRUNCATE"}
	scenarios := make([]FuzzScenario, 0, len(modes))

	for _, mode := range modes {
		scenarios = append(scenarios, FuzzScenario{
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: fmt.Sprintf(
				"1.5%% of $3333.30 = 49.9995 rounded with %s at scale=2; "+
					"tests exact .5 at the rounding digit for this mode",
				mode,
			),
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "5-mode-stress " + mode + " scale=2", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: mode,
				Items: []FuzzItemSpec{{
					Name: "pct-stress", Priority: 1,
					StructureType: "PERCENTAGE", Structure: pctStructure("0.015"),
				}},
			},
			GrossAmount: "3333.30",
		})
	}

	return scenarios
}

// roundingScaleExtremes tests scale 0 (whole numbers) and scale 10 (maximum precision).
func roundingScaleExtremes(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			// Scale 0: whole numbers only. 2.5% of $199.99 = 4.99975 varies by mode.
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: "Scale 0 forces whole-number fees; 2.5% of $199.99 = 4.99975 rounds to 5 or 4 depending on mode",
			Difficulty:   difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Scale 0 extreme", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleWholeNumber, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "pct", Priority: 1,
					StructureType: "PERCENTAGE", Structure: pctStructure("0.025"),
				}},
			},
			GrossAmount: "199.99",
		},
		{
			// Scale 10: 1/7 of $100 tests maximum internal precision.
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: "Scale 10 with repeating decimal rate 1/7; tests that engine carries sufficient internal precision to scale=10",
			Difficulty:   difficultyHard,
			Schedule: FuzzScheduleSpec{
				Name: "Scale 10 repeating", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleMaxPrecision, RoundingMode: "BANKERS",
				Items: []FuzzItemSpec{{
					Name: "pct-seventh", Priority: 1,
					StructureType: "PERCENTAGE",
					Structure:     pctStructure("0.14285714285714285"),
				}},
			},
			GrossAmount: "100.00",
		},
	}
}

// roundingRepeatingDecimals tests infinite repeating decimal results.
func roundingRepeatingDecimals(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			// 1/3 as percentage of $100 = 33.3333... with TRUNCATE.
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: "Repeating decimal 1/3 of $100 = 33.3333...; tests infinite precision handling at scale=2 with TRUNCATE",
			Difficulty:   difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Repeating 1/3", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "TRUNCATE",
				Items: []FuzzItemSpec{{
					Name: "pct-third", Priority: 1,
					StructureType: "PERCENTAGE",
					Structure:     pctStructure("0.33333333333333333"),
				}},
			},
			GrossAmount: "100.00",
		},
	}
}

// roundingModeDivergence tests the same amount across all 5 modes to expose mode-specific differences.
func roundingModeDivergence(next func() string) []FuzzScenario {
	// $100 with 1.555% fee = 1.555 exactly. Different modes produce different results at scale=2.
	modes := []string{"HALF_UP", "BANKERS", "FLOOR", "CEIL", "TRUNCATE"}
	scenarios := make([]FuzzScenario, 0, len(modes))

	for _, mode := range modes {
		scenarios = append(scenarios, FuzzScenario{
			ID: next(), Source: "deterministic", Category: "rounding",
			AttackVector: fmt.Sprintf(
				"1.555%% of $100 = 1.555 exactly; %s at scale=2 tests the .5 tie for this mode",
				mode,
			),
			Difficulty: difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "1.555pct " + mode, Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: mode,
				Items: []FuzzItemSpec{{
					Name: "pct-1555", Priority: 1,
					StructureType: "PERCENTAGE", Structure: pctStructure("0.01555"),
				}},
			},
			GrossAmount: "100.00",
		})
	}

	return scenarios
}

// Tiered boundary generators.

// GenerateTieredBoundaryScenarios produces deterministic scenarios targeting
// marginal/progressive tier edge cases: exact boundaries, off-by-one, and
// degenerate tier configurations.
func GenerateTieredBoundaryScenarios() []FuzzScenario {
	next := idSequencer("tiered")
	scenarios := make([]FuzzScenario, 0, 12) //nolint:mnd // pre-calculated: 5 boundaries + 4 degenerate + 1 complex + 2 rounding

	scenarios = append(scenarios, tieredExactBoundaryScenarios(next)...)
	scenarios = append(scenarios, tieredDegenerateScenarios(next)...)
	scenarios = append(scenarios, tieredComplexScenarios(next)...)
	scenarios = append(scenarios, tieredRoundingInteractionScenarios(next)...)

	return scenarios
}

// threeTierSchedule returns a standard 3-tier structure reused across scenarios.
func threeTierSchedule() map[string]any {
	return tieredStructure(
		tier("1000", "0.05"), // 0-1000:    5%.
		tier("5000", "0.03"), // 1000-5000: 3%.
		infiniteTier("0.01"), // 5000+:     1%.
	)
}

// tieredExactBoundaryScenarios tests exact, above, and below tier boundaries.
func tieredExactBoundaryScenarios(next func() string) []FuzzScenario {
	threeTier := threeTierSchedule()

	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "Amount exactly equals first tier boundary ($1000); all amount falls in tier 1 at 5%, fee=$50.00",
			Difficulty:   difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Exact boundary $1000", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-3tier", Priority: 1,
					StructureType: "TIERED", Structure: threeTier,
				}},
			},
			GrossAmount: "1000.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "$1000.01: $1000 in tier 1 at 5% ($50) + $0.01 in tier 2 at 3% ($0.0003); " +
				"tests marginal split with sub-penny fee in second tier",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "One penny above $1000", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-3tier", Priority: 1,
					StructureType: "TIERED", Structure: threeTier,
				}},
			},
			GrossAmount: "1000.01",
		},
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "$999.99: entirely in tier 1 at 5%; verifies no amount leaks into tier 2",
			Difficulty:   difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "One penny below $1000", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-3tier", Priority: 1,
					StructureType: "TIERED", Structure: threeTier,
				}},
			},
			GrossAmount: "999.99",
		},
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "$50000 through 3 tiers: $1000x5% + $4000x3% + $45000x1%; " +
				"most amount ($45000) falls in the unbounded catch-all tier",
			Difficulty: difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Mostly in infinite tier", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-3tier", Priority: 1,
					StructureType: "TIERED", Structure: threeTier,
				}},
			},
			GrossAmount: "50000.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "$5000 exactly fills tiers 1+2 ($1000x5% + $4000x3% = $170); " +
				"zero amount should reach the infinite tier",
			Difficulty: difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Exact fill two tiers", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-3tier", Priority: 1,
					StructureType: "TIERED", Structure: threeTier,
				}},
			},
			GrossAmount: "5000.00",
		},
	}
}

// tieredDegenerateScenarios tests single tiers, zero-rate tiers, and many tiny tiers.
func tieredDegenerateScenarios(next func() string) []FuzzScenario {
	// 11 tiers at $100 increments with decreasing rates.
	manyTiers := tieredStructure(
		tier("100", "0.10"), tier("200", "0.09"), tier("300", "0.08"),
		tier("400", "0.07"), tier("500", "0.06"), tier("600", "0.05"),
		tier("700", "0.04"), tier("800", "0.03"), tier("900", "0.02"),
		tier("1000", "0.01"), infiniteTier("0.005"),
	)

	// 15 tiers with $1000 boundaries.
	fifteenTiers := tieredStructure(
		tier("1000", "0.10"), tier("2000", "0.09"), tier("3000", "0.08"),
		tier("4000", "0.07"), tier("5000", "0.06"), tier("6000", "0.05"),
		tier("7000", "0.04"), tier("8000", "0.03"), tier("9000", "0.02"),
		tier("10000", "0.015"), tier("11000", "0.012"), tier("12000", "0.010"),
		tier("13000", "0.008"), tier("14000", "0.006"), infiniteTier("0.005"),
	)

	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "Single unbounded tier at 2%: degenerate tiered structure equivalent to PERCENTAGE; " +
				"tests that tiered engine handles 1-tier correctly",
			Difficulty: difficultyTrivial,
			Schedule: FuzzScheduleSpec{
				Name: "Single catch-all tier", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-single", Priority: 1,
					StructureType: "TIERED", Structure: tieredStructure(infiniteTier("0.02")),
				}},
			},
			GrossAmount: "5000.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "11 tiers at $100 increments with decreasing rates; $750 spans 8 tiers " +
				"testing cumulative marginal calculation accuracy",
			Difficulty: difficultyHard,
			Schedule: FuzzScheduleSpec{
				Name: "Many tiny tiers", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-many", Priority: 1,
					StructureType: "TIERED", Structure: manyTiers,
				}},
			},
			GrossAmount: "750.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "First tier has rate 0 (free bracket 0-500), second tier at 5%; " +
				"$800 should produce fee only on $300 in tier 2 = $15.00",
			Difficulty: difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Zero-rate first tier", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-free-bracket", Priority: 1,
					StructureType: "TIERED",
					Structure:     tieredStructure(tier("500", "0"), infiniteTier("0.05")),
				}},
			},
			GrossAmount: "800.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "15 tiers but only $50 gross — all amount in tier 1; tests that engine " +
				"correctly skips 14 unreached tiers without error",
			Difficulty: difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "15 tiers tiny amount", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-15", Priority: 1,
					StructureType: "TIERED", Structure: fifteenTiers,
				}},
			},
			GrossAmount: "50.00",
		},
	}
}

// tieredComplexScenarios tests tiered combined with other fee types in cascading mode.
func tieredComplexScenarios(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "CASCADING: tiered fee first (priority 1), then flat $25 on remaining (priority 2); " +
				"the flat fee's base should be gross minus the rounded tiered fee",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Tiered then flat cascading", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{
						Name: "tiered-primary", Priority: 1,
						StructureType: "TIERED",
						Structure:     tieredStructure(tier("1000", "0.03"), infiniteTier("0.01")),
					},
					{
						Name: "flat-secondary", Priority: 2, //nolint:mnd // item priority is positional, not magic
						StructureType: "FLAT", Structure: flatStructure("25.00"),
					},
				},
			},
			GrossAmount: "3000.00",
		},
	}
}

// tieredRoundingInteractionScenarios tests tiered fees with different rounding modes and scales.
func tieredRoundingInteractionScenarios(next func() string) []FuzzScenario {
	twoTier := tieredStructure(tier("1000", "0.03"), infiniteTier("0.01"))

	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "Tiered fee with BANKERS rounding; $1500.50 splits across tiers producing " +
				"a raw fee of $1000x0.03 + $500.50x0.01 = 30 + 5.005 = 35.005; " +
				"BANKERS at scale=2 should round 35.005 to 35.00 (0 is even)",
			Difficulty: difficultyHard,
			Schedule: FuzzScheduleSpec{
				Name: "Tiered BANKERS boundary", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "BANKERS",
				Items: []FuzzItemSpec{{
					Name: "tiered-bankers", Priority: 1,
					StructureType: "TIERED", Structure: twoTier,
				}},
			},
			GrossAmount: "1500.50",
		},
		{
			ID: next(), Source: "deterministic", Category: "tiered_boundary",
			AttackVector: "Same tiered schedule at scale=0: $1500 raw fee = $1000x0.03 + $500x0.01 " +
				"= 30 + 5 = 35 exactly; no rounding needed but tests scale=0 with exact integer result",
			Difficulty: difficultyTrivial,
			Schedule: FuzzScheduleSpec{
				Name: "Tiered scale 0 exact", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleWholeNumber, RoundingMode: "TRUNCATE",
				Items: []FuzzItemSpec{{
					Name: "tiered-scale0", Priority: 1,
					StructureType: "TIERED", Structure: twoTier,
				}},
			},
			GrossAmount: "1500.00",
		},
	}
}

// Cascading stress generators.

// GenerateCascadingStressScenarios produces deterministic scenarios stressing
// cascading (waterfall) fee composition: many items, fee overflow, order
// sensitivity, and the interaction of FLAT + PERCENTAGE + TIERED in sequence.
func GenerateCascadingStressScenarios() []FuzzScenario {
	next := idSequencer("cascading")
	scenarios := make([]FuzzScenario, 0, 12) //nolint:mnd // pre-calculated: 1 many + 1 overflow + 2 comparison + 1 zero + 2 order + 5 precision

	scenarios = append(scenarios, cascadingManyItemsScenarios(next)...)
	scenarios = append(scenarios, cascadingOverflowScenarios(next)...)
	scenarios = append(scenarios, cascadingParallelComparison(next)...)
	scenarios = append(scenarios, cascadingZeroRemaining(next)...)
	scenarios = append(scenarios, cascadingOrderSensitivity(next)...)
	scenarios = append(scenarios, cascadingPrecisionScenarios(next)...)

	return scenarios
}

// cascadingManyItemsScenarios tests 8+ items cascading with mixed types.
func cascadingManyItemsScenarios(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "8 cascading items (FLAT+PERCENTAGE+TIERED mix); each item reduces the base " +
				"for the next, testing cumulative balance tracking through many steps",
			Difficulty: difficultyPathological,
			Schedule: FuzzScheduleSpec{
				Name: "8-item cascade", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: eightItemCascade(),
			},
			GrossAmount: "1000.00",
		},
	}
}

// eightItemCascade returns an 8-item mixed-type fee schedule for cascading stress tests.
func eightItemCascade() []FuzzItemSpec {
	return []FuzzItemSpec{
		{Name: "platform-fee", Priority: 1, StructureType: "FLAT", Structure: flatStructure("10.00")},
		{Name: "processing-pct", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.029")}, //nolint:mnd // item priority is positional
		{Name: "network-fee", Priority: 3, StructureType: "FLAT", Structure: flatStructure("0.30")},          //nolint:mnd // item priority is positional
		{Name: "risk-pct", Priority: 4, StructureType: "PERCENTAGE", Structure: pctStructure("0.005")},       //nolint:mnd // item priority is positional
		{Name: "compliance-fee", Priority: 5, StructureType: "FLAT", Structure: flatStructure("1.50")},       //nolint:mnd // item priority is positional
		{Name: "fx-markup", Priority: 6, StructureType: "PERCENTAGE", Structure: pctStructure("0.015")},      //nolint:mnd // item priority is positional
		{Name: "tiered-volume", Priority: 7, StructureType: "TIERED", Structure: tieredStructure( //nolint:mnd // item priority is positional
			tier("100", "0.02"), tier("500", "0.01"), infiniteTier("0.005"),
		)},
		{Name: "settlement-fee", Priority: 8, StructureType: "FLAT", Structure: flatStructure("2.00")}, //nolint:mnd // item priority is positional
	}
}

// cascadingOverflowScenarios tests when flat fees exceed the gross amount.
func cascadingOverflowScenarios(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "Flat fees total $150 on $100 gross in CASCADING mode; after the first $100 fee " +
				"the base clamps to 0, second flat $50 still applies on base 0 (fee=50); " +
				"totalFee=$150 but net should clamp to $0",
			Difficulty: difficultyHard,
			Schedule: FuzzScheduleSpec{
				Name: "Fees exceed gross", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{Name: "big-flat", Priority: 1, StructureType: "FLAT", Structure: flatStructure("100.00")},
					{Name: "extra-flat", Priority: 2, StructureType: "FLAT", Structure: flatStructure("50.00")}, //nolint:mnd // item priority is positional
				},
			},
			GrossAmount: "100.00",
		},
	}
}

// cascadingParallelComparison produces paired PARALLEL/CASCADING scenarios for the same items.
func cascadingParallelComparison(next func() string) []FuzzScenario {
	pctItems := []FuzzItemSpec{
		{Name: "fee-a", Priority: 1, StructureType: "PERCENTAGE", Structure: pctStructure("0.10")},
		{Name: "fee-b", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.05")}, //nolint:mnd // item priority is positional
		{Name: "fee-c", Priority: 3, StructureType: "PERCENTAGE", Structure: pctStructure("0.02")}, //nolint:mnd // item priority is positional
	}

	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "PARALLEL with 3 percentage fees (10%+5%+2%): all compute on $1000, " +
				"total=100+50+20=$170; compare with cascading counterpart",
			Difficulty: difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Parallel pct comparison", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: pctItems,
			},
			GrossAmount: "1000.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "CASCADING with same 3 percentage fees as parallel counterpart (10%, 5%, 2%); " +
				"cascading total should be less than parallel ($170) because each subsequent " +
				"fee computes on the reduced base: 100 + 45 + 17.10 = $162.10",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Cascading pct comparison", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: pctItems,
			},
			GrossAmount: "1000.00",
		},
	}
}

// cascadingZeroRemaining tests cascading when the first item drains the entire balance.
func cascadingZeroRemaining(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "First item takes 100% (rate=1.0) leaving $0; all subsequent items should " +
				"compute fee on base $0 producing $0 fees; tests zero-base handling in cascading",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "100% first item", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{Name: "take-all", Priority: 1, StructureType: "PERCENTAGE", Structure: pctStructure("1.0")},
					{Name: "on-nothing-a", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.05")}, //nolint:mnd // item priority is positional
					{Name: "on-nothing-b", Priority: 3, StructureType: "FLAT", Structure: flatStructure("10.00")},     //nolint:mnd // item priority is positional
				},
			},
			GrossAmount: "500.00",
		},
	}
}

// cascadingOrderSensitivity produces two scenarios with swapped priorities to prove order matters.
func cascadingOrderSensitivity(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "CASCADING: flat $200 (priority 1) then 10% (priority 2) on $1000; " +
				"flat first: base=1000->fee=200, then 10% of $800=80; total=280",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Flat first then pct", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{Name: "flat-fee", Priority: 1, StructureType: "FLAT", Structure: flatStructure("200.00")},
					{Name: "pct-fee", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.10")}, //nolint:mnd // item priority is positional
				},
			},
			GrossAmount: "1000.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "CASCADING: same items but swapped priorities — 10% first (priority 1) then flat $200 (priority 2); " +
				"pct first: 10% of $1000=100, then flat=200 on $900; total=300; " +
				"different from flat-first total of 280, proving order sensitivity",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Pct first then flat", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{Name: "pct-fee", Priority: 1, StructureType: "PERCENTAGE", Structure: pctStructure("0.10")},
					{Name: "flat-fee", Priority: 2, StructureType: "FLAT", Structure: flatStructure("200.00")}, //nolint:mnd // item priority is positional
				},
			},
			GrossAmount: "1000.00",
		},
	}
}

// cascadingPrecisionScenarios tests sub-penny precision, mixed types, and deep cascades.
func cascadingPrecisionScenarios(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "5 cascading percentage fees (each 30%) on $10: balance shrinks rapidly " +
				"(10->7->4.90->3.43->2.401->1.6807); tests sub-penny intermediate precision and cumulative rounding",
			Difficulty: difficultyHard,
			Schedule: FuzzScheduleSpec{
				Name: "Rapid decay cascade", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: fiveDecayItems(),
			},
			GrossAmount: "10.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "CASCADING: flat $500 (priority 1), then 10% on remaining $500 = $50 (priority 2), " +
				"then flat $100 on remaining $450 (priority 3); verifies cascading balance is " +
				"$1000->$500->$450->$350; net=$350",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Mixed flat+pct cascade", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{Name: "flat-first", Priority: 1, StructureType: "FLAT", Structure: flatStructure("500.00")},
					{Name: "pct-middle", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.10")}, //nolint:mnd // item priority is positional
					{Name: "flat-last", Priority: 3, StructureType: "FLAT", Structure: flatStructure("100.00")},     //nolint:mnd // item priority is positional
				},
			},
			GrossAmount: "1000.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "CASCADING with FLAT+PERCENTAGE+TIERED in sequence using BANKERS rounding; " +
				"intermediate rounding after each item affects subsequent bases, " +
				"testing that rounding is applied per-item not at the end",
			Difficulty: difficultyPathological,
			Schedule: FuzzScheduleSpec{
				Name: "Full mix cascade BANKERS", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "BANKERS",
				Items: []FuzzItemSpec{
					{Name: "flat-setup", Priority: 1, StructureType: "FLAT", Structure: flatStructure("7.77")},
					{Name: "pct-main", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.033")}, //nolint:mnd // item priority is positional
					{
						Name: "tiered-final", Priority: 3, //nolint:mnd // item priority is positional
						StructureType: "TIERED",
						Structure:     tieredStructure(tier("200", "0.02"), tier("500", "0.01"), infiniteTier("0.005")),
					},
				},
			},
			GrossAmount: "999.99",
		},
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "Single-item cascading schedule should produce identical results to parallel; " +
				"2.5% of $4000 = $100; validates that cascading with 1 item has no off-by-one",
			Difficulty: difficultyTrivial,
			Schedule: FuzzScheduleSpec{
				Name: "Single-item cascade", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "only-fee", Priority: 1,
					StructureType: "PERCENTAGE", Structure: pctStructure("0.025"),
				}},
			},
			GrossAmount: "4000.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "cascading_stress",
			AttackVector: "6-item CASCADING with TRUNCATE rounding at scale=4; truncation always loses " +
				"fractional digits and should never round up, causing systematic fee under-count " +
				"compared to HALF_UP on the same schedule",
			Difficulty: difficultyHard,
			Schedule: FuzzScheduleSpec{
				Name: "Deep cascade TRUNCATE", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleSubPenny, RoundingMode: "TRUNCATE",
				Items: sixItemTruncateCascade(),
			},
			GrossAmount: "777.77",
		},
	}
}

// fiveDecayItems returns 5 cascading 30% fees for rapid balance decay testing.
func fiveDecayItems() []FuzzItemSpec {
	return []FuzzItemSpec{
		{Name: "fee-1", Priority: 1, StructureType: "PERCENTAGE", Structure: pctStructure("0.30")},
		{Name: "fee-2", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.30")}, //nolint:mnd // item priority is positional
		{Name: "fee-3", Priority: 3, StructureType: "PERCENTAGE", Structure: pctStructure("0.30")}, //nolint:mnd // item priority is positional
		{Name: "fee-4", Priority: 4, StructureType: "PERCENTAGE", Structure: pctStructure("0.30")}, //nolint:mnd // item priority is positional
		{Name: "fee-5", Priority: 5, StructureType: "PERCENTAGE", Structure: pctStructure("0.30")}, //nolint:mnd // item priority is positional
	}
}

// sixItemTruncateCascade returns 6 mixed-type items for deep cascade TRUNCATE testing.
func sixItemTruncateCascade() []FuzzItemSpec {
	return []FuzzItemSpec{
		{Name: "fee-a", Priority: 1, StructureType: "PERCENTAGE", Structure: pctStructure("0.033")},
		{Name: "fee-b", Priority: 2, StructureType: "FLAT", Structure: flatStructure("3.3333")},     //nolint:mnd // item priority is positional
		{Name: "fee-c", Priority: 3, StructureType: "PERCENTAGE", Structure: pctStructure("0.017")}, //nolint:mnd // item priority is positional
		{Name: "fee-d", Priority: 4, StructureType: "PERCENTAGE", Structure: pctStructure("0.009")}, //nolint:mnd // item priority is positional
		{Name: "fee-e", Priority: 5, StructureType: "FLAT", Structure: flatStructure("0.5555")},     //nolint:mnd // item priority is positional
		{Name: "fee-f", Priority: 6, StructureType: "PERCENTAGE", Structure: pctStructure("0.001")}, //nolint:mnd // item priority is positional
	}
}

// Convergence generators.

// GenerateConvergenceScenarios produces deterministic scenarios whose
// characteristics would stress a GrossFromNet inverse solver.
func GenerateConvergenceScenarios() []FuzzScenario {
	next := idSequencer("convergence")
	scenarios := make([]FuzzScenario, 0, 9) //nolint:mnd // pre-calculated: 5 simple + 4 complex

	scenarios = append(scenarios, convergenceSimpleScenarios(next)...)
	scenarios = append(scenarios, convergenceComplexScenarios(next)...)

	return scenarios
}

// convergenceSimpleScenarios tests simple flat, percentage, and extreme-ratio convergence.
func convergenceSimpleScenarios(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "Simple flat $5 fee on $105 gross; forward calculation should yield " +
				"totalFee=$5 net=$100; inverse solver should converge in 1 iteration",
			Difficulty: difficultyTrivial,
			Schedule: FuzzScheduleSpec{
				Name: "Simple flat convergence", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "flat-fee", Priority: 1,
					StructureType: "FLAT", Structure: flatStructure("5.00"),
				}},
			},
			GrossAmount: "105.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "10% fee: to get net $100, gross must be $111.11 (rounded); " +
				"10% of 111.11 = 11.11, net = 100.00; tests repeating decimal convergence",
			Difficulty: difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Single pct convergence", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "pct-fee", Priority: 1,
					StructureType: "PERCENTAGE", Structure: pctStructure("0.10"),
				}},
			},
			GrossAmount: "111.11",
		},
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "80% fee rate: gross $100, fee $80, net $20; extreme ratio tests that " +
				"inverse solver doesn't overshoot or oscillate around the solution",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "High fee ratio", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "huge-pct", Priority: 1,
					StructureType: "PERCENTAGE", Structure: pctStructure("0.80"),
				}},
			},
			GrossAmount: "100.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "Minimum meaningful amount: $0.01 gross with 1% fee = $0.0001; " +
				"at scale=2 rounds to $0.00 fee; tests near-zero precision without underflow",
			Difficulty: difficultySimple,
			Schedule: FuzzScheduleSpec{
				Name: "Sub-penny fee convergence", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiny-pct", Priority: 1,
					StructureType: "PERCENTAGE", Structure: pctStructure("0.01"),
				}},
			},
			GrossAmount: "0.01",
		},
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "Zero gross amount: all percentage and tiered fees should compute to $0.00; " +
				"flat fees still apply; tests that engine handles zero gracefully",
			Difficulty: difficultyTrivial,
			Schedule: FuzzScheduleSpec{
				Name: "Zero gross convergence", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{Name: "pct-fee", Priority: 1, StructureType: "PERCENTAGE", Structure: pctStructure("0.05")},
					{
						Name: "tiered-fee", Priority: 2, //nolint:mnd // item priority is positional
						StructureType: "TIERED",
						Structure:     tieredStructure(tier("100", "0.03"), infiniteTier("0.01")),
					},
				},
			},
			GrossAmount: "0.00",
		},
	}
}

// convergenceComplexScenarios tests cascading, multi-percentage, and tiered convergence.
func convergenceComplexScenarios(next func() string) []FuzzScenario {
	return []FuzzScenario{
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "CASCADING: 5% first then flat $10; gross=$200: fee1=10, fee2=10, net=180; " +
				"inverse solver must handle the cascading dependency where flat fee is constant " +
				"but percentage fee varies with gross",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Cascade pct+flat convergence", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{Name: "pct-fee", Priority: 1, StructureType: "PERCENTAGE", Structure: pctStructure("0.05")},
					{Name: "flat-fee", Priority: 2, StructureType: "FLAT", Structure: flatStructure("10.00")}, //nolint:mnd // item priority is positional
				},
			},
			GrossAmount: "200.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "PARALLEL: 3 percentage fees (5%+3%+2%=10% total) on $1000; " +
				"each independently rounded then summed; convergence solver must handle " +
				"the sum-of-rounded vs round-of-sum difference",
			Difficulty: difficultyModerate,
			Schedule: FuzzScheduleSpec{
				Name: "Multi-pct convergence", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{Name: "fee-a", Priority: 1, StructureType: "PERCENTAGE", Structure: pctStructure("0.05")},
					{Name: "fee-b", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.03")}, //nolint:mnd // item priority is positional
					{Name: "fee-c", Priority: 3, StructureType: "PERCENTAGE", Structure: pctStructure("0.02")}, //nolint:mnd // item priority is positional
				},
			},
			GrossAmount: "1000.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "Tiered fee creates non-linear gross->fee mapping; inverse solver must " +
				"handle the discontinuity at tier boundaries; $2500 spans 2 tiers: " +
				"$1000x3% + $1500x1% = $30+$15=$45",
			Difficulty: difficultyHard,
			Schedule: FuzzScheduleSpec{
				Name: "Tiered convergence", Currency: "USD",
				ApplicationOrder: "PARALLEL", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{{
					Name: "tiered-fee", Priority: 1,
					StructureType: "TIERED",
					Structure:     tieredStructure(tier("1000", "0.03"), infiniteTier("0.01")),
				}},
			},
			GrossAmount: "2500.00",
		},
		{
			ID: next(), Source: "deterministic", Category: "convergence",
			AttackVector: "CASCADING: tiered fee first then 2% on remaining; inverse solver must " +
				"iterate because the tiered component is non-linear and the percentage " +
				"depends on the tiered result; tests multi-step convergence",
			Difficulty: difficultyPathological,
			Schedule: FuzzScheduleSpec{
				Name: "Cascade tiered+pct convergence", Currency: "USD",
				ApplicationOrder: "CASCADING", RoundingScale: scaleCents, RoundingMode: "HALF_UP",
				Items: []FuzzItemSpec{
					{
						Name: "tiered-primary", Priority: 1,
						StructureType: "TIERED",
						Structure:     tieredStructure(tier("500", "0.04"), tier("2000", "0.02"), infiniteTier("0.01")),
					},
					{Name: "pct-secondary", Priority: 2, StructureType: "PERCENTAGE", Structure: pctStructure("0.02")}, //nolint:mnd // item priority is positional
				},
			},
			GrossAmount: "3000.00",
		},
	}
}

// Aggregator.

// GenerateAllDeterministicScenarios returns all pre-built deterministic fuzz
// scenarios across all categories.
func GenerateAllDeterministicScenarios() []FuzzScenario {
	rounding := GenerateRoundingTortureScenarios()
	tiered := GenerateTieredBoundaryScenarios()
	cascading := GenerateCascadingStressScenarios()
	convergence := GenerateConvergenceScenarios()

	all := make([]FuzzScenario, 0, len(rounding)+len(tiered)+len(cascading)+len(convergence))
	all = append(all, rounding...)
	all = append(all, tiered...)
	all = append(all, cascading...)
	all = append(all, convergence...)

	return all
}
