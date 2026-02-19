//go:build e2e

package e2e

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/shopspring/decimal"
)

// Oracle fee structure type constants (independent of production code).
const (
	oracleStructureFlat       = "FLAT"
	oracleStructurePercentage = "PERCENTAGE"
	oracleStructureTiered     = "TIERED"
)

// Oracle sentinel errors.
var (
	errOracleNegativeGross     = errors.New("oracle: gross amount must be non-negative")
	errOracleNoItems           = errors.New("oracle: schedule has no items")
	errOracleUnknownOrder      = errors.New("oracle: unknown application order")
	errOracleUnknownStructure  = errors.New("oracle: unknown structure type")
	errOracleMissingKey        = errors.New("oracle: missing required key in structure")
	errOracleInvalidType       = errors.New("oracle: unexpected value type in structure")
	errOracleNegativeValue     = errors.New("oracle: value must be non-negative")
	errOracleNonPositiveUpTo   = errors.New("oracle: tier upTo must be positive")
	errOracleEmptyTiers        = errors.New("oracle: tiered structure must have at least one tier")
	errOracleTierInvalidObject = errors.New("oracle: tier element must be an object")
)

// oracleParsedTier holds a parsed tier after extraction from the raw structure map.
type oracleParsedTier struct {
	rate decimal.Decimal
	upTo *decimal.Decimal // nil means infinite.
}

// GoOracleCalculate is an independent fee schedule calculator that serves as a
// test oracle. It is intentionally written from scratch, sharing ZERO code with
// the production fee calculation engine, so that bugs in one implementation
// cannot mask bugs in the other.
//
// The oracle takes a FuzzScheduleSpec (defined in fuzz_types.go) and a gross
// amount string, then computes the expected total fee, net amount, per-item
// fees, and a human-readable reasoning string showing each calculation step.
func GoOracleCalculate(spec FuzzScheduleSpec, grossAmount string) (*FuzzExpectedResult, error) {
	gross, err := decimal.NewFromString(grossAmount)
	if err != nil {
		return nil, fmt.Errorf("oracle: parse gross amount %q: %w", grossAmount, err)
	}

	if gross.IsNegative() {
		return nil, fmt.Errorf("%w: got %s", errOracleNegativeGross, grossAmount)
	}

	if len(spec.Items) == 0 {
		return nil, errOracleNoItems
	}

	// Sort items by priority ascending (copy to avoid mutating the input).
	sorted := make([]FuzzItemSpec, len(spec.Items))
	copy(sorted, spec.Items)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})

	switch strings.ToUpper(spec.ApplicationOrder) {
	case "PARALLEL":
		return oracleParallel(gross, sorted, spec.RoundingScale, spec.RoundingMode)
	case "CASCADING":
		return oracleCascading(gross, sorted, spec.RoundingScale, spec.RoundingMode)
	default:
		return nil, fmt.Errorf("%w: %q", errOracleUnknownOrder, spec.ApplicationOrder)
	}
}

// oracleParallel computes fees in PARALLEL mode: every item calculates
// independently on the same gross amount.
func oracleParallel(
	gross decimal.Decimal,
	items []FuzzItemSpec,
	scale int,
	mode string,
) (*FuzzExpectedResult, error) {
	totalFee := decimal.Zero
	itemFees := make([]FuzzItemResult, 0, len(items))

	var reasoning strings.Builder

	for i, item := range items {
		rawFee, err := oracleCalculateItemFee(item.StructureType, item.Structure, gross)
		if err != nil {
			return nil, fmt.Errorf("oracle: item %q (priority=%d): %w", item.Name, item.Priority, err)
		}

		rounded := oracleRound(rawFee, scale, mode)
		totalFee = totalFee.Add(rounded)

		itemFees = append(itemFees, FuzzItemResult{
			Name:     item.Name,
			Fee:      rounded.StringFixed(int32(scale)), //nolint:gosec // scale clamped in oracleRound
			BaseUsed: gross.StringFixed(int32(scale)),   //nolint:gosec // scale clamped in oracleRound
		})

		fmt.Fprintf(&reasoning,
			"Step %d: '%s' (%s) on base %s = %s, rounded(%s,%d) = %s\n",
			i+1, item.Name,
			oracleStructureSummary(item.StructureType, item.Structure),
			gross.String(), rawFee.String(),
			mode, scale, rounded.StringFixed(int32(scale)), //nolint:gosec // scale clamped in oracleRound
		)
	}

	net := gross.Sub(totalFee)

	fmt.Fprintf(&reasoning, "Total fee: %s, Net: %s",
		totalFee.StringFixed(int32(scale)), //nolint:gosec // scale clamped in oracleRound
		net.StringFixed(int32(scale)),      //nolint:gosec // scale clamped in oracleRound
	)

	return &FuzzExpectedResult{
		TotalFee:  totalFee.StringFixed(int32(scale)), //nolint:gosec // scale clamped in oracleRound
		NetAmount: net.StringFixed(int32(scale)),      //nolint:gosec // scale clamped in oracleRound
		ItemFees:  itemFees,
		Reasoning: reasoning.String(),
	}, nil
}

// oracleCascading computes fees in CASCADING mode: each item calculates on the
// remaining balance after all previous items' fees have been subtracted.
func oracleCascading(
	gross decimal.Decimal,
	items []FuzzItemSpec,
	scale int,
	mode string,
) (*FuzzExpectedResult, error) {
	currentBase := gross
	totalFee := decimal.Zero
	itemFees := make([]FuzzItemResult, 0, len(items))

	var reasoning strings.Builder

	for i, item := range items {
		rawFee, err := oracleCalculateItemFee(item.StructureType, item.Structure, currentBase)
		if err != nil {
			return nil, fmt.Errorf("oracle: item %q (priority=%d): %w", item.Name, item.Priority, err)
		}

		rounded := oracleRound(rawFee, scale, mode)
		totalFee = totalFee.Add(rounded)

		itemFees = append(itemFees, FuzzItemResult{
			Name:     item.Name,
			Fee:      rounded.StringFixed(int32(scale)),     //nolint:gosec // scale clamped in oracleRound
			BaseUsed: currentBase.StringFixed(int32(scale)), //nolint:gosec // scale clamped in oracleRound
		})

		currentBase = currentBase.Sub(rounded)

		// Clamp to zero: fees are contractual obligations even if they exceed gross.
		if currentBase.IsNegative() {
			currentBase = decimal.Zero
		}

		fmt.Fprintf(&reasoning,
			"Step %d: '%s' (%s) on base %s = %s, rounded = %s, remaining: %s\n",
			i+1, item.Name,
			oracleStructureSummary(item.StructureType, item.Structure),
			itemFees[i].BaseUsed, rawFee.String(),
			rounded.StringFixed(int32(scale)),     //nolint:gosec // scale clamped in oracleRound
			currentBase.StringFixed(int32(scale)), //nolint:gosec // scale clamped in oracleRound
		)
	}

	fmt.Fprintf(&reasoning, "Total fee: %s, Net: %s",
		totalFee.StringFixed(int32(scale)),    //nolint:gosec // scale clamped in oracleRound
		currentBase.StringFixed(int32(scale)), //nolint:gosec // scale clamped in oracleRound
	)

	return &FuzzExpectedResult{
		TotalFee:  totalFee.StringFixed(int32(scale)),    //nolint:gosec // scale clamped in oracleRound
		NetAmount: currentBase.StringFixed(int32(scale)), //nolint:gosec // scale clamped in oracleRound
		ItemFees:  itemFees,
		Reasoning: reasoning.String(),
	}, nil
}

// oracleCalculateItemFee calculates a single item's fee from its structure map.
// This is a pure function: no rounding is applied here (the caller rounds).
func oracleCalculateItemFee(
	structureType string,
	structure map[string]any,
	baseAmount decimal.Decimal,
) (decimal.Decimal, error) {
	switch strings.ToUpper(structureType) {
	case oracleStructureFlat:
		return oracleCalcFlat(structure)
	case oracleStructurePercentage:
		return oracleCalcPercentage(structure, baseAmount)
	case oracleStructureTiered:
		return oracleCalcTiered(structure, baseAmount)
	default:
		return decimal.Zero, fmt.Errorf("%w: %q", errOracleUnknownStructure, structureType)
	}
}

// oracleCalcFlat extracts and returns the fixed fee amount.
func oracleCalcFlat(structure map[string]any) (decimal.Decimal, error) {
	raw, ok := structure["amount"]
	if !ok {
		return decimal.Zero, fmt.Errorf("%w: FLAT structure missing 'amount'", errOracleMissingKey)
	}

	amountStr, ok := raw.(string)
	if !ok {
		return decimal.Zero, fmt.Errorf("%w: FLAT 'amount' must be a string, got %T", errOracleInvalidType, raw)
	}

	amount, err := decimal.NewFromString(amountStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("FLAT 'amount' parse %q: %w", amountStr, err)
	}

	if amount.IsNegative() {
		return decimal.Zero, fmt.Errorf("%w: FLAT 'amount' got %s", errOracleNegativeValue, amountStr)
	}

	return amount, nil
}

// oracleCalcPercentage computes baseAmount × rate.
func oracleCalcPercentage(structure map[string]any, baseAmount decimal.Decimal) (decimal.Decimal, error) {
	raw, ok := structure["rate"]
	if !ok {
		return decimal.Zero, fmt.Errorf("%w: PERCENTAGE structure missing 'rate'", errOracleMissingKey)
	}

	rateStr, ok := raw.(string)
	if !ok {
		return decimal.Zero, fmt.Errorf("%w: PERCENTAGE 'rate' must be a string, got %T", errOracleInvalidType, raw)
	}

	rate, err := decimal.NewFromString(rateStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("PERCENTAGE 'rate' parse %q: %w", rateStr, err)
	}

	if rate.IsNegative() {
		return decimal.Zero, fmt.Errorf("%w: PERCENTAGE 'rate' got %s", errOracleNegativeValue, rateStr)
	}

	return baseAmount.Mul(rate), nil
}

// oracleCalcTiered computes a marginal/progressive fee across tiers.
//
// The tiers are extracted from structure["tiers"] which arrives as []any
// from JSON deserialization. Each element is a map[string]any with:
//   - "rate" (string): the marginal rate for this bracket
//   - "upTo" (string or nil): the upper bound of the bracket (nil = infinity)
func oracleCalcTiered(structure map[string]any, baseAmount decimal.Decimal) (decimal.Decimal, error) {
	parsed, err := oracleParseTiers(structure)
	if err != nil {
		return decimal.Zero, err
	}

	oracleSortTiers(parsed)

	return oracleComputeMarginalFee(parsed, baseAmount), nil
}

// oracleParseTiers extracts and validates tier definitions from the raw structure map.
func oracleParseTiers(structure map[string]any) ([]oracleParsedTier, error) {
	rawTiers, ok := structure["tiers"]
	if !ok {
		return nil, fmt.Errorf("%w: TIERED structure missing 'tiers'", errOracleMissingKey)
	}

	tiersSlice, ok := rawTiers.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: TIERED 'tiers' must be an array, got %T", errOracleInvalidType, rawTiers)
	}

	if len(tiersSlice) == 0 {
		return nil, errOracleEmptyTiers
	}

	parsed := make([]oracleParsedTier, 0, len(tiersSlice))

	for idx, rawTier := range tiersSlice {
		tier, err := oracleParseSingleTier(idx, rawTier)
		if err != nil {
			return nil, err
		}

		parsed = append(parsed, tier)
	}

	return parsed, nil
}

// oracleParseSingleTier parses one tier element from the raw []any slice.
func oracleParseSingleTier(idx int, rawTier any) (oracleParsedTier, error) {
	tierMap, ok := rawTier.(map[string]any)
	if !ok {
		return oracleParsedTier{}, fmt.Errorf("%w: tier[%d] got %T", errOracleTierInvalidObject, idx, rawTier)
	}

	rate, err := oracleExtractTierRate(idx, tierMap)
	if err != nil {
		return oracleParsedTier{}, err
	}

	upTo, err := oracleExtractTierUpTo(idx, tierMap)
	if err != nil {
		return oracleParsedTier{}, err
	}

	return oracleParsedTier{rate: rate, upTo: upTo}, nil
}

// oracleExtractTierRate extracts and validates the "rate" field from a tier map.
func oracleExtractTierRate(idx int, tierMap map[string]any) (decimal.Decimal, error) {
	rawRate, ok := tierMap["rate"]
	if !ok {
		return decimal.Zero, fmt.Errorf("%w: tier[%d] missing 'rate'", errOracleMissingKey, idx)
	}

	rateStr, ok := rawRate.(string)
	if !ok {
		return decimal.Zero, fmt.Errorf("%w: tier[%d] 'rate' must be a string, got %T", errOracleInvalidType, idx, rawRate)
	}

	rate, err := decimal.NewFromString(rateStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("tier[%d] 'rate' parse %q: %w", idx, rateStr, err)
	}

	if rate.IsNegative() {
		return decimal.Zero, fmt.Errorf("%w: tier[%d] 'rate' got %s", errOracleNegativeValue, idx, rateStr)
	}

	return rate, nil
}

// oracleExtractTierUpTo extracts and validates the optional "upTo" field from a tier map.
// Returns nil for infinite tiers (absent or nil value).
func oracleExtractTierUpTo(idx int, tierMap map[string]any) (*decimal.Decimal, error) {
	rawUpTo, exists := tierMap["upTo"]
	if !exists || rawUpTo == nil {
		return nil, nil
	}

	upToStr, ok := rawUpTo.(string)
	if !ok {
		return nil, fmt.Errorf("%w: tier[%d] 'upTo' must be a string or nil, got %T", errOracleInvalidType, idx, rawUpTo)
	}

	upTo, err := decimal.NewFromString(upToStr)
	if err != nil {
		return nil, fmt.Errorf("tier[%d] 'upTo' parse %q: %w", idx, upToStr, err)
	}

	if !upTo.IsPositive() {
		return nil, fmt.Errorf("%w: tier[%d] got %s", errOracleNonPositiveUpTo, idx, upToStr)
	}

	return &upTo, nil
}

// oracleSortTiers sorts parsed tiers: finite upTo ascending, nil-upTo (infinite) last.
func oracleSortTiers(tiers []oracleParsedTier) {
	sort.SliceStable(tiers, func(i, j int) bool {
		iInf := tiers[i].upTo == nil
		jInf := tiers[j].upTo == nil

		if iInf != jInf {
			return !iInf // finite before infinite
		}

		if iInf {
			return false // both infinite — preserve order
		}

		return tiers[i].upTo.LessThan(*tiers[j].upTo)
	})
}

// oracleComputeMarginalFee walks through sorted tiers and applies marginal rates.
func oracleComputeMarginalFee(tiers []oracleParsedTier, baseAmount decimal.Decimal) decimal.Decimal {
	remaining := baseAmount
	total := decimal.Zero
	lower := decimal.Zero

	for _, tier := range tiers {
		if remaining.LessThanOrEqual(decimal.Zero) {
			break
		}

		tierCap := oracleTierCap(tier, lower, remaining)
		if tierCap.LessThanOrEqual(decimal.Zero) {
			continue
		}

		total = total.Add(tierCap.Mul(tier.rate))
		remaining = remaining.Sub(tierCap)

		if tier.upTo != nil {
			lower = *tier.upTo
		}
	}

	return total
}

// oracleTierCap calculates how much of the remaining amount falls within a tier bracket.
func oracleTierCap(tier oracleParsedTier, lower, remaining decimal.Decimal) decimal.Decimal {
	if tier.upTo == nil {
		return remaining
	}

	maxInBracket := tier.upTo.Sub(lower)
	if maxInBracket.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}

	if remaining.GreaterThan(maxInBracket) {
		return maxInBracket
	}

	return remaining
}

// oracleRound rounds a decimal using the specified mode and scale.
//
// Supported modes: HALF_UP, BANKERS, FLOOR, CEIL, TRUNCATE.
// Unknown modes default to HALF_UP for resilience.
func oracleRound(amount decimal.Decimal, scale int, mode string) decimal.Decimal {
	// Clamp scale to valid range.
	const maxScale = 10

	if scale < 0 {
		scale = 0
	}

	if scale > maxScale {
		scale = maxScale
	}

	decimalScale := int32(scale) //nolint:gosec // scale is clamped to [0, 10] above

	switch strings.ToUpper(mode) {
	case "HALF_UP":
		return amount.Round(decimalScale)
	case "BANKERS":
		return amount.RoundBank(decimalScale)
	case "FLOOR":
		return amount.RoundFloor(decimalScale)
	case "CEIL":
		return amount.RoundCeil(decimalScale)
	case "TRUNCATE":
		return amount.Truncate(decimalScale)
	default:
		// Fallback: standard rounding.
		return amount.Round(decimalScale)
	}
}

// oracleStructureSummary returns a human-readable summary of a fee structure
// for use in reasoning strings.
func oracleStructureSummary(structureType string, structure map[string]any) string {
	switch strings.ToUpper(structureType) {
	case oracleStructureFlat:
		if amt, ok := structure["amount"]; ok {
			return fmt.Sprintf("FLAT amount=%v", amt)
		}

		return oracleStructureFlat

	case oracleStructurePercentage:
		if rate, ok := structure["rate"]; ok {
			return fmt.Sprintf("PERCENTAGE rate=%v", rate)
		}

		return oracleStructurePercentage

	case oracleStructureTiered:
		if tiers, ok := structure["tiers"]; ok {
			if tiersSlice, sliceOK := tiers.([]any); sliceOK {
				return fmt.Sprintf("TIERED %d-tier", len(tiersSlice))
			}
		}

		return oracleStructureTiered

	default:
		return structureType
	}
}
