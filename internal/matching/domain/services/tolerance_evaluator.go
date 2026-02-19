package services

import (
	"errors"
	"math"
	"strings"

	"github.com/shopspring/decimal"
)

// Constants for tolerance matching calculations.
const (
	averageDivisor = 2
)

// averageDivisorDecimal is the pre-computed decimal form of averageDivisor
// to avoid per-call allocation in toleranceThreshold.
var averageDivisorDecimal = decimal.NewFromInt(averageDivisor)

// Sentinel errors for tolerance matching validation.
var (
	ErrToleranceConfigRequired = errors.New("tolerance config is required")
	ErrInvalidRoundingMode     = errors.New("invalid rounding mode")
	ErrInvalidRoundingScale    = errors.New("rounding scale must be >= 0")
	ErrInvalidDateWindowDays   = errors.New("date window days must be >= 0")
	ErrInvalidTolerance        = errors.New("tolerances must be non-negative")
)

// ToleranceMatch evaluates whether two transactions match within configured tolerances.
func ToleranceMatch(left, right CandidateTransaction, cfg *ToleranceConfig) (bool, error) {
	preflightOk, err := validateToleranceMatchInputs(left, right, cfg)
	if !preflightOk || err != nil {
		return preflightOk, err
	}

	leftAmount := left.Amount
	rightAmount := right.Amount
	absTolerance := cfg.AbsAmountTolerance
	percentTolerance := cfg.PercentTolerance

	if cfg.MatchBaseAmount {
		leftAmount = *left.AmountBase
		rightAmount = *right.AmountBase
	}

	leftRounded, err := roundAmount(leftAmount, cfg.RoundingScale, cfg.RoundingMode)
	if err != nil {
		return false, err
	}

	rightRounded, err := roundAmount(rightAmount, cfg.RoundingScale, cfg.RoundingMode)
	if err != nil {
		return false, err
	}

	diff := leftRounded.Sub(rightRounded).Abs()
	threshold := toleranceThreshold(absTolerance, percentTolerance, leftRounded, rightRounded, cfg.PercentageBase)

	if diff.GreaterThan(threshold) {
		return false, nil
	}

	return true, nil
}

func validateToleranceMatchInputs(
	left, right CandidateTransaction,
	cfg *ToleranceConfig,
) (bool, error) {
	if err := validateToleranceConfig(cfg); err != nil {
		return false, err
	}

	if !toleranceCurrencyMatch(left, right, cfg) {
		return false, nil
	}

	if !toleranceDateMatch(left, right, cfg) {
		return false, nil
	}

	if !toleranceBaseAmountMatch(left, right, cfg) {
		return false, nil
	}

	if !toleranceReferenceMatch(left, right, cfg) {
		return false, nil
	}

	return true, nil
}

func validateToleranceConfig(cfg *ToleranceConfig) error {
	if cfg == nil {
		return ErrToleranceConfigRequired
	}

	if cfg.DateWindowDays < 0 {
		return ErrInvalidDateWindowDays
	}

	if cfg.AbsAmountTolerance.IsNegative() || cfg.PercentTolerance.IsNegative() {
		return ErrInvalidTolerance
	}

	if _, err := roundAmount(decimal.Zero, cfg.RoundingScale, cfg.RoundingMode); err != nil {
		return err
	}

	return nil
}

func toleranceCurrencyMatch(left, right CandidateTransaction, cfg *ToleranceConfig) bool {
	if cfg.MatchCurrency && left.Currency != right.Currency {
		return false
	}

	if cfg.MatchBaseCurrency {
		if left.CurrencyBase == "" || right.CurrencyBase == "" {
			return false
		}

		if left.CurrencyBase != right.CurrencyBase {
			return false
		}
	}

	return true
}

func toleranceDateMatch(left, right CandidateTransaction, cfg *ToleranceConfig) bool {
	return AbsDayDiff(left.Date, right.Date) <= cfg.DateWindowDays
}

func toleranceBaseAmountMatch(left, right CandidateTransaction, cfg *ToleranceConfig) bool {
	if cfg.MatchBaseAmount && (left.AmountBase == nil || right.AmountBase == nil) {
		return false
	}

	return true
}

// toleranceReferenceMatch checks if references match when reference matching is enabled.
// When MatchReference is false (default), this always returns true.
func toleranceReferenceMatch(left, right CandidateTransaction, cfg *ToleranceConfig) bool {
	if !cfg.MatchReference {
		return true
	}

	leftRef := strings.TrimSpace(left.Reference)
	rightRef := strings.TrimSpace(right.Reference)

	if cfg.ReferenceMustSet && (leftRef == "" || rightRef == "") {
		return false
	}

	if leftRef == "" && rightRef == "" {
		return true
	}

	if cfg.CaseInsensitive {
		return equalFold(leftRef, rightRef)
	}

	return leftRef == rightRef
}

func roundAmount(amount decimal.Decimal, scale int, mode RoundingMode) (decimal.Decimal, error) {
	if scale < 0 {
		return decimal.Decimal{}, ErrInvalidRoundingScale
	}

	if scale > math.MaxInt32 {
		return decimal.Decimal{}, ErrInvalidRoundingScale
	}

	precision := int32(scale)

	switch mode {
	case RoundingHalfUp:
		return amount.Round(precision), nil
	case RoundingBankers:
		return amount.RoundBank(precision), nil
	case RoundingFloor:
		return amount.RoundFloor(precision), nil
	case RoundingCeil:
		return amount.RoundCeil(precision), nil
	case RoundingTruncate:
		return amount.Truncate(precision), nil
	default:
		return decimal.Decimal{}, ErrInvalidRoundingMode
	}
}

// toleranceThreshold returns the larger of the absolute tolerance or the percentage
// tolerance (pctTol as a decimal fraction, e.g., 0.05 for 5%) applied to a base amount
// determined by the TolerancePercentageBase strategy.
//
// The base parameter controls which amount is used for percentage calculation:
//   - MAX (default): uses the maximum of both amounts (backward compatible)
//   - MIN: uses the minimum of both amounts (stricter matching)
//   - AVERAGE: uses the average of both amounts
//   - LEFT: uses the left (source) amount only
//   - RIGHT: uses the right (target) amount only
//
// For asymmetric transactions (e.g., $10 vs $1000 with 5% tolerance), the threshold
// varies significantly by strategy: MAX yields $50, MIN yields $0.50, AVERAGE yields $25.25.
func toleranceThreshold(absTol, pctTol, left, right decimal.Decimal, base TolerancePercentageBase) decimal.Decimal {
	var baseAmount decimal.Decimal

	leftAbs := left.Abs()
	rightAbs := right.Abs()

	switch base {
	case TolerancePercentageBaseMin:
		baseAmount = leftAbs
		if rightAbs.LessThan(baseAmount) {
			baseAmount = rightAbs
		}
	case TolerancePercentageBaseAvg:
		baseAmount = leftAbs.Add(rightAbs).Div(averageDivisorDecimal)
	case TolerancePercentageBaseLeft:
		baseAmount = leftAbs
	case TolerancePercentageBaseRight:
		baseAmount = rightAbs
	default: // MAX (default, backward compatible)
		baseAmount = leftAbs
		if rightAbs.GreaterThan(baseAmount) {
			baseAmount = rightAbs
		}
	}

	percentThreshold := pctTol.Mul(baseAmount)
	if percentThreshold.GreaterThan(absTol) {
		return percentThreshold
	}

	return absTol
}
