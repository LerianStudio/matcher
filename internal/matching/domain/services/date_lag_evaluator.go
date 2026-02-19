package services

import (
	"errors"

	"github.com/shopspring/decimal"
)

// Sentinel errors for date lag evaluation.
var (
	ErrDateLagConfigRequired       = errors.New("date lag config is required")
	ErrDateLagDaysMustBeNonNeg     = errors.New("minDays/maxDays must be >= 0")
	ErrDateLagMaxMustBeGTEMin      = errors.New("maxDays must be >= minDays")
	ErrDateLagFeeToleranceNegative = errors.New("feeTolerance must be non-negative")
	ErrDateLagInvalidDirection     = errors.New("invalid date lag direction")
)

// DateLagMatch evaluates if two transactions match based on date lag rules.
func DateLagMatch(left, right CandidateTransaction, cfg *DateLagConfig) (bool, error) {
	if err := validateDateLagConfig(cfg); err != nil {
		return false, err
	}

	if !dateLagCurrencyMatch(left, right, cfg) {
		return false, nil
	}

	if err := validateFeeTolerance(cfg.FeeTolerance); err != nil {
		return false, err
	}

	if !amountWithinFeeTolerance(left, right, cfg.FeeTolerance) {
		return false, nil
	}

	diff, err := computeDateLagDiff(left, right, cfg)
	if err != nil {
		return false, err
	}

	if diff < 0 {
		return false, nil
	}

	return isWithinDateLagBounds(diff, cfg), nil
}

func validateDateLagConfig(cfg *DateLagConfig) error {
	if cfg == nil {
		return ErrDateLagConfigRequired
	}

	if cfg.MinDays < 0 || cfg.MaxDays < 0 {
		return ErrDateLagDaysMustBeNonNeg
	}

	if cfg.MaxDays < cfg.MinDays {
		return ErrDateLagMaxMustBeGTEMin
	}

	return nil
}

func dateLagCurrencyMatch(left, right CandidateTransaction, cfg *DateLagConfig) bool {
	if cfg.MatchCurrency && left.Currency != right.Currency {
		return false
	}

	if !cfg.MatchCurrency && (left.Currency == "" || right.Currency == "") {
		return false
	}

	return true
}

func validateFeeTolerance(feeTolerance decimal.Decimal) error {
	if feeTolerance.IsNegative() {
		return ErrDateLagFeeToleranceNegative
	}

	return nil
}

func amountWithinFeeTolerance(left, right CandidateTransaction, feeTolerance decimal.Decimal) bool {
	return !left.Amount.Sub(right.Amount).Abs().GreaterThan(feeTolerance)
}

func computeDateLagDiff(left, right CandidateTransaction, cfg *DateLagConfig) (int, error) {
	signed := SignedDayDiff(left.Date, right.Date)
	abs := signed

	if abs < 0 {
		abs = -abs
	}

	switch cfg.Direction {
	case DateLagDirectionAbs:
		return abs, nil
	case DateLagDirectionLeftBeforeRight:
		return computeLeftBeforeRightDiff(signed, cfg.MinDays)
	case DateLagDirectionRightBeforeLeft:
		return computeRightBeforeLeftDiff(signed, cfg.MinDays)
	default:
		return 0, ErrDateLagInvalidDirection
	}
}

func computeLeftBeforeRightDiff(signed, minDays int) (int, error) {
	if signed < 0 || (signed == 0 && minDays > 0) {
		return -1, nil // Signal no match
	}

	return signed, nil
}

func computeRightBeforeLeftDiff(signed, minDays int) (int, error) {
	if signed > 0 || (signed == 0 && minDays > 0) {
		return -1, nil // Signal no match
	}

	return -signed, nil
}

// isWithinDateLagBounds checks if the day difference is within the configured bounds.
//
// IMPORTANT: When Inclusive=false (exclusive bounds) with MinDays=0, same-day transactions
// (diff=0) will NOT match because the condition requires diff > MinDays (i.e., diff > 0).
// This is mathematically correct for exclusive ranges: (0, MaxDays) excludes 0.
// If same-day matching is needed with exclusive upper bound, use Inclusive=true with MinDays=0,
// or set MinDays=-1 (not supported, would require config validation change).
func isWithinDateLagBounds(diff int, cfg *DateLagConfig) bool {
	if cfg.Inclusive {
		return diff >= cfg.MinDays && diff <= cfg.MaxDays
	}

	return diff > cfg.MinDays && diff < cfg.MaxDays
}
