// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import (
	"math"

	"github.com/shopspring/decimal"
)

// Confidence scoring constants per PRD AC-001.
//
// These weights are intentionally static for the current PRD requirements.
// Note that for date-lag matches, ReferenceScore is always 0.0, so the maximum
// achievable score is 90 (Amount 40 + Currency 30 + Date 20 + Reference 0),
// which equals the auto-confirm threshold. This is by design: date-lag matches
// require human review.
//
// If these weights become configurable in the future, callers must validate
// that they sum to 1.0 to preserve the 0-100 scoring range.
const (
	weightAmount    = 0.40
	weightCurrency  = 0.30
	weightDate      = 0.20
	weightReference = 0.10
	scoreMaxValue   = 100
)

// ConfidenceWeights defines the weighted components for financial-first scoring.
// Per PRD AC-001 and research.md:
//   - Amount match: 40% (highest weight - financial accuracy is paramount)
//   - Currency match: 30% (high weight - must match for valid reconciliation)
//   - Date tolerance: 20% (medium weight - timing flexibility allowed)
//   - Reference/description: 10% (low weight - supplementary matching)
type ConfidenceWeights struct {
	Amount    float64
	Currency  float64
	Date      float64
	Reference float64
}

// DefaultConfidenceWeights returns the PRD-defined weights for financial-first scoring.
func DefaultConfidenceWeights() ConfidenceWeights {
	return ConfidenceWeights{
		Amount:    weightAmount,
		Currency:  weightCurrency,
		Date:      weightDate,
		Reference: weightReference,
	}
}

// ConfidenceComponents captures individual match results for score calculation.
type ConfidenceComponents struct {
	AmountMatch    bool    // Exact amount match (or within tolerance for tolerance rules)
	CurrencyMatch  bool    // Currency codes match
	DateMatch      bool    // Date within acceptable tolerance
	ReferenceScore float64 // 0.0-1.0 for reference/description matching
}

// CalculateConfidenceScore computes a 0-100 confidence score using weighted components.
// This implements the financial-first approach from PRD AC-001.
func CalculateConfidenceScore(components ConfidenceComponents, weights ConfidenceWeights) int {
	var score float64

	if components.AmountMatch {
		score += weights.Amount
	}

	if components.CurrencyMatch {
		score += weights.Currency
	}

	if components.DateMatch {
		score += weights.Date
	}

	score += components.ReferenceScore * weights.Reference

	return int(math.Round(score * scoreMaxValue))
}

// ScoreExactConfidence calculates a financial-first confidence score for exact matching.
func ScoreExactConfidence(cfg *ExactConfig, left, right CandidateTransaction) int {
	if cfg == nil {
		return 0
	}

	weights := DefaultConfidenceWeights()
	components := extractExactComponents(cfg, left, right)

	return CalculateConfidenceScore(components, weights)
}

// ScoreToleranceConfidence calculates a financial-first confidence score for tolerance matching.
func ScoreToleranceConfidence(cfg *ToleranceConfig, left, right CandidateTransaction) int {
	if cfg == nil {
		return 0
	}

	weights := DefaultConfidenceWeights()
	components := extractToleranceComponents(cfg, left, right)

	return CalculateConfidenceScore(components, weights)
}

// ScoreDateLagConfidence calculates a financial-first confidence score for date-lag matching.
func ScoreDateLagConfidence(cfg *DateLagConfig, left, right CandidateTransaction) int {
	if cfg == nil {
		return 0
	}

	weights := DefaultConfidenceWeights()
	components := extractDateLagComponents(cfg, left, right)

	return CalculateConfidenceScore(components, weights)
}

func extractExactComponents(
	cfg *ExactConfig,
	left, right CandidateTransaction,
) ConfidenceComponents {
	return ConfidenceComponents{
		AmountMatch:    evaluateExactAmountMatch(cfg, left, right),
		CurrencyMatch:  evaluateExactCurrencyMatch(cfg, left, right),
		DateMatch:      evaluateExactDateMatch(cfg, left, right),
		ReferenceScore: evaluateExactReferenceScore(cfg, left, right),
	}
}

func evaluateExactAmountMatch(cfg *ExactConfig, left, right CandidateTransaction) bool {
	if cfg.MatchBaseAmount {
		if left.AmountBase == nil || right.AmountBase == nil {
			return false
		}

		return left.AmountBase.Equal(*right.AmountBase)
	}

	if cfg.MatchAmount {
		return left.Amount.Equal(right.Amount)
	}

	return true
}

func evaluateExactCurrencyMatch(cfg *ExactConfig, left, right CandidateTransaction) bool {
	if cfg.MatchBaseCurrency {
		return left.CurrencyBase != "" && left.CurrencyBase == right.CurrencyBase
	}

	if cfg.MatchCurrency {
		return left.Currency != "" && left.Currency == right.Currency
	}

	if left.Currency == "" || right.Currency == "" {
		return false
	}

	return left.Currency == right.Currency
}

func evaluateExactDateMatch(cfg *ExactConfig, left, right CandidateTransaction) bool {
	if !cfg.MatchDate {
		return true
	}

	switch cfg.DatePrecision {
	case DatePrecisionDay:
		return DayUTC(left.Date).Equal(DayUTC(right.Date))
	case DatePrecisionTimestamp:
		return left.Date.UTC().Equal(right.Date.UTC())
	default:
		return true
	}
}

func evaluateExactReferenceScore(cfg *ExactConfig, left, right CandidateTransaction) float64 {
	if !cfg.MatchReference {
		return 1.0
	}

	return calculateReferenceScoreWithOptions(ReferenceMatchOptions{
		CaseInsensitive:  cfg.CaseInsensitive,
		ReferenceMustSet: cfg.ReferenceMustSet,
	}, left, right)
}

func extractToleranceComponents(
	cfg *ToleranceConfig,
	left, right CandidateTransaction,
) ConfidenceComponents {
	components := ConfidenceComponents{}

	if cfg.MatchBaseAmount {
		if left.AmountBase != nil && right.AmountBase != nil {
			components.AmountMatch = isWithinTolerance(
				*left.AmountBase,
				*right.AmountBase,
				cfg.AbsAmountTolerance,
				cfg.PercentTolerance,
				cfg.PercentageBase,
			)
		} else {
			components.AmountMatch = false
		}
	} else {
		components.AmountMatch = isWithinTolerance(left.Amount, right.Amount, cfg.AbsAmountTolerance, cfg.PercentTolerance, cfg.PercentageBase)
	}

	if cfg.MatchBaseCurrency {
		components.CurrencyMatch = left.CurrencyBase != "" &&
			left.CurrencyBase == right.CurrencyBase
	} else if cfg.MatchCurrency {
		components.CurrencyMatch = left.Currency != "" && left.Currency == right.Currency
	} else {
		components.CurrencyMatch = left.Currency != "" && right.Currency != "" && left.Currency == right.Currency
	}

	if cfg.DateWindowDays >= 0 {
		components.DateMatch = AbsDayDiff(left.Date, right.Date) <= cfg.DateWindowDays
	} else {
		components.DateMatch = true
	}

	components.ReferenceScore = evaluateToleranceReferenceScore(cfg, left, right)

	return components
}

// evaluateToleranceReferenceScore calculates reference score for tolerance matching.
// When MatchReference is false (default), returns 0.0 to avoid inflating scores.
// When enabled, uses the shared reference scoring logic.
func evaluateToleranceReferenceScore(
	cfg *ToleranceConfig,
	left, right CandidateTransaction,
) float64 {
	if !cfg.MatchReference {
		return 0.0
	}

	return calculateReferenceScoreWithOptions(ReferenceMatchOptions{
		CaseInsensitive:  cfg.CaseInsensitive,
		ReferenceMustSet: cfg.ReferenceMustSet,
	}, left, right)
}

func extractDateLagComponents(
	cfg *DateLagConfig,
	left, right CandidateTransaction,
) ConfidenceComponents {
	components := ConfidenceComponents{}

	amountWithinTolerance := isWithinFeeTolerance(left.Amount, right.Amount, cfg.FeeTolerance)
	components.AmountMatch = amountWithinTolerance

	if cfg.MatchCurrency {
		components.CurrencyMatch = left.Currency != "" && left.Currency == right.Currency
	} else {
		components.CurrencyMatch = left.Currency != "" && right.Currency != "" && left.Currency == right.Currency
	}

	dayDiff := AbsDayDiff(left.Date, right.Date)
	components.DateMatch = dayDiff >= cfg.MinDays &&
		((cfg.Inclusive && dayDiff <= cfg.MaxDays) || (!cfg.Inclusive && dayDiff < cfg.MaxDays))

	components.ReferenceScore = 0.0

	return components
}

// ReferenceMatchOptions defines options for reference matching used by both
// ExactConfig and ToleranceConfig to avoid code duplication.
type ReferenceMatchOptions struct {
	CaseInsensitive  bool
	ReferenceMustSet bool
}

// calculateReferenceScoreWithOptions computes a 0.0 or 1.0 reference score based on options.
// This is the shared implementation used by both exact and tolerance matching.
func calculateReferenceScoreWithOptions(
	opts ReferenceMatchOptions,
	left, right CandidateTransaction,
) float64 {
	leftRef := left.Reference
	rightRef := right.Reference

	if opts.ReferenceMustSet && (leftRef == "" || rightRef == "") {
		return 0.0
	}

	if leftRef == "" && rightRef == "" {
		return 1.0
	}

	if opts.CaseInsensitive {
		if equalFold(leftRef, rightRef) {
			return 1.0
		}
	} else {
		if leftRef == rightRef {
			return 1.0
		}
	}

	return 0.0
}

// equalFold performs ASCII-only case-insensitive comparison for references.
// This avoids Unicode case folding for predictable, allocation-free behavior.
func equalFold(left, right string) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range len(left) {
		leftChar := left[i]
		rightChar := right[i]

		if leftChar != rightChar {
			if 'A' <= leftChar && leftChar <= 'Z' {
				leftChar += 'a' - 'A'
			}

			if 'A' <= rightChar && rightChar <= 'Z' {
				rightChar += 'a' - 'A'
			}

			if leftChar != rightChar {
				return false
			}
		}
	}

	return true
}

// isWithinTolerance checks if the difference between two amounts is within acceptable tolerance.
func isWithinTolerance(left, right, absTolerance, percentTolerance decimal.Decimal, base TolerancePercentageBase) bool {
	diff := left.Sub(right).Abs()
	threshold := toleranceThreshold(absTolerance, percentTolerance, left, right, base)

	return !diff.GreaterThan(threshold)
}

// isWithinFeeTolerance checks if the difference is within fee tolerance.
func isWithinFeeTolerance(left, right, feeTolerance decimal.Decimal) bool {
	diff := left.Abs().Sub(right.Abs()).Abs()
	return !diff.GreaterThan(feeTolerance)
}
