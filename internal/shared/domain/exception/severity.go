// Package exception provides shared exception domain types used across bounded contexts.
package exception

import (
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

var (
	// ErrInvalidExceptionSeverity is returned when parsing an invalid severity.
	ErrInvalidExceptionSeverity = errors.New("invalid exception severity")
	// ErrEmptySeverityRules indicates the severity rules list is empty.
	ErrEmptySeverityRules = errors.New("severity rules are required")
)

// ExceptionSeverity represents exception priority.
type ExceptionSeverity string

// ExceptionSeverity values.
const (
	ExceptionSeverityLow      ExceptionSeverity = "LOW"
	ExceptionSeverityMedium   ExceptionSeverity = "MEDIUM"
	ExceptionSeverityHigh     ExceptionSeverity = "HIGH"
	ExceptionSeverityCritical ExceptionSeverity = "CRITICAL"
)

// IsValid checks if the severity is valid.
func (severity ExceptionSeverity) IsValid() bool {
	switch severity {
	case ExceptionSeverityLow,
		ExceptionSeverityMedium,
		ExceptionSeverityHigh,
		ExceptionSeverityCritical:
		return true
	default:
		return false
	}
}

// String returns the string representation of the severity.
func (severity ExceptionSeverity) String() string {
	return string(severity)
}

// ParseExceptionSeverity parses a string into an ExceptionSeverity.
// Input is normalized to uppercase for case-insensitive parsing.
func ParseExceptionSeverity(value string) (ExceptionSeverity, error) {
	sev := ExceptionSeverity(strings.ToUpper(strings.TrimSpace(value)))
	if !sev.IsValid() {
		return "", ErrInvalidExceptionSeverity
	}

	return sev, nil
}

// ReasonFXRateUnavailable flags missing FX rates used during severity classification.
const ReasonFXRateUnavailable = "FX_RATE_UNAVAILABLE"

// SeverityClassificationInput contains normalized values for classification.
type SeverityClassificationInput struct {
	AmountAbsBase decimal.Decimal
	AgeHours      int
	SourceType    string
	FXMissing     bool
}

// SeverityRule defines ordered thresholds for severity classification.
type SeverityRule struct {
	Severity              ExceptionSeverity
	MinAmountAbsBase      *decimal.Decimal
	MinAgeHours           *int
	RegulatorySourceTypes []string
}

// SeverityClassificationResult is returned from classification.
type SeverityClassificationResult struct {
	Severity ExceptionSeverity
	Reasons  []string
}

// PRD-defined severity thresholds (AC-002).
const (
	criticalAmountThreshold = 100000
	criticalAgeHours        = 120
	highAmountThreshold     = 10000
	highAgeHours            = 72
	mediumAmountThreshold   = 1000
	mediumAgeHours          = 24
)

// DefaultSeverityRules returns the PRD default rules in descending severity.
func DefaultSeverityRules(regulatorySourceTypes []string) []SeverityRule {
	criticalAmount := decimal.NewFromInt(criticalAmountThreshold)
	criticalAge := criticalAgeHours
	highAmount := decimal.NewFromInt(highAmountThreshold)
	highAge := highAgeHours
	mediumAmount := decimal.NewFromInt(mediumAmountThreshold)
	mediumAge := mediumAgeHours

	return []SeverityRule{
		{
			Severity:              ExceptionSeverityCritical,
			MinAmountAbsBase:      &criticalAmount,
			MinAgeHours:           &criticalAge,
			RegulatorySourceTypes: cloneStringSlice(regulatorySourceTypes),
		},
		{
			Severity:         ExceptionSeverityHigh,
			MinAmountAbsBase: &highAmount,
			MinAgeHours:      &highAge,
		},
		{
			Severity:         ExceptionSeverityMedium,
			MinAmountAbsBase: &mediumAmount,
			MinAgeHours:      &mediumAge,
		},
		{
			Severity: ExceptionSeverityLow,
		},
	}
}

// ClassifyExceptionSeverity evaluates ordered rules and returns the first match.
func ClassifyExceptionSeverity(
	input SeverityClassificationInput,
	rules []SeverityRule,
) (SeverityClassificationResult, error) {
	if len(rules) == 0 {
		return SeverityClassificationResult{}, ErrEmptySeverityRules
	}

	normalized := normalizeSeverityInput(input)

	reasons := make([]string, 0, 1)
	if normalized.FXMissing {
		reasons = append(reasons, ReasonFXRateUnavailable)
	}

	for index, rule := range rules {
		if !rule.Severity.IsValid() {
			return SeverityClassificationResult{}, fmt.Errorf(
				"%w: rule %d",
				ErrInvalidExceptionSeverity,
				index,
			)
		}

		if matchesSeverityRule(normalized, rule) {
			return SeverityClassificationResult{
				Severity: rule.Severity,
				Reasons:  reasons,
			}, nil
		}
	}

	return SeverityClassificationResult{
		Severity: ExceptionSeverityLow,
		Reasons:  reasons,
	}, nil
}

func normalizeSeverityInput(input SeverityClassificationInput) SeverityClassificationInput {
	normalized := input

	normalized.AmountAbsBase = input.AmountAbsBase.Abs()

	if normalized.AgeHours < 0 {
		normalized.AgeHours = 0
	}

	normalized.SourceType = strings.TrimSpace(input.SourceType)

	return normalized
}

func matchesSeverityRule(input SeverityClassificationInput, rule SeverityRule) bool {
	if rule.MinAmountAbsBase == nil && rule.MinAgeHours == nil &&
		len(rule.RegulatorySourceTypes) == 0 {
		return true
	}

	amountMatch := rule.MinAmountAbsBase != nil &&
		!input.AmountAbsBase.LessThan(*rule.MinAmountAbsBase)
	ageMatch := rule.MinAgeHours != nil && input.AgeHours >= *rule.MinAgeHours
	sourceMatch := len(rule.RegulatorySourceTypes) > 0 &&
		isSourceTypeAllowed(input.SourceType, rule.RegulatorySourceTypes)

	return amountMatch || ageMatch || sourceMatch
}

func isSourceTypeAllowed(sourceType string, allowed []string) bool {
	if sourceType == "" {
		return false
	}

	for _, candidate := range allowed {
		if strings.EqualFold(sourceType, strings.TrimSpace(candidate)) {
			return true
		}
	}

	return false
}

func cloneStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	copied := make([]string, len(values))
	copy(copied, values)

	return copied
}
