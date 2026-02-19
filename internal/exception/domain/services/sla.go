// Package services provides domain services for exception handling.
package services

import (
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// SLA service errors.
var (
	ErrEmptySLARules     = errors.New("sla rules are required")
	ErrInvalidSLARule    = errors.New("invalid sla rule")
	ErrNoMatchingSLARule = errors.New("no matching sla rule")
)

// SLAInput contains input values for SLA computation.
type SLAInput struct {
	AmountAbsBase decimal.Decimal
	AgeHours      int
	ReferenceTime time.Time
}

// SLARule defines an SLA rule with thresholds and due duration.
type SLARule struct {
	Name             string
	MinAmountAbsBase *decimal.Decimal
	MinAgeHours      *int
	DueIn            time.Duration
}

// SLAResult contains the computed SLA due date and matched rule.
type SLAResult struct {
	DueAt     time.Time
	RuleName  string
	RuleIndex int
}

// PRD-defined SLA thresholds.
const (
	slaCriticalAmountThreshold = 100000
	slaCriticalAgeHours        = 120
	slaHighAmountThreshold     = 10000
	slaHighAgeHours            = 72
	slaMediumAmountThreshold   = 1000
	slaMediumAgeHours          = 24

	slaCriticalDueHours = 24
	slaHighDueHours     = 72
	slaMediumDueHours   = 120
	slaLowDueHours      = 168
)

// DefaultSLARules returns the PRD-defined SLA rules in descending severity order.
//
// Because SLA uses OR logic (see matchesSLARule), each rule's age threshold alone
// can trigger that severity level. For example, any exception older than 120 hours
// (5 days) escalates to CRITICAL even if the amount is trivial. This ensures stale
// exceptions are surfaced for urgent attention regardless of monetary value.
//
// Rules are evaluated in order: CRITICAL, HIGH, MEDIUM, LOW. The first matching
// rule determines the SLA. The LOW rule is a catch-all with no thresholds.
//
// See ExampleDefaultSLARules, ExampleComputeSLADueAt_severityClassification,
// and ExampleComputeSLADueAt_dueDateCalculation in sla_example_test.go for
// runnable usage demonstrations.
func DefaultSLARules() []SLARule {
	criticalAmount := decimal.NewFromInt(slaCriticalAmountThreshold)
	highAmount := decimal.NewFromInt(slaHighAmountThreshold)
	mediumAmount := decimal.NewFromInt(slaMediumAmountThreshold)

	criticalAge := slaCriticalAgeHours
	highAge := slaHighAgeHours
	mediumAge := slaMediumAgeHours

	return []SLARule{
		{
			Name:             "CRITICAL",
			MinAmountAbsBase: &criticalAmount,
			MinAgeHours:      &criticalAge,
			DueIn:            time.Duration(slaCriticalDueHours) * time.Hour,
		},
		{
			Name:             "HIGH",
			MinAmountAbsBase: &highAmount,
			MinAgeHours:      &highAge,
			DueIn:            time.Duration(slaHighDueHours) * time.Hour,
		},
		{
			Name:             "MEDIUM",
			MinAmountAbsBase: &mediumAmount,
			MinAgeHours:      &mediumAge,
			DueIn:            time.Duration(slaMediumDueHours) * time.Hour,
		},
		{
			Name:  "LOW",
			DueIn: time.Duration(slaLowDueHours) * time.Hour,
		},
	}
}

// ComputeSLADueAt computes the SLA due date based on input and rules.
//
// SLA uses OR logic: a rule matches if EITHER amount OR age threshold is met.
// This differs from routing which uses AND logic (all selectors must match).
// The difference is intentional:
//   - SLA: Lenient matching - any high-risk indicator triggers faster SLA
//   - Routing: Precise matching for specific exception profiles
func ComputeSLADueAt(input SLAInput, rules []SLARule) (SLAResult, error) {
	if len(rules) == 0 {
		return SLAResult{}, ErrEmptySLARules
	}

	for index, rule := range rules {
		if err := validateSLARule(rule, index); err != nil {
			return SLAResult{}, err
		}
	}

	normalized := normalizeSLAInput(input)

	for index, rule := range rules {
		if matchesSLARule(normalized, rule) {
			return SLAResult{
				DueAt:     normalized.ReferenceTime.Add(rule.DueIn),
				RuleName:  rule.Name,
				RuleIndex: index,
			}, nil
		}
	}

	return SLAResult{}, ErrNoMatchingSLARule
}

func normalizeSLAInput(input SLAInput) SLAInput {
	normalized := input
	normalized.AmountAbsBase = input.AmountAbsBase.Abs()

	if normalized.AgeHours < 0 {
		normalized.AgeHours = 0
	}

	if normalized.ReferenceTime.IsZero() {
		normalized.ReferenceTime = time.Now().UTC()
	} else {
		normalized.ReferenceTime = normalized.ReferenceTime.UTC()
	}

	return normalized
}

func validateSLARule(rule SLARule, index int) error {
	if rule.DueIn <= 0 {
		return fmt.Errorf("%w: rule %d due duration", ErrInvalidSLARule, index)
	}

	if rule.MinAmountAbsBase != nil && rule.MinAmountAbsBase.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: rule %d min amount", ErrInvalidSLARule, index)
	}

	if rule.MinAgeHours != nil && *rule.MinAgeHours < 0 {
		return fmt.Errorf("%w: rule %d min age", ErrInvalidSLARule, index)
	}

	return nil
}

// matchesSLARule checks whether a given input matches an SLA rule using OR logic.
//
// NOTE: With OR logic, any exception older than slaCriticalAgeHours (120h = 5 days)
// receives CRITICAL SLA regardless of amount. This is intentional -- stale exceptions
// indicate process failures that need urgent attention. To tune this behavior,
// customize the rules via NewSLAEvaluator with domain-specific thresholds.
func matchesSLARule(input SLAInput, rule SLARule) bool {
	if rule.MinAmountAbsBase == nil && rule.MinAgeHours == nil {
		return true
	}

	amountMatch := rule.MinAmountAbsBase != nil &&
		!input.AmountAbsBase.LessThan(*rule.MinAmountAbsBase)
	ageMatch := rule.MinAgeHours != nil && input.AgeHours >= *rule.MinAgeHours

	return amountMatch || ageMatch
}
