package services

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

// Routing service errors.
var (
	ErrEmptyRoutingRules      = errors.New("routing rules are required")
	ErrInvalidRoutingRule     = errors.New("invalid routing rule")
	ErrNoMatchingRoutingRule  = errors.New("no matching routing rule")
	ErrOverrideReasonRequired = errors.New("override reason is required")
)

// RoutingTarget represents the target system for routing exceptions.
type RoutingTarget string

// RoutingTarget values.
const (
	RoutingTargetManual     RoutingTarget = "MANUAL"
	RoutingTargetJira       RoutingTarget = "JIRA"
	RoutingTargetServiceNow RoutingTarget = "SERVICENOW"
	RoutingTargetWebhook    RoutingTarget = "WEBHOOK"
)

// IsValid checks if the routing target is valid.
func (target RoutingTarget) IsValid() bool {
	switch target {
	case RoutingTargetManual, RoutingTargetJira, RoutingTargetServiceNow, RoutingTargetWebhook:
		return true
	default:
		return false
	}
}

// RoutingInput contains input values for routing evaluation.
type RoutingInput struct {
	Severity      sharedexception.ExceptionSeverity
	AmountAbsBase decimal.Decimal
	AgeHours      int
	SourceType    string
	Reason        string
}

// RoutingRule defines a routing rule with selectors and target configuration.
type RoutingRule struct {
	Name             string
	Priority         int
	MatchAll         bool
	Severities       []sharedexception.ExceptionSeverity
	MinAmountAbsBase *decimal.Decimal
	MinAgeHours      *int
	SourceTypes      []string
	Reasons          []string
	Target           RoutingTarget
	Queue            string
	Assignee         string
	OverrideSeverity *sharedexception.ExceptionSeverity
	OverrideReason   *value_objects.OverrideReason
}

// RoutingDecision contains the routing result with target and rule information.
type RoutingDecision struct {
	Target           RoutingTarget
	Queue            string
	Assignee         string
	OverrideSeverity *sharedexception.ExceptionSeverity
	OverrideReason   *value_objects.OverrideReason
	RuleName         string
	RuleIndex        int
}

// SortRoutingRules sorts rules by priority (ascending) then by name.
func SortRoutingRules(rules []RoutingRule) {
	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority < rules[j].Priority
		}

		return rules[i].Name < rules[j].Name
	})
}

// EvaluateRouting evaluates routing rules and returns the first matching decision.
//
// Routing uses AND logic: ALL specified selectors must match for a rule to fire.
// This differs from SLA evaluation which uses OR logic (any threshold triggers).
// The difference is intentional:
//   - Routing: Precise matching for specific exception profiles (e.g., "high severity AND >$10k")
//   - SLA: Lenient matching where any risk indicator triggers faster response
func EvaluateRouting(input RoutingInput, rules []RoutingRule) (RoutingDecision, error) {
	if len(rules) == 0 {
		return RoutingDecision{}, ErrEmptyRoutingRules
	}

	SortRoutingRules(rules)

	normalized := normalizeRoutingInput(input)

	for index, rule := range rules {
		if err := validateRoutingRule(rule, index); err != nil {
			return RoutingDecision{}, err
		}

		if matchesRoutingRule(normalized, rule) {
			return RoutingDecision{
				Target:           rule.Target,
				Queue:            strings.TrimSpace(rule.Queue),
				Assignee:         strings.TrimSpace(rule.Assignee),
				OverrideSeverity: rule.OverrideSeverity,
				OverrideReason:   rule.OverrideReason,
				RuleName:         rule.Name,
				RuleIndex:        index,
			}, nil
		}
	}

	return RoutingDecision{}, ErrNoMatchingRoutingRule
}

func normalizeRoutingInput(input RoutingInput) RoutingInput {
	normalized := input
	normalized.AmountAbsBase = input.AmountAbsBase.Abs()

	if normalized.AgeHours < 0 {
		normalized.AgeHours = 0
	}

	normalized.SourceType = strings.TrimSpace(input.SourceType)
	normalized.Reason = strings.TrimSpace(input.Reason)

	return normalized
}

func validateRoutingRule(rule RoutingRule, index int) error {
	if err := validateRoutingRuleTarget(rule, index); err != nil {
		return err
	}

	if err := validateRoutingRuleOverride(rule, index); err != nil {
		return err
	}

	if err := validateRoutingRuleThresholds(rule, index); err != nil {
		return err
	}

	return validateRoutingRuleSelectors(rule, index)
}

func validateRoutingRuleTarget(rule RoutingRule, index int) error {
	if !rule.Target.IsValid() {
		return fmt.Errorf("%w: rule %d target", ErrInvalidRoutingRule, index)
	}

	return nil
}

func validateRoutingRuleOverride(rule RoutingRule, index int) error {
	if rule.OverrideSeverity != nil && !rule.OverrideSeverity.IsValid() {
		return fmt.Errorf("%w: rule %d override severity", ErrInvalidRoutingRule, index)
	}

	if rule.OverrideSeverity != nil {
		if rule.OverrideReason == nil || !rule.OverrideReason.IsValid() {
			return fmt.Errorf("%w: rule %d", ErrOverrideReasonRequired, index)
		}
	}

	return nil
}

func validateRoutingRuleThresholds(rule RoutingRule, index int) error {
	if rule.MinAmountAbsBase != nil && rule.MinAmountAbsBase.LessThan(decimal.Zero) {
		return fmt.Errorf("%w: rule %d min amount", ErrInvalidRoutingRule, index)
	}

	if rule.MinAgeHours != nil && *rule.MinAgeHours < 0 {
		return fmt.Errorf("%w: rule %d min age", ErrInvalidRoutingRule, index)
	}

	return nil
}

func validateRoutingRuleSelectors(rule RoutingRule, index int) error {
	hasSelectors := rule.MatchAll ||
		len(rule.Severities) > 0 ||
		rule.MinAmountAbsBase != nil ||
		rule.MinAgeHours != nil ||
		len(rule.SourceTypes) > 0 ||
		len(rule.Reasons) > 0

	if !hasSelectors {
		return fmt.Errorf("%w: rule %d selectors", ErrInvalidRoutingRule, index)
	}

	for _, sev := range rule.Severities {
		if !sev.IsValid() {
			return fmt.Errorf("%w: rule %d severity", ErrInvalidRoutingRule, index)
		}
	}

	return nil
}

func matchesRoutingRule(input RoutingInput, rule RoutingRule) bool {
	if rule.MatchAll {
		return true
	}

	if len(rule.Severities) > 0 && !containsSeverity(rule.Severities, input.Severity) {
		return false
	}

	if rule.MinAmountAbsBase != nil && input.AmountAbsBase.LessThan(*rule.MinAmountAbsBase) {
		return false
	}

	if rule.MinAgeHours != nil && input.AgeHours < *rule.MinAgeHours {
		return false
	}

	if len(rule.SourceTypes) > 0 && !containsString(rule.SourceTypes, input.SourceType) {
		return false
	}

	if len(rule.Reasons) > 0 && !containsString(rule.Reasons, input.Reason) {
		return false
	}

	return true
}

func containsSeverity(
	values []sharedexception.ExceptionSeverity,
	candidate sharedexception.ExceptionSeverity,
) bool {
	return slices.Contains(values, candidate)
}

func containsString(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), candidate) {
			return true
		}
	}

	return false
}
