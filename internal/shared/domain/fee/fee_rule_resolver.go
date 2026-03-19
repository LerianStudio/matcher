package fee

import "github.com/google/uuid"

// ResolveFeeSchedule evaluates fee rules in priority order against a transaction's
// metadata and returns the schedule from the first matching rule.
// Returns nil if no rule matches (no normalization applied).
// The caller must guarantee rules are pre-filtered by side and sorted by Priority ASC.
func ResolveFeeSchedule(
	metadata map[string]any,
	rules []*FeeRule,
	schedules map[uuid.UUID]*FeeSchedule,
) *FeeSchedule {
	for _, rule := range rules {
		if rule == nil {
			continue
		}

		if !allPredicatesMatch(metadata, rule.Predicates) {
			continue
		}

		schedule, ok := schedules[rule.FeeScheduleID]
		if !ok || schedule == nil {
			// Defensive: FK constraint should prevent this. Skip and try next rule.
			continue
		}

		return schedule
	}

	return nil
}

// allPredicatesMatch returns true if all predicates match the metadata (AND semantics).
// An empty predicate slice is a catch-all (always matches).
func allPredicatesMatch(metadata map[string]any, predicates []FieldPredicate) bool {
	for _, pred := range predicates {
		if !pred.Evaluate(metadata) {
			return false
		}
	}

	return true
}

// SplitRulesBySide partitions rules into left-applicable and right-applicable slices.
// LEFT + ANY rules go into leftRules. RIGHT + ANY rules go into rightRules.
// Order within each slice is preserved from the input.
func SplitRulesBySide(rules []*FeeRule) (leftRules, rightRules []*FeeRule) {
	for _, rule := range rules {
		if rule == nil {
			continue
		}

		if rule.Side.AppliesToLeft() {
			leftRules = append(leftRules, rule)
		}

		if rule.Side.AppliesToRight() {
			rightRules = append(rightRules, rule)
		}
	}

	return leftRules, rightRules
}
