//go:build unit

package fee

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestResolveFeeSchedule_FirstMatchWins(t *testing.T) {
	t.Parallel()

	scheduleA := &FeeSchedule{ID: uuid.New(), Name: "A"}
	scheduleB := &FeeSchedule{ID: uuid.New(), Name: "B"}

	rules := []*FeeRule{
		{Priority: 0, FeeScheduleID: scheduleA.ID, Predicates: []FieldPredicate{
			{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
		}},
		{Priority: 1, FeeScheduleID: scheduleB.ID, Predicates: []FieldPredicate{
			{Field: "institution", Operator: PredicateOperatorEquals, Value: "Santander"},
		}},
	}

	schedules := map[uuid.UUID]*FeeSchedule{
		scheduleA.ID: scheduleA,
		scheduleB.ID: scheduleB,
	}

	result := ResolveFeeSchedule(map[string]any{"institution": "Itau"}, rules, schedules)
	assert.Equal(t, scheduleA, result)
}

func TestResolveFeeSchedule_SecondRuleMatches(t *testing.T) {
	t.Parallel()

	scheduleA := &FeeSchedule{ID: uuid.New(), Name: "A"}
	scheduleB := &FeeSchedule{ID: uuid.New(), Name: "B"}

	rules := []*FeeRule{
		{Priority: 0, FeeScheduleID: scheduleA.ID, Predicates: []FieldPredicate{
			{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
		}},
		{Priority: 1, FeeScheduleID: scheduleB.ID, Predicates: []FieldPredicate{
			{Field: "institution", Operator: PredicateOperatorEquals, Value: "Santander"},
		}},
	}

	schedules := map[uuid.UUID]*FeeSchedule{
		scheduleA.ID: scheduleA,
		scheduleB.ID: scheduleB,
	}

	result := ResolveFeeSchedule(map[string]any{"institution": "Santander"}, rules, schedules)
	assert.Equal(t, scheduleB, result)
}

func TestResolveFeeSchedule_NoMatch_ReturnsNil(t *testing.T) {
	t.Parallel()

	schedule := &FeeSchedule{ID: uuid.New()}

	rules := []*FeeRule{
		{Priority: 0, FeeScheduleID: schedule.ID, Predicates: []FieldPredicate{
			{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
		}},
	}

	schedules := map[uuid.UUID]*FeeSchedule{schedule.ID: schedule}

	result := ResolveFeeSchedule(map[string]any{"institution": "Santander"}, rules, schedules)
	assert.Nil(t, result)
}

func TestResolveFeeSchedule_EmptyPredicates_CatchAll(t *testing.T) {
	t.Parallel()

	schedule := &FeeSchedule{ID: uuid.New(), Name: "Default"}

	rules := []*FeeRule{
		{Priority: 99, FeeScheduleID: schedule.ID, Predicates: nil},
	}

	schedules := map[uuid.UUID]*FeeSchedule{schedule.ID: schedule}

	result := ResolveFeeSchedule(map[string]any{"anything": "value"}, rules, schedules)
	assert.Equal(t, schedule, result)
}

func TestResolveFeeSchedule_EmptyRules_ReturnsNil(t *testing.T) {
	t.Parallel()

	result := ResolveFeeSchedule(map[string]any{"x": "y"}, nil, nil)
	assert.Nil(t, result)
}

func TestResolveFeeSchedule_ScheduleNotInMap_SkipsRule(t *testing.T) {
	t.Parallel()

	missingID := uuid.New()

	rules := []*FeeRule{
		{Priority: 0, FeeScheduleID: missingID, Predicates: nil},
	}

	result := ResolveFeeSchedule(map[string]any{}, rules, map[uuid.UUID]*FeeSchedule{})
	assert.Nil(t, result)
}

func TestResolveFeeSchedule_ANDSemantics_AllPredicatesMustMatch(t *testing.T) {
	t.Parallel()

	schedule := &FeeSchedule{ID: uuid.New()}

	rules := []*FeeRule{
		{Priority: 0, FeeScheduleID: schedule.ID, Predicates: []FieldPredicate{
			{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
			{Field: "card_brand", Operator: PredicateOperatorEquals, Value: "Visa"},
		}},
	}

	schedules := map[uuid.UUID]*FeeSchedule{schedule.ID: schedule}

	// Only one predicate matches.
	result := ResolveFeeSchedule(map[string]any{"institution": "Itau", "card_brand": "Mastercard"}, rules, schedules)
	assert.Nil(t, result)

	// Both match.
	result = ResolveFeeSchedule(map[string]any{"institution": "Itau", "card_brand": "Visa"}, rules, schedules)
	assert.Equal(t, schedule, result)
}

func TestSplitRulesBySide(t *testing.T) {
	t.Parallel()

	leftRule := &FeeRule{Side: MatchingSideLeft, Priority: 0}
	rightRule := &FeeRule{Side: MatchingSideRight, Priority: 1}
	anyRule := &FeeRule{Side: MatchingSideAny, Priority: 2}

	all := []*FeeRule{leftRule, rightRule, anyRule}

	leftRules, rightRules := SplitRulesBySide(all)

	assert.Equal(t, []*FeeRule{leftRule, anyRule}, leftRules)
	assert.Equal(t, []*FeeRule{rightRule, anyRule}, rightRules)
}

func TestSplitRulesBySide_NilInput(t *testing.T) {
	t.Parallel()

	left, right := SplitRulesBySide(nil)
	assert.Empty(t, left)
	assert.Empty(t, right)
}
