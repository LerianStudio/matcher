//go:build unit

package fee

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFeeRule_ValidInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	scheduleID := uuid.New()

	rule, err := NewFeeRule(ctx, contextID, scheduleID, MatchingSideLeft, "Test Rule", 0, []FieldPredicate{
		{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
	})

	require.NoError(t, err)
	require.NotNil(t, rule)
	assert.Equal(t, contextID, rule.ContextID)
	assert.Equal(t, scheduleID, rule.FeeScheduleID)
	assert.Equal(t, MatchingSideLeft, rule.Side)
	assert.Equal(t, "Test Rule", rule.Name)
	assert.Equal(t, 0, rule.Priority)
	assert.Len(t, rule.Predicates, 1)
	assert.NotEqual(t, uuid.Nil, rule.ID)
	assert.False(t, rule.CreatedAt.IsZero())
	assert.False(t, rule.UpdatedAt.IsZero())
}

func TestNewFeeRule_EmptyPredicates_CatchAll(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	rule, err := NewFeeRule(ctx, uuid.New(), uuid.New(), MatchingSideAny, "Catch All", 99, nil)
	require.NoError(t, err)
	require.NotNil(t, rule)
	assert.Empty(t, rule.Predicates)
}

func TestNewFeeRule_NilContextID(t *testing.T) {
	t.Parallel()

	_, err := NewFeeRule(context.Background(), uuid.Nil, uuid.New(), MatchingSideAny, "X", 0, nil)
	require.ErrorIs(t, err, ErrFeeRuleContextIDRequired)
}

func TestNewFeeRule_NilScheduleID(t *testing.T) {
	t.Parallel()

	_, err := NewFeeRule(context.Background(), uuid.New(), uuid.Nil, MatchingSideAny, "X", 0, nil)
	require.ErrorIs(t, err, ErrFeeRuleScheduleIDRequired)
}

func TestNewFeeRule_InvalidSide(t *testing.T) {
	t.Parallel()

	_, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSide("BOTH"), "X", 0, nil)
	require.ErrorIs(t, err, ErrInvalidMatchingSide)
}

func TestNewFeeRule_EmptyName(t *testing.T) {
	t.Parallel()

	_, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "", 0, nil)
	require.ErrorIs(t, err, ErrFeeRuleNameRequired)
}

func TestNewFeeRule_WhitespaceOnlyName(t *testing.T) {
	t.Parallel()

	_, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "   ", 0, nil)
	require.ErrorIs(t, err, ErrFeeRuleNameRequired)
}

func TestNewFeeRule_NameTooLong(t *testing.T) {
	t.Parallel()

	longName := strings.Repeat("a", 101)

	_, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, longName, 0, nil)
	require.ErrorIs(t, err, ErrFeeRuleNameTooLong)
}

func TestNewFeeRule_NegativePriority(t *testing.T) {
	t.Parallel()

	_, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "X", -1, nil)
	require.ErrorIs(t, err, ErrFeeRulePriorityNegative)
}

func TestNewFeeRule_InvalidPredicate(t *testing.T) {
	t.Parallel()

	_, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "X", 0, []FieldPredicate{
		{Field: "", Operator: PredicateOperatorEquals, Value: "x"},
	})
	require.ErrorIs(t, err, ErrPredicateFieldRequired)
}

func TestNewFeeRule_TrimsNameAndPredicateField(t *testing.T) {
	t.Parallel()

	rule, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "  trimmed  ", 0, []FieldPredicate{{
		Field:    " institution ",
		Operator: PredicateOperatorEquals,
		Value:    "x",
	}})
	require.NoError(t, err)
	assert.Equal(t, "trimmed", rule.Name)
	assert.Equal(t, "institution", rule.Predicates[0].Field)
}

func TestNewFeeRule_NameExactlyMaxLength(t *testing.T) {
	t.Parallel()

	name := strings.Repeat("a", 100)

	rule, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, name, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, rule)
	assert.Equal(t, name, rule.Name)
}

func TestFeeRule_Update_NoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	rule, err := NewFeeRule(ctx, uuid.New(), uuid.New(), MatchingSideLeft, "original", 5, []FieldPredicate{
		{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"},
	})
	require.NoError(t, err)

	originalContextID := rule.ContextID
	originalFeeScheduleID := rule.FeeScheduleID
	originalSide := rule.Side
	originalName := rule.Name
	originalPriority := rule.Priority
	originalPredicates := rule.Predicates
	originalCreatedAt := rule.CreatedAt
	originalUpdatedAt := rule.UpdatedAt

	err = rule.Update(ctx, UpdateFeeRuleInput{})
	require.NoError(t, err)

	assert.Equal(t, originalContextID, rule.ContextID)
	assert.Equal(t, originalFeeScheduleID, rule.FeeScheduleID)
	assert.Equal(t, originalSide, rule.Side)
	assert.Equal(t, originalName, rule.Name)
	assert.Equal(t, originalPriority, rule.Priority)
	assert.Equal(t, originalPredicates, rule.Predicates)
	assert.Equal(t, originalCreatedAt, rule.CreatedAt)
	assert.False(t, rule.UpdatedAt.Before(originalUpdatedAt))
}

func TestFeeRule_Update_Success(t *testing.T) {
	t.Parallel()

	rule, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "original", 1, nil)
	require.NoError(t, err)

	newSide := "LEFT"
	newScheduleID := uuid.New().String()
	newName := "updated"
	newPriority := 2
	newPredicates := []FieldPredicate{{Field: "institution", Operator: PredicateOperatorEquals, Value: "Itau"}}

	oldUpdatedAt := rule.UpdatedAt

	err = rule.Update(context.Background(), UpdateFeeRuleInput{
		Side:          &newSide,
		FeeScheduleID: &newScheduleID,
		Name:          &newName,
		Priority:      &newPriority,
		Predicates:    &newPredicates,
	})
	require.NoError(t, err)
	assert.Equal(t, MatchingSideLeft, rule.Side)
	assert.Equal(t, newName, rule.Name)
	assert.Equal(t, newPriority, rule.Priority)
	assert.Equal(t, uuid.MustParse(newScheduleID), rule.FeeScheduleID)
	assert.Len(t, rule.Predicates, 1)
	assert.False(t, rule.UpdatedAt.Before(oldUpdatedAt))
}

func TestFeeRule_Update_InvalidFeeScheduleID(t *testing.T) {
	t.Parallel()

	rule, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "original", 1, nil)
	require.NoError(t, err)

	invalidScheduleID := "not-a-uuid"
	err = rule.Update(context.Background(), UpdateFeeRuleInput{FeeScheduleID: &invalidScheduleID})
	require.ErrorIs(t, err, ErrFeeRuleScheduleIDRequired)
}

func TestFeeRule_Update_TooManyPredicates(t *testing.T) {
	t.Parallel()

	rule, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "original", 1, nil)
	require.NoError(t, err)

	predicates := make([]FieldPredicate, 0, 51)
	for i := 0; i < 51; i++ {
		predicates = append(predicates, FieldPredicate{Field: "institution", Operator: PredicateOperatorExists})
	}

	err = rule.Update(context.Background(), UpdateFeeRuleInput{Predicates: &predicates})
	require.ErrorIs(t, err, ErrFeeRuleTooManyPredicates)
}

func TestFeeRule_Update_InvalidPredicate(t *testing.T) {
	t.Parallel()

	rule, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "original", 1, nil)
	require.NoError(t, err)

	predicates := []FieldPredicate{{Field: "", Operator: PredicateOperatorEquals, Value: "x"}}
	err = rule.Update(context.Background(), UpdateFeeRuleInput{Predicates: &predicates})
	require.ErrorIs(t, err, ErrPredicateFieldRequired)
}

func TestFeeRule_Update_WhitespaceOnlyName(t *testing.T) {
	t.Parallel()

	rule, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "original", 1, nil)
	require.NoError(t, err)

	name := "   "
	err = rule.Update(context.Background(), UpdateFeeRuleInput{Name: &name})
	require.ErrorIs(t, err, ErrFeeRuleNameRequired)
}

func TestFeeRule_Update_TrimsNameAndPredicateField(t *testing.T) {
	t.Parallel()

	rule, err := NewFeeRule(context.Background(), uuid.New(), uuid.New(), MatchingSideAny, "original", 1, nil)
	require.NoError(t, err)

	name := "  updated  "
	predicates := []FieldPredicate{{Field: " institution ", Operator: PredicateOperatorExists}}
	err = rule.Update(context.Background(), UpdateFeeRuleInput{Name: &name, Predicates: &predicates})
	require.NoError(t, err)
	assert.Equal(t, "updated", rule.Name)
	assert.Equal(t, "institution", rule.Predicates[0].Field)
}
