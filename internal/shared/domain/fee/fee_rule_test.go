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
