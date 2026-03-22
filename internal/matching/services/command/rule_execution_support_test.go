//go:build unit

package command

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestValidateExecuteRulesInput_NilContextID(t *testing.T) {
	t.Parallel()

	err := validateExecuteRulesInput(context.Background(), nil, nil, uuid.Nil)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestValidateExecuteRulesInput_ValidContextID(t *testing.T) {
	t.Parallel()

	err := validateExecuteRulesInput(context.Background(), nil, nil, uuid.New())
	require.NoError(t, err)
}

func TestLoadRuleDefinitions_ProviderError(t *testing.T) {
	t.Parallel()

	provider := &stubRuleProviderForExec{err: errors.New("provider error")}
	_, err := loadRuleDefinitions(context.Background(), nil, nil, provider, uuid.New())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load match rules")
}

func TestLoadRuleDefinitions_EmptyRules(t *testing.T) {
	t.Parallel()

	provider := &stubRuleProviderForExec{rules: shared.MatchRules{}}
	defs, err := loadRuleDefinitions(context.Background(), nil, nil, provider, uuid.New())
	require.NoError(t, err)
	assert.Empty(t, defs)
}

func TestLoadRuleDefinitions_ValidExactRule(t *testing.T) {
	t.Parallel()

	rule := &shared.MatchRule{
		ID:       uuid.New(),
		Priority: 1,
		Type:     shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":   true,
			"matchCurrency": true,
		},
	}

	provider := &stubRuleProviderForExec{rules: shared.MatchRules{rule}}
	defs, err := loadRuleDefinitions(context.Background(), nil, nil, provider, uuid.New())
	require.NoError(t, err)
	require.Len(t, defs, 1)
}

func TestCountMissingBaseFields_AllPresent(t *testing.T) {
	t.Parallel()

	leftBase := decimal.NewFromInt(100)
	rightBase := decimal.NewFromInt(200)
	left := []matching.CandidateTransaction{
		{AmountBase: &leftBase, CurrencyBase: "USD"},
	}
	right := []matching.CandidateTransaction{
		{AmountBase: &rightBase, CurrencyBase: "EUR"},
	}

	leftAmt, rightAmt, leftCur, rightCur := countMissingBaseFields(left, right)
	assert.Equal(t, 0, leftAmt)
	assert.Equal(t, 0, rightAmt)
	assert.Equal(t, 0, leftCur)
	assert.Equal(t, 0, rightCur)
}

func TestCountMissingBaseFields_AllMissing(t *testing.T) {
	t.Parallel()

	left := []matching.CandidateTransaction{
		{AmountBase: nil, CurrencyBase: ""},
		{AmountBase: nil, CurrencyBase: ""},
	}
	right := []matching.CandidateTransaction{
		{AmountBase: nil, CurrencyBase: ""},
	}

	leftAmt, rightAmt, leftCur, rightCur := countMissingBaseFields(left, right)
	assert.Equal(t, 2, leftAmt)
	assert.Equal(t, 1, rightAmt)
	assert.Equal(t, 2, leftCur)
	assert.Equal(t, 1, rightCur)
}

func TestCountMissingBaseFields_Empty(t *testing.T) {
	t.Parallel()

	leftAmt, rightAmt, leftCur, rightCur := countMissingBaseFields(nil, nil)
	assert.Equal(t, 0, leftAmt)
	assert.Equal(t, 0, rightAmt)
	assert.Equal(t, 0, leftCur)
	assert.Equal(t, 0, rightCur)
}

func TestRequiresBaseMatching_NoRules(t *testing.T) {
	t.Parallel()

	assert.False(t, requiresBaseMatching(nil))
}

func TestRequiresBaseMatching_ToleranceBaseAmount(t *testing.T) {
	t.Parallel()

	defs := []matching.RuleDefinition{
		{Tolerance: &matching.ToleranceRuleConfig{MatchBaseAmount: true}},
	}
	assert.True(t, requiresBaseMatching(defs))
}

func TestRequiresBaseMatching_ToleranceBaseCurrency(t *testing.T) {
	t.Parallel()

	defs := []matching.RuleDefinition{
		{Tolerance: &matching.ToleranceRuleConfig{MatchBaseCurrency: true}},
	}
	assert.True(t, requiresBaseMatching(defs))
}

func TestRequiresBaseMatching_ExactBaseAmount(t *testing.T) {
	t.Parallel()

	defs := []matching.RuleDefinition{
		{Exact: &matching.ExactRuleConfig{MatchBaseAmount: true}},
	}
	assert.True(t, requiresBaseMatching(defs))
}

func TestRequiresBaseMatching_ExactBaseCurrency(t *testing.T) {
	t.Parallel()

	defs := []matching.RuleDefinition{
		{Exact: &matching.ExactRuleConfig{MatchBaseCurrency: true}},
	}
	assert.True(t, requiresBaseMatching(defs))
}

func TestRequiresBaseMatching_AllocationUseBase(t *testing.T) {
	t.Parallel()

	defs := []matching.RuleDefinition{
		{Allocation: &matching.AllocationConfig{UseBaseAmount: true}},
	}
	assert.True(t, requiresBaseMatching(defs))
}

func TestRequiresBaseMatching_NoneRequiresBase(t *testing.T) {
	t.Parallel()

	defs := []matching.RuleDefinition{
		{Exact: &matching.ExactRuleConfig{MatchBaseAmount: false}},
		{Tolerance: &matching.ToleranceRuleConfig{MatchBaseAmount: false}},
	}
	assert.False(t, requiresBaseMatching(defs))
}

func TestSafeRuleID_Nil(t *testing.T) {
	t.Parallel()

	assert.Empty(t, safeRuleID(nil))
}

func TestSafeRuleID_NonNil(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000270001")
	rule := &shared.MatchRule{ID: id}
	assert.Equal(t, id.String(), safeRuleID(rule))
}

func TestExecuteByContextTypeDetailed_NilEngine(t *testing.T) {
	t.Parallel()

	_, err := executeByContextTypeDetailed(nil, nil, nil, nil, shared.ContextTypeOneToOne)
	require.ErrorIs(t, err, ErrEngineIsNil)
}

func TestExecuteByContextTypeDetailed_ManyToMany_Unsupported(t *testing.T) {
	t.Parallel()

	engine := matching.NewEngine()
	_, err := executeByContextTypeDetailed(engine, nil, nil, nil, shared.ContextTypeManyToMany)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsupportedContextType)
}

func TestExecuteByContextTypeDetailed_UnknownType_Unsupported(t *testing.T) {
	t.Parallel()

	engine := matching.NewEngine()
	_, err := executeByContextTypeDetailed(engine, nil, nil, nil, shared.ContextType("weird"))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsupportedContextType)
}

func TestExecuteByContextTypeDetailed_OneToOne_EmptyInput(t *testing.T) {
	t.Parallel()

	engine := matching.NewEngine()
	result, err := executeByContextTypeDetailed(engine, nil, nil, nil, shared.ContextTypeOneToOne)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Proposals)
}

func TestExecuteByContextTypeDetailed_EmptyString_FallsToOneToOne(t *testing.T) {
	t.Parallel()

	engine := matching.NewEngine()
	result, err := executeByContextTypeDetailed(engine, nil, nil, nil, "")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Proposals)
}

// --- stubs local to this file ---

type stubRuleProviderForExec struct {
	rules shared.MatchRules
	err   error
}

func (s *stubRuleProviderForExec) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
) (shared.MatchRules, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.rules, nil
}
