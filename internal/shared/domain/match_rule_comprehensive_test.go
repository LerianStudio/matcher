//go:build unit

package shared

import (
	"context"
	"sort"
	"testing"

	"github.com/LerianStudio/matcher/internal/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuleType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ruleType RuleType
		want     string
	}{
		{"exact", RuleTypeExact, "EXACT"},
		{"tolerance", RuleTypeTolerance, "TOLERANCE"},
		{"date_lag", RuleTypeDateLag, "DATE_LAG"},
		{"empty", RuleType(""), ""},
		{"invalid", RuleType("INVALID"), "INVALID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.ruleType.String())
		})
	}
}

func TestRuleType_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ruleType RuleType
		want     bool
	}{
		{"exact is valid", RuleTypeExact, true},
		{"tolerance is valid", RuleTypeTolerance, true},
		{"date_lag is valid", RuleTypeDateLag, true},
		{"empty is invalid", RuleType(""), false},
		{"lowercase is invalid", RuleType("exact"), false},
		{"unknown is invalid", RuleType("UNKNOWN"), false},
		{"partial match is invalid", RuleType("EXAC"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.ruleType.Valid())
		})
	}
}

func TestRuleType_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ruleType RuleType
		want     bool
	}{
		{"exact", RuleTypeExact, true},
		{"tolerance", RuleTypeTolerance, true},
		{"date_lag", RuleTypeDateLag, true},
		{"empty", RuleType(""), false},
		{"invalid", RuleType("INVALID"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.ruleType.IsValid())
			assert.Equal(t, tt.ruleType.Valid(), tt.ruleType.IsValid())
		})
	}
}

func TestParseRuleType_AllValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  RuleType
	}{
		{"EXACT", RuleTypeExact},
		{"exact", RuleTypeExact},
		{" EXACT ", RuleTypeExact},
		{"Exact", RuleTypeExact},
		{"TOLERANCE", RuleTypeTolerance},
		{"tolerance", RuleTypeTolerance},
		{" tolerance ", RuleTypeTolerance},
		{"DATE_LAG", RuleTypeDateLag},
		{"date_lag", RuleTypeDateLag},
		{" Date_Lag ", RuleTypeDateLag},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got, err := ParseRuleType(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseRuleType_Invalid(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		" ",
		"invalid",
		"INVALID",
		"EXAC",
		"EXACT_MATCH",
		"123",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, err := ParseRuleType(input)
			require.ErrorIs(t, err, ErrInvalidRuleType)
		})
	}
}

func TestContextType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contextType ContextType
		want        string
	}{
		{"one_to_one", ContextTypeOneToOne, "1:1"},
		{"one_to_many", ContextTypeOneToMany, "1:N"},
		{"many_to_many", ContextTypeManyToMany, "N:M"},
		{"empty", ContextType(""), ""},
		{"invalid", ContextType("INVALID"), "INVALID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.contextType.String())
		})
	}
}

func TestContextType_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contextType ContextType
		want        bool
	}{
		{"one_to_one", ContextTypeOneToOne, true},
		{"one_to_many", ContextTypeOneToMany, true},
		{"many_to_many", ContextTypeManyToMany, true},
		{"empty", ContextType(""), false},
		{"lowercase", ContextType("1:n"), false},
		{"unknown", ContextType("2:2"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.contextType.Valid())
		})
	}
}

func TestContextType_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contextType ContextType
		want        bool
	}{
		{"one_to_one", ContextTypeOneToOne, true},
		{"one_to_many", ContextTypeOneToMany, true},
		{"many_to_many", ContextTypeManyToMany, true},
		{"empty", ContextType(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.contextType.IsValid())
			assert.Equal(t, tt.contextType.Valid(), tt.contextType.IsValid())
		})
	}
}

func TestParseContextType_AllValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  ContextType
	}{
		{"1:1", ContextTypeOneToOne},
		{" 1:1 ", ContextTypeOneToOne},
		{"1:N", ContextTypeOneToMany},
		{" 1:N ", ContextTypeOneToMany},
		{"N:M", ContextTypeManyToMany},
		{" N:M ", ContextTypeManyToMany},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got, err := ParseContextType(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseContextType_Invalid(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		" ",
		"1:n",
		"n:m",
		"2:2",
		"one-to-one",
		"1-1",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, err := ParseContextType(input)
			require.ErrorIs(t, err, ErrInvalidContextType)
		})
	}
}

func TestNewMatchRule_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	input := CreateMatchRuleInput{
		Priority: 1,
		Type:     RuleTypeExact,
		Config:   map[string]any{"field": "amount"},
	}

	rule, err := NewMatchRule(ctx, contextID, input)
	require.NoError(t, err)
	require.NotNil(t, rule)

	assert.NotEqual(t, uuid.Nil, rule.ID)
	assert.Equal(t, contextID, rule.ContextID)
	assert.Equal(t, 1, rule.Priority)
	assert.Equal(t, RuleTypeExact, rule.Type)
	assert.Equal(t, "amount", rule.Config["field"])
	assert.False(t, rule.CreatedAt.IsZero())
	assert.False(t, rule.UpdatedAt.IsZero())
}

func TestNewMatchRule_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	tests := []struct {
		name      string
		contextID uuid.UUID
		input     CreateMatchRuleInput
		wantErr   error
	}{
		{
			name:      "nil context ID",
			contextID: uuid.Nil,
			input: CreateMatchRuleInput{
				Priority: 1,
				Type:     RuleTypeExact,
				Config:   map[string]any{"field": "amount"},
			},
			wantErr: ErrRuleContextRequired,
		},
		{
			name:      "priority zero",
			contextID: contextID,
			input: CreateMatchRuleInput{
				Priority: 0,
				Type:     RuleTypeExact,
				Config:   map[string]any{"field": "amount"},
			},
			wantErr: ErrRulePriorityInvalid,
		},
		{
			name:      "priority too high",
			contextID: contextID,
			input: CreateMatchRuleInput{
				Priority: 1001,
				Type:     RuleTypeExact,
				Config:   map[string]any{"field": "amount"},
			},
			wantErr: ErrRulePriorityInvalid,
		},
		{
			name:      "negative priority",
			contextID: contextID,
			input: CreateMatchRuleInput{
				Priority: -1,
				Type:     RuleTypeExact,
				Config:   map[string]any{"field": "amount"},
			},
			wantErr: ErrRulePriorityInvalid,
		},
		{
			name:      "invalid rule type",
			contextID: contextID,
			input: CreateMatchRuleInput{
				Priority: 1,
				Type:     RuleType("INVALID"),
				Config:   map[string]any{"field": "amount"},
			},
			wantErr: ErrRuleTypeInvalid,
		},
		{
			name:      "empty rule type",
			contextID: contextID,
			input: CreateMatchRuleInput{
				Priority: 1,
				Type:     RuleType(""),
				Config:   map[string]any{"field": "amount"},
			},
			wantErr: ErrRuleTypeInvalid,
		},
		{
			name:      "nil config",
			contextID: contextID,
			input: CreateMatchRuleInput{
				Priority: 1,
				Type:     RuleTypeExact,
				Config:   nil,
			},
			wantErr: ErrRuleConfigRequired,
		},
		{
			name:      "empty config",
			contextID: contextID,
			input: CreateMatchRuleInput{
				Priority: 1,
				Type:     RuleTypeExact,
				Config:   map[string]any{},
			},
			wantErr: ErrRuleConfigRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewMatchRule(ctx, tt.contextID, tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestNewMatchRule_BoundaryPriorities(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	tests := []struct {
		name     string
		priority int
		wantErr  bool
	}{
		{"priority 1 valid", 1, false},
		{"priority 500 valid", 500, false},
		{"priority 1000 valid", 1000, false},
		{"priority 0 invalid", 0, true},
		{"priority 1001 invalid", 1001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			input := CreateMatchRuleInput{
				Priority: tt.priority,
				Type:     RuleTypeExact,
				Config:   map[string]any{"field": "amount"},
			}
			_, err := NewMatchRule(ctx, contextID, input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMatchRule_Update_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	rule, err := NewMatchRule(ctx, contextID, CreateMatchRuleInput{
		Priority: 1,
		Type:     RuleTypeExact,
		Config:   map[string]any{"field": "amount"},
	})
	require.NoError(t, err)

	originalUpdatedAt := rule.UpdatedAt

	newPriority := 5
	newType := RuleTypeTolerance
	newConfig := map[string]any{"field": "date", "tolerance": 0.01}

	err = rule.Update(ctx, UpdateMatchRuleInput{
		Priority: &newPriority,
		Type:     &newType,
		Config:   newConfig,
	})
	require.NoError(t, err)

	assert.Equal(t, 5, rule.Priority)
	assert.Equal(t, RuleTypeTolerance, rule.Type)
	assert.Equal(t, "date", rule.Config["field"])
	assert.False(t, rule.UpdatedAt.Before(originalUpdatedAt))
}

func TestMatchRule_Update_PartialUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	rule, err := NewMatchRule(ctx, contextID, CreateMatchRuleInput{
		Priority: 1,
		Type:     RuleTypeExact,
		Config:   map[string]any{"field": "amount"},
	})
	require.NoError(t, err)

	newPriority := 10
	err = rule.Update(ctx, UpdateMatchRuleInput{
		Priority: &newPriority,
	})
	require.NoError(t, err)

	assert.Equal(t, 10, rule.Priority)
	assert.Equal(t, RuleTypeExact, rule.Type)
	assert.Equal(t, "amount", rule.Config["field"])
}

func TestMatchRule_Update_NilReceiver(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var rule *MatchRule

	err := rule.Update(ctx, UpdateMatchRuleInput{Priority: testutil.IntPtr(5)})
	require.ErrorIs(t, err, ErrMatchRuleNil)
}

func TestMatchRule_Update_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	tests := []struct {
		name    string
		input   UpdateMatchRuleInput
		wantErr error
	}{
		{
			name:    "priority zero",
			input:   UpdateMatchRuleInput{Priority: testutil.IntPtr(0)},
			wantErr: ErrRulePriorityInvalid,
		},
		{
			name:    "priority too high",
			input:   UpdateMatchRuleInput{Priority: testutil.IntPtr(1001)},
			wantErr: ErrRulePriorityInvalid,
		},
		{
			name:    "negative priority",
			input:   UpdateMatchRuleInput{Priority: testutil.IntPtr(-1)},
			wantErr: ErrRulePriorityInvalid,
		},
		{
			name:    "invalid type",
			input:   UpdateMatchRuleInput{Type: ruleTypePtr(RuleType("INVALID"))},
			wantErr: ErrRuleTypeInvalid,
		},
		{
			name:    "empty type",
			input:   UpdateMatchRuleInput{Type: ruleTypePtr(RuleType(""))},
			wantErr: ErrRuleTypeInvalid,
		},
		{
			name:    "empty config",
			input:   UpdateMatchRuleInput{Config: map[string]any{}},
			wantErr: ErrRuleConfigRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rule, err := NewMatchRule(ctx, contextID, CreateMatchRuleInput{
				Priority: 1,
				Type:     RuleTypeExact,
				Config:   map[string]any{"field": "amount"},
			})
			require.NoError(t, err)

			err = rule.Update(ctx, tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestMatchRule_ConfigJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	t.Run("valid config", func(t *testing.T) {
		t.Parallel()
		rule, err := NewMatchRule(ctx, contextID, CreateMatchRuleInput{
			Priority: 1,
			Type:     RuleTypeExact,
			Config:   map[string]any{"field": "amount", "nested": map[string]any{"key": "value"}},
		})
		require.NoError(t, err)

		jsonBytes, err := rule.ConfigJSON()
		require.NoError(t, err)
		assert.Contains(t, string(jsonBytes), "amount")
		assert.Contains(t, string(jsonBytes), "nested")
	})

	t.Run("nil rule", func(t *testing.T) {
		t.Parallel()
		var rule *MatchRule
		jsonBytes, err := rule.ConfigJSON()
		require.NoError(t, err)
		assert.Equal(t, "null", string(jsonBytes))
	})
}

func TestMatchRules_Sorting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	rule1, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 5, Type: RuleTypeExact, Config: map[string]any{"f": "a"}},
	)
	require.NoError(t, err)
	rule2, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 1, Type: RuleTypeExact, Config: map[string]any{"f": "b"}},
	)
	require.NoError(t, err)
	rule3, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 10, Type: RuleTypeExact, Config: map[string]any{"f": "c"}},
	)
	require.NoError(t, err)
	rule4, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 3, Type: RuleTypeExact, Config: map[string]any{"f": "d"}},
	)
	require.NoError(t, err)

	rules := MatchRules{rule1, rule2, rule3, rule4}
	sort.Sort(rules)

	assert.Equal(t, 1, rules[0].Priority)
	assert.Equal(t, 3, rules[1].Priority)
	assert.Equal(t, 5, rules[2].Priority)
	assert.Equal(t, 10, rules[3].Priority)
}

func TestMatchRules_SortingWithNils(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	rule1, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 5, Type: RuleTypeExact, Config: map[string]any{"f": "a"}},
	)
	require.NoError(t, err)
	rule2, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 1, Type: RuleTypeExact, Config: map[string]any{"f": "b"}},
	)
	require.NoError(t, err)

	rules := MatchRules{nil, rule1, nil, rule2, nil}

	assert.Equal(t, 5, rules.Len())

	sort.Sort(rules)

	var nonNilCount int
	for _, r := range rules {
		if r != nil {
			nonNilCount++
		}
	}
	assert.Equal(t, 2, nonNilCount)
}

func TestMatchRules_LenSwap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	rule1, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 1, Type: RuleTypeExact, Config: map[string]any{"f": "a"}},
	)
	require.NoError(t, err)
	rule2, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 2, Type: RuleTypeExact, Config: map[string]any{"f": "b"}},
	)
	require.NoError(t, err)

	rules := MatchRules{rule1, rule2}

	assert.Equal(t, 2, rules.Len())

	rules.Swap(0, 1)
	assert.Equal(t, 2, rules[0].Priority)
	assert.Equal(t, 1, rules[1].Priority)
}

func TestMatchRules_Less_NilHandling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()

	rule1, err := NewMatchRule(
		ctx,
		contextID,
		CreateMatchRuleInput{Priority: 1, Type: RuleTypeExact, Config: map[string]any{"f": "a"}},
	)
	require.NoError(t, err)

	t.Run("both nil", func(t *testing.T) {
		t.Parallel()
		rules := MatchRules{nil, nil}
		assert.False(t, rules.Less(0, 1))
	})

	t.Run("first nil", func(t *testing.T) {
		t.Parallel()
		rules := MatchRules{nil, rule1}
		assert.False(t, rules.Less(0, 1))
	})

	t.Run("second nil", func(t *testing.T) {
		t.Parallel()
		rules := MatchRules{rule1, nil}
		assert.True(t, rules.Less(0, 1))
	})
}

// ruleTypePtr is kept local because RuleType is domain-specific to this package.
// Generic pointer helpers (IntPtr, StringPtr, etc.) live in internal/testutil.
func ruleTypePtr(v RuleType) *RuleType {
	return &v
}
