//go:build unit

package entities

import (
	"context"
	"math/rand"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestMatchRulesSortingProperty(t *testing.T) {
	t.Parallel()

	seed := int64(42)
	random := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	for iteration := 0; iteration < 25; iteration++ {
		ruleCount := random.Intn(8) + 2
		rules := make(MatchRules, 0, ruleCount+1)

		for index := 0; index < ruleCount; index++ {
			priority := random.Intn(20) + 1
			rule, err := NewMatchRule(ctx, contextID, CreateMatchRuleInput{
				Priority: priority,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			})
			require.NoError(t, err)

			rules = append(rules, rule)
		}

		if iteration%5 == 0 {
			rules = append(rules, nil)
		}

		sort.Sort(rules)

		for index := 1; index < len(rules); index++ {
			if rules[index] == nil || rules[index-1] == nil {
				continue
			}

			assert.LessOrEqual(t, rules[index-1].Priority, rules[index].Priority)
		}
	}
}

func TestMatchRulesSortingEdgeCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	createRule := func(t *testing.T, priority int) *MatchRule {
		t.Helper()

		rule, err := NewMatchRule(ctx, contextID, CreateMatchRuleInput{
			Priority: priority,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		})
		require.NoError(t, err)

		return rule
	}

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		rules := MatchRules{}
		sort.Sort(rules)
		assert.Empty(t, rules)
	})

	t.Run("single element", func(t *testing.T) {
		t.Parallel()

		rule := createRule(t, 1)
		rules := MatchRules{rule}
		sort.Sort(rules)
		require.Len(t, rules, 1)
		assert.Equal(t, 1, rules[0].Priority)
	})

	t.Run("identical priorities", func(t *testing.T) {
		t.Parallel()

		rules := MatchRules{
			createRule(t, 2),
			createRule(t, 2),
			createRule(t, 2),
		}
		sort.Sort(rules)
		require.Len(t, rules, 3)

		for _, rule := range rules {
			assert.Equal(t, 2, rule.Priority)
		}
	})

	t.Run("already sorted", func(t *testing.T) {
		t.Parallel()

		rules := MatchRules{
			createRule(t, 1),
			createRule(t, 2),
			createRule(t, 3),
		}
		sort.Sort(rules)
		require.Len(t, rules, 3)
		assert.Equal(
			t,
			[]int{1, 2, 3},
			[]int{rules[0].Priority, rules[1].Priority, rules[2].Priority},
		)
	})

	t.Run("reverse sorted", func(t *testing.T) {
		t.Parallel()

		rules := MatchRules{
			createRule(t, 3),
			createRule(t, 2),
			createRule(t, 1),
		}
		sort.Sort(rules)
		require.Len(t, rules, 3)
		assert.Equal(
			t,
			[]int{1, 2, 3},
			[]int{rules[0].Priority, rules[1].Priority, rules[2].Priority},
		)
	})
}
