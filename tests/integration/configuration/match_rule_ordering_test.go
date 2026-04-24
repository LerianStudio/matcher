//go:build integration

// Package configuration contains integration tests for configuration domain operations
// including match rule ordering and reordering.
package configuration

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	configQuery "github.com/LerianStudio/matcher/internal/configuration/services/query"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Configuration_MatchRuleOrdering_CreateAndReorder(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		ctx := h.Ctx()

		contextRepository := configContextRepo.NewRepository(provider)
		sourceRepository, err := configSourceRepo.NewRepository(provider)
		require.NoError(t, err)
		fieldMapRepository := configFieldMapRepo.NewRepository(provider)
		matchRuleRepository := configMatchRuleRepo.NewRepository(provider)

		commandUseCase, err := configCommand.NewUseCase(
			contextRepository,
			sourceRepository,
			fieldMapRepository,
			matchRuleRepository,
		)
		require.NoError(t, err)

		queryUseCase, err := configQuery.NewUseCase(
			contextRepository,
			sourceRepository,
			fieldMapRepository,
			matchRuleRepository,
		)
		require.NoError(t, err)

		contextEntity, err := commandUseCase.CreateContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Ordering Context",
				Type:     shared.ContextTypeOneToMany,
				Interval: "0 1 * * *",
			},
		)
		require.NoError(t, err)

		ruleOne, err := commandUseCase.CreateMatchRule(
			ctx,
			contextEntity.ID,
			entities.CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
		)
		require.NoError(t, err)

		ruleTwo, err := commandUseCase.CreateMatchRule(
			ctx,
			contextEntity.ID,
			entities.CreateMatchRuleInput{
				Priority: 2,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "1.00"},
			},
		)
		require.NoError(t, err)

		ruleThree, err := commandUseCase.CreateMatchRule(
			ctx,
			contextEntity.ID,
			entities.CreateMatchRuleInput{
				Priority: 3,
				Type:     shared.RuleTypeDateLag,
				Config:   map[string]any{"minDays": 1, "maxDays": 1, "direction": "ABS"},
			},
		)
		require.NoError(t, err)

		ordered, _, err := queryUseCase.ListMatchRules(ctx, contextEntity.ID, "", 10, nil)
		require.NoError(t, err)
		require.Len(t, ordered, 3)
		assert.Equal(
			t,
			[]uuid.UUID{ruleOne.ID, ruleTwo.ID, ruleThree.ID},
			[]uuid.UUID{ordered[0].ID, ordered[1].ID, ordered[2].ID},
		)

		reorderIDs := []uuid.UUID{ruleThree.ID, ruleOne.ID, ruleTwo.ID}
		err = commandUseCase.ReorderMatchRulePriorities(ctx, contextEntity.ID, reorderIDs)
		require.NoError(t, err)

		reordered, _, err := queryUseCase.ListMatchRules(ctx, contextEntity.ID, "", 10, nil)
		require.NoError(t, err)
		require.Len(t, reordered, 3)
		assert.Equal(t, reorderIDs, []uuid.UUID{reordered[0].ID, reordered[1].ID, reordered[2].ID})
	})
}
