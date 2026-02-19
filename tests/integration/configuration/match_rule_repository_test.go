//go:build integration

package configuration

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestMatchRuleRepository_CreateAndFindByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewMatchRule(ctx, h.Seed.ContextID, entities.CreateMatchRuleInput{
			Priority: 1,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		})
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, 1, created.Priority)
		require.Equal(t, value_objects.RuleTypeExact, created.Type)

		fetched, err := repo.FindByID(ctx, h.Seed.ContextID, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.Priority, fetched.Priority)
	})
}

func TestMatchRuleRepository_FindByContextID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		for i := 1; i <= 3; i++ {
			entity, err := entities.NewMatchRule(
				ctx,
				h.Seed.ContextID,
				entities.CreateMatchRuleInput{
					Priority: i * 10,
					Type:     value_objects.RuleTypeExact,
					Config:   map[string]any{"matchCurrency": true},
				},
			)
			require.NoError(t, err)
			_, err = repo.Create(ctx, entity)
			require.NoError(t, err)
		}

		rules, _, err := repo.FindByContextID(ctx, h.Seed.ContextID, "", 10)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(rules), 3)

		for i := 0; i < len(rules)-1; i++ {
			require.LessOrEqual(
				t,
				rules[i].Priority,
				rules[i+1].Priority,
				"rules should be ordered by priority ASC",
			)
		}
	})
}

func TestMatchRuleRepository_FindByContextIDAndType(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		exactRule, err := entities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			entities.CreateMatchRuleInput{
				Priority: 100,
				Type:     value_objects.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, exactRule)
		require.NoError(t, err)

		toleranceRule, err := entities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			entities.CreateMatchRuleInput{
				Priority: 200,
				Type:     value_objects.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "1.00"},
			},
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, toleranceRule)
		require.NoError(t, err)

		exactRules, _, err := repo.FindByContextIDAndType(
			ctx,
			h.Seed.ContextID,
			value_objects.RuleTypeExact,
			"",
			10,
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(exactRules), 1)

		for _, r := range exactRules {
			require.Equal(t, value_objects.RuleTypeExact, r.Type)
		}
	})
}

func TestMatchRuleRepository_FindByPriority(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewMatchRule(ctx, h.Seed.ContextID, entities.CreateMatchRuleInput{
			Priority: 999,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		})
		require.NoError(t, err)
		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)

		fetched, err := repo.FindByPriority(ctx, h.Seed.ContextID, 999)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
	})
}

func TestMatchRuleRepository_Update(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewMatchRule(ctx, h.Seed.ContextID, entities.CreateMatchRuleInput{
			Priority: 50,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		})
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)

		created.Priority = 51
		updated, err := repo.Update(ctx, created)
		require.NoError(t, err)
		require.Equal(t, 51, updated.Priority)

		fetched, err := repo.FindByID(ctx, h.Seed.ContextID, created.ID)
		require.NoError(t, err)
		require.Equal(t, 51, fetched.Priority)
	})
}

func TestMatchRuleRepository_Delete(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewMatchRule(ctx, h.Seed.ContextID, entities.CreateMatchRuleInput{
			Priority: 75,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		})
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)

		err = repo.Delete(ctx, h.Seed.ContextID, created.ID)
		require.NoError(t, err)

		_, err = repo.FindByID(ctx, h.Seed.ContextID, created.ID)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestMatchRuleRepository_ReorderPriorities(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		var ruleIDs []uuid.UUID
		for i := 1; i <= 3; i++ {
			entity, err := entities.NewMatchRule(
				ctx,
				h.Seed.ContextID,
				entities.CreateMatchRuleInput{
					Priority: i * 100,
					Type:     value_objects.RuleTypeExact,
					Config:   map[string]any{"matchCurrency": true},
				},
			)
			require.NoError(t, err)
			created, err := repo.Create(ctx, entity)
			require.NoError(t, err)
			ruleIDs = append(ruleIDs, created.ID)
		}

		reordered := []uuid.UUID{ruleIDs[2], ruleIDs[0], ruleIDs[1]}
		err := repo.ReorderPriorities(ctx, h.Seed.ContextID, reordered)
		require.NoError(t, err)

		rules, _, err := repo.FindByContextID(ctx, h.Seed.ContextID, "", 10)
		require.NoError(t, err)

		priorityMap := make(map[uuid.UUID]int)
		for _, r := range rules {
			priorityMap[r.ID] = r.Priority
		}

		require.Equal(t, 1, priorityMap[ruleIDs[2]])
		require.Equal(t, 2, priorityMap[ruleIDs[0]])
		require.Equal(t, 3, priorityMap[ruleIDs[1]])
	})
}

func TestMatchRuleRepository_FindByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		_, err := repo.FindByID(ctx, h.Seed.ContextID, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}
