//go:build integration

package matching

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Matching_MatchGroupRepository_CreateBatchAndList(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		rule, err := configEntities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			configEntities.CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
		)
		require.NoError(t, err)
		createdRule, err := ruleRepo.Create(ctx, rule)
		require.NoError(t, err)

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)

		confidence, err := matchingVO.ParseConfidenceScore(80)
		require.NoError(t, err)

		itemA, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(10),
			"USD",
			decimal.NewFromInt(10),
		)
		require.NoError(t, err)
		itemB, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(10),
			"USD",
			decimal.NewFromInt(10),
		)
		require.NoError(t, err)

		group, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			createdRule.ID,
			confidence,
			[]*matchingEntities.MatchItem{itemA, itemB},
		)
		require.NoError(t, err)

		created, err := groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.NoError(t, err)
		require.Len(t, created, 1)

		listed, _, err := groupRepo.ListByRunID(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			matchingRepositories.CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, group.ID, listed[0].ID)
	})
}

func TestIntegration_Matching_MatchGroupRepository_MultipleGroups(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		rule, err := configEntities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			configEntities.CreateMatchRuleInput{
				Priority: 2,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "0.01"},
			},
		)
		require.NoError(t, err)
		createdRule, err := ruleRepo.Create(ctx, rule)
		require.NoError(t, err)

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)

		confidence, err := matchingVO.ParseConfidenceScore(95)
		require.NoError(t, err)

		var groups []*matchingEntities.MatchGroup

		for i := 0; i < 3; i++ {
			itemA, err := matchingEntities.NewMatchItem(
				ctx,
				uuid.New(),
				decimal.NewFromInt(int64(10+i)),
				"USD",
				decimal.NewFromInt(int64(10+i)),
			)
			require.NoError(t, err)
			itemB, err := matchingEntities.NewMatchItem(
				ctx,
				uuid.New(),
				decimal.NewFromInt(int64(10+i)),
				"USD",
				decimal.NewFromInt(int64(10+i)),
			)
			require.NoError(t, err)

			group, err := matchingEntities.NewMatchGroup(
				ctx,
				h.Seed.ContextID,
				createdRun.ID,
				createdRule.ID,
				confidence,
				[]*matchingEntities.MatchItem{itemA, itemB},
			)
			require.NoError(t, err)
			groups = append(groups, group)
		}

		created, err := groupRepo.CreateBatch(ctx, groups)
		require.NoError(t, err)
		require.Len(t, created, 3)

		listed, _, err := groupRepo.ListByRunID(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			matchingRepositories.CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		require.Len(t, listed, 3)
	})
}
