//go:build integration

package matching

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestMatchRunRepository_FindByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		_, err := repo.FindByID(ctx, h.Seed.ContextID, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestMatchRunRepository_ForeignKeyConstraint(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentContextID := uuid.New()

		run, err := matchingEntities.NewMatchRun(
			ctx,
			nonExistentContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)

		_, err = repo.Create(ctx, run)
		require.Error(t, err)
	})
}

func TestMatchGroupRepository_ForeignKeyConstraint_Run(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		rule, err := configEntities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			configEntities.CreateMatchRuleInput{
				Priority: 1,
				Type:     configVO.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
		)
		require.NoError(t, err)
		createdRule, err := ruleRepo.Create(ctx, rule)
		require.NoError(t, err)

		nonExistentRunID := uuid.New()
		confidence, err := matchingVO.ParseConfidenceScore(90)
		require.NoError(t, err)

		item1, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(100),
			"USD",
			decimal.NewFromInt(100),
		)
		require.NoError(t, err)
		item2, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(100),
			"USD",
			decimal.NewFromInt(100),
		)
		require.NoError(t, err)

		group, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			nonExistentRunID,
			createdRule.ID,
			confidence,
			[]*matchingEntities.MatchItem{item1, item2},
		)
		require.NoError(t, err)

		_, err = groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.Error(t, err)
	})
}

func TestMatchGroupRepository_ForeignKeyConstraint_Rule(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)

		nonExistentRuleID := uuid.New()
		confidence, err := matchingVO.ParseConfidenceScore(90)
		require.NoError(t, err)

		item1, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(100),
			"USD",
			decimal.NewFromInt(100),
		)
		require.NoError(t, err)
		item2, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(100),
			"USD",
			decimal.NewFromInt(100),
		)
		require.NoError(t, err)

		group, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			nonExistentRuleID,
			confidence,
			[]*matchingEntities.MatchItem{item1, item2},
		)
		require.NoError(t, err)

		_, err = groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.Error(t, err)
	})
}

func TestMatchGroupRepository_CreateBatch_Empty(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		result, err := groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{})
		require.NoError(t, err)
		require.Empty(t, result)
	})
}

func TestMatchRunRepository_ListByContextID_EmptyResult(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentContextID := uuid.New()
		runs, _, err := repo.ListByContextID(
			ctx,
			nonExistentContextID,
			matchingRepositories.CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		require.Empty(t, runs)
	})
}

func TestMatchGroupRepository_ListByRunID_EmptyResult(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentRunID := uuid.New()
		groups, _, err := groupRepo.ListByRunID(
			ctx,
			h.Seed.ContextID,
			nonExistentRunID,
			matchingRepositories.CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		require.Empty(t, groups)
	})
}

func TestMatchRunRepository_UpdateNonExistent(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)

		run.ID = uuid.New()

		_, err = repo.Update(ctx, run)
		require.Error(t, err)
	})
}

func TestMatchRunRepository_Pagination(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		for i := 0; i < 10; i++ {
			run, err := matchingEntities.NewMatchRun(
				ctx,
				h.Seed.ContextID,
				matchingVO.MatchRunModeCommit,
			)
			require.NoError(t, err)
			_, err = repo.Create(ctx, run)
			require.NoError(t, err)
		}

		page1, cursor1, err := repo.ListByContextID(
			ctx,
			h.Seed.ContextID,
			matchingRepositories.CursorFilter{Limit: 5},
		)
		require.NoError(t, err)
		require.Len(t, page1, 5)

		page2, _, err := repo.ListByContextID(
			ctx,
			h.Seed.ContextID,
			matchingRepositories.CursorFilter{Limit: 5, Cursor: cursor1.Next},
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(page2), 5)

		for _, p1 := range page1 {
			for _, p2 := range page2 {
				require.NotEqual(t, p1.ID, p2.ID, "Pages should not overlap")
			}
		}
	})
}

func TestMatchGroupRepository_Pagination(t *testing.T) {
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
				Type:     configVO.RuleTypeExact,
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

		confidence, err := matchingVO.ParseConfidenceScore(90)
		require.NoError(t, err)

		var groups []*matchingEntities.MatchGroup
		for i := 0; i < 10; i++ {
			item1, err := matchingEntities.NewMatchItem(
				ctx,
				uuid.New(),
				decimal.NewFromInt(int64(i+1)),
				"USD",
				decimal.NewFromInt(int64(i+1)),
			)
			require.NoError(t, err)
			item2, err := matchingEntities.NewMatchItem(
				ctx,
				uuid.New(),
				decimal.NewFromInt(int64(i+1)),
				"USD",
				decimal.NewFromInt(int64(i+1)),
			)
			require.NoError(t, err)

			group, err := matchingEntities.NewMatchGroup(
				ctx,
				h.Seed.ContextID,
				createdRun.ID,
				createdRule.ID,
				confidence,
				[]*matchingEntities.MatchItem{item1, item2},
			)
			require.NoError(t, err)
			groups = append(groups, group)
		}

		_, err = groupRepo.CreateBatch(ctx, groups)
		require.NoError(t, err)

		page1, pagination, err := groupRepo.ListByRunID(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			matchingRepositories.CursorFilter{Limit: 5},
		)
		require.NoError(t, err)
		require.Len(t, page1, 5)
		require.NotEmpty(t, pagination.Next)

		page2, _, err := groupRepo.ListByRunID(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			matchingRepositories.CursorFilter{
				Limit:  5,
				Cursor: pagination.Next,
			},
		)
		require.NoError(t, err)
		require.Len(t, page2, 5)

		for _, p1 := range page1 {
			for _, p2 := range page2 {
				require.NotEqual(t, p1.ID, p2.ID, "Pages should not overlap")
			}
		}
	})
}

func TestConfidenceScore_Boundaries(t *testing.T) {
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
				Type:     configVO.RuleTypeExact,
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

		minConfidence, err := matchingVO.ParseConfidenceScore(60)
		require.NoError(t, err)

		item1a, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(1),
			"USD",
			decimal.NewFromInt(1),
		)
		require.NoError(t, err)
		item1b, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(1),
			"USD",
			decimal.NewFromInt(1),
		)
		require.NoError(t, err)

		minGroup, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			createdRule.ID,
			minConfidence,
			[]*matchingEntities.MatchItem{item1a, item1b},
		)
		require.NoError(t, err)

		maxConfidence, err := matchingVO.ParseConfidenceScore(100)
		require.NoError(t, err)

		item2a, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(2),
			"USD",
			decimal.NewFromInt(2),
		)
		require.NoError(t, err)
		item2b, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(2),
			"USD",
			decimal.NewFromInt(2),
		)
		require.NoError(t, err)

		maxGroup, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			createdRule.ID,
			maxConfidence,
			[]*matchingEntities.MatchItem{item2a, item2b},
		)
		require.NoError(t, err)

		created, err := groupRepo.CreateBatch(
			ctx,
			[]*matchingEntities.MatchGroup{minGroup, maxGroup},
		)
		require.NoError(t, err)
		require.Len(t, created, 2)
	})
}

func TestMatchRun_StatusTransitions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)

		createdRun, err := repo.Create(ctx, run)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusProcessing, createdRun.Status)

		stats := map[string]int{"transactions": 10}
		require.NoError(t, createdRun.Complete(ctx, stats))
		completedRun, err := repo.Update(ctx, createdRun)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, completedRun.Status)
		require.NotNil(t, completedRun.CompletedAt)

		run2, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)

		createdRun2, err := repo.Create(ctx, run2)
		require.NoError(t, err)

		require.NoError(t, createdRun2.Fail(ctx, "test failure"))
		failedRun, err := repo.Update(ctx, createdRun2)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusFailed, failedRun.Status)
		require.NotNil(t, failedRun.FailureReason)
		require.NotNil(t, failedRun.CompletedAt)
	})
}

func TestMatchGroup_LargeConfidenceDecimal(t *testing.T) {
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
				Type:     configVO.RuleTypeExact,
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

		confidence, err := matchingVO.ParseConfidenceScore(87)
		require.NoError(t, err)

		largeAmount := decimal.NewFromFloat(999999999.999999)
		item1, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			largeAmount,
			"USD",
			largeAmount,
		)
		require.NoError(t, err)
		item2, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			largeAmount,
			"USD",
			largeAmount,
		)
		require.NoError(t, err)

		group, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			createdRule.ID,
			confidence,
			[]*matchingEntities.MatchItem{item1, item2},
		)
		require.NoError(t, err)

		created, err := groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.NoError(t, err)
		require.Len(t, created, 1)
	})
}
