//go:build integration

package matching

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	jobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	txRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchItemRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_item"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestMatchItemRepository_CreateBatch(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		itemRepo := matchItemRepo.NewRepository(h.Provider())
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())

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

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"items.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 2, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		txA, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"ITEM-TX-A",
			decimal.NewFromInt(100),
			"USD",
			time.Now().UTC(),
			"tx A",
			map[string]any{},
		)
		require.NoError(t, err)
		txA.ExtractionStatus = shared.ExtractionStatusComplete
		createdTxA, err := tRepo.Create(ctx, txA)
		require.NoError(t, err)

		txB, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"ITEM-TX-B",
			decimal.NewFromInt(100),
			"USD",
			time.Now().UTC(),
			"tx B",
			map[string]any{},
		)
		require.NoError(t, err)
		txB.ExtractionStatus = shared.ExtractionStatusComplete
		createdTxB, err := tRepo.Create(ctx, txB)
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

		itemA, err := matchingEntities.NewMatchItem(
			ctx,
			createdTxA.ID,
			decimal.NewFromInt(100),
			"USD",
			decimal.NewFromInt(100),
		)
		require.NoError(t, err)
		itemB, err := matchingEntities.NewMatchItem(
			ctx,
			createdTxB.ID,
			decimal.NewFromInt(100),
			"USD",
			decimal.NewFromInt(100),
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

		createdGroups, err := groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.NoError(t, err)
		require.Len(t, createdGroups, 1)

		for _, item := range []*matchingEntities.MatchItem{itemA, itemB} {
			item.MatchGroupID = createdGroups[0].ID
		}

		createdItems, err := itemRepo.CreateBatch(ctx, []*matchingEntities.MatchItem{itemA, itemB})
		require.NoError(t, err)
		require.Len(t, createdItems, 2)
		require.Equal(t, createdGroups[0].ID, createdItems[0].MatchGroupID)
		require.Equal(t, createdGroups[0].ID, createdItems[1].MatchGroupID)
	})
}

func TestMatchItemRepository_CreateBatch_Empty(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		itemRepo := matchItemRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		result, err := itemRepo.CreateBatch(ctx, []*matchingEntities.MatchItem{})
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestMatchItemRepository_CreateBatch_PartialAllocation(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		itemRepo := matchItemRepo.NewRepository(h.Provider())
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		rule, err := configEntities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			configEntities.CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "0.01"},
			},
		)
		require.NoError(t, err)
		createdRule, err := ruleRepo.Create(ctx, rule)
		require.NoError(t, err)

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"partial.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 2, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		txA, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"PARTIAL-TX-A",
			decimal.NewFromFloat(100.00),
			"USD",
			time.Now().UTC(),
			"tx A",
			map[string]any{},
		)
		require.NoError(t, err)
		txA.ExtractionStatus = shared.ExtractionStatusComplete
		createdTxA, err := tRepo.Create(ctx, txA)
		require.NoError(t, err)

		txB, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"PARTIAL-TX-B",
			decimal.NewFromFloat(50.00),
			"USD",
			time.Now().UTC(),
			"tx B",
			map[string]any{},
		)
		require.NoError(t, err)
		txB.ExtractionStatus = shared.ExtractionStatusComplete
		createdTxB, err := tRepo.Create(ctx, txB)
		require.NoError(t, err)

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)

		confidence, err := matchingVO.ParseConfidenceScore(75)
		require.NoError(t, err)

		itemA, err := matchingEntities.NewMatchItemWithPolicy(
			ctx,
			createdTxA.ID,
			decimal.NewFromFloat(50.00),
			"USD",
			decimal.NewFromFloat(100.00),
			true,
		)
		require.NoError(t, err)

		itemB, err := matchingEntities.NewMatchItem(
			ctx,
			createdTxB.ID,
			decimal.NewFromFloat(50.00),
			"USD",
			decimal.NewFromFloat(50.00),
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

		createdGroups, err := groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.NoError(t, err)

		for _, item := range []*matchingEntities.MatchItem{itemA, itemB} {
			item.MatchGroupID = createdGroups[0].ID
		}

		createdItems, err := itemRepo.CreateBatch(ctx, []*matchingEntities.MatchItem{itemA, itemB})
		require.NoError(t, err)
		require.Len(t, createdItems, 2)
		require.True(t, createdItems[0].AllowPartial)
		require.False(t, createdItems[1].AllowPartial)
	})
}

func TestMatchItemRepository_CreateBatch_LargeSet(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		itemRepo := matchItemRepo.NewRepository(h.Provider())
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())

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

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"largeset.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 50, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		const itemCount = 50
		items := make([]*matchingEntities.MatchItem, itemCount)

		for i := 0; i < itemCount; i++ {
			tx, err := shared.NewTransaction(
				ctx,
				h.Seed.TenantID,
				createdJob.ID,
				h.Seed.SourceID,
				"LARGE-TX-"+string(rune('A'+i%26))+string(rune('0'+i/26)),
				decimal.NewFromInt(int64(i+1)),
				"USD",
				time.Now().UTC(),
				"tx",
				map[string]any{},
			)
			require.NoError(t, err)
			tx.ExtractionStatus = shared.ExtractionStatusComplete
			createdTx, err := tRepo.Create(ctx, tx)
			require.NoError(t, err)

			item, err := matchingEntities.NewMatchItem(
				ctx,
				createdTx.ID,
				decimal.NewFromInt(int64(i+1)),
				"USD",
				decimal.NewFromInt(int64(i+1)),
			)
			require.NoError(t, err)
			items[i] = item
		}

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)

		confidence, err := matchingVO.ParseConfidenceScore(100)
		require.NoError(t, err)

		group, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			createdRule.ID,
			confidence,
			items,
		)
		require.NoError(t, err)

		createdGroups, err := groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.NoError(t, err)

		for _, item := range items {
			item.MatchGroupID = createdGroups[0].ID
		}

		createdItems, err := itemRepo.CreateBatch(ctx, items)
		require.NoError(t, err)
		require.Len(t, createdItems, itemCount)
	})
}
