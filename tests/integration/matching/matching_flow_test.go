//go:build integration

package matching

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	jobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	txRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	exceptionRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/exception_creator"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchItemRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_item"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestMatchingFlow_CompleteReconciliation(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		itemRepo := matchItemRepo.NewRepository(h.Provider())
		excRepo := exceptionRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		rule, err := configEntities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			configEntities.CreateMatchRuleInput{
				Priority: 1,
				Type:     configVO.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true, "matchAmount": true},
			},
		)
		require.NoError(t, err)
		createdRule, err := ruleRepo.Create(ctx, rule)
		require.NoError(t, err)

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"reconcile.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 4, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx1, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"LEDGER-001",
			decimal.NewFromFloat(100.00),
			"USD",
			time.Now().UTC(),
			"Ledger entry",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx1, err := tRepo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"BANK-001",
			decimal.NewFromFloat(100.00),
			"USD",
			time.Now().UTC(),
			"Bank statement",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx2, err := tRepo.Create(ctx, tx2)
		require.NoError(t, err)

		tx3, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"LEDGER-002",
			decimal.NewFromFloat(50.00),
			"EUR",
			time.Now().UTC(),
			"Ledger orphan",
			map[string]any{},
		)
		require.NoError(t, err)
		tx3.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx3, err := tRepo.Create(ctx, tx3)
		require.NoError(t, err)

		tx4, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"BANK-002",
			decimal.NewFromFloat(75.00),
			"GBP",
			time.Now().UTC(),
			"Bank orphan",
			map[string]any{},
		)
		require.NoError(t, err)
		tx4.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx4, err := tRepo.Create(ctx, tx4)
		require.NoError(t, err)

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusProcessing, createdRun.Status)

		confidence, err := matchingVO.ParseConfidenceScore(100)
		require.NoError(t, err)

		item1, err := matchingEntities.NewMatchItem(
			ctx,
			createdTx1.ID,
			decimal.NewFromFloat(100.00),
			"USD",
			decimal.NewFromFloat(100.00),
		)
		require.NoError(t, err)
		item2, err := matchingEntities.NewMatchItem(
			ctx,
			createdTx2.ID,
			decimal.NewFromFloat(100.00),
			"USD",
			decimal.NewFromFloat(100.00),
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

		createdGroups, err := groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.NoError(t, err)
		require.Len(t, createdGroups, 1)

		for _, item := range []*matchingEntities.MatchItem{item1, item2} {
			item.MatchGroupID = createdGroups[0].ID
		}
		_, err = itemRepo.CreateBatch(ctx, []*matchingEntities.MatchItem{item1, item2})
		require.NoError(t, err)

		err = tRepo.MarkMatched(ctx, h.Seed.ContextID, []uuid.UUID{createdTx1.ID, createdTx2.ID})
		require.NoError(t, err)

		unmatched, err := tRepo.ListUnmatchedByContext(ctx, h.Seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, 2)

		inputs := make([]matchingPorts.ExceptionTransactionInput, 0, len(unmatched))
		for _, tx := range unmatched {
			inputs = append(inputs, buildExceptionInputFromTx(t, tx, "", ""))
		}

		err = excRepo.CreateExceptions(ctx, h.Seed.ContextID, createdRun.ID, inputs, nil)
		require.NoError(t, err)

		stats := map[string]int{
			"transactions_processed": 4,
			"groups_created":         1,
			"exceptions_created":     2,
		}
		require.NoError(t, createdRun.Complete(ctx, stats))
		completedRun, err := runRepo.Update(ctx, createdRun)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, completedRun.Status)
		require.Equal(t, 4, completedRun.Stats["transactions_processed"])
		require.Equal(t, 1, completedRun.Stats["groups_created"])
		require.Equal(t, 2, completedRun.Stats["exceptions_created"])

		verified1, err := tRepo.FindByID(ctx, createdTx1.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusMatched, verified1.Status)

		verified3, err := tRepo.FindByID(ctx, createdTx3.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusUnmatched, verified3.Status)

		verified4, err := tRepo.FindByID(ctx, createdTx4.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusUnmatched, verified4.Status)
	})
}

func TestMatchingFlow_DryRunMode(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		itemRepo := matchItemRepo.NewRepository(h.Provider())

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

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"dryrun.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 2, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx1, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"DRY-001",
			decimal.NewFromFloat(500.00),
			"USD",
			time.Now().UTC(),
			"Dry run tx 1",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx1, err := tRepo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"DRY-002",
			decimal.NewFromFloat(500.00),
			"USD",
			time.Now().UTC(),
			"Dry run tx 2",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx2, err := tRepo.Create(ctx, tx2)
		require.NoError(t, err)

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeDryRun,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunModeDryRun, createdRun.Mode)

		confidence, err := matchingVO.ParseConfidenceScore(100)
		require.NoError(t, err)

		item1, err := matchingEntities.NewMatchItem(
			ctx,
			createdTx1.ID,
			decimal.NewFromFloat(500.00),
			"USD",
			decimal.NewFromFloat(500.00),
		)
		require.NoError(t, err)
		item2, err := matchingEntities.NewMatchItem(
			ctx,
			createdTx2.ID,
			decimal.NewFromFloat(500.00),
			"USD",
			decimal.NewFromFloat(500.00),
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

		createdGroups, err := groupRepo.CreateBatch(ctx, []*matchingEntities.MatchGroup{group})
		require.NoError(t, err)
		require.Len(t, createdGroups, 1)

		for _, item := range []*matchingEntities.MatchItem{item1, item2} {
			item.MatchGroupID = createdGroups[0].ID
		}
		_, err = itemRepo.CreateBatch(ctx, []*matchingEntities.MatchItem{item1, item2})
		require.NoError(t, err)

		stats := map[string]int{"transactions_processed": 2, "groups_created": 1}
		require.NoError(t, createdRun.Complete(ctx, stats))
		completedRun, err := runRepo.Update(ctx, createdRun)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, completedRun.Status)

		verified1, err := tRepo.FindByID(ctx, createdTx1.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusUnmatched, verified1.Status)

		verified2, err := tRepo.FindByID(ctx, createdTx2.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusUnmatched, verified2.Status)
	})
}

func TestMatchingFlow_FailedRun(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		runRepo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)

		failReason := "Rule evaluation engine crashed: nil pointer dereference"
		require.NoError(t, createdRun.Fail(ctx, failReason))
		failedRun, err := runRepo.Update(ctx, createdRun)
		require.NoError(t, err)

		require.Equal(t, matchingVO.MatchRunStatusFailed, failedRun.Status)
		require.NotNil(t, failedRun.FailureReason)
		require.Equal(t, failReason, *failedRun.FailureReason)
		require.NotNil(t, failedRun.CompletedAt)
	})
}

func TestMatchingFlow_MultipleRulesWithPriority(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ruleRepo := matchRuleRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		runRepo := matchRunRepo.NewRepository(h.Provider())
		groupRepo := matchGroupRepo.NewRepository(h.Provider())
		itemRepo := matchItemRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		exactRule, err := configEntities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			configEntities.CreateMatchRuleInput{
				Priority: 1,
				Type:     configVO.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true, "matchAmount": true},
			},
		)
		require.NoError(t, err)
		createdExactRule, err := ruleRepo.Create(ctx, exactRule)
		require.NoError(t, err)

		toleranceRule, err := configEntities.NewMatchRule(
			ctx,
			h.Seed.ContextID,
			configEntities.CreateMatchRuleInput{
				Priority: 2,
				Type:     configVO.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "5.00"},
			},
		)
		require.NoError(t, err)
		createdToleranceRule, err := ruleRepo.Create(ctx, toleranceRule)
		require.NoError(t, err)

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"multi_rule.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 4, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx1, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"EXACT-MATCH-1",
			decimal.NewFromFloat(100.00),
			"USD",
			time.Now().UTC(),
			"",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx1, err := tRepo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"EXACT-MATCH-2",
			decimal.NewFromFloat(100.00),
			"USD",
			time.Now().UTC(),
			"",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx2, err := tRepo.Create(ctx, tx2)
		require.NoError(t, err)

		tx3, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"TOLERANCE-1",
			decimal.NewFromFloat(200.00),
			"USD",
			time.Now().UTC(),
			"",
			map[string]any{},
		)
		require.NoError(t, err)
		tx3.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx3, err := tRepo.Create(ctx, tx3)
		require.NoError(t, err)

		tx4, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"TOLERANCE-2",
			decimal.NewFromFloat(203.50),
			"USD",
			time.Now().UTC(),
			"",
			map[string]any{},
		)
		require.NoError(t, err)
		tx4.ExtractionStatus = shared.ExtractionStatusComplete
		createdTx4, err := tRepo.Create(ctx, tx4)
		require.NoError(t, err)

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)

		exactConfidence, err := matchingVO.ParseConfidenceScore(100)
		require.NoError(t, err)
		toleranceConfidence, err := matchingVO.ParseConfidenceScore(85)
		require.NoError(t, err)

		exactItem1, err := matchingEntities.NewMatchItem(
			ctx,
			createdTx1.ID,
			decimal.NewFromFloat(100.00),
			"USD",
			decimal.NewFromFloat(100.00),
		)
		require.NoError(t, err)
		exactItem2, err := matchingEntities.NewMatchItem(
			ctx,
			createdTx2.ID,
			decimal.NewFromFloat(100.00),
			"USD",
			decimal.NewFromFloat(100.00),
		)
		require.NoError(t, err)

		exactGroup, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			createdExactRule.ID,
			exactConfidence,
			[]*matchingEntities.MatchItem{exactItem1, exactItem2},
		)
		require.NoError(t, err)

		toleranceItem1, err := matchingEntities.NewMatchItem(
			ctx,
			createdTx3.ID,
			decimal.NewFromFloat(200.00),
			"USD",
			decimal.NewFromFloat(200.00),
		)
		require.NoError(t, err)
		toleranceItem2, err := matchingEntities.NewMatchItem(
			ctx,
			createdTx4.ID,
			decimal.NewFromFloat(203.50),
			"USD",
			decimal.NewFromFloat(203.50),
		)
		require.NoError(t, err)

		toleranceGroup, err := matchingEntities.NewMatchGroup(
			ctx,
			h.Seed.ContextID,
			createdRun.ID,
			createdToleranceRule.ID,
			toleranceConfidence,
			[]*matchingEntities.MatchItem{toleranceItem1, toleranceItem2},
		)
		require.NoError(t, err)

		createdGroups, err := groupRepo.CreateBatch(
			ctx,
			[]*matchingEntities.MatchGroup{exactGroup, toleranceGroup},
		)
		require.NoError(t, err)
		require.Len(t, createdGroups, 2)

		exactItem1.MatchGroupID = createdGroups[0].ID
		exactItem2.MatchGroupID = createdGroups[0].ID
		toleranceItem1.MatchGroupID = createdGroups[1].ID
		toleranceItem2.MatchGroupID = createdGroups[1].ID

		_, err = itemRepo.CreateBatch(
			ctx,
			[]*matchingEntities.MatchItem{exactItem1, exactItem2, toleranceItem1, toleranceItem2},
		)
		require.NoError(t, err)

		err = tRepo.MarkMatched(
			ctx,
			h.Seed.ContextID,
			[]uuid.UUID{createdTx1.ID, createdTx2.ID, createdTx3.ID, createdTx4.ID},
		)
		require.NoError(t, err)

		stats := map[string]int{"transactions_processed": 4, "groups_created": 2}
		require.NoError(t, createdRun.Complete(ctx, stats))
		_, err = runRepo.Update(ctx, createdRun)
		require.NoError(t, err)

		unmatched, err := tRepo.ListUnmatchedByContext(ctx, h.Seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Empty(t, unmatched)
	})
}

func TestMatchingFlow_ListRunsByContext(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		runRepo := matchRunRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		for i := 0; i < 5; i++ {
			run, err := matchingEntities.NewMatchRun(
				ctx,
				h.Seed.ContextID,
				matchingVO.MatchRunModeCommit,
			)
			require.NoError(t, err)

			createdRun, err := runRepo.Create(ctx, run)
			require.NoError(t, err)

			if i%2 == 0 {
				stats := map[string]int{"transactions_processed": i * 10}
				require.NoError(t, createdRun.Complete(ctx, stats))
				_, err = runRepo.Update(ctx, createdRun)
				require.NoError(t, err)
			}
		}

		runs, _, err := runRepo.ListByContextID(
			ctx,
			h.Seed.ContextID,
			matchingRepositories.CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(runs), 5)

		paginatedRuns, _, err := runRepo.ListByContextID(
			ctx,
			h.Seed.ContextID,
			matchingRepositories.CursorFilter{Limit: 2},
		)
		require.NoError(t, err)
		require.Len(t, paginatedRuns, 2)
	})
}
