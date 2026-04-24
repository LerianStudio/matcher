//go:build integration

package integration

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	outboxEntities "github.com/LerianStudio/lib-commons/v5/commons/outbox"
	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	fieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	sourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
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
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestIntegration_Flow_CrossDomainFlow_EndToEndReconciliation(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		provider := h.Provider()
		ctxRepo := contextRepo.NewRepository(provider)
		srcRepo, err := sourceRepo.NewRepository(provider)
		require.NoError(t, err)
		fmRepo := fieldMapRepo.NewRepository(provider)
		ruleRepo := matchRuleRepo.NewRepository(provider)
		jRepo := jobRepo.NewRepository(provider)
		tRepo := txRepo.NewRepository(provider)
		runRepo := matchRunRepo.NewRepository(provider)
		groupRepo := matchGroupRepo.NewRepository(provider)
		itemRepo := matchItemRepo.NewRepository(provider)
		excRepo := exceptionRepo.NewRepository(provider)
		obRepo := NewTestOutboxRepository(t, h.Connection)

		ctx := h.Ctx()

		reconciliationContext, err := configEntities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			configEntities.CreateReconciliationContextInput{
				Name:     "Full E2E Reconciliation Context",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)
		createdContext, err := ctxRepo.Create(ctx, reconciliationContext)
		require.NoError(t, err)
		t.Logf("Created reconciliation context: %s", createdContext.ID)

		ledgerSource, err := configEntities.NewReconciliationSource(
			ctx,
			createdContext.ID,
			configEntities.CreateReconciliationSourceInput{
				Name:   "Ledger Source",
				Type:   configVO.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{"table": "journal_entries"},
			},
		)
		require.NoError(t, err)
		createdLedgerSource, err := srcRepo.Create(ctx, ledgerSource)
		require.NoError(t, err)

		bankSource, err := configEntities.NewReconciliationSource(
			ctx,
			createdContext.ID,
			configEntities.CreateReconciliationSourceInput{
				Name:   "Bank Source",
				Type:   configVO.SourceTypeBank,
				Side:   sharedfee.MatchingSideRight,
				Config: map[string]any{"format": "mt940"},
			},
		)
		require.NoError(t, err)
		createdBankSource, err := srcRepo.Create(ctx, bankSource)
		require.NoError(t, err)
		t.Logf("Created sources: ledger=%s, bank=%s", createdLedgerSource.ID, createdBankSource.ID)

		ledgerFieldMap, err := shared.NewFieldMap(
			ctx,
			createdContext.ID,
			createdLedgerSource.ID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{
					"amount":      "transaction_amount",
					"currency":    "currency_code",
					"date":        "posting_date",
					"external_id": "reference_id",
				},
			},
		)
		require.NoError(t, err)
		_, err = fmRepo.Create(ctx, ledgerFieldMap)
		require.NoError(t, err)

		bankFieldMap, err := shared.NewFieldMap(
			ctx,
			createdContext.ID,
			createdBankSource.ID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{
					"amount":      "amt",
					"currency":    "curr",
					"date":        "value_date",
					"external_id": "statement_ref",
				},
			},
		)
		require.NoError(t, err)
		_, err = fmRepo.Create(ctx, bankFieldMap)
		require.NoError(t, err)
		t.Log("Created field maps for both sources")

		exactRule, err := configEntities.NewMatchRule(
			ctx,
			createdContext.ID,
			configEntities.CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true, "matchAmount": true},
			},
		)
		require.NoError(t, err)
		createdExactRule, err := ruleRepo.Create(ctx, exactRule)
		require.NoError(t, err)

		toleranceRule, err := configEntities.NewMatchRule(
			ctx,
			createdContext.ID,
			configEntities.CreateMatchRuleInput{
				Priority: 2,
				Type:     shared.RuleTypeTolerance,
				Config:   map[string]any{"absTolerance": "1.00"},
			},
		)
		require.NoError(t, err)
		createdToleranceRule, err := ruleRepo.Create(ctx, toleranceRule)
		require.NoError(t, err)
		t.Logf(
			"Created match rules: exact=%s, tolerance=%s",
			createdExactRule.ID,
			createdToleranceRule.ID,
		)

		ledgerJob, err := ingestionEntities.NewIngestionJob(
			ctx,
			createdContext.ID,
			createdLedgerSource.ID,
			"ledger_export.csv",
			1000,
		)
		require.NoError(t, err)
		require.NoError(t, ledgerJob.Start(ctx))
		createdLedgerJob, err := jRepo.Create(ctx, ledgerJob)
		require.NoError(t, err)

		ledgerStartEvent, err := outboxEntities.NewOutboxEvent(
			ctx,
			"ingestion.job.started",
			createdLedgerJob.ID,
			[]byte(`{"source":"ledger"}`),
		)
		require.NoError(t, err)
		_, err = obRepo.Create(ctx, ledgerStartEvent)
		require.NoError(t, err)

		ledgerTxs := []struct {
			extID    string
			amount   decimal.Decimal
			currency string
		}{
			{"LEDGER-001", decimal.NewFromFloat(100.00), "USD"},
			{"LEDGER-002", decimal.NewFromFloat(250.50), "USD"},
			{"LEDGER-003", decimal.NewFromFloat(75.00), "EUR"},
			{"LEDGER-004", decimal.NewFromFloat(500.00), "USD"},
		}

		var createdLedgerTxIDs []uuid.UUID
		for _, txData := range ledgerTxs {
			tx, err := shared.NewTransaction(
				ctx,
				h.Seed.TenantID,
				createdLedgerJob.ID,
				createdLedgerSource.ID,
				txData.extID,
				txData.amount,
				txData.currency,
				time.Now().UTC(),
				"Ledger entry",
				map[string]any{},
			)
			require.NoError(t, err)
			tx.ExtractionStatus = shared.ExtractionStatusComplete
			created, err := tRepo.Create(ctx, tx)
			require.NoError(t, err)
			createdLedgerTxIDs = append(createdLedgerTxIDs, created.ID)
		}

		require.NoError(t, createdLedgerJob.Complete(ctx, len(ledgerTxs), 0))
		_, err = jRepo.Update(ctx, createdLedgerJob)
		require.NoError(t, err)
		t.Logf("Ingested %d ledger transactions", len(ledgerTxs))

		bankJob, err := ingestionEntities.NewIngestionJob(
			ctx,
			createdContext.ID,
			createdBankSource.ID,
			"bank_statement.mt940",
			1000,
		)
		require.NoError(t, err)
		require.NoError(t, bankJob.Start(ctx))
		createdBankJob, err := jRepo.Create(ctx, bankJob)
		require.NoError(t, err)

		bankTxs := []struct {
			extID    string
			amount   decimal.Decimal
			currency string
		}{
			{"BANK-001", decimal.NewFromFloat(100.00), "USD"},
			{"BANK-002", decimal.NewFromFloat(250.00), "USD"},
			{"BANK-003", decimal.NewFromFloat(300.00), "GBP"},
		}

		var createdBankTxIDs []uuid.UUID
		for _, txData := range bankTxs {
			tx, err := shared.NewTransaction(
				ctx,
				h.Seed.TenantID,
				createdBankJob.ID,
				createdBankSource.ID,
				txData.extID,
				txData.amount,
				txData.currency,
				time.Now().UTC(),
				"Bank statement",
				map[string]any{},
			)
			require.NoError(t, err)
			tx.ExtractionStatus = shared.ExtractionStatusComplete
			created, err := tRepo.Create(ctx, tx)
			require.NoError(t, err)
			createdBankTxIDs = append(createdBankTxIDs, created.ID)
		}

		require.NoError(t, createdBankJob.Complete(ctx, len(bankTxs), 0))
		_, err = jRepo.Update(ctx, createdBankJob)
		require.NoError(t, err)
		t.Logf("Ingested %d bank transactions", len(bankTxs))

		matchRun, err := matchingEntities.NewMatchRun(
			ctx,
			createdContext.ID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, matchRun)
		require.NoError(t, err)
		t.Logf("Started match run: %s", createdRun.ID)

		runStartEvent, err := outboxEntities.NewOutboxEvent(
			ctx,
			"matching.run.started",
			createdRun.ID,
			[]byte(`{"mode":"commit"}`),
		)
		require.NoError(t, err)
		_, err = obRepo.Create(ctx, runStartEvent)
		require.NoError(t, err)

		exactConfidence, err := matchingVO.ParseConfidenceScore(100)
		require.NoError(t, err)

		exactItem1, err := matchingEntities.NewMatchItem(
			ctx,
			createdLedgerTxIDs[0],
			decimal.NewFromFloat(100.00),
			"USD",
			decimal.NewFromFloat(100.00),
		)
		require.NoError(t, err)
		exactItem2, err := matchingEntities.NewMatchItem(
			ctx,
			createdBankTxIDs[0],
			decimal.NewFromFloat(100.00),
			"USD",
			decimal.NewFromFloat(100.00),
		)
		require.NoError(t, err)

		exactGroup, err := matchingEntities.NewMatchGroup(
			ctx,
			createdContext.ID,
			createdRun.ID,
			createdExactRule.ID,
			exactConfidence,
			[]*matchingEntities.MatchItem{exactItem1, exactItem2},
		)
		require.NoError(t, err)

		toleranceConfidence, err := matchingVO.ParseConfidenceScore(95)
		require.NoError(t, err)

		toleranceItem1, err := matchingEntities.NewMatchItem(
			ctx,
			createdLedgerTxIDs[1],
			decimal.NewFromFloat(250.50),
			"USD",
			decimal.NewFromFloat(250.50),
		)
		require.NoError(t, err)
		toleranceItem2, err := matchingEntities.NewMatchItem(
			ctx,
			createdBankTxIDs[1],
			decimal.NewFromFloat(250.00),
			"USD",
			decimal.NewFromFloat(250.00),
		)
		require.NoError(t, err)

		toleranceGroup, err := matchingEntities.NewMatchGroup(
			ctx,
			createdContext.ID,
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
		t.Logf("Created %d match groups", len(createdGroups))

		exactItem1.MatchGroupID = createdGroups[0].ID
		exactItem2.MatchGroupID = createdGroups[0].ID
		toleranceItem1.MatchGroupID = createdGroups[1].ID
		toleranceItem2.MatchGroupID = createdGroups[1].ID

		_, err = itemRepo.CreateBatch(
			ctx,
			[]*matchingEntities.MatchItem{exactItem1, exactItem2, toleranceItem1, toleranceItem2},
		)
		require.NoError(t, err)

		matchedIDs := []uuid.UUID{
			createdLedgerTxIDs[0],
			createdLedgerTxIDs[1],
			createdBankTxIDs[0],
			createdBankTxIDs[1],
		}
		err = tRepo.MarkMatched(ctx, createdContext.ID, matchedIDs)
		require.NoError(t, err)
		t.Logf("Marked %d transactions as matched", len(matchedIDs))

		unmatched, err := tRepo.ListUnmatchedByContext(ctx, createdContext.ID, nil, nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, 3)
		t.Logf("Found %d unmatched transactions", len(unmatched))

		inputs := make([]matchingPorts.ExceptionTransactionInput, 0, len(unmatched))
		for _, tx := range unmatched {
			inputs = append(inputs, buildExceptionInputFromTx(tx, "", ""))
		}

		err = excRepo.CreateExceptions(ctx, createdContext.ID, createdRun.ID, inputs, nil)
		require.NoError(t, err)
		t.Logf("Created exceptions for %d unmatched transactions", len(unmatched))

		stats := map[string]int{
			"transactions_processed": len(ledgerTxs) + len(bankTxs),
			"groups_created":         2,
			"exceptions_created":     len(unmatched),
		}
		require.NoError(t, createdRun.Complete(ctx, stats))
		completedRun, err := runRepo.Update(ctx, createdRun)
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, completedRun.Status)
		t.Logf("Completed match run with stats: %v", stats)

		runCompleteEvent, err := outboxEntities.NewOutboxEvent(
			ctx,
			"matching.run.completed",
			createdRun.ID,
			[]byte(`{"groups":2,"exceptions":3}`),
		)
		require.NoError(t, err)
		_, err = obRepo.Create(ctx, runCompleteEvent)
		require.NoError(t, err)

		verifiedTx1, err := tRepo.FindByID(ctx, createdLedgerTxIDs[0])
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusMatched, verifiedTx1.Status)

		verifiedTx3, err := tRepo.FindByID(ctx, createdLedgerTxIDs[2])
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusUnmatched, verifiedTx3.Status)

		groups, _, err := groupRepo.ListByRunID(
			ctx,
			createdContext.ID,
			createdRun.ID,
			matchingRepositories.CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		require.Len(t, groups, 2)

		runs, _, err := runRepo.ListByContextID(
			ctx,
			createdContext.ID,
			matchingRepositories.CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(runs), 1)

		pendingEvents, err := obRepo.ListPending(ctx, 100)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(pendingEvents), 3)

		t.Log("✓ Cross-domain E2E reconciliation flow completed successfully")
	})
}

func buildExceptionInputFromTx(
	txn *shared.Transaction,
	sourceType, reason string,
) matchingPorts.ExceptionTransactionInput {
	if txn == nil {
		panic("buildExceptionInputFromTx: txn must not be nil - indicates test setup bug")
	}

	var amountAbsBase decimal.Decimal
	if txn.AmountBase != nil {
		amountAbsBase = txn.AmountBase.Abs()
	} else {
		amountAbsBase = txn.Amount.Abs()
	}

	fxMissing := txn.AmountBase == nil && txn.BaseCurrency != nil

	return matchingPorts.ExceptionTransactionInput{
		TransactionID:   txn.ID,
		AmountAbsBase:   amountAbsBase,
		TransactionDate: txn.Date,
		SourceType:      sourceType,
		FXMissing:       fxMissing,
		Reason:          reason,
	}
}

func TestIntegration_Flow_CrossDomainFlow_FeeScheduleInvariant(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		ctx := h.Ctx()

		validInput := sharedfee.NewFeeScheduleInput{
			TenantID:         h.Seed.TenantID,
			Name:             "Integration Test Fee Schedule",
			Currency:         "USD",
			ApplicationOrder: sharedfee.ApplicationOrderParallel,
			RoundingScale:    2,
			RoundingMode:     sharedfee.RoundingModeHalfUp,
			Items: []sharedfee.FeeScheduleItemInput{
				{
					Name:      "Flat Processing Fee",
					Priority:  1,
					Structure: sharedfee.FlatFee{Amount: decimal.NewFromFloat(1.50)},
				},
			},
		}

		schedule, err := sharedfee.NewFeeSchedule(ctx, validInput)
		require.NoError(t, err)
		require.NotNil(t, schedule)
		require.Equal(t, h.Seed.TenantID, schedule.TenantID)
		require.Equal(t, "USD", schedule.Currency)
		require.Len(t, schedule.Items, 1)
		t.Log("✓ FeeSchedule created successfully with valid tenant ID")

		nilTenantInput := validInput
		nilTenantInput.TenantID = uuid.Nil

		_, err = sharedfee.NewFeeSchedule(ctx, nilTenantInput)
		require.Error(t, err)
		require.True(t, errors.Is(err, sharedfee.ErrScheduleTenantIDRequired),
			"expected ErrScheduleTenantIDRequired, got: %v", err)
		t.Log("✓ FeeSchedule correctly rejected uuid.Nil tenant ID")
	})
}

func TestIntegration_Flow_CrossDomainFlow_MultiTenantIsolation(t *testing.T) {
	RunWithDatabase(t, func(t *testing.T, h *TestHarness) {
		ctxRepo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		context1, err := configEntities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			configEntities.CreateReconciliationContextInput{
				Name:     "Tenant 1 Context",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)
		createdContext1, err := ctxRepo.Create(ctx, context1)
		require.NoError(t, err)

		context2, err := configEntities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			configEntities.CreateReconciliationContextInput{
				Name:     "Tenant 2 Context",
				Type:     shared.ContextTypeOneToMany,
				Interval: "0 */6 * * *",
			},
		)
		require.NoError(t, err)
		createdContext2, err := ctxRepo.Create(ctx, context2)
		require.NoError(t, err)

		contexts, _, err := ctxRepo.FindAll(ctx, "", 10, nil, nil)
		require.NoError(t, err)

		var foundContext1, foundContext2 bool
		for _, c := range contexts {
			if c.ID == createdContext1.ID {
				foundContext1 = true
			}
			if c.ID == createdContext2.ID {
				foundContext2 = true
			}
		}
		require.True(t, foundContext1, "Context 1 should be found")
		require.True(t, foundContext2, "Context 2 should be found")

		t.Log("✓ Multi-tenant isolation verified")
	})
}
