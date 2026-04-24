// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

//nolint:dupl
package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	repositoriesmocks "github.com/LerianStudio/matcher/internal/matching/domain/repositories/mocks"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestRunMatch_OrchestrationCommit(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002001")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002002")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002003")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002004")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002005"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":     true,
			"matchCurrency":   true,
			"matchDate":       true,
			"datePrecision":   "DAY",
			"matchReference":  true,
			"caseInsensitive": true,
		},
	}

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002006"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002007"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}
	outboxRepo := newMockOutboxRepository(t, nil)
	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepo,
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	run, groups, err := uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Len(t, groups, 1)
	require.Equal(t, matchingVO.MatchRunStatusCompleted, run.Status)
	require.True(t, matchGroupRepo.called)
	require.True(t, matchItemRepo.called)
	require.True(t, exceptionCreator.called)
	require.Equal(t, 1, txRepo.markCalls)
	require.Len(t, txRepo.markedIDs, 2)
	require.Empty(t, exceptionCreator.inputs)
	require.Equal(t, 1, run.Stats["matches"])
	require.Equal(t, 0, run.Stats["unmatched_left"])
	require.Equal(t, 0, run.Stats["unmatched_right"])
	require.Equal(t, 2, run.Stats["auto_matched_left"]+run.Stats["auto_matched_right"])
	require.Equal(t, 0, run.Stats["pending_review_left"]+run.Stats["pending_review_right"])
	require.Equal(t, 0, run.Stats["proposed_left"])
	require.Equal(t, 0, run.Stats["proposed_right"])
}

func TestRunMatch_OutboxEventContent(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000003001")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000003002")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000003003")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000003004")
	tenantSlug := "default-tenant"

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000003005"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":     true,
			"matchCurrency":   true,
			"matchDate":       true,
			"datePrecision":   "DAY",
			"matchReference":  true,
			"caseInsensitive": true,
		},
	}

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000003006"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000003007"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	outboxRepoEvents := make([]*shared.OutboxEvent, 0)
	outboxRepo := newMockOutboxRepository(
		t,
		func(_ context.Context, _ *sql.Tx, event *shared.OutboxEvent) (*shared.OutboxEvent, error) {
			outboxRepoEvents = append(outboxRepoEvents, event)
			return event, nil
		},
	)
	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepo,
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	ctx = context.WithValue(ctx, auth.TenantSlugKey, tenantSlug)

	run, groups, err := uc.RunMatch(
		ctx,
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Len(t, groups, 1)

	require.Len(t, outboxRepoEvents, 1, "expected one outbox event for confirmed match")

	outboxEvent := outboxRepoEvents[0]
	require.Equal(t, shared.EventTypeMatchConfirmed, outboxEvent.EventType)

	var payload shared.MatchConfirmedEvent
	require.NoError(t, json.Unmarshal(outboxEvent.Payload, &payload))

	require.Equal(t, shared.EventTypeMatchConfirmed, payload.EventType)
	require.Equal(t, tenantID, payload.TenantID)
	require.Equal(t, tenantSlug, payload.TenantSlug)
	require.Equal(t, contextID, payload.ContextID)
	require.Equal(t, run.ID, payload.RunID)
	require.Equal(t, groups[0].ID, payload.MatchID)
	require.Equal(t, rule.ID, payload.RuleID)

	require.Len(t, payload.TransactionIDs, 2)

	expectedTxIDs := []uuid.UUID{leftTx.ID, rightTx.ID}
	sort.Slice(
		expectedTxIDs,
		func(i, j int) bool { return expectedTxIDs[i].String() < expectedTxIDs[j].String() },
	)
	require.Equal(t, expectedTxIDs, payload.TransactionIDs, "transaction IDs should be sorted")

	require.False(t, payload.ConfirmedAt.IsZero())
	require.False(t, payload.Timestamp.IsZero())
}

func TestRunMatch_Locking(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002201")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002202")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002203")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002204")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002205"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":     true,
			"matchCurrency":   true,
			"matchDate":       true,
			"datePrecision":   "DAY",
			"matchReference":  true,
			"caseInsensitive": true,
		},
	}

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002206"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("75.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 13, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002207"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("75.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 13, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}
	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	outboxRepo := newMockOutboxRepository(t, nil)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepo,
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	_, _, err = uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeDryRun,
		},
	)
	require.NoError(t, err)
	require.True(t, lockManager.called)
	require.Equal(t, contextID, lockManager.contextID)
	require.Empty(t, lockManager.gotIDs)
	require.Equal(t, 15*time.Minute, lockManager.gotTTL)
	require.NotNil(t, lockManager.lock)
	require.True(t, lockManager.lock.released)
}

func TestRunMatch_UsesBaseAmountForExpected(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002301")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002302")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002303")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002304")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002305"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":              true,
			"matchCurrency":            true,
			"matchDate":                true,
			"datePrecision":            "DAY",
			"matchReference":           true,
			"caseInsensitive":          true,
			"allowPartial":             true,
			"allocationToleranceMode":  "ABS",
			"allocationToleranceValue": "0",
			"allocationDirection":      "LEFT_TO_RIGHT",
			"allocationUseBaseAmount":  true,
		},
	}

	baseCurrency := "USD"
	leftBaseAmount := decimal.RequireFromString("101.00")
	rightBaseAmount := decimal.RequireFromString("101.00")
	leftTx := &shared.Transaction{
		ID:           uuid.MustParse("00000000-0000-0000-0000-000000002306"),
		SourceID:     ledgerSourceID,
		Amount:       decimal.RequireFromString("100.00"),
		Currency:     "USD",
		AmountBase:   &leftBaseAmount,
		BaseCurrency: &baseCurrency,
		Date:         time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC),
		ExternalID:   "REF",
	}
	rightTx := &shared.Transaction{
		ID:           uuid.MustParse("00000000-0000-0000-0000-000000002307"),
		SourceID:     rightSourceID,
		Amount:       decimal.RequireFromString("100.00"),
		Currency:     "USD",
		AmountBase:   &rightBaseAmount,
		BaseCurrency: &baseCurrency,
		Date:         time.Date(2026, 1, 14, 9, 30, 0, 0, time.UTC),
		ExternalID:   "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToMany,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}
	outboxRepo := newMockOutboxRepository(t, nil)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepo,
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	_, groups, err := uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.True(t, matchItemRepo.called)
	require.Len(t, matchItemRepo.created, 2)

	for _, item := range matchItemRepo.created {
		require.Equal(t, baseCurrency, item.AllocatedCurrency)
		require.True(t, item.ExpectedAmount.Equal(decimal.RequireFromString("101.00")))
	}
}

func TestRunMatch_NoTransactions(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002501")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002502")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002503")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002504")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002505"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":     true,
			"matchCurrency":   true,
			"matchDate":       true,
			"datePrecision":   "DAY",
			"matchReference":  true,
			"caseInsensitive": true,
		},
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	txRepo := &stubTxRepo{transactions: nil}
	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}
	outboxRepo := newMockOutboxRepository(t, nil)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepo,
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	run, groups, err := uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeDryRun,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Empty(t, groups)
	require.Equal(t, matchingVO.MatchRunStatusCompleted, run.Status)
	require.True(t, lockManager.called)
	require.False(t, matchGroupRepo.called)
	require.False(t, matchItemRepo.called)
	require.False(t, exceptionCreator.called)
	require.Equal(t, 0, txRepo.markCalls)
	require.Equal(t, 0, run.Stats["matches"])
	require.Equal(t, 0, run.Stats["unmatched_left"])
	require.Equal(t, 0, run.Stats["unmatched_right"])
	require.Equal(t, 0, run.Stats["auto_matched_left"])
	require.Equal(t, 0, run.Stats["auto_matched_right"])
	require.Equal(t, 0, run.Stats["pending_review_left"])
	require.Equal(t, 0, run.Stats["pending_review_right"])
	require.Equal(t, 0, run.Stats["proposed_left"])
	require.Equal(t, 0, run.Stats["proposed_right"])
}

func TestRunMatch_NoTransactionsCommitCreatesExceptions(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002511")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002512")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002513")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002514")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002515"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":     true,
			"matchCurrency":   true,
			"matchDate":       true,
			"datePrecision":   "DAY",
			"matchReference":  true,
			"caseInsensitive": true,
		},
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002516"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 16, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002517"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("20.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 16, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}
	outboxRepo := newMockOutboxRepository(t, nil)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepo,
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	run, groups, err := uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Empty(t, groups)
	require.False(t, matchGroupRepo.called)
	require.False(t, matchItemRepo.called)
	require.Equal(t, 0, txRepo.markCalls)
	require.True(t, exceptionCreator.called)
	require.Len(t, exceptionCreator.inputs, 2)
	require.Equal(t, 0, run.Stats["matches"])
	require.Equal(t, 1, run.Stats["unmatched_left"])
	require.Equal(t, 1, run.Stats["unmatched_right"])
	require.Equal(t, 0, run.Stats["pending_review_left"])
	require.Equal(t, 0, run.Stats["pending_review_right"])
	require.Equal(t, 0, run.Stats["proposed_left"])
	require.Equal(t, 0, run.Stats["proposed_right"])
	require.Equal(t, matchingVO.MatchRunStatusCompleted, run.Status)
}

func TestFinalizeRunFailure_NilRun(t *testing.T) {
	t.Parallel()

	cause := ErrRunFailed
	err := finalizeRunFailure(context.Background(), &UseCase{}, nil, cause)
	require.ErrorIs(t, err, cause)
}

func TestFinalizeRunFailure_UpdateError(t *testing.T) {
	t.Parallel()

	cause := ErrRunFailed
	updateErr := ErrUpdateFailed

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	matchRunRepo := repositoriesmocks.NewMockMatchRunRepository(ctrl)
	matchRunRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(nil, updateErr)
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(
		context.Background(),
		uuid.New(),
		matchingVO.MatchRunModeCommit,
	)

	require.NoError(t, err)

	err = finalizeRunFailure(context.Background(), uc, run, cause)
	require.Error(t, err)
	require.ErrorIs(t, err, updateErr)
	require.ErrorIs(t, err, cause)
}

func TestValidateRunMatchDependencies(t *testing.T) {
	t.Parallel()

	newValidUseCase := func() *UseCase {
		outboxRepo := newMockOutboxRepository(t, nil)

		return &UseCase{
			contextProvider:  stubContextProvider{},
			sourceProvider:   stubSourceProvider{},
			ruleProvider:     stubRuleProvider{},
			txRepo:           &stubTxRepo{},
			lockManager:      &stubLockManager{},
			matchRunRepo:     &stubMatchRunRepo{},
			matchGroupRepo:   &stubMatchGroupRepo{},
			matchItemRepo:    &stubMatchItemRepo{},
			exceptionCreator: &stubExceptionCreator{},
			outboxRepoTx:     outboxRepo,
			feeVarianceRepo:  &stubFeeVarianceRepo{},
			feeRuleProvider:  &stubFeeRuleProviderWithResult{},
			feeScheduleRepo:  &stubFeeScheduleRepoWithResult{},
		}
	}

	testCases := []struct {
		name     string
		mutate   func(*UseCase)
		expected error
	}{
		{
			name: "nil context provider",
			mutate: func(uc *UseCase) {
				uc.contextProvider = nil
			},
			expected: ErrNilContextRepository,
		},
		{
			name: "nil source provider",
			mutate: func(uc *UseCase) {
				uc.sourceProvider = nil
			},
			expected: ErrNilSourceRepository,
		},
		{
			name: "nil rule provider",
			mutate: func(uc *UseCase) {
				uc.ruleProvider = nil
			},
			expected: ErrNilMatchRuleProvider,
		},
		{
			name: "nil transaction repository",
			mutate: func(uc *UseCase) {
				uc.txRepo = nil
			},
			expected: ErrNilTransactionRepository,
		},
		{
			name: "nil lock manager",
			mutate: func(uc *UseCase) {
				uc.lockManager = nil
			},
			expected: ErrNilLockManager,
		},
		{
			name: "nil match run repository",
			mutate: func(uc *UseCase) {
				uc.matchRunRepo = nil
			},
			expected: ErrNilMatchRunRepository,
		},
		{
			name: "nil match group repository",
			mutate: func(uc *UseCase) {
				uc.matchGroupRepo = nil
			},
			expected: ErrNilMatchGroupRepository,
		},
		{
			name: "nil match item repository",
			mutate: func(uc *UseCase) {
				uc.matchItemRepo = nil
			},
			expected: ErrNilMatchItemRepository,
		},
		{
			name: "nil exception creator",
			mutate: func(uc *UseCase) {
				uc.exceptionCreator = nil
			},
			expected: ErrNilExceptionCreator,
		},
		{
			name: "nil outbox repository",
			mutate: func(uc *UseCase) {
				uc.outboxRepoTx = nil
			},
			expected: ErrOutboxRepoNotConfigured,
		},
		{
			name: "nil fee variance repository",
			mutate: func(uc *UseCase) {
				uc.feeVarianceRepo = nil
			},
			expected: ErrNilFeeVarianceRepository,
		},
		{
			name: "nil fee rule provider",
			mutate: func(uc *UseCase) {
				uc.feeRuleProvider = nil
			},
			expected: ErrNilFeeRuleProvider,
		},
		{
			name: "nil fee schedule repository",
			mutate: func(uc *UseCase) {
				uc.feeScheduleRepo = nil
			},
			expected: ErrNilFeeScheduleRepository,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			uc := newValidUseCase()
			testCase.mutate(uc)

			err := uc.validateRunMatchDependencies()
			require.ErrorIs(t, err, testCase.expected)
		})
	}

	uc := newValidUseCase()
	require.NoError(t, uc.validateRunMatchDependencies())
}

func TestRunMatch_CallOrderCommit(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000009001")

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000009002")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000009003")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000009004")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000009005"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":     true,
			"matchCurrency":   true,
			"matchDate":       true,
			"datePrecision":   "DAY",
			"matchReference":  true,
			"caseInsensitive": true,
		},
	}

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000009006"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000009007"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	order := make([]string, 0)

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}, order: &order}
	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{order: &order}
	matchItemRepo := &stubMatchItemRepo{order: &order}
	exceptionCreator := &stubExceptionCreator{order: &order}

	outboxRepo := newMockOutboxRepository(t, nil)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepo,
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	ucWithProposals := &stubProposalUseCase{UseCase: uc, proposals: []matching.MatchProposal{
		{
			RuleID:   rule.ID,
			LeftIDs:  []uuid.UUID{leftTx.ID},
			RightIDs: []uuid.UUID{rightTx.ID},
			Score:    70,
			Mode:     "1:1",
		},
	}}
	ucWithProposals.bind()

	_, _, err = ucWithProposals.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.NoError(t, err)

	require.Equal(
		t,
		[]string{"create_groups", "create_items", "mark_pending_review", "create_exceptions"},
		order,
	)
	require.Len(t, txRepo.pendingIDs, 2)
}

func TestRunMatch_LockingArgs_IDsAndTTL(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000d001")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-00000000d002")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000d003")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000d004")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-00000000d005"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":     true,
			"matchCurrency":   true,
			"matchDate":       true,
			"datePrecision":   "DAY",
			"matchReference":  true,
			"caseInsensitive": true,
		},
	}

	leftID := uuid.MustParse("00000000-0000-0000-0000-00000000d006")
	rightID := uuid.MustParse("00000000-0000-0000-0000-00000000d007")

	leftTx := &shared.Transaction{
		ID:         leftID,
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         rightID,
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}

	outboxRepo := newMockOutboxRepository(t, nil)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepo,
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	_, _, err = uc.RunMatch(
		ctx,
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeDryRun,
		},
	)
	require.NoError(t, err)

	require.True(t, lockManager.called)
	require.Equal(t, contextID, lockManager.contextID)
	require.Equal(t, 15*time.Minute, lockManager.gotTTL)

	require.Empty(t, lockManager.gotIDs)
}

func TestRunMatch_FeeNormalizationEnabledButNoFeeRules(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000f001")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-00000000f002")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000f003")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000f004")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-00000000f005"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":   true,
			"matchCurrency": true,
		},
	}

	feeNorm := string(fee.NormalizationModeNet)

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:               contextID,
		Type:             shared.ContextTypeOneToOne,
		Active:           true,
		FeeNormalization: &feeNorm,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}
	txs := []*shared.Transaction{
		{ID: uuid.MustParse("00000000-0000-0000-0000-00000000f006"), SourceID: ledgerSourceID, Amount: decimal.RequireFromString("100.00"), Currency: "USD", Date: time.Now().UTC()},
		{ID: uuid.MustParse("00000000-0000-0000-0000-00000000f007"), SourceID: rightSourceID, Amount: decimal.RequireFromString("100.00"), Currency: "USD", Date: time.Now().UTC()},
	}

	// Fee rule provider returns NO rules → triggers ErrFeeRulesRequiredForNormalization.
	feeRuleProvider := &stubFeeRuleProviderWithResult{rules: nil}

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           &stubTxRepo{transactions: txs},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       newMockOutboxRepository(t, nil),
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  feeRuleProvider,
	})
	require.NoError(t, err)

	_, _, err = uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFeeRulesRequiredForNormalization)
}

func TestPrepareMatchRun_LoadsFeeRulesWithoutNormalization(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000f101")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-00000000f102")
	leftSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000f103")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000f104")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-00000000f105")

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: leftSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}
	txs := []*shared.Transaction{
		{ID: uuid.MustParse("00000000-0000-0000-0000-00000000f106"), SourceID: leftSourceID, Amount: decimal.RequireFromString("100.00"), Currency: "USD", Date: time.Now().UTC()},
		{ID: uuid.MustParse("00000000-0000-0000-0000-00000000f107"), SourceID: rightSourceID, Amount: decimal.RequireFromString("100.00"), Currency: "USD", Date: time.Now().UTC()},
	}

	rule, err := fee.NewFeeRule(
		context.Background(),
		contextID,
		scheduleID,
		fee.MatchingSideLeft,
		"left fee rule",
		1,
		nil,
	)
	require.NoError(t, err)

	feeRuleProvider := &stubFeeRuleProviderWithResult{rules: []*fee.FeeRule{rule}}
	feeScheduleRepo := &stubFeeScheduleRepoWithResult{
		schedules: map[uuid.UUID]*fee.FeeSchedule{
			scheduleID: {ID: scheduleID, Currency: "USD"},
		},
	}

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: nil},
		TxRepo:           &stubTxRepo{transactions: txs},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       newMockOutboxRepository(t, nil),
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  feeScheduleRepo,
		FeeRuleProvider:  feeRuleProvider,
	})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	mrc, err := uc.prepareMatchRun(ctx, nil, nil, RunMatchInput{
		TenantID:  tenantID,
		ContextID: contextID,
		Mode:      matchingVO.MatchRunModeCommit,
	})
	require.NoError(t, err)
	require.Len(t, mrc.leftRules, 1)
	require.Empty(t, mrc.rightRules)
	require.Contains(t, mrc.allSchedules, scheduleID)
}

func TestPrepareMatchRun_LoadsFeeRulesForEmptyRun(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000f111")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-00000000f112")
	leftSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000f113")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000f114")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-00000000f115")

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: leftSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	schedule, err := fee.NewFeeSchedule(context.Background(), fee.NewFeeScheduleInput{
		TenantID:         tenantID,
		Name:             "empty-run-fee-schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItemInput{{
			Name:      "flat",
			Priority:  1,
			Structure: fee.FlatFee{Amount: decimal.RequireFromString("10.00")},
		}},
	})
	require.NoError(t, err)
	schedule.ID = scheduleID

	rule, err := fee.NewFeeRule(context.Background(), contextID, scheduleID, fee.MatchingSideAny, "empty-run-rule", 1, nil)
	require.NoError(t, err)

	feeRuleProvider := &stubFeeRuleProviderWithResult{rules: []*fee.FeeRule{rule}}

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: nil},
		TxRepo:           &stubTxRepo{transactions: nil},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       newMockOutboxRepository(t, nil),
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &stubFeeScheduleRepoWithResult{schedules: map[uuid.UUID]*fee.FeeSchedule{scheduleID: schedule}},
		FeeRuleProvider:  feeRuleProvider,
	})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	mrc, err := uc.prepareMatchRun(ctx, nil, nil, RunMatchInput{
		TenantID:  tenantID,
		ContextID: contextID,
		Mode:      matchingVO.MatchRunModeCommit,
	})
	require.NoError(t, err)
	require.Empty(t, mrc.leftCandidates)
	require.Empty(t, mrc.rightCandidates)
	assert.Len(t, mrc.leftRules, 1)
	assert.Len(t, mrc.rightRules, 1)
	assert.Contains(t, mrc.allSchedules, scheduleID)
}

func TestRunMatch_FeeRulesReferenceMissingSchedules(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000f011")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-00000000f012")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000f013")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000f014")
	missingScheduleID := uuid.MustParse("00000000-0000-0000-0000-00000000f015")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-00000000f016"),
		ContextID: contextID,
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":   true,
			"matchCurrency": true,
		},
	}

	feeNorm := string(fee.NormalizationModeNet)

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:               contextID,
		Type:             shared.ContextTypeOneToOne,
		Active:           true,
		FeeNormalization: &feeNorm,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}
	txs := []*shared.Transaction{
		{ID: uuid.MustParse("00000000-0000-0000-0000-00000000f018"), SourceID: ledgerSourceID, Amount: decimal.RequireFromString("100.00"), Currency: "USD", Date: time.Now().UTC()},
		{ID: uuid.MustParse("00000000-0000-0000-0000-00000000f019"), SourceID: rightSourceID, Amount: decimal.RequireFromString("100.00"), Currency: "USD", Date: time.Now().UTC()},
	}

	// Fee rules exist but reference a schedule that does not exist.
	feeRuleProvider := &stubFeeRuleProviderWithResult{
		rules: []*fee.FeeRule{{
			ID:            uuid.MustParse("00000000-0000-0000-0000-00000000f017"),
			ContextID:     contextID,
			Side:          fee.MatchingSideLeft,
			FeeScheduleID: missingScheduleID,
			Name:          "Left rule",
			Priority:      1,
		}},
	}

	// Schedule repo returns empty map → count mismatch triggers ErrFeeRulesReferenceMissingSchedules.
	feeScheduleRepo := &stubFeeScheduleRepoWithResult{
		schedules: map[uuid.UUID]*fee.FeeSchedule{},
	}

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           &stubTxRepo{transactions: txs},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       newMockOutboxRepository(t, nil),
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  feeScheduleRepo,
		FeeRuleProvider:  feeRuleProvider,
	})
	require.NoError(t, err)

	_, _, err = uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFeeRulesReferenceMissingSchedules)
}
