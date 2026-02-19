//go:build unit

//nolint:dupl
package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"
	libRedis "github.com/LerianStudio/lib-uncommons/v2/uncommons/redis"

	"github.com/LerianStudio/matcher/internal/auth"
	governanceEntities "github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceRepositories "github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	repositoriesmocks "github.com/LerianStudio/matcher/internal/matching/domain/repositories/mocks"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	outboxmocks "github.com/LerianStudio/matcher/internal/outbox/domain/repositories/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
	}

	outboxRepoEvents := make([]*outboxEntities.OutboxEvent, 0)
	outboxRepo := newMockOutboxRepository(
		t,
		func(_ context.Context, _ *sql.Tx, event *outboxEntities.OutboxEvent) (*outboxEntities.OutboxEvent, error) {
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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

func TestRunMatch_PersistFailureRunsFinalize(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002751")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002752")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002753")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002754")

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
	}

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002755"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 16, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002756"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 16, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002757"),
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

	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{createErr: ErrGroupFailed}
	matchItemRepo := &stubMatchItemRepo{}
	lockManager := &stubLockManager{}
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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
	require.NotNil(t, matchRunRepo.updated)
	require.NotNil(t, matchRunRepo.updated.FailureReason)
	require.Equal(t, "group failed", *matchRunRepo.updated.FailureReason)
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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

func TestRunMatch_InvalidProposalIsSkipped(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002601")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002602")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002603")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002604")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002605"),
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
			"minConfidence":   200,
		},
	}

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002606"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 16, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002607"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 16, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
	}

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	lockManager := &stubLockManager{}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}
	outboxRepo := newMockOutboxRepository(t, nil)

	baseUseCase, err := New(UseCaseDeps{
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})
	require.NoError(t, err)

	unknownLeftID := uuid.MustParse("00000000-0000-0000-0000-000000002608")

	uc := &stubProposalUseCase{UseCase: baseUseCase, proposals: []matching.MatchProposal{
		{
			RuleID:   rule.ID,
			LeftIDs:  []uuid.UUID{unknownLeftID},
			RightIDs: []uuid.UUID{rightTx.ID},
			Score:    100,
			Mode:     "1:1",
		},
	}}
	uc.bind()

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

func TestRunMatch_LockFailure(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002401")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002402")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002403")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002404")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002405"),
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
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002406"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002407"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
	}

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	lockManager := &stubLockManager{err: ErrLockFailed}
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})
	require.NoError(t, err)

	_, groups, err := uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeDryRun,
		},
	)
	require.Error(t, err)
	require.EqualError(t, err, "failed to acquire context lock: lock failed")
	require.Nil(t, groups)
	require.True(t, lockManager.called)
	require.False(t, matchGroupRepo.called)
	require.False(t, matchItemRepo.called)
	require.Equal(t, 0, txRepo.markCalls)
	require.Nil(t, matchRunRepo.updated)
}

func TestRunMatch_SkipsProposalWithMissingTransaction(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002801")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002802")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002803")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002804")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002805"),
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
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002806"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 17, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002807"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 17, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
	}

	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	lockManager := &stubLockManager{}
	exceptionCreator := &stubExceptionCreator{}
	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	outboxRepo := newMockOutboxRepository(t, nil)

	baseUseCase, err := New(UseCaseDeps{
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})
	require.NoError(t, err)

	unknownLeftID := uuid.MustParse("00000000-0000-0000-0000-000000002808")

	uc := &stubProposalUseCase{UseCase: baseUseCase, proposals: []matching.MatchProposal{
		{
			RuleID:   rule.ID,
			LeftIDs:  []uuid.UUID{unknownLeftID},
			RightIDs: []uuid.UUID{rightTx.ID},
			Score:    100,
			Mode:     "1:1",
		},
	}}
	uc.bind()

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

func TestRunMatch_SkipsProposalWithMissingBaseCurrency(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002901")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002902")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002903")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002904")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002905"),
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

	leftBaseAmount := decimal.RequireFromString("10.00")
	rightBaseAmount := decimal.RequireFromString("10.00")
	rightBaseCurrency := "USD"
	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002906"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		AmountBase: &leftBaseAmount,
		Date:       time.Date(2026, 1, 18, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:           uuid.MustParse("00000000-0000-0000-0000-000000002907"),
		SourceID:     rightSourceID,
		Amount:       decimal.RequireFromString("10.00"),
		Currency:     "USD",
		AmountBase:   &rightBaseAmount,
		BaseCurrency: &rightBaseCurrency,
		Date:         time.Date(2026, 1, 18, 9, 30, 0, 0, time.UTC),
		ExternalID:   "ref",
	}

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToMany,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
	}

	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	lockManager := &stubLockManager{}
	exceptionCreator := &stubExceptionCreator{}
	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	outboxRepo := newMockOutboxRepository(t, nil)

	baseUseCase, err := New(UseCaseDeps{
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})
	require.NoError(t, err)

	uc := &stubProposalUseCase{UseCase: baseUseCase, proposals: []matching.MatchProposal{
		{
			RuleID:   rule.ID,
			LeftIDs:  []uuid.UUID{leftTx.ID},
			RightIDs: []uuid.UUID{rightTx.ID},
			Score:    100,
			Mode:     "1:1",
			LeftAllocations: []matching.Allocation{
				{
					TransactionID:   leftTx.ID,
					AllocatedAmount: leftTx.Amount,
					Currency:        leftTx.Currency,
					UseBaseAmount:   true,
				},
			},
		},
	}}
	uc.bind()

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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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

var (
	ErrRunFailed    = ErrGroupFailed
	ErrUpdateFailed = ErrLockFailed
)

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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
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

func uuidStrings(ids []uuid.UUID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}

	return out
}

func newMockOutboxRepository(
	t *testing.T,
	createWithTx func(context.Context, *sql.Tx, *outboxEntities.OutboxEvent) (*outboxEntities.OutboxEvent, error),
) *outboxmocks.MockOutboxRepository {
	t.Helper()

	controller := gomock.NewController(t)
	t.Cleanup(controller.Finish)

	repo := outboxmocks.NewMockOutboxRepository(controller)
	repo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		Return(&outboxEntities.OutboxEvent{}, nil).
		AnyTimes()

	if createWithTx != nil {
		repo.EXPECT().
			CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(createWithTx).
			AnyTimes()
	} else {
		repo.EXPECT().CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(&outboxEntities.OutboxEvent{}, nil).AnyTimes()
	}

	repo.EXPECT().
		ListPending(gomock.Any(), gomock.Any()).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().ListTenants(gomock.Any()).Return([]string{}, nil).AnyTimes()
	repo.EXPECT().GetByID(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	repo.EXPECT().MarkPublished(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	repo.EXPECT().MarkFailed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	repo.EXPECT().
		ListFailedForRetry(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().
		ResetForRetry(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*outboxEntities.OutboxEvent{}, nil).
		AnyTimes()

	return repo
}

type stubContextProvider struct {
	contextInfo *ports.ReconciliationContextInfo
	err         error
}

func (s stubContextProvider) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*ports.ReconciliationContextInfo, error) {
	return s.contextInfo, s.err
}

type stubSourceProvider struct {
	sources []*ports.SourceInfo
	err     error
}

func (s stubSourceProvider) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
) ([]*ports.SourceInfo, error) {
	return s.sources, s.err
}

type stubRuleProvider struct {
	rules shared.MatchRules
	err   error
}

func (s stubRuleProvider) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
) (shared.MatchRules, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.rules, nil
}

type stubProposalUseCase struct {
	*UseCase
	proposals []matching.MatchProposal
	err       error
}

func (stub *stubProposalUseCase) bind() {
	stub.executeRulesDetailed = func(ctx context.Context, in ExecuteRulesInput) (*ExecuteRulesResult, error) {
		if stub.err != nil {
			return nil, stub.err
		}

		if stub.proposals != nil {
			return &ExecuteRulesResult{
				Proposals:     stub.proposals,
				AllocFailures: make(map[uuid.UUID]*matching.AllocationFailure),
			}, nil
		}

		return stub.ExecuteRulesDetailed(ctx, in)
	}
}

type stubTxRepo struct {
	transactions []*shared.Transaction
	markCalls    int
	pendingCalls int
	markedIDs    []uuid.UUID
	pendingIDs   []uuid.UUID
	listErr      error
	markErr      error
	order        *[]string
}

func (stub *stubTxRepo) ListUnmatchedByContext(
	_ context.Context,
	_ uuid.UUID,
	_, _ *time.Time,
	_, _ int,
) ([]*shared.Transaction, error) {
	if stub.listErr != nil {
		return nil, stub.listErr
	}

	if stub.transactions == nil {
		return nil, nil
	}

	return stub.transactions, nil
}

func (stub *stubTxRepo) FindByContextAndIDs(
	_ context.Context,
	_ uuid.UUID,
	transactionIDs []uuid.UUID,
) ([]*shared.Transaction, error) {
	if stub.listErr != nil {
		return nil, stub.listErr
	}

	if stub.transactions == nil {
		return nil, nil
	}

	result := make([]*shared.Transaction, 0)

	for _, txn := range stub.transactions {
		for _, id := range transactionIDs {
			if txn.ID == id {
				result = append(result, txn)

				break
			}
		}
	}

	return result, nil
}

func (stub *stubTxRepo) MarkMatched(
	_ context.Context,
	_ uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	stub.markCalls++

	if stub.order != nil {
		*stub.order = append(*stub.order, "mark_matched")
	}

	if stub.markErr != nil {
		return stub.markErr
	}

	copiedIDs := append([]uuid.UUID{}, transactionIDs...)
	stub.markedIDs = append(stub.markedIDs, copiedIDs...)

	return nil
}

func (stub *stubTxRepo) MarkMatchedWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	return stub.MarkMatched(ctx, contextID, transactionIDs)
}

func (stub *stubTxRepo) MarkPendingReview(
	_ context.Context,
	_ uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	stub.pendingCalls++

	if stub.order != nil {
		*stub.order = append(*stub.order, "mark_pending_review")
	}

	if stub.markErr != nil {
		return stub.markErr
	}

	copiedIDs := append([]uuid.UUID{}, transactionIDs...)
	stub.pendingIDs = append(stub.pendingIDs, copiedIDs...)

	return nil
}

func (stub *stubTxRepo) MarkPendingReviewWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	return stub.MarkPendingReview(ctx, contextID, transactionIDs)
}

func (stub *stubTxRepo) MarkUnmatched(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return nil
}

func (stub *stubTxRepo) MarkUnmatchedWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (stub *stubTxRepo) WithTx(_ context.Context, fn func(matchingRepositories.Tx) error) error {
	if fn == nil {
		return nil
	}

	return fn(new(sql.Tx))
}

type stubLock struct {
	released bool
}

func (s *stubLock) Release(_ context.Context) error {
	s.released = true
	return nil
}

type stubLockManager struct {
	lock      *stubLock
	called    bool
	err       error
	contextID uuid.UUID
	gotIDs    []uuid.UUID
	gotTTL    time.Duration
}

var ErrLockFailed = errors.New("lock failed")

func (stub *stubLockManager) AcquireTransactionsLock(
	_ context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
	ttl time.Duration,
) (ports.Lock, error) {
	stub.called = true
	stub.contextID = contextID

	stub.gotIDs = append([]uuid.UUID{}, transactionIDs...)
	stub.gotTTL = ttl

	if stub.err != nil {
		return nil, stub.err
	}

	stub.lock = &stubLock{}

	return stub.lock, nil
}

func (stub *stubLockManager) AcquireContextLock(
	_ context.Context,
	contextID uuid.UUID,
	ttl time.Duration,
) (ports.Lock, error) {
	stub.called = true
	stub.contextID = contextID
	stub.gotTTL = ttl
	stub.gotIDs = nil

	if stub.err != nil {
		return nil, stub.err
	}

	stub.lock = &stubLock{}

	return stub.lock, nil
}

type stubMatchRunRepo struct {
	created   *matchingEntities.MatchRun
	updated   *matchingEntities.MatchRun
	createErr error
	updateErr error
}

func (s *stubMatchRunRepo) Create(
	_ context.Context,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	s.created = entity
	if s.createErr != nil {
		return nil, s.createErr
	}

	return entity, nil
}

func (s *stubMatchRunRepo) CreateWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return s.Create(ctx, entity)
}

func (s *stubMatchRunRepo) Update(
	_ context.Context,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	s.updated = entity
	if s.updateErr != nil {
		return nil, s.updateErr
	}

	return entity, nil
}

func (s *stubMatchRunRepo) UpdateWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	entity *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return s.Update(ctx, entity)
}

func (s *stubMatchRunRepo) WithTx(_ context.Context, fn func(matchingRepositories.Tx) error) error {
	if fn == nil {
		return nil
	}

	return fn(new(sql.Tx))
}

func (s *stubMatchRunRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

func (s *stubMatchRunRepo) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.MatchRun, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

var ErrGroupFailed = errors.New("group failed")

type stubMatchGroupRepo struct {
	created   []*matchingEntities.MatchGroup
	called    bool
	createErr error
	order     *[]string
}

func (stub *stubMatchGroupRepo) CreateBatch(
	_ context.Context,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	stub.called = true
	stub.created = groups

	if stub.order != nil {
		*stub.order = append(*stub.order, "create_groups")
	}

	if stub.createErr != nil {
		return nil, stub.createErr
	}

	return groups, nil
}

func (stub *stubMatchGroupRepo) CreateBatchWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return stub.CreateBatch(ctx, groups)
}

func (stub *stubMatchGroupRepo) ListByRunID(
	_ context.Context,
	_, _ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.MatchGroup, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (stub *stubMatchGroupRepo) FindByID(
	_ context.Context,
	_, id uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	for _, g := range stub.created {
		if g.ID == id {
			return g, nil
		}
	}

	return nil, nil
}

func (stub *stubMatchGroupRepo) Update(
	_ context.Context,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	return group, nil
}

func (stub *stubMatchGroupRepo) UpdateWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	return stub.Update(ctx, group)
}

type stubMatchItemRepo struct {
	created   []*matchingEntities.MatchItem
	called    bool
	createErr error
	order     *[]string
}

func (stub *stubMatchItemRepo) CreateBatch(
	_ context.Context,
	items []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	stub.called = true
	stub.created = items

	if stub.order != nil {
		*stub.order = append(*stub.order, "create_items")
	}

	if stub.createErr != nil {
		return nil, stub.createErr
	}

	return items, nil
}

func (stub *stubMatchItemRepo) CreateBatchWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	items []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	return stub.CreateBatch(ctx, items)
}

func (stub *stubMatchItemRepo) ListByMatchGroupID(
	_ context.Context,
	matchGroupID uuid.UUID,
) ([]*matchingEntities.MatchItem, error) {
	result := make([]*matchingEntities.MatchItem, 0)

	for _, item := range stub.created {
		if item.MatchGroupID == matchGroupID {
			result = append(result, item)
		}
	}

	return result, nil
}

func (stub *stubMatchItemRepo) ListByMatchGroupIDs(
	_ context.Context,
	matchGroupIDs []uuid.UUID,
) (map[uuid.UUID][]*matchingEntities.MatchItem, error) {
	result := make(map[uuid.UUID][]*matchingEntities.MatchItem, len(matchGroupIDs))

	for _, groupID := range matchGroupIDs {
		for _, item := range stub.created {
			if item.MatchGroupID == groupID {
				result[groupID] = append(result[groupID], item)
			}
		}
	}

	return result, nil
}

type stubExceptionCreator struct {
	called    bool
	contextID uuid.UUID
	runID     uuid.UUID
	inputs    []ports.ExceptionTransactionInput
	err       error
	order     *[]string
}

func (stub *stubExceptionCreator) CreateExceptions(
	_ context.Context,
	contextID, runID uuid.UUID,
	inputs []ports.ExceptionTransactionInput,
	_ []string,
) error {
	stub.called = true
	stub.contextID = contextID
	stub.runID = runID

	if stub.order != nil {
		*stub.order = append(*stub.order, "create_exceptions")
	}

	stub.inputs = append([]ports.ExceptionTransactionInput{}, inputs...)

	return stub.err
}

func (stub *stubExceptionCreator) CreateExceptionsWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	contextID, runID uuid.UUID,
	inputs []ports.ExceptionTransactionInput,
	regulatorySourceTypes []string,
) error {
	return stub.CreateExceptions(ctx, contextID, runID, inputs, regulatorySourceTypes)
}

type stubRateRepo struct {
	rate *fee.Rate
	err  error
}

func (s *stubRateRepo) GetByID(_ context.Context, _ uuid.UUID) (*fee.Rate, error) {
	return s.rate, s.err
}

type stubFeeVarianceRepo struct {
	variances []*matchingEntities.FeeVariance
	err       error
	called    bool
}

func (s *stubFeeVarianceRepo) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	rows []*matchingEntities.FeeVariance,
) ([]*matchingEntities.FeeVariance, error) {
	s.called = true
	s.variances = append(s.variances, rows...)

	return rows, s.err
}

type stubAdjustmentRepo struct{}

func (s *stubAdjustmentRepo) Create(
	_ context.Context,
	adj *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (s *stubAdjustmentRepo) CreateWithTx(
	_ context.Context,
	_ any,
	adj *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (s *stubAdjustmentRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.Adjustment, error) {
	return nil, nil
}

func (s *stubAdjustmentRepo) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.Adjustment, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubAdjustmentRepo) ListByMatchGroupID(
	_ context.Context,
	_, _ uuid.UUID,
) ([]*matchingEntities.Adjustment, error) {
	return nil, nil
}

// stubInfraProviderForRun implements sharedPorts.InfrastructureProvider for testing.
type stubInfraProviderForRun struct {
	tx  *sql.Tx
	err error
}

func (s *stubInfraProviderForRun) GetPostgresConnection(
	_ context.Context,
) (*libPostgres.Client, error) {
	return nil, nil
}

func (s *stubInfraProviderForRun) GetRedisConnection(
	_ context.Context,
) (*libRedis.Client, error) {
	return nil, nil
}

func (s *stubInfraProviderForRun) BeginTx(_ context.Context) (*sql.Tx, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.tx, nil
}

func (s *stubInfraProviderForRun) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

// stubAuditLogRepoForRun implements governanceRepositories.AuditLogRepository for testing.
type stubAuditLogRepoForRun struct {
	createErr error
}

func (s *stubAuditLogRepoForRun) Create(
	_ context.Context,
	auditLog *governanceEntities.AuditLog,
) (*governanceEntities.AuditLog, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}

	return auditLog, nil
}

func (s *stubAuditLogRepoForRun) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	auditLog *governanceEntities.AuditLog,
) (*governanceEntities.AuditLog, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}

	return auditLog, nil
}

func (s *stubAuditLogRepoForRun) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*governanceEntities.AuditLog, error) {
	return nil, nil
}

func (s *stubAuditLogRepoForRun) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*governanceEntities.AuditLog, string, error) {
	return nil, "", nil
}

func (s *stubAuditLogRepoForRun) List(
	_ context.Context,
	_ governanceEntities.AuditLogFilter,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*governanceEntities.AuditLog, string, error) {
	return nil, "", nil
}

var (
	_ ports.ContextProvider                      = (*stubContextProvider)(nil)
	_ ports.SourceProvider                       = (*stubSourceProvider)(nil)
	_ ports.MatchRuleProvider                    = (*stubRuleProvider)(nil)
	_ ports.TransactionRepository                = (*stubTxRepo)(nil)
	_ ports.LockManager                          = (*stubLockManager)(nil)
	_ matchingRepositories.MatchRunRepository    = (*stubMatchRunRepo)(nil)
	_ matchingRepositories.MatchGroupRepository  = (*stubMatchGroupRepo)(nil)
	_ matchingRepositories.MatchItemRepository   = (*stubMatchItemRepo)(nil)
	_ ports.ExceptionCreator                     = (*stubExceptionCreator)(nil)
	_ matchingRepositories.RateRepository        = (*stubRateRepo)(nil)
	_ matchingRepositories.FeeVarianceRepository = (*stubFeeVarianceRepo)(nil)
	_ matchingRepositories.AdjustmentRepository  = (*stubAdjustmentRepo)(nil)
	_ sharedPorts.InfrastructureProvider         = (*stubInfraProviderForRun)(nil)
	_ governanceRepositories.AuditLogRepository  = (*stubAuditLogRepoForRun)(nil)
)

func TestRunMatch_ContextCancelled(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000cc01")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-00000000cc02")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000cc03")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000cc04")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-00000000cc05"),
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
		ID:         uuid.MustParse("00000000-0000-0000-0000-00000000cc06"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-00000000cc07"),
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger},
		{ID: rightSourceID, Type: ports.SourceTypeFile},
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
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	cancel() // Cancel immediately before running

	_, _, err = uc.RunMatch(ctx, RunMatchInput{
		TenantID:  tenantID,
		ContextID: contextID,
		Mode:      matchingVO.MatchRunModeCommit,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextCancelled)
}

func TestEnqueueMatchConfirmedEvents_NilOutboxRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{outboxRepoTx: nil}
	err := uc.enqueueMatchConfirmedEvents(context.Background(), new(sql.Tx), nil)
	require.ErrorIs(t, err, ErrOutboxRepoNotConfigured)
}

type fakeTx struct{}

func TestEnqueueMatchConfirmedEvents_NonSQLTx(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	err := uc.enqueueMatchConfirmedEvents(context.Background(), &fakeTx{}, nil)
	require.ErrorIs(t, err, ErrOutboxRequiresSQLTx)
}

func TestEnqueueMatchConfirmedEvents_InvalidTenantID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	confidence, _ := matchingVO.ParseConfidenceScore(90)
	now := time.Now().UTC()
	groups := []*matchingEntities.MatchGroup{
		{
			ID:          uuid.New(),
			ContextID:   uuid.New(),
			RunID:       uuid.New(),
			RuleID:      uuid.New(),
			Status:      matchingVO.MatchGroupStatusConfirmed,
			Confidence:  confidence,
			ConfirmedAt: &now,
			Items:       []*matchingEntities.MatchItem{{TransactionID: uuid.New()}},
		},
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")
	err := uc.enqueueMatchConfirmedEvents(ctx, new(sql.Tx), groups)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse tenant id")
}

func TestEnqueueGroupEvent_NilGroup(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}
	err := uc.enqueueGroupEvent(context.Background(), new(sql.Tx), nil, uuid.New(), "slug")
	require.NoError(t, err)
}

func TestEnqueueGroupEvent_NonConfirmedStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	confidence, _ := matchingVO.ParseConfidenceScore(50)
	group := &matchingEntities.MatchGroup{
		ID:         uuid.New(),
		ContextID:  uuid.New(),
		RunID:      uuid.New(),
		RuleID:     uuid.New(),
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
		Items:      []*matchingEntities.MatchItem{{TransactionID: uuid.New()}},
	}

	err := uc.enqueueGroupEvent(context.Background(), new(sql.Tx), group, uuid.New(), "slug")
	require.NoError(t, err)
}

func TestEnqueueGroupEvent_EventCreationError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	confidence, _ := matchingVO.ParseConfidenceScore(90)
	group := &matchingEntities.MatchGroup{
		ID:          uuid.New(),
		ContextID:   uuid.New(),
		RunID:       uuid.New(),
		RuleID:      uuid.New(),
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: nil,
		Items:       []*matchingEntities.MatchItem{{TransactionID: uuid.New()}},
	}

	err := uc.enqueueGroupEvent(context.Background(), new(sql.Tx), group, uuid.New(), "slug")
	require.Error(t, err)
	require.Contains(t, err.Error(), "build match confirmed event")
}
