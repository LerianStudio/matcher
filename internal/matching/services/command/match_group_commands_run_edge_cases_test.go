//go:build unit

//nolint:dupl
package command

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	outboxmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
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
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.Error(t, err)
	require.NotNil(t, matchRunRepo.updated)
	require.NotNil(t, matchRunRepo.updated.FailureReason)
	require.Equal(t, "group failed", *matchRunRepo.updated.FailureReason)
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
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
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

func TestRunMatch_MalformedProposalScoreFailsRun(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000002651")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000002652")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002653")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000002654")

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000002655"),
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
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002656"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 16, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF-SCORE",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000002657"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 16, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref-score",
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
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	// Proposal has valid transaction IDs but an out-of-range confidence score.
	// ParseConfidenceScore rejects values outside [0, 100]. An invalid score
	// indicates the matching engine produced corrupt output, so the run must fail.
	uc := &stubProposalUseCase{UseCase: baseUseCase, proposals: []matching.MatchProposal{
		{
			RuleID:   rule.ID,
			LeftIDs:  []uuid.UUID{leftTx.ID},
			RightIDs: []uuid.UUID{rightTx.ID},
			Score:    200, // out of range — triggers ParseConfidenceScore error
			Mode:     "1:1",
		},
	}}
	uc.bind()

	_, _, runErr := uc.RunMatch(
		context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String()),
		RunMatchInput{
			TenantID:  tenantID,
			ContextID: contextID,
			Mode:      matchingVO.MatchRunModeCommit,
		},
	)
	require.Error(t, runErr, "invalid proposal score must fail the run")
	require.ErrorIs(t, runErr, matchingVO.ErrConfidenceScoreOutOfRange)
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
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
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
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
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
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
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
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

func TestEnqueueMatchConfirmedEvents_NilTx(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	uc := &UseCase{outboxRepoTx: outboxmocks.NewMockOutboxRepository(ctrl)}

	err := uc.enqueueMatchConfirmedEvents(context.Background(), nil, nil)
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
