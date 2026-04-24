// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	outboxmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

func TestRunMatch_DryRun_DoesNotPersistOrMarkTransactions(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000d001")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-00000000d002")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000d003")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000d004")

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

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

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-00000000d006"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-00000000d007"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: &stubExceptionCreator{},
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
	require.Len(t, groups, 1)

	require.False(t, matchGroupRepo.called, "dry-run must not persist groups")
	require.False(t, matchItemRepo.called, "dry-run must not persist items")
	require.Equal(t, 0, txRepo.markCalls, "dry-run must not mark transactions")
}

func TestRunMatch_DryRun_DoesNotPersistFeeArtifacts(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-00000000d101")
	tenantID := uuid.MustParse("00000000-0000-0000-0000-00000000d102")
	ledgerSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000d103")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-00000000d104")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-00000000d105")

	ctxInfo := &ports.ReconciliationContextInfo{
		ID:     contextID,
		Type:   shared.ContextTypeOneToOne,
		Active: true,
	}
	sources := []*ports.SourceInfo{
		{ID: ledgerSourceID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightSourceID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-00000000d106"),
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

	feeSchedule, err := fee.NewFeeSchedule(context.Background(), fee.NewFeeScheduleInput{
		TenantID:         tenantID,
		Name:             "dry-run-fee-schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItemInput{{
			Name:      "flat-fee",
			Priority:  1,
			Structure: fee.FlatFee{Amount: decimal.RequireFromString("10.00")},
		}},
	})
	require.NoError(t, err)
	feeSchedule.ID = scheduleID

	feeRule, err := fee.NewFeeRule(context.Background(), contextID, scheduleID, fee.MatchingSideAny, "dry-run-rule", 1, nil)
	require.NoError(t, err)

	leftTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-00000000d107"),
		SourceID:   ledgerSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
		ExternalID: "REF",
		Metadata: map[string]any{
			"fee": map[string]any{"amount": "12.00", "currency": "USD"},
		},
	}
	rightTx := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-00000000d108"),
		SourceID:   rightSourceID,
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 10, 9, 30, 0, 0, time.UTC),
		ExternalID: "ref",
		Metadata: map[string]any{
			"fee": map[string]any{"amount": "12.00", "currency": "USD"},
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	txRepo := &stubTxRepo{transactions: []*shared.Transaction{leftTx, rightTx}}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}
	exceptionCreator := &stubExceptionCreator{}
	feeVarianceRepo := &stubFeeVarianceRepo{}

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{contextInfo: ctxInfo},
		SourceProvider:   stubSourceProvider{sources: sources},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           txRepo,
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   matchGroupRepo,
		MatchItemRepo:    matchItemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  feeVarianceRepo,
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &stubFeeScheduleRepoWithResult{schedules: map[uuid.UUID]*fee.FeeSchedule{scheduleID: feeSchedule}},
		FeeRuleProvider:  &stubFeeRuleProviderWithResult{rules: []*fee.FeeRule{feeRule}},
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
	require.Len(t, groups, 1)

	require.False(t, feeVarianceRepo.called, "dry-run must not persist fee variances")
	require.False(t, exceptionCreator.called, "dry-run must not create fee exceptions")
	require.False(t, matchGroupRepo.called, "dry-run must not persist groups")
	require.False(t, matchItemRepo.called, "dry-run must not persist items")
	require.Equal(t, 0, txRepo.markCalls, "dry-run must not mark transactions")
}
