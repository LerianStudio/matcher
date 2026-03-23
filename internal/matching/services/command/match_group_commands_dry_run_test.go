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
	outboxmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
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
		RateRepo:         &stubRateRepo{},
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
