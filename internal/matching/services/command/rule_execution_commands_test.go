//go:build unit

//nolint:dupl
package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	outboxmocks "github.com/LerianStudio/matcher/internal/outbox/domain/repositories/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestExecuteRules_Success(t *testing.T) {
	t.Parallel()

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000001001"),
		ContextID: uuid.MustParse("00000000-0000-0000-0000-000000001002"),
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

	left := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000001003"),
		SourceID:   uuid.MustParse("00000000-0000-0000-0000-000000001004"),
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		ExternalID: "REF",
	}
	right := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000001005"),
		SourceID:   uuid.MustParse("00000000-0000-0000-0000-000000001006"),
		Amount:     decimal.RequireFromString("10.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
		ExternalID: "ref",
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{},
		SourceProvider:   stubSourceProvider{},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           &stubTxRepo{transactions: []*shared.Transaction{left, right}},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       outboxRepo,
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})
	require.NoError(t, err)

	out, err := uc.ExecuteRules(
		context.Background(),
		ExecuteRulesInput{
			ContextID:   rule.ContextID,
			ContextType: shared.ContextTypeOneToOne,
			Left:        []*shared.Transaction{left},
			Right:       []*shared.Transaction{right},
		},
	)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, rule.ID, out[0].RuleID)
	require.Equal(t, []uuid.UUID{left.ID}, out[0].LeftIDs)
	require.Equal(t, []uuid.UUID{right.ID}, out[0].RightIDs)
}

func TestExecuteRules_InvalidContext(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{},
		SourceProvider:   stubSourceProvider{},
		RuleProvider:     stubRuleProvider{},
		TxRepo:           &stubTxRepo{},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       outboxRepo,
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})

	require.NoError(t, err)

	_, err = uc.ExecuteRules(context.Background(), ExecuteRulesInput{})
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestExecuteRules_DecodeError(t *testing.T) {
	t.Parallel()

	rule := &shared.MatchRule{
		ID:        uuid.MustParse("00000000-0000-0000-0000-000000001007"),
		ContextID: uuid.MustParse("00000000-0000-0000-0000-000000001008"),
		Priority:  1,
		Type:      shared.RuleTypeExact,
		Config: map[string]any{
			"datePrecision": 123,
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{},
		SourceProvider:   stubSourceProvider{},
		RuleProvider:     stubRuleProvider{rules: shared.MatchRules{rule}},
		TxRepo:           &stubTxRepo{},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       outboxRepo,
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})
	require.NoError(t, err)

	_, err = uc.ExecuteRules(
		context.Background(),
		ExecuteRulesInput{ContextID: rule.ContextID, ContextType: shared.ContextTypeOneToOne},
	)
	require.ErrorContains(t, err, "datePrecision must be string")
}

func TestExecuteRules_ProviderError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	uc, err := New(UseCaseDeps{
		ContextProvider:  stubContextProvider{},
		SourceProvider:   stubSourceProvider{},
		RuleProvider:     stubRuleProvider{err: matching.ErrNilMatchRule},
		TxRepo:           &stubTxRepo{},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       outboxRepo,
		RateRepo:         &stubRateRepo{},
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
	})
	require.NoError(t, err)

	_, err = uc.ExecuteRules(
		context.Background(),
		ExecuteRulesInput{
			ContextID:   uuid.MustParse("00000000-0000-0000-0000-000000001009"),
			ContextType: shared.ContextTypeOneToOne,
		},
	)
	require.ErrorIs(t, err, matching.ErrNilMatchRule)
}

func TestExecuteByContextTypeDetailed_ManyToMany_ReturnsUnsupported(t *testing.T) {
	t.Parallel()

	engine := matching.NewEngine()

	result, err := executeByContextTypeDetailed(
		engine,
		[]matching.RuleDefinition{},
		[]matching.CandidateTransaction{},
		[]matching.CandidateTransaction{},
		shared.ContextTypeManyToMany,
	)

	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsupportedContextType)
	require.ErrorContains(t, err, "M:N matching is not yet implemented")
}

func TestExecuteByContextTypeDetailed_NilEngine_ReturnsError(t *testing.T) {
	t.Parallel()

	result, err := executeByContextTypeDetailed(
		nil,
		[]matching.RuleDefinition{},
		[]matching.CandidateTransaction{},
		[]matching.CandidateTransaction{},
		shared.ContextTypeOneToOne,
	)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrEngineIsNil)
}

func TestExecuteByContextTypeDetailed_UnknownType_ReturnsUnsupported(t *testing.T) {
	t.Parallel()

	engine := matching.NewEngine()

	result, err := executeByContextTypeDetailed(
		engine,
		[]matching.RuleDefinition{},
		[]matching.CandidateTransaction{},
		[]matching.CandidateTransaction{},
		shared.ContextType("UNKNOWN"),
	)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrUnsupportedContextType)
}
