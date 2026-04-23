//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	outboxmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrNilContextRepository", ErrNilContextRepository, "context repository is required"},
		{"ErrNilSourceRepository", ErrNilSourceRepository, "source repository is required"},
		{"ErrNilMatchRuleProvider", ErrNilMatchRuleProvider, "match rule provider is required"},
		{
			"ErrNilTransactionRepository",
			ErrNilTransactionRepository,
			"transaction repository is required",
		},
		{"ErrNilLockManager", ErrNilLockManager, "lock manager is required"},
		{"ErrNilMatchRunRepository", ErrNilMatchRunRepository, "match run repository is required"},
		{
			"ErrNilMatchGroupRepository",
			ErrNilMatchGroupRepository,
			"match group repository is required",
		},
		{
			"ErrNilMatchItemRepository",
			ErrNilMatchItemRepository,
			"match item repository is required",
		},
		{"ErrNilExceptionCreator", ErrNilExceptionCreator, "exception creator is required"},
		{"ErrNilOutboxRepository", ErrNilOutboxRepository, "outbox repository is required"},
		{
			"ErrNilFeeVarianceRepository",
			ErrNilFeeVarianceRepository,
			"fee variance repository is required",
		},
		{
			"ErrNilAdjustmentRepository",
			ErrNilAdjustmentRepository,
			"adjustment repository is required",
		},
		{
			"ErrNilInfrastructureProvider",
			ErrNilInfrastructureProvider,
			"infrastructure provider is required",
		},
		{"ErrNilAuditLogRepository", ErrNilAuditLogRepository, "audit log repository is required"},
		{"ErrNilFeeScheduleRepository", ErrNilFeeScheduleRepository, "fee schedule repository is required"},
		{"ErrNilFeeRuleProvider", ErrNilFeeRuleProvider, "fee rule provider is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

type mockContextProvider struct{}

func (m *mockContextProvider) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*ports.ReconciliationContextInfo, error) {
	return &ports.ReconciliationContextInfo{}, nil
}

type mockSourceProvider struct{}

func (m *mockSourceProvider) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
) ([]*ports.SourceInfo, error) {
	return nil, nil
}

type mockRuleProvider struct{}

func (m *mockRuleProvider) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
) (shared.MatchRules, error) {
	return nil, nil
}

type mockTransactionRepository struct{}

func (m *mockTransactionRepository) ListUnmatchedByContext(
	_ context.Context,
	_ uuid.UUID,
	_, _ *time.Time,
	_, _ int,
) ([]*shared.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionRepository) MarkMatchedWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (m *mockTransactionRepository) MarkPendingReviewWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (m *mockTransactionRepository) MarkUnmatchedWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (m *mockTransactionRepository) FindByContextAndIDs(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
) ([]*shared.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionRepository) WithTx(
	_ context.Context,
	_ func(matchingRepositories.Tx) error,
) error {
	return nil
}

type mockLock struct{}

func (m *mockLock) Release(_ context.Context) error { return nil }

type mockLockManager struct{}

func (m *mockLockManager) AcquireTransactionsLock(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
	_ time.Duration,
) (ports.Lock, error) {
	return &mockLock{}, nil
}

func (m *mockLockManager) AcquireContextLock(
	_ context.Context,
	_ uuid.UUID,
	_ time.Duration,
) (ports.Lock, error) {
	return &mockLock{}, nil
}

type mockMatchRunRepository struct{}

func (m *mockMatchRunRepository) Create(
	_ context.Context,
	_ *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

func (m *mockMatchRunRepository) CreateWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

func (m *mockMatchRunRepository) Update(
	_ context.Context,
	_ *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

func (m *mockMatchRunRepository) UpdateWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

func (m *mockMatchRunRepository) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

func (m *mockMatchRunRepository) WithTx(
	_ context.Context,
	_ func(matchingRepositories.Tx) error,
) error {
	return nil
}

func (m *mockMatchRunRepository) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.MatchRun, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

type mockMatchGroupRepository struct{}

func (m *mockMatchGroupRepository) CreateBatch(
	_ context.Context,
	_ []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return nil, nil
}

func (m *mockMatchGroupRepository) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return nil, nil
}

func (m *mockMatchGroupRepository) ListByRunID(
	_ context.Context,
	_, _ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.MatchGroup, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockMatchGroupRepository) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	return nil, nil
}

func (m *mockMatchGroupRepository) Update(
	_ context.Context,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	return group, nil
}

func (m *mockMatchGroupRepository) UpdateWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	return m.Update(ctx, group)
}

type mockMatchItemRepository struct{}

func (m *mockMatchItemRepository) CreateBatch(
	_ context.Context,
	_ []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	return nil, nil
}

func (m *mockMatchItemRepository) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	return nil, nil
}

func (m *mockMatchItemRepository) ListByMatchGroupID(
	_ context.Context,
	_ uuid.UUID,
) ([]*matchingEntities.MatchItem, error) {
	return nil, nil
}

func (m *mockMatchItemRepository) ListByMatchGroupIDs(
	_ context.Context,
	_ []uuid.UUID,
) (map[uuid.UUID][]*matchingEntities.MatchItem, error) {
	return make(map[uuid.UUID][]*matchingEntities.MatchItem), nil
}

type mockExceptionCreator struct{}

func (m *mockExceptionCreator) CreateExceptions(
	_ context.Context,
	_, _ uuid.UUID,
	_ []ports.ExceptionTransactionInput,
	_ []string,
) error {
	return nil
}

func (m *mockExceptionCreator) CreateExceptionsWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_, _ uuid.UUID,
	_ []ports.ExceptionTransactionInput,
	_ []string,
) error {
	return nil
}

type mockFeeVarianceRepo struct{}

func (m *mockFeeVarianceRepo) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	rows []*matchingEntities.FeeVariance,
) ([]*matchingEntities.FeeVariance, error) {
	return rows, nil
}

var _ matchingRepositories.FeeVarianceRepository = (*mockFeeVarianceRepo)(nil)

type mockAdjustmentRepo struct{}

func (m *mockAdjustmentRepo) Create(
	_ context.Context,
	adj *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (m *mockAdjustmentRepo) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	adj *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (m *mockAdjustmentRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.Adjustment, error) {
	return nil, nil
}

func (m *mockAdjustmentRepo) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.Adjustment, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockAdjustmentRepo) CreateWithAuditLog(
	_ context.Context,
	adj *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (m *mockAdjustmentRepo) CreateWithAuditLogWithTx(
	_ context.Context,
	_ *sql.Tx,
	adj *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (m *mockAdjustmentRepo) ListByMatchGroupID(
	_ context.Context,
	_, _ uuid.UUID,
) ([]*matchingEntities.Adjustment, error) {
	return nil, nil
}

var _ matchingRepositories.AdjustmentRepository = (*mockAdjustmentRepo)(nil)

type mockFeeScheduleRepo struct{}

func (m *mockFeeScheduleRepo) Create(
	_ context.Context,
	s *fee.FeeSchedule,
) (*fee.FeeSchedule, error) {
	return s, nil
}

func (m *mockFeeScheduleRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*fee.FeeSchedule, error) {
	return nil, nil
}

func (m *mockFeeScheduleRepo) Update(
	_ context.Context,
	s *fee.FeeSchedule,
) (*fee.FeeSchedule, error) {
	return s, nil
}

func (m *mockFeeScheduleRepo) Delete(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (m *mockFeeScheduleRepo) List(_ context.Context, _ int) ([]*fee.FeeSchedule, error) {
	return nil, nil
}

func (m *mockFeeScheduleRepo) GetByIDs(
	_ context.Context,
	_ []uuid.UUID,
) (map[uuid.UUID]*fee.FeeSchedule, error) {
	return nil, nil
}

var _ sharedPorts.FeeScheduleRepository = (*mockFeeScheduleRepo)(nil)

type mockFeeRuleProvider struct{}

func (m *mockFeeRuleProvider) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
) ([]*fee.FeeRule, error) {
	return nil, nil
}

var _ ports.FeeRuleProvider = (*mockFeeRuleProvider)(nil)

type mockInfraProvider struct {
	tx  *sql.Tx
	err error
}

func (m *mockInfraProvider) GetRedisConnection(
	_ context.Context,
) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (m *mockInfraProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	if m.err != nil {
		return nil, m.err
	}

	return sharedPorts.NewTxLease(m.tx, nil), nil
}

func (m *mockInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (m *mockInfraProvider) GetPrimaryDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

type mockAuditLogRepo struct {
	createErr error
}

func (m *mockAuditLogRepo) Create(
	_ context.Context,
	auditLog *shared.AuditLog,
) (*shared.AuditLog, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}

	return auditLog, nil
}

func (m *mockAuditLogRepo) CreateWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	auditLog *shared.AuditLog,
) (*shared.AuditLog, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}

	return auditLog, nil
}

func (m *mockAuditLogRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.AuditLog, error) {
	return nil, nil
}

func (m *mockAuditLogRepo) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*shared.AuditLog, string, error) {
	return nil, "", nil
}

func (m *mockAuditLogRepo) List(
	_ context.Context,
	_ shared.AuditLogFilter,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*shared.AuditLog, string, error) {
	return nil, "", nil
}

func TestNewUseCase(t *testing.T) {
	t.Parallel()

	validDeps := func() UseCaseDeps {
		ctrl := gomock.NewController(t)
		outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)
		outboxRepo.EXPECT().
			Create(gomock.Any(), gomock.Any()).
			Return(&shared.OutboxEvent{}, nil).
			AnyTimes()
		outboxRepo.EXPECT().
			CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&shared.OutboxEvent{}, nil).
			AnyTimes()
		outboxRepo.EXPECT().
			ListPending(gomock.Any(), gomock.Any()).
			Return([]*shared.OutboxEvent{}, nil).
			AnyTimes()
		outboxRepo.EXPECT().ListTenants(gomock.Any()).Return([]string{}, nil).AnyTimes()
		outboxRepo.EXPECT().GetByID(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		outboxRepo.EXPECT().
			MarkPublished(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			AnyTimes()
		outboxRepo.EXPECT().
			MarkFailed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			AnyTimes()
		outboxRepo.EXPECT().
			ListFailedForRetry(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return([]*shared.OutboxEvent{}, nil).
			AnyTimes()
		outboxRepo.EXPECT().
			ResetForRetry(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return([]*shared.OutboxEvent{}, nil).
			AnyTimes()
		outboxRepo.EXPECT().
			ResetStuckProcessing(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return([]*shared.OutboxEvent{}, nil).
			AnyTimes()
		outboxRepo.EXPECT().
			MarkInvalid(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			AnyTimes()

		return UseCaseDeps{
			ContextProvider:  &mockContextProvider{},
			SourceProvider:   &mockSourceProvider{},
			RuleProvider:     &mockRuleProvider{},
			TxRepo:           &mockTransactionRepository{},
			LockManager:      &mockLockManager{},
			MatchRunRepo:     &mockMatchRunRepository{},
			MatchGroupRepo:   &mockMatchGroupRepository{},
			MatchItemRepo:    &mockMatchItemRepository{},
			ExceptionCreator: &mockExceptionCreator{},
			OutboxRepo:       outboxRepo,
			FeeVarianceRepo:  &mockFeeVarianceRepo{},
			AdjustmentRepo:   &mockAdjustmentRepo{},
			InfraProvider:    &mockInfraProvider{},
			AuditLogRepo:     &mockAuditLogRepo{},
			FeeScheduleRepo:  &mockFeeScheduleRepo{},
			FeeRuleProvider:  &mockFeeRuleProvider{},
		}
	}

	t.Run("success with all dependencies", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()

		uc, err := New(deps)

		require.NoError(t, err)
		require.NotNil(t, uc)
	})

	t.Run("nil context provider returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.ContextProvider = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilContextRepository)
	})

	t.Run("nil source provider returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.SourceProvider = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilSourceRepository)
	})

	t.Run("nil rule provider returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.RuleProvider = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilMatchRuleProvider)
	})

	t.Run("nil transaction repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.TxRepo = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilTransactionRepository)
	})

	t.Run("nil lock manager returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.LockManager = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilLockManager)
	})

	t.Run("nil match run repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.MatchRunRepo = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilMatchRunRepository)
	})

	t.Run("nil match group repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.MatchGroupRepo = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilMatchGroupRepository)
	})

	t.Run("nil match item repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.MatchItemRepo = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilMatchItemRepository)
	})

	t.Run("nil exception creator returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.ExceptionCreator = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilExceptionCreator)
	})

	t.Run("nil outbox repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.OutboxRepo = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilOutboxRepository)
	})

	t.Run("typed-nil outbox repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		var typedNilOutboxRepo *outboxmocks.MockOutboxRepository
		deps.OutboxRepo = typedNilOutboxRepo

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilOutboxRepository)
	})

	t.Run("nil fee variance repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.FeeVarianceRepo = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilFeeVarianceRepository)
	})

	t.Run("nil adjustment repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.AdjustmentRepo = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilAdjustmentRepository)
	})

	t.Run("nil fee schedule repository returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.FeeScheduleRepo = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
	})

	t.Run("nil fee rule provider returns error", func(t *testing.T) {
		t.Parallel()

		deps := validDeps()
		deps.FeeRuleProvider = nil

		uc, err := New(deps)

		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilFeeRuleProvider)
	})
}

func TestUseCaseFieldsInitialized(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)
	outboxRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		Return(&shared.OutboxEvent{}, nil).
		AnyTimes()
	outboxRepo.EXPECT().
		CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&shared.OutboxEvent{}, nil).
		AnyTimes()
	outboxRepo.EXPECT().
		ListPending(gomock.Any(), gomock.Any()).
		Return([]*shared.OutboxEvent{}, nil).
		AnyTimes()
	outboxRepo.EXPECT().ListTenants(gomock.Any()).Return([]string{}, nil).AnyTimes()
	outboxRepo.EXPECT().GetByID(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	outboxRepo.EXPECT().
		MarkPublished(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	outboxRepo.EXPECT().MarkFailed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	outboxRepo.EXPECT().
		ListFailedForRetry(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*shared.OutboxEvent{}, nil).
		AnyTimes()
	outboxRepo.EXPECT().
		ResetForRetry(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*shared.OutboxEvent{}, nil).
		AnyTimes()
	outboxRepo.EXPECT().
		ResetStuckProcessing(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*shared.OutboxEvent{}, nil).
		AnyTimes()

	uc, err := New(UseCaseDeps{
		ContextProvider:  &mockContextProvider{},
		SourceProvider:   &mockSourceProvider{},
		RuleProvider:     &mockRuleProvider{},
		TxRepo:           &mockTransactionRepository{},
		LockManager:      &mockLockManager{},
		MatchRunRepo:     &mockMatchRunRepository{},
		MatchGroupRepo:   &mockMatchGroupRepository{},
		MatchItemRepo:    &mockMatchItemRepository{},
		ExceptionCreator: &mockExceptionCreator{},
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &mockFeeVarianceRepo{},
		AdjustmentRepo:   &mockAdjustmentRepo{},
		InfraProvider:    &mockInfraProvider{},
		AuditLogRepo:     &mockAuditLogRepo{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})

	require.NoError(t, err)
	require.NotNil(t, uc)

	assert.NotNil(t, uc.contextProvider)
	assert.NotNil(t, uc.sourceProvider)
	assert.NotNil(t, uc.ruleProvider)
	assert.NotNil(t, uc.txRepo)
	assert.NotNil(t, uc.lockManager)
	assert.NotNil(t, uc.matchRunRepo)
	assert.NotNil(t, uc.matchGroupRepo)
	assert.NotNil(t, uc.matchItemRepo)
	assert.NotNil(t, uc.exceptionCreator)
	assert.NotNil(t, uc.outboxRepo)
	assert.NotNil(t, uc.feeVarianceRepo)
	assert.NotNil(t, uc.adjustmentRepo)
	assert.NotNil(t, uc.feeScheduleRepo)
	assert.NotNil(t, uc.executeRules)
	assert.Greater(t, uc.lockRefreshInterval, time.Duration(0))
	assert.Positive(t, uc.maxLockBatchSize)
}
