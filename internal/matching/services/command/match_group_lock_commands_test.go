//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestEnsureLockFresh_RefreshFailed_AtomicTrue(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	failed := &atomic.Bool{}
	failed.Store(true)

	err := uc.ensureLockFresh(context.Background(), nil, &mockLock{}, failed)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
}

func TestEnsureLockFresh_NonRefreshableLock_NoError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.ensureLockFresh(context.Background(), nil, &mockLock{}, &atomic.Bool{})
	require.NoError(t, err)
}

func TestEnsureLockFresh_NilLockArg(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.ensureLockFresh(context.Background(), nil, nil, &atomic.Bool{})
	require.NoError(t, err)
}

func TestEnsureLockFresh_NilRefreshFailedArg(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.ensureLockFresh(context.Background(), nil, &mockLock{}, nil)
	require.NoError(t, err)
}

func TestEnsureLockFresh_RefreshableSuccess(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	lock := &stubRefreshableLockForLockTest{}

	err := uc.ensureLockFresh(context.Background(), nil, lock, &atomic.Bool{})
	require.NoError(t, err)
}

func TestEnsureLockFresh_RefreshableFailure_SetsAtomicFlag(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	lock := &stubRefreshableLockForLockTest{refreshErr: errors.New("redis timeout")}
	failed := &atomic.Bool{}

	err := uc.ensureLockFresh(context.Background(), nil, lock, failed)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
	assert.True(t, failed.Load())
}

func TestValidateRunMatchDependencies_AllNil(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.validateRunMatchDependencies()
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestValidateRunMatchDependencies_AllPresent(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextProvider:  &mockContextProvider{},
		sourceProvider:   &mockSourceProvider{},
		ruleProvider:     &mockRuleProvider{},
		txRepo:           &mockTransactionRepository{},
		lockManager:      &mockLockManager{},
		matchRunRepo:     &mockMatchRunRepository{},
		matchGroupRepo:   &mockMatchGroupRepository{},
		matchItemRepo:    &mockMatchItemRepository{},
		exceptionCreator: &mockExceptionCreator{},
		outboxRepoTx:     &stubOutboxTxCreatorForLockTest{},
		feeVarianceRepo:  &stubFeeVarianceRepo{},
		feeRuleProvider:  &stubFeeRuleProviderWithResult{},
		feeScheduleRepo:  &stubFeeScheduleRepoWithResult{},
	}
	err := uc.validateRunMatchDependencies()
	require.NoError(t, err)
}

func TestValidateRunMatchDependencies_EachNil(t *testing.T) {
	t.Parallel()

	baseDeps := func() *UseCase {
		return &UseCase{
			contextProvider:  &mockContextProvider{},
			sourceProvider:   &mockSourceProvider{},
			ruleProvider:     &mockRuleProvider{},
			txRepo:           &mockTransactionRepository{},
			lockManager:      &mockLockManager{},
			matchRunRepo:     &mockMatchRunRepository{},
			matchGroupRepo:   &mockMatchGroupRepository{},
			matchItemRepo:    &mockMatchItemRepository{},
			exceptionCreator: &mockExceptionCreator{},
			outboxRepoTx:     &stubOutboxTxCreatorForLockTest{},
			feeVarianceRepo:  &stubFeeVarianceRepo{},
			feeRuleProvider:  &stubFeeRuleProviderWithResult{},
			feeScheduleRepo:  &stubFeeScheduleRepoWithResult{},
		}
	}

	tests := []struct {
		name    string
		modify  func(*UseCase)
		wantErr error
	}{
		{"nil contextProvider", func(uc *UseCase) { uc.contextProvider = nil }, ErrNilContextRepository},
		{"nil sourceProvider", func(uc *UseCase) { uc.sourceProvider = nil }, ErrNilSourceRepository},
		{"nil ruleProvider", func(uc *UseCase) { uc.ruleProvider = nil }, ErrNilMatchRuleProvider},
		{"nil txRepo", func(uc *UseCase) { uc.txRepo = nil }, ErrNilTransactionRepository},
		{"nil lockManager", func(uc *UseCase) { uc.lockManager = nil }, ErrNilLockManager},
		{"nil matchRunRepo", func(uc *UseCase) { uc.matchRunRepo = nil }, ErrNilMatchRunRepository},
		{"nil matchGroupRepo", func(uc *UseCase) { uc.matchGroupRepo = nil }, ErrNilMatchGroupRepository},
		{"nil matchItemRepo", func(uc *UseCase) { uc.matchItemRepo = nil }, ErrNilMatchItemRepository},
		{"nil exceptionCreator", func(uc *UseCase) { uc.exceptionCreator = nil }, ErrNilExceptionCreator},
		{"nil outboxRepoTx", func(uc *UseCase) { uc.outboxRepoTx = nil }, ErrOutboxRepoNotConfigured},
		{"nil feeVarianceRepo", func(uc *UseCase) { uc.feeVarianceRepo = nil }, ErrNilFeeVarianceRepository},
		{"nil feeRuleProvider", func(uc *UseCase) { uc.feeRuleProvider = nil }, ErrNilFeeRuleProvider},
		{"nil feeScheduleRepo", func(uc *UseCase) { uc.feeScheduleRepo = nil }, ErrNilFeeScheduleRepository},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			uc := baseDeps()
			tt.modify(uc)
			err := uc.validateRunMatchDependencies()
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAcquireContextLock_Success(t *testing.T) {
	t.Parallel()

	lockMgr := &stubLockManager{}
	uc := &UseCase{lockManager: lockMgr}

	lock, err := uc.acquireContextLock(context.Background(), nil, uuid.New())
	require.NoError(t, err)
	require.NotNil(t, lock)
}

func TestAcquireContextLock_AlreadyHeld(t *testing.T) {
	t.Parallel()

	lockMgr := &stubLockManager{err: ports.ErrLockAlreadyHeld}
	uc := &UseCase{lockManager: lockMgr}

	lock, err := uc.acquireContextLock(context.Background(), nil, uuid.New())
	require.ErrorIs(t, err, ErrMatchRunLocked)
	assert.Nil(t, lock)
}

func TestAcquireContextLock_OtherError(t *testing.T) {
	t.Parallel()

	lockMgr := &stubLockManager{err: errors.New("redis unavailable")}
	uc := &UseCase{lockManager: lockMgr}

	lock, err := uc.acquireContextLock(context.Background(), nil, uuid.New())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to acquire context lock")
	assert.Nil(t, lock)
}

func TestReleaseMatchLock_NilLock_NoOp(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	uc.releaseMatchLock(context.Background(), nil, nil, &libLog.NopLogger{})
}

func TestReleaseMatchLock_ReleaseError_LogsOnly(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	lock := &stubLockWithReleaseErrForLockTest{err: errors.New("release error")}
	uc.releaseMatchLock(context.Background(), nil, lock, &libLog.NopLogger{})
}

func TestFinalizeRunFailure_NilRun_ReturnsOriginalCause(t *testing.T) {
	t.Parallel()

	uc := &UseCase{matchRunRepo: &stubMatchRunRepo{}}
	cause := errors.New("test failure")

	result := finalizeRunFailure(context.Background(), uc, nil, cause)
	assert.Equal(t, cause, result)
}

func TestFinalizeRunFailure_SetsFailureReasonOnRun(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeCommit)
	require.NoError(t, err)

	cause := errors.New("rule engine exploded")
	result := finalizeRunFailure(context.Background(), uc, run, cause)
	require.Error(t, result)
	assert.Equal(t, cause, result)
	require.NotNil(t, matchRunRepo.updated)
	require.NotNil(t, matchRunRepo.updated.FailureReason)
}

func TestFinalizeRunFailure_WrapsUpdateError(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{updateErr: errors.New("db error")}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeCommit)
	require.NoError(t, err)

	cause := errors.New("original failure")
	result := finalizeRunFailure(context.Background(), uc, run, cause)
	require.Error(t, result)
	assert.Contains(t, result.Error(), "updating match run failed")
	assert.Contains(t, result.Error(), "original failure")
}

func TestWatchLockRefresh_NilRefreshFailed_Returns_NoopCleanup(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	cleanup := uc.watchLockRefresh(context.Background(), nil, &mockLock{}, &libLog.NopLogger{}, nil, nil, nil)
	require.NotNil(t, cleanup)
	cleanup()
}

func TestWatchLockRefresh_NilLock_Returns_NoopCleanup(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	cleanup := uc.watchLockRefresh(context.Background(), nil, nil, &libLog.NopLogger{}, nil, &atomic.Bool{}, &atomic.Bool{})
	require.NotNil(t, cleanup)
	cleanup()
}

func TestWatchLockRefresh_NonRefreshableLock_ReleasesOnCleanup(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	lock := &stubLockTrackingForLockTest{}
	cleanup := uc.watchLockRefresh(context.Background(), nil, lock, &libLog.NopLogger{}, nil, &atomic.Bool{}, &atomic.Bool{})
	require.NotNil(t, cleanup)
	cleanup()
	assert.True(t, lock.released)
}

// --- stubs local to this file ---

var _ ports.Lock = (*stubLockWithReleaseErrForLockTest)(nil)

type stubLockWithReleaseErrForLockTest struct {
	err error
}

func (s *stubLockWithReleaseErrForLockTest) Release(_ context.Context) error {
	return s.err
}

var _ ports.RefreshableLock = (*stubRefreshableLockForLockTest)(nil)

type stubRefreshableLockForLockTest struct {
	refreshErr error
}

func (s *stubRefreshableLockForLockTest) Release(_ context.Context) error { return nil }
func (s *stubRefreshableLockForLockTest) Refresh(_ context.Context, _ time.Duration) error {
	return s.refreshErr
}

type stubLockTrackingForLockTest struct {
	released bool
}

func (s *stubLockTrackingForLockTest) Release(_ context.Context) error {
	s.released = true
	return nil
}

var _ outboxTxCreator = (*stubOutboxTxCreatorForLockTest)(nil)

type stubOutboxTxCreatorForLockTest struct{}

func (s *stubOutboxTxCreatorForLockTest) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return event, nil
}
