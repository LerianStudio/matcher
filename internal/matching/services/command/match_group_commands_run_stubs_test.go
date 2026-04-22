//go:build unit

//nolint:dupl
package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	governanceRepositories "github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	outboxmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

// ---------------------------------------------------------------------------
// Package-level sentinel errors used across RunMatch tests
// ---------------------------------------------------------------------------

var ErrLockFailed = errors.New("lock failed")

var ErrGroupFailed = errors.New("group failed")

var (
	ErrRunFailed    = ErrGroupFailed
	ErrUpdateFailed = ErrLockFailed
)

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

// outboxMockOpts configures optional expectations for newMockOutboxRepository.
type outboxMockOpts struct {
	// expectedCreateWithTxCalls limits how many times CreateWithTx may be called.
	// A negative value means AnyTimes(); 0 means no calls expected.
	// The default via newMockOutboxRepository is AnyTimes (-1), which is a
	// deliberate trade-off: the RunMatch orchestration path may or may not
	// publish outbox events depending on match outcome, and most tests
	// exercise the matching engine — not the outbox plumbing.  Forcing
	// explicit counts on 20+ tests would couple them to outbox internals
	// without improving coverage of the outbox itself (which has its own
	// dedicated tests).  New tests should prefer newMockOutboxRepositoryWithOpts
	// with an explicit count when the test's purpose includes verifying
	// that outbox events were (or were not) published.
	expectedCreateWithTxCalls int
}

// newMockOutboxRepository builds a mock OutboxRepository pre-wired for the
// common happy-path.  Most methods use AnyTimes() so tests that don't care
// about them aren't cluttered.
//
// Design decision: CreateWithTx uses AnyTimes() by default.  This is
// intentional — see outboxMockOpts.expectedCreateWithTxCalls for rationale.
// Tests that specifically verify outbox publication should use
// newMockOutboxRepositoryWithOpts with an explicit call count.
func newMockOutboxRepository(
	t *testing.T,
	createWithTx func(context.Context, *sql.Tx, *shared.OutboxEvent) (*shared.OutboxEvent, error),
) *outboxmocks.MockOutboxRepository {
	t.Helper()

	return newMockOutboxRepositoryWithOpts(t, createWithTx, outboxMockOpts{
		expectedCreateWithTxCalls: -1, // AnyTimes (backward-compatible default)
	})
}

// newMockOutboxRepositoryWithOpts is the configurable variant of
// newMockOutboxRepository.  Use it in new tests that need tighter assertions.
func newMockOutboxRepositoryWithOpts(
	t *testing.T,
	createWithTx func(context.Context, *sql.Tx, *shared.OutboxEvent) (*shared.OutboxEvent, error),
	opts outboxMockOpts,
) *outboxmocks.MockOutboxRepository {
	t.Helper()

	controller := gomock.NewController(t)
	t.Cleanup(controller.Finish)

	repo := outboxmocks.NewMockOutboxRepository(controller)
	repo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		Return(&shared.OutboxEvent{}, nil).
		AnyTimes()

	createCall := repo.EXPECT().
		CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any())

	if createWithTx != nil {
		createCall = createCall.DoAndReturn(createWithTx)
	} else {
		createCall = createCall.Return(&shared.OutboxEvent{}, nil)
	}

	if opts.expectedCreateWithTxCalls >= 0 {
		createCall.Times(opts.expectedCreateWithTxCalls)
	} else {
		createCall.AnyTimes()
	}

	repo.EXPECT().
		ListPending(gomock.Any(), gomock.Any()).
		Return([]*shared.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().ListTenants(gomock.Any()).Return([]string{}, nil).AnyTimes()
	repo.EXPECT().GetByID(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	repo.EXPECT().MarkPublished(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	repo.EXPECT().MarkFailed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	repo.EXPECT().
		ListFailedForRetry(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*shared.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().
		ResetForRetry(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*shared.OutboxEvent{}, nil).
		AnyTimes()
	repo.EXPECT().
		ResetStuckProcessing(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*shared.OutboxEvent{}, nil).
		AnyTimes()

	return repo
}

// ---------------------------------------------------------------------------
// Stub types: providers
// ---------------------------------------------------------------------------

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
	if s.err != nil {
		return s.sources, s.err
	}

	// Return sources as-is — test data must provide explicit LEFT/RIGHT sides
	// to reflect production behavior. The production adapter always sets side
	// from the database; tests should not silently auto-repair missing sides.
	return s.sources, nil
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

// ---------------------------------------------------------------------------
// Stub types: proposal override
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Stub types: transaction repository
// ---------------------------------------------------------------------------

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

func (stub *stubTxRepo) MarkMatchedWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
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

func (stub *stubTxRepo) MarkPendingReviewWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
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

// ---------------------------------------------------------------------------
// Stub types: locking
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Stub types: match run repository
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Stub types: match group repository
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Stub types: match item repository
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Stub types: exception creator
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Stub types: fee variance, adjustment repositories
// ---------------------------------------------------------------------------

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
	_ *sql.Tx,
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

func (s *stubAdjustmentRepo) CreateWithAuditLog(
	_ context.Context,
	adj *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (s *stubAdjustmentRepo) CreateWithAuditLogWithTx(
	_ context.Context,
	_ *sql.Tx,
	adj *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (s *stubAdjustmentRepo) ListByMatchGroupID(
	_ context.Context,
	_, _ uuid.UUID,
) ([]*matchingEntities.Adjustment, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// Stub types: infrastructure provider & audit log
// ---------------------------------------------------------------------------

// stubInfraProviderForRun implements sharedPorts.InfrastructureProvider for testing.
type stubInfraProviderForRun struct {
	tx  *sql.Tx
	err error
}

func (s *stubInfraProviderForRun) GetRedisConnection(
	_ context.Context,
) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (s *stubInfraProviderForRun) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	if s.err != nil {
		return nil, s.err
	}

	return sharedPorts.NewTxLease(s.tx, nil), nil
}

func (s *stubInfraProviderForRun) GetReplicaDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (s *stubInfraProviderForRun) GetPrimaryDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

// stubAuditLogRepoForRun implements governanceRepositories.AuditLogRepository for testing.
type stubAuditLogRepoForRun struct {
	createErr error
}

func (s *stubAuditLogRepoForRun) Create(
	_ context.Context,
	auditLog *shared.AuditLog,
) (*shared.AuditLog, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}

	return auditLog, nil
}

func (s *stubAuditLogRepoForRun) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	auditLog *shared.AuditLog,
) (*shared.AuditLog, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}

	return auditLog, nil
}

func (s *stubAuditLogRepoForRun) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.AuditLog, error) {
	return nil, nil
}

func (s *stubAuditLogRepoForRun) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*shared.AuditLog, string, error) {
	return nil, "", nil
}

func (s *stubAuditLogRepoForRun) List(
	_ context.Context,
	_ shared.AuditLogFilter,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*shared.AuditLog, string, error) {
	return nil, "", nil
}

// ---------------------------------------------------------------------------
// Stub types: misc
// ---------------------------------------------------------------------------

type fakeTx struct{}

// ---------------------------------------------------------------------------
// Interface compliance assertions
// ---------------------------------------------------------------------------

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
	_ matchingRepositories.FeeVarianceRepository = (*stubFeeVarianceRepo)(nil)
	_ matchingRepositories.AdjustmentRepository  = (*stubAdjustmentRepo)(nil)
	_ sharedPorts.InfrastructureProvider         = (*stubInfraProviderForRun)(nil)
	_ governanceRepositories.AuditLogRepository  = (*stubAuditLogRepoForRun)(nil)
)
