//go:build unit

package http

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	matchingQuery "github.com/LerianStudio/matcher/internal/matching/services/query"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	matchingFee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errTestBoom            = errors.New("boom error")
	errTestDatabaseError   = errors.New("database error")
	errTestDatabaseTimeout = errors.New("database timeout")
)

type stubMatchRunRepo struct {
	run *matchingEntities.MatchRun
	err error
}

type stubMatchGroupRepo struct {
	groups     []*matchingEntities.MatchGroup
	pagination libHTTP.CursorPagination
	err        error
}

func (r *stubMatchRunRepo) Create(
	_ context.Context,
	run *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return run, r.err
}

func (r *stubMatchRunRepo) CreateWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	run *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return run, r.err
}

func (r *stubMatchRunRepo) Update(
	_ context.Context,
	run *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return run, r.err
}

func (r *stubMatchRunRepo) UpdateWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	run *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return run, r.err
}

func (r *stubMatchRunRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.MatchRun, error) {
	return r.run, r.err
}

func (r *stubMatchRunRepo) WithTx(
	_ context.Context,
	fn func(matchingRepositories.Tx) error,
) error {
	return fn(nil)
}

func (r *stubMatchRunRepo) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.MatchRun, libHTTP.CursorPagination, error) {
	if r.run != nil {
		return []*matchingEntities.MatchRun{r.run}, libHTTP.CursorPagination{}, r.err
	}

	return nil, libHTTP.CursorPagination{}, r.err
}

func (repo *stubMatchGroupRepo) CreateBatch(
	_ context.Context,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return groups, repo.err
}

func (repo *stubMatchGroupRepo) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return groups, repo.err
}

func (repo *stubMatchGroupRepo) ListByRunID(
	_ context.Context,
	_, _ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.MatchGroup, libHTTP.CursorPagination, error) {
	return repo.groups, repo.pagination, repo.err
}

func (repo *stubMatchGroupRepo) FindByID(
	_ context.Context,
	_, id uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	for _, g := range repo.groups {
		if g.ID == id {
			return g, repo.err
		}
	}

	return nil, repo.err
}

func (repo *stubMatchGroupRepo) Update(
	_ context.Context,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	return group, repo.err
}

func (repo *stubMatchGroupRepo) UpdateWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	return group, repo.err
}

type stubContextProvider struct {
	info *ports.ReconciliationContextInfo
	err  error
}

func (s *stubContextProvider) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*ports.ReconciliationContextInfo, error) {
	return s.info, s.err
}

type runMatchSourceProvider struct {
	sources []*ports.SourceInfo
	err     error
}

func (r *runMatchSourceProvider) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
) ([]*ports.SourceInfo, error) {
	return r.sources, r.err
}

type runMatchRuleProvider struct {
	rules shared.MatchRules
	err   error
}

func (r *runMatchRuleProvider) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
) (shared.MatchRules, error) {
	return r.rules, r.err
}

type runMatchTxRepo struct {
	candidates []*shared.Transaction
	err        error
}

func (repo *runMatchTxRepo) ListUnmatchedByContext(
	_ context.Context,
	_ uuid.UUID,
	_ *time.Time,
	_ *time.Time,
	_ int,
	_ int,
) ([]*shared.Transaction, error) {
	return repo.candidates, repo.err
}

func (repo *runMatchTxRepo) FindByID(
	_ context.Context,
	_, id uuid.UUID,
) (*shared.Transaction, error) {
	for _, txn := range repo.candidates {
		if txn != nil && txn.ID == id {
			return txn, repo.err
		}
	}

	return nil, repo.err
}

func (repo *runMatchTxRepo) MarkMatched(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return nil
}

func (repo *runMatchTxRepo) MarkMatchedWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (repo *runMatchTxRepo) MarkPendingReview(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return nil
}

func (repo *runMatchTxRepo) MarkPendingReviewWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (repo *runMatchTxRepo) WithTx(
	_ context.Context,
	fn func(matchingRepositories.Tx) error,
) error {
	return fn(nil)
}

func (repo *runMatchTxRepo) FindByContextAndIDs(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
) ([]*shared.Transaction, error) {
	return repo.candidates, repo.err
}

func (repo *runMatchTxRepo) MarkUnmatched(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return nil
}

func (repo *runMatchTxRepo) MarkUnmatchedWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

type runMatchLock struct{}

func (r *runMatchLock) Release(_ context.Context) error {
	return nil
}

type runMatchLockManager struct {
	lock ports.Lock
	err  error
}

func (r *runMatchLockManager) AcquireTransactionsLock(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
	_ time.Duration,
) (ports.Lock, error) {
	if r.lock == nil {
		return &runMatchLock{}, r.err
	}

	return r.lock, r.err
}

func (r *runMatchLockManager) AcquireContextLock(
	_ context.Context,
	_ uuid.UUID,
	_ time.Duration,
) (ports.Lock, error) {
	return &runMatchLock{}, r.err
}

type runMatchRunRepo struct{}

func (r *runMatchRunRepo) Create(
	_ context.Context,
	_ *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

func (r *runMatchRunRepo) CreateWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	run *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return run, nil
}

func (r *runMatchRunRepo) Update(
	_ context.Context,
	run *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return run, nil
}

func (r *runMatchRunRepo) UpdateWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	run *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return run, nil
}

func (r *runMatchRunRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

func (r *runMatchRunRepo) WithTx(_ context.Context, fn func(matchingRepositories.Tx) error) error {
	return fn(nil)
}

func (r *runMatchRunRepo) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.MatchRun, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

type runMatchGroupRepo struct {
	groups []*matchingEntities.MatchGroup
	err    error
}

func (r *runMatchGroupRepo) CreateBatch(
	_ context.Context,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return groups, nil
}

func (r *runMatchGroupRepo) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	groups []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return groups, nil
}

func (r *runMatchGroupRepo) ListByRunID(
	_ context.Context,
	_, _ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.MatchGroup, libHTTP.CursorPagination, error) {
	return r.groups, libHTTP.CursorPagination{}, r.err
}

func (r *runMatchGroupRepo) FindByID(
	_ context.Context,
	_, id uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	for _, group := range r.groups {
		if group != nil && group.ID == id {
			return group, r.err
		}
	}

	return nil, r.err
}

func (r *runMatchGroupRepo) Update(
	_ context.Context,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	return group, r.err
}

func (r *runMatchGroupRepo) UpdateWithTx(
	ctx context.Context,
	_ matchingRepositories.Tx,
	group *matchingEntities.MatchGroup,
) (*matchingEntities.MatchGroup, error) {
	return r.Update(ctx, group)
}

func (r *runMatchGroupRepo) Confirm(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

type runMatchItemRepo struct{}

func (r *runMatchItemRepo) CreateBatch(
	_ context.Context,
	items []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	return items, nil
}

func (r *runMatchItemRepo) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	items []*matchingEntities.MatchItem,
) ([]*matchingEntities.MatchItem, error) {
	return items, nil
}

func (r *runMatchItemRepo) ListByMatchGroupID(
	_ context.Context,
	_ uuid.UUID,
) ([]*matchingEntities.MatchItem, error) {
	return nil, nil
}

func (r *runMatchItemRepo) ListByMatchGroupIDs(
	_ context.Context,
	_ []uuid.UUID,
) (map[uuid.UUID][]*matchingEntities.MatchItem, error) {
	return make(map[uuid.UUID][]*matchingEntities.MatchItem), nil
}

type runMatchExceptionCreator struct{}

func (r *runMatchExceptionCreator) CreateExceptions(
	_ context.Context,
	_, _ uuid.UUID,
	_ []ports.ExceptionTransactionInput,
	_ []string,
) error {
	return nil
}

func (r *runMatchExceptionCreator) CreateExceptionsWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_, _ uuid.UUID,
	_ []ports.ExceptionTransactionInput,
	_ []string,
) error {
	return nil
}

type runMatchOutboxRepo struct{}

func (r *runMatchOutboxRepo) Create(
	_ context.Context,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return event, nil
}

func (r *runMatchOutboxRepo) CreateWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return event, nil
}

func (r *runMatchOutboxRepo) ListPending(
	_ context.Context,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (r *runMatchOutboxRepo) ListPendingByType(
	_ context.Context,
	_ string,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (r *runMatchOutboxRepo) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (r *runMatchOutboxRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.OutboxEvent, error) {
	return nil, nil
}

func (r *runMatchOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (r *runMatchOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ int) error {
	return nil
}

func (r *runMatchOutboxRepo) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (r *runMatchOutboxRepo) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (r *runMatchOutboxRepo) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (r *runMatchOutboxRepo) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

type runMatchRateRepo struct{}

func (r *runMatchRateRepo) GetByID(_ context.Context, _ uuid.UUID) (*matchingFee.Rate, error) {
	return nil, nil
}

type runMatchFeeVarianceRepo struct{}

func (r *runMatchFeeVarianceRepo) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	rows []*matchingEntities.FeeVariance,
) ([]*matchingEntities.FeeVariance, error) {
	return rows, nil
}

type runMatchAdjustmentRepo struct{}

func (r *runMatchAdjustmentRepo) Create(
	_ context.Context,
	adj *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return adj, nil
}

func (r *runMatchAdjustmentRepo) CreateWithTx(
	ctx context.Context,
	_ *sql.Tx,
	adj *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return r.Create(ctx, adj)
}

func (r *runMatchAdjustmentRepo) CreateWithAuditLog(
	ctx context.Context,
	adj *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return r.Create(ctx, adj)
}

func (r *runMatchAdjustmentRepo) CreateWithAuditLogWithTx(
	ctx context.Context,
	_ *sql.Tx,
	adj *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return r.Create(ctx, adj)
}

func (r *runMatchAdjustmentRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.Adjustment, error) {
	return nil, nil
}

func (r *runMatchAdjustmentRepo) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepositories.CursorFilter,
) ([]*matchingEntities.Adjustment, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (r *runMatchAdjustmentRepo) ListByMatchGroupID(
	_ context.Context,
	_, _ uuid.UUID,
) ([]*matchingEntities.Adjustment, error) {
	return nil, nil
}

type runMatchInfraProvider struct{}

func (r *runMatchInfraProvider) GetRedisConnection(
	_ context.Context,
) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (r *runMatchInfraProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	return nil, nil
}

func (r *runMatchInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (r *runMatchInfraProvider) GetPrimaryDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

type runMatchAuditLogRepo struct{}

func (r *runMatchAuditLogRepo) Create(
	_ context.Context,
	auditLog *shared.AuditLog,
) (*shared.AuditLog, error) {
	return auditLog, nil
}

func (r *runMatchAuditLogRepo) CreateWithTx(
	_ context.Context,
	_ sharedPorts.Tx,
	auditLog *shared.AuditLog,
) (*shared.AuditLog, error) {
	return auditLog, nil
}

func (r *runMatchAuditLogRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.AuditLog, error) {
	return nil, nil
}

func (r *runMatchAuditLogRepo) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*shared.AuditLog, string, error) {
	return nil, "", nil
}

func (r *runMatchAuditLogRepo) List(
	_ context.Context,
	_ shared.AuditLogFilter,
	_ *libHTTP.TimestampCursor,
	_ int,
) ([]*shared.AuditLog, string, error) {
	return nil, "", nil
}

type runMatchFeeScheduleRepo struct{}

func (r *runMatchFeeScheduleRepo) Create(
	_ context.Context,
	_ *matchingFee.FeeSchedule,
) (*matchingFee.FeeSchedule, error) {
	return nil, nil
}

func (r *runMatchFeeScheduleRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*matchingFee.FeeSchedule, error) {
	return nil, nil
}

func (r *runMatchFeeScheduleRepo) Update(
	_ context.Context,
	_ *matchingFee.FeeSchedule,
) (*matchingFee.FeeSchedule, error) {
	return nil, nil
}

func (r *runMatchFeeScheduleRepo) Delete(
	_ context.Context,
	_ uuid.UUID,
) error {
	return nil
}

func (r *runMatchFeeScheduleRepo) List(
	_ context.Context,
	_ int,
) ([]*matchingFee.FeeSchedule, error) {
	return nil, nil
}

func (r *runMatchFeeScheduleRepo) GetByIDs(
	_ context.Context,
	_ []uuid.UUID,
) (map[uuid.UUID]*matchingFee.FeeSchedule, error) {
	return nil, nil
}

// runMatchFeeRuleProvider is a minimal stub for fee rule provider in tests.
type runMatchFeeRuleProvider struct{}

func (r *runMatchFeeRuleProvider) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
) ([]*matchingFee.FeeRule, error) {
	return nil, nil
}

// stubMatchItemRepo is a minimal stub for match item repository in tests.
type stubMatchItemRepo struct{}

func (r *stubMatchItemRepo) CreateBatch(_ context.Context, items []*matchingEntities.MatchItem) ([]*matchingEntities.MatchItem, error) {
	return items, nil
}

func (r *stubMatchItemRepo) CreateBatchWithTx(_ context.Context, _ matchingRepositories.Tx, items []*matchingEntities.MatchItem) ([]*matchingEntities.MatchItem, error) {
	return items, nil
}

func (r *stubMatchItemRepo) ListByMatchGroupID(_ context.Context, _ uuid.UUID) ([]*matchingEntities.MatchItem, error) {
	return nil, nil
}

func (r *stubMatchItemRepo) ListByMatchGroupIDs(_ context.Context, _ []uuid.UUID) (map[uuid.UUID][]*matchingEntities.MatchItem, error) {
	return make(map[uuid.UUID][]*matchingEntities.MatchItem), nil
}

func newQueryUseCase(
	t *testing.T,
	runRepo *stubMatchRunRepo,
	groupRepo *stubMatchGroupRepo,
) *matchingQuery.UseCase {
	t.Helper()

	uc, err := matchingQuery.NewUseCase(runRepo, groupRepo, &stubMatchItemRepo{})
	require.NoError(t, err)

	return uc
}

func newFiberTestApp(ctx context.Context) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.SetUserContext(ctx)
		return c.Next()
	})

	return app
}

func newRunMatchUseCase(
	t *testing.T,
	ctxProvider ports.ContextProvider,
	candidates []*shared.Transaction,
	txErr error,
) *command.UseCase {
	t.Helper()

	sourceProvider := &runMatchSourceProvider{sources: []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: matchingFee.MatchingSideLeft},
		{ID: uuid.New(), Type: ports.SourceTypeAPI, Side: matchingFee.MatchingSideRight},
	}}

	ruleProvider := &runMatchRuleProvider{rules: shared.MatchRules{}}
	txRepo := &runMatchTxRepo{candidates: candidates, err: txErr}
	lockManager := &runMatchLockManager{}
	runRepo := &runMatchRunRepo{}
	groupRepo := &runMatchGroupRepo{}
	itemRepo := &runMatchItemRepo{}
	exceptionCreator := &runMatchExceptionCreator{}
	outboxRepo := &runMatchOutboxRepo{}
	rateRepo := &runMatchRateRepo{}
	feeVarianceRepo := &runMatchFeeVarianceRepo{}
	adjustmentRepo := &runMatchAdjustmentRepo{}
	infraProvider := &runMatchInfraProvider{}
	auditLogRepo := &runMatchAuditLogRepo{}
	feeScheduleRepo := &runMatchFeeScheduleRepo{}
	feeRuleProvider := &runMatchFeeRuleProvider{}

	uc, err := command.New(command.UseCaseDeps{
		ContextProvider:  ctxProvider,
		SourceProvider:   sourceProvider,
		RuleProvider:     ruleProvider,
		TxRepo:           txRepo,
		LockManager:      lockManager,
		MatchRunRepo:     runRepo,
		MatchGroupRepo:   groupRepo,
		MatchItemRepo:    itemRepo,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		RateRepo:         rateRepo,
		FeeVarianceRepo:  feeVarianceRepo,
		AdjustmentRepo:   adjustmentRepo,
		InfraProvider:    infraProvider,
		AuditLogRepo:     auditLogRepo,
		FeeScheduleRepo:  feeScheduleRepo,
		FeeRuleProvider:  feeRuleProvider,
	})
	require.NoError(t, err)

	return uc
}
