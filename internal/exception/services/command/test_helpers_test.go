//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// newMockTx creates a mock sql.Tx using sqlmock.
// Only sets ExpectBegin - caller must add ExpectCommit if needed.
func newMockTx(ctx context.Context) (*sql.Tx, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, nil, fmt.Errorf("create sqlmock: %w", err)
	}

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("begin transaction: %w", err)
	}

	return tx, mock, nil
}

// newMockTxWithCommit creates a mock sql.Tx that expects Begin and Commit.
func newMockTxWithCommit(ctx context.Context) (*sql.Tx, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, fmt.Errorf("create sqlmock: %w", err)
	}

	mock.ExpectBegin()
	mock.ExpectCommit()
	mock.ExpectRollback()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	return tx, nil
}

type stubActorExtractor struct {
	actor string
}

func (e *stubActorExtractor) GetActor(_ context.Context) string {
	return e.actor
}

// actorCtxKey is an unexported context key for embedding actor in test contexts.
type actorCtxKey struct{}

func ctxWithActor(actor string) context.Context {
	return context.WithValue(context.Background(), actorCtxKey{}, actor)
}

func actorExtractor(actor string) *stubActorExtractor {
	return &stubActorExtractor{actor: actor}
}

// Sentinel errors for testing.
var (
	errTestFind     = errors.New("test: find failed")
	errTestUpdate   = errors.New("test: update failed")
	errTestExecutor = errors.New("test: executor failed")
	errTestAudit    = errors.New("test: audit failed")
)

type stubExceptionRepo struct {
	exception   *entities.Exception
	findErr     error
	updateErr   error
	findByIDs   []*entities.Exception // optional override for FindByIDs
	findIDsErr  error
	findIDsCall int // number of FindByIDs invocations (bulk regression)
}

func (repo *stubExceptionRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.Exception, error) {
	if repo.findErr != nil {
		return nil, repo.findErr
	}

	return repo.exception, nil
}

func (repo *stubExceptionRepo) FindByIDs(
	_ context.Context,
	ids []uuid.UUID,
) ([]*entities.Exception, error) {
	repo.findIDsCall++

	if repo.findIDsErr != nil {
		return nil, repo.findIDsErr
	}

	// Explicit override wins.
	if repo.findByIDs != nil {
		return repo.findByIDs, nil
	}

	// Default fallback: return the single configured exception once per
	// requested id (tests that only set `exception` keep working without
	// per-id wiring). Empty id slice yields an empty result, matching the
	// postgres adapter's early-return semantics.
	if repo.exception == nil || len(ids) == 0 {
		return []*entities.Exception{}, nil
	}

	result := make([]*entities.Exception, 0, len(ids))
	for range ids {
		result = append(result, repo.exception)
	}

	return result, nil
}

func (repo *stubExceptionRepo) List(
	_ context.Context,
	_ repositories.ExceptionFilter,
	_ repositories.CursorFilter,
) ([]*entities.Exception, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (repo *stubExceptionRepo) Update(
	_ context.Context,
	exception *entities.Exception,
) (*entities.Exception, error) {
	if repo.updateErr != nil {
		return nil, repo.updateErr
	}

	return exception, nil
}

func (repo *stubExceptionRepo) UpdateWithTx(
	ctx context.Context,
	_ repositories.Tx,
	exception *entities.Exception,
) (*entities.Exception, error) {
	return repo.Update(ctx, exception)
}

type stubResolutionExecutor struct {
	forceMatchErr  error
	adjustEntryErr error
}

func (exec *stubResolutionExecutor) ForceMatch(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ value_objects.OverrideReason,
) error {
	return exec.forceMatchErr
}

func (exec *stubResolutionExecutor) AdjustEntry(
	_ context.Context,
	_ uuid.UUID,
	_ ports.AdjustmentInput,
) error {
	return exec.adjustEntryErr
}

type stubAuditPublisher struct {
	mu        sync.Mutex
	lastEvent *ports.AuditEvent
	err       error
	called    chan struct{} // optional; closed on first PublishExceptionEvent call
	once      sync.Once
	callCount int // total PublishExceptionEvent invocations (bulk regression)
}

func (audit *stubAuditPublisher) PublishExceptionEvent(
	_ context.Context,
	event ports.AuditEvent,
) error {
	audit.mu.Lock()
	audit.lastEvent = &event
	audit.callCount++
	audit.mu.Unlock()

	audit.once.Do(func() {
		if audit.called != nil {
			close(audit.called)
		}
	})

	return audit.err
}

func (audit *stubAuditPublisher) PublishExceptionEventWithTx(
	ctx context.Context,
	_ *sql.Tx,
	event ports.AuditEvent,
) error {
	return audit.PublishExceptionEvent(ctx, event)
}

// waitForPublish blocks until PublishExceptionEvent is called or times out.
// Only works when the called channel is initialised (make(chan struct{})).
func (audit *stubAuditPublisher) waitForPublish(t *testing.T) {
	t.Helper()

	if audit.called == nil {
		return
	}

	select {
	case <-audit.called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for audit publish")
	}
}

// getLastEvent returns the last published event in a thread-safe way.
func (audit *stubAuditPublisher) getLastEvent() *ports.AuditEvent {
	audit.mu.Lock()
	defer audit.mu.Unlock()

	return audit.lastEvent
}

// getCallCount returns how many times PublishExceptionEvent was called in a
// thread-safe way.
func (audit *stubAuditPublisher) getCallCount() int {
	audit.mu.Lock()
	defer audit.mu.Unlock()

	return audit.callCount
}

// stubCallbackRateLimiter implements ports.CallbackRateLimiter for testing.
type stubCallbackRateLimiter struct {
	allowed bool
	err     error
	lastKey string
}

func (s *stubCallbackRateLimiter) Allow(_ context.Context, key string) (bool, error) {
	s.lastKey = key

	if s.err != nil {
		return false, s.err
	}

	return s.allowed, nil
}

// stubInfraProvider implements sharedPorts.InfrastructureProvider for testing.
type stubInfraProvider struct {
	postgresConn *libPostgres.Client
	redisConn    *libRedis.Client
	postgresErr  error
	redisErr     error
	txErr        error
	tx           *sql.Tx
	beginTxCall  atomic.Int64 // number of BeginTx invocations (bulk regression)
}

func (provider *stubInfraProvider) GetRedisConnection(
	_ context.Context,
) (*sharedPorts.RedisConnectionLease, error) {
	if provider.redisErr != nil {
		return nil, provider.redisErr
	}

	return sharedPorts.NewRedisConnectionLease(provider.redisConn, nil), nil
}

// BeginTx returns a mock transaction for testing.
// Uses sqlmock to create a valid *sql.Tx that supports Commit and Rollback.
func (provider *stubInfraProvider) BeginTx(ctx context.Context) (*sharedPorts.TxLease, error) {
	provider.beginTxCall.Add(1)

	if provider.txErr != nil {
		return nil, provider.txErr
	}

	if provider.tx != nil {
		return sharedPorts.NewTxLease(provider.tx, nil), nil
	}

	tx, err := newMockTxWithCommit(ctx)
	if err != nil {
		return nil, err
	}

	return sharedPorts.NewTxLease(tx, nil), nil
}

// GetReplicaDB returns nil for tests (read replica not used in these tests).
func (provider *stubInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (provider *stubInfraProvider) GetPrimaryDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

// mockCommentRepository is a simple mock for comment repository in constructor tests.
type mockCommentRepository struct{}

func (m *mockCommentRepository) Create(
	_ context.Context,
	comment *entities.ExceptionComment,
) (*entities.ExceptionComment, error) {
	return comment, nil
}

func (m *mockCommentRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.ExceptionComment, error) {
	return nil, nil
}

func (m *mockCommentRepository) FindByExceptionID(
	_ context.Context,
	_ uuid.UUID,
) ([]*entities.ExceptionComment, error) {
	return nil, nil
}

func (m *mockCommentRepository) DeleteByExceptionAndID(_ context.Context, _, _ uuid.UUID) error {
	return nil
}

// stubCommentRepository is a configurable stub for comment repository in behavior tests.
type stubCommentRepository struct {
	comment   *entities.ExceptionComment
	comments  []*entities.ExceptionComment
	findErr   error
	createErr error
	deleteErr error
}

func (s *stubCommentRepository) Create(
	_ context.Context,
	comment *entities.ExceptionComment,
) (*entities.ExceptionComment, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}

	return comment, nil
}

func (s *stubCommentRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.ExceptionComment, error) {
	if s.findErr != nil {
		return nil, s.findErr
	}

	return s.comment, nil
}

func (s *stubCommentRepository) FindByExceptionID(
	_ context.Context,
	_ uuid.UUID,
) ([]*entities.ExceptionComment, error) {
	if s.findErr != nil {
		return nil, s.findErr
	}

	return s.comments, nil
}

func (s *stubCommentRepository) DeleteByExceptionAndID(_ context.Context, _, _ uuid.UUID) error {
	return s.deleteErr
}

// mockExceptionRepository wraps stubExceptionRepo for naming consistency.
type mockExceptionRepository = stubExceptionRepo

// mockActorExtractor wraps stubActorExtractor for naming consistency.
type mockActorExtractor = stubActorExtractor

// finderAsRepo adapts a ports.ExceptionFinder (narrow interface used by
// dispatch tests) to the full repositories.ExceptionRepository surface the
// merged ExceptionUseCase constructor requires. Dispatch operations only
// invoke FindByID, so the other methods panic to surface any unintended
// call path during tests.
type finderAsRepo struct {
	finder ports.ExceptionFinder
}

// wrapFinder returns an ExceptionRepository implementation around a narrow
// ExceptionFinder. When finder is nil we return a nil ExceptionRepository
// interface (untyped nil) so the constructor's `repo == nil` check still
// triggers — returning a typed nil pointer would produce a non-nil
// interface value with a nil dynamic value, silently masking the error.
func wrapFinder(finder ports.ExceptionFinder) repositories.ExceptionRepository {
	if finder == nil {
		return nil
	}

	return &finderAsRepo{finder: finder}
}

func (f *finderAsRepo) FindByID(
	ctx context.Context,
	id uuid.UUID,
) (*entities.Exception, error) {
	return f.finder.FindByID(ctx, id)
}

func (f *finderAsRepo) FindByIDs(
	_ context.Context,
	_ []uuid.UUID,
) ([]*entities.Exception, error) {
	panic("finderAsRepo.FindByIDs: dispatch path should not call this")
}

func (f *finderAsRepo) List(
	_ context.Context,
	_ repositories.ExceptionFilter,
	_ repositories.CursorFilter,
) ([]*entities.Exception, libHTTP.CursorPagination, error) {
	panic("finderAsRepo.List: dispatch path should not call this")
}

func (f *finderAsRepo) Update(
	_ context.Context,
	_ *entities.Exception,
) (*entities.Exception, error) {
	panic("finderAsRepo.Update: dispatch path should not call this")
}

func (f *finderAsRepo) UpdateWithTx(
	_ context.Context,
	_ repositories.Tx,
	_ *entities.Exception,
) (*entities.Exception, error) {
	panic("finderAsRepo.UpdateWithTx: dispatch path should not call this")
}
