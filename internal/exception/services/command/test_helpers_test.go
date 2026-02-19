//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"
	libRedis "github.com/LerianStudio/lib-uncommons/v2/uncommons/redis"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
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
	exception *entities.Exception
	findErr   error
	updateErr error
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
}

func (audit *stubAuditPublisher) PublishExceptionEvent(
	_ context.Context,
	event ports.AuditEvent,
) error {
	audit.mu.Lock()
	audit.lastEvent = &event
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
}

func (provider *stubInfraProvider) GetPostgresConnection(
	_ context.Context,
) (*libPostgres.Client, error) {
	if provider.postgresErr != nil {
		return nil, provider.postgresErr
	}

	return provider.postgresConn, nil
}

func (provider *stubInfraProvider) GetRedisConnection(
	_ context.Context,
) (*libRedis.Client, error) {
	if provider.redisErr != nil {
		return nil, provider.redisErr
	}

	return provider.redisConn, nil
}

// BeginTx returns a mock transaction for testing.
// Uses sqlmock to create a valid *sql.Tx that supports Commit and Rollback.
func (provider *stubInfraProvider) BeginTx(ctx context.Context) (*sql.Tx, error) {
	if provider.txErr != nil {
		return nil, provider.txErr
	}

	if provider.tx != nil {
		return provider.tx, nil
	}

	return newMockTxWithCommit(ctx)
}

// GetReplicaDB returns nil for tests (read replica not used in these tests).
func (provider *stubInfraProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
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

func (m *mockCommentRepository) Delete(_ context.Context, _ uuid.UUID) error {
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

func (s *stubCommentRepository) Delete(_ context.Context, _ uuid.UUID) error {
	return s.deleteErr
}

// mockExceptionRepository wraps stubExceptionRepo for naming consistency.
type mockExceptionRepository = stubExceptionRepo

// mockActorExtractor wraps stubActorExtractor for naming consistency.
type mockActorExtractor = stubActorExtractor
