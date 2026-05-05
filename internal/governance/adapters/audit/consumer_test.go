// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package audit

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	streaming "github.com/LerianStudio/lib-streaming/v2"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

// errConsumerRepoFailure is a sentinel error for repository failures in tests.
var errConsumerRepoFailure = errors.New("consumer repo failure")

type stubAuditLogRepo struct {
	created            *entities.AuditLog
	err                error
	listByEntityResult []*entities.AuditLog
	listByEntityErr    error
}

func newStreamingConsumer(t *testing.T, repo *stubAuditLogRepo) (*Consumer, sqlmock.Sqlmock) {
	return newStreamingConsumerWithEmitter(t, repo, streaming.NewNoopEmitter())
}

func newStreamingConsumerWithEmitter(
	t *testing.T,
	repo *stubAuditLogRepo,
	emitter streaming.Emitter,
) (*Consumer, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	provider := &infraTestutil.MockInfrastructureProvider{
		PostgresConn: infraTestutil.NewClientWithResolver(dbresolver.New(dbresolver.WithPrimaryDBs(db))),
	}
	consumer, err := NewConsumer(repo, ConsumerConfig{
		Infrastructure:   provider,
		StreamingEmitter: emitter,
	})
	require.NoError(t, err)

	return consumer, mock
}

type failingAuditEmitter struct {
	err error
}

func (emitter failingAuditEmitter) Emit(context.Context, streaming.EmitRequest) error {
	return emitter.err
}

func (emitter failingAuditEmitter) Close() error { return nil }

func (emitter failingAuditEmitter) Healthy(context.Context) error { return nil }

type typedNilAuditEmitter struct{}

func (*typedNilAuditEmitter) Emit(context.Context, streaming.EmitRequest) error { return nil }

func (*typedNilAuditEmitter) Close() error { return nil }

func (*typedNilAuditEmitter) Healthy(context.Context) error { return nil }

func expectAuditStreamingTx(mock sqlmock.Sqlmock, commit bool) {
	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	if commit {
		mock.ExpectCommit()
		return
	}

	mock.ExpectRollback()
}

func (s *stubAuditLogRepo) Create(
	_ context.Context,
	log *entities.AuditLog,
) (*entities.AuditLog, error) {
	if s.err != nil {
		return nil, s.err
	}

	s.created = log

	return log, nil
}

func (s *stubAuditLogRepo) CreateWithTx(
	ctx context.Context,
	_ *sql.Tx,
	log *entities.AuditLog,
) (*entities.AuditLog, error) {
	return s.Create(ctx, log)
}

func (s *stubAuditLogRepo) GetByID(_ context.Context, _ uuid.UUID) (*entities.AuditLog, error) {
	return nil, nil
}

func (s *stubAuditLogRepo) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *sharedhttp.TimestampCursor,
	_ int,
) ([]*entities.AuditLog, string, error) {
	return s.listByEntityResult, "", s.listByEntityErr
}

func (s *stubAuditLogRepo) List(
	_ context.Context,
	_ entities.AuditLogFilter,
	_ *sharedhttp.TimestampCursor,
	_ int,
) ([]*entities.AuditLog, string, error) {
	return nil, "", nil
}

func TestNewConsumer_Success(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{}
	consumer, _ := newStreamingConsumer(t, repo)
	// Construction does not exercise the persistence path; the streaming
	// fast-path (M3) is asserted by the persistence tests below.
	require.NotNil(t, consumer)
}

func TestNewConsumer_DedupWindowFallback(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{}

	t.Run("zero dedup window falls back to default", func(t *testing.T) {
		t.Parallel()

		consumer, err := NewConsumer(repo, ConsumerConfig{DedupWindow: 0})
		require.NoError(t, err)
		require.NotNil(t, consumer)
		assert.Equal(t, defaultDedupWindow, consumer.dedupWindow)
	})

	t.Run("negative dedup window falls back to default", func(t *testing.T) {
		t.Parallel()

		consumer, err := NewConsumer(repo, ConsumerConfig{DedupWindow: -1 * time.Second})
		require.NoError(t, err)
		require.NotNil(t, consumer)
		assert.Equal(t, defaultDedupWindow, consumer.dedupWindow)
	})

	t.Run("positive dedup window is preserved", func(t *testing.T) {
		t.Parallel()

		customWindow := 12 * time.Second
		consumer, err := NewConsumer(repo, ConsumerConfig{DedupWindow: customWindow})
		require.NoError(t, err)
		require.NotNil(t, consumer)
		assert.Equal(t, customWindow, consumer.dedupWindow)
	})
}

func TestNewConsumer_NilRepo(t *testing.T) {
	t.Parallel()

	consumer, err := NewConsumer(nil)
	require.ErrorIs(t, err, ErrNilAuditRepository)
	require.Nil(t, consumer)
}

func TestConsumer_PublishAuditLogCreated_Success(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{}
	// Default emitter is the lib-streaming NoopEmitter — exercises the
	// streaming-disabled fast-path (M3): repo.Create autocommit, no
	// BeginTx → CreateWithTx → Commit envelope.
	consumer, mock := newStreamingConsumer(t, repo)

	actor := "user-123"
	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-success-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-test-success-tenant"),
		EntityType: "context",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-success-entity"),
		Action:     "create",
		Actor:      &actor,
		Changes:    map[string]any{"name": "test-context"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	err := consumer.PublishAuditLogCreated(auditDispatchContext(event), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.Equal(t, event.EntityType, repo.created.EntityType)
	assert.Equal(t, event.EntityID, repo.created.EntityID)
	assert.Equal(t, event.Action, repo.created.Action)
	assert.Equal(t, event.TenantID, repo.created.TenantID)
	// Fast-path bypasses tx — no Begin / Commit / Rollback should have been
	// observed on sqlmock.
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsumer_PublishAuditLogCreated_NilConsumer(t *testing.T) {
	t.Parallel()

	var consumer *Consumer

	err := consumer.PublishAuditLogCreated(context.Background(), &sharedDomain.AuditLogCreatedEvent{})
	require.ErrorIs(t, err, ErrNilAuditRepository)
}

func TestConsumer_PublishAuditLogCreated_NilEvent(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{}
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

	err = consumer.PublishAuditLogCreated(context.Background(), nil)
	require.ErrorIs(t, err, ErrAuditEventRequired)
}

func TestConsumer_PublishAuditLogCreated_RepoError(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{err: errConsumerRepoFailure}
	// Streaming-disabled fast-path: repo.Create returns the configured
	// failure; no transaction is opened so no Begin / Rollback expectations.
	consumer, mock := newStreamingConsumer(t, repo)

	actor := "admin"
	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-repo-error-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-test-repo-error-tenant"),
		EntityType: "source",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-repo-error-entity"),
		Action:     "update",
		Actor:      &actor,
		Changes:    map[string]any{"status": "active"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	err := consumer.PublishAuditLogCreated(auditDispatchContext(event), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist audit log")
	// Fast-path: error surfaces from repo.Create — no tx state to assert.
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsumer_PublishAuditLogCreated_WithoutActor(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{}
	// Streaming-disabled fast-path: repo.Create autocommit, no tx envelope.
	consumer, mock := newStreamingConsumer(t, repo)

	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-no-actor-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-test-no-actor-tenant"),
		EntityType: "field_map",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-no-actor-entity"),
		Action:     "delete",
		Actor:      nil,
		Changes:    map[string]any{"id": "test"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	err := consumer.PublishAuditLogCreated(auditDispatchContext(event), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.Nil(t, repo.created.ActorID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsumer_PublishAuditLogCreated_RejectsPayloadTenantMismatchWithDispatchContext(t *testing.T) {
	repo := &stubAuditLogRepo{}
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

	dispatchTenantID := testutil.MustDeterministicUUID("consumer-test-dispatch-tenant")
	payloadTenantID := testutil.MustDeterministicUUID("consumer-test-payload-tenant")
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-mismatch-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   payloadTenantID,
		EntityType: "context",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-mismatch-entity"),
		Action:     "create",
		OccurredAt: testutil.FixedTime(),
		Timestamp:  testutil.FixedTime(),
	}
	ctx := tmcore.ContextWithTenantID(context.Background(), dispatchTenantID.String())

	err = consumer.PublishAuditLogCreated(ctx, event)

	require.ErrorIs(t, err, ErrAuditTenantMismatch)
	require.Nil(t, repo.created)
}

func TestConsumer_PublishAuditLogCreated_RejectsMissingDispatchTenantContext(t *testing.T) {
	repo := &stubAuditLogRepo{}
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-missing-context-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-test-missing-context-tenant"),
		EntityType: "context",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-missing-context-entity"),
		Action:     "create",
		OccurredAt: testutil.FixedTime(),
		Timestamp:  testutil.FixedTime(),
	}

	err = consumer.PublishAuditLogCreated(context.Background(), event)

	require.ErrorIs(t, err, ErrAuditTenantContextMissing)
	require.Nil(t, repo.created)
}

func TestConsumer_PublishAuditLogCreated_WithNilChanges(t *testing.T) {
	repo := &stubAuditLogRepo{}
	// Streaming-disabled fast-path: repo.Create autocommit, no tx envelope.
	consumer, mock := newStreamingConsumer(t, repo)

	actor := "system"
	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-nil-changes-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-test-nil-changes-tenant"),
		EntityType: "match_rule",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-nil-changes-entity"),
		Action:     "create",
		Actor:      &actor,
		Changes:    nil,
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	err := consumer.PublishAuditLogCreated(auditDispatchContext(event), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsumer_PublishAuditLogCreated_DuplicateDeliverySkipped(t *testing.T) {
	t.Parallel()

	entityType := "context"
	entityID := testutil.MustDeterministicUUID("consumer-dedup-entity")
	action := "create"

	repo := &stubAuditLogRepo{
		listByEntityResult: []*entities.AuditLog{
			{
				EntityType: entityType,
				EntityID:   entityID,
				Action:     action,
				CreatedAt:  time.Now().UTC().Add(-1 * time.Second), // safely within dedup window even on slow CI
			},
		},
	}
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

	actor := "user-123"
	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-dedup-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-dedup-tenant"),
		EntityType: entityType,
		EntityID:   entityID,
		Action:     action,
		Actor:      &actor,
		Changes:    map[string]any{"name": "test"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	require.NotPanics(t, func() {
		err = consumer.PublishAuditLogCreated(auditDispatchContext(event), event)
	})

	require.NoError(t, err)
	assert.Nil(t, repo.created, "duplicate delivery should be skipped — no new audit log created")
}

// TestConsumer_PublishAuditLogCreated_NilEmitterFallsBackToAutocommit verifies
// the M3 streaming-disabled fast-path: when the emitter is nil (interface-nil),
// emission is treated as soft-disabled and the consumer persists via
// repo.Create (autocommit) without opening a transaction. This is the
// pre-streaming compliance posture — every audit row still lands; only the
// broker emission (which would no-op anyway) is skipped along with its
// transactional envelope. The test deliberately wires a nil
// InfrastructureProvider to prove the fast-path never reaches BeginTx.
func TestConsumer_PublishAuditLogCreated_NilEmitterFallsBackToAutocommit(t *testing.T) {
	repo := &stubAuditLogRepo{}
	consumer, err := NewConsumer(repo, ConsumerConfig{Infrastructure: &nilAuditInfraProvider{}})
	require.NoError(t, err)
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-nil-emitter-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-test-nil-emitter-tenant"),
		EntityType: "context",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-nil-emitter-entity"),
		Action:     "create",
		OccurredAt: testutil.FixedTime(),
		Timestamp:  testutil.FixedTime(),
	}

	err = consumer.PublishAuditLogCreated(auditDispatchContext(event), event)

	require.NoError(t, err)
	require.NotNil(t, repo.created, "fast-path persists via autocommit Create")
	assert.Equal(t, event.EntityType, repo.created.EntityType)
	assert.Equal(t, event.Action, repo.created.Action)
}

// TestConsumer_PublishAuditLogCreated_TypedNilEmitterFallsBackToAutocommit
// verifies emission.IsNilEmitter detects a typed-nil pointer (nil
// *typedNilAuditEmitter wrapped in a non-nil interface header) and routes it
// through the same fast-path as bare-nil. Without this, a typed-nil escape
// would dereference inside Emit() and the consumer would drag the audit log
// down with it.
func TestConsumer_PublishAuditLogCreated_TypedNilEmitterFallsBackToAutocommit(t *testing.T) {
	repo := &stubAuditLogRepo{}
	var emitter *typedNilAuditEmitter
	consumer, err := NewConsumer(repo, ConsumerConfig{
		Infrastructure:   &nilAuditInfraProvider{},
		StreamingEmitter: emitter,
	})
	require.NoError(t, err)
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-typed-nil-emitter-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-test-typed-nil-emitter-tenant"),
		EntityType: "context",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-typed-nil-emitter-entity"),
		Action:     "create",
		OccurredAt: testutil.FixedTime(),
		Timestamp:  testutil.FixedTime(),
	}

	err = consumer.PublishAuditLogCreated(auditDispatchContext(event), event)

	require.NoError(t, err)
	require.NotNil(t, repo.created, "fast-path persists via autocommit Create")
	assert.Equal(t, event.EntityType, repo.created.EntityType)
}

func TestConsumer_PublishAuditLogCreated_StreamingEmitFailureRollsBackTransaction(t *testing.T) {
	repo := &stubAuditLogRepo{}
	streamErr := errors.New("streaming emit failed")
	consumer, mock := newStreamingConsumerWithEmitter(t, repo, failingAuditEmitter{err: streamErr})
	expectAuditStreamingTx(mock, false)

	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-test-stream-fail-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-test-stream-fail-tenant"),
		EntityType: "context",
		EntityID:   testutil.MustDeterministicUUID("consumer-test-stream-fail-entity"),
		Action:     "create",
		OccurredAt: testutil.FixedTime(),
		Timestamp:  testutil.FixedTime(),
	}

	err := consumer.PublishAuditLogCreated(auditDispatchContext(event), event)

	require.ErrorIs(t, err, streamErr)
	require.NotNil(t, repo.created, "repository CreateWithTx succeeds before streaming failure")
	assert.NoError(t, mock.ExpectationsWereMet())
}

type nilAuditInfraProvider struct{}

func (nilAuditInfraProvider) GetRedisConnection(context.Context) (*ports.RedisConnectionLease, error) {
	return nil, nil
}

func (nilAuditInfraProvider) GetReplicaDB(context.Context) (*ports.DBLease, error) { return nil, nil }

func (nilAuditInfraProvider) GetPrimaryDB(context.Context) (*ports.DBLease, error) { return nil, nil }

func (nilAuditInfraProvider) BeginTx(context.Context) (*ports.TxLease, error) { return nil, nil }

func TestConsumer_PublishAuditLogCreated_DifferentAction_NotSkipped(t *testing.T) {
	t.Parallel()

	entityType := "context"
	entityID := testutil.MustDeterministicUUID("consumer-dedup-diff-action-entity")

	repo := &stubAuditLogRepo{
		listByEntityResult: []*entities.AuditLog{
			{
				EntityType: entityType,
				EntityID:   entityID,
				Action:     "create",
				CreatedAt:  time.Now().UTC().Add(-1 * time.Second), // safely within dedup window even on slow CI
			},
		},
	}
	// Streaming-disabled fast-path: repo.Create autocommit, no tx envelope.
	consumer, mock := newStreamingConsumer(t, repo)

	actor := "user-456"
	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-dedup-diff-action-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-dedup-diff-action-tenant"),
		EntityType: entityType,
		EntityID:   entityID,
		Action:     "update", // different action — should NOT be deduped
		Actor:      &actor,
		Changes:    map[string]any{"status": "active"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	err := consumer.PublishAuditLogCreated(auditDispatchContext(event), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created, "different action should not be skipped")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsumer_PublishAuditLogCreated_InterleavedActions_DuplicateSkipped(t *testing.T) {
	t.Parallel()

	entityType := "context"
	entityID := testutil.MustDeterministicUUID("consumer-dedup-interleaved-actions-entity")

	repo := &stubAuditLogRepo{
		listByEntityResult: []*entities.AuditLog{
			{
				EntityType: entityType,
				EntityID:   entityID,
				Action:     "update",
				CreatedAt:  time.Now().UTC().Add(-500 * time.Millisecond),
			},
			{
				EntityType: entityType,
				EntityID:   entityID,
				Action:     "create",
				CreatedAt:  time.Now().UTC().Add(-1 * time.Second),
			},
		},
	}

	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

	actor := "user-456"
	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-dedup-interleaved-actions-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-dedup-interleaved-actions-tenant"),
		EntityType: entityType,
		EntityID:   entityID,
		Action:     "create",
		Actor:      &actor,
		Changes:    map[string]any{"status": "active"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	err = consumer.PublishAuditLogCreated(auditDispatchContext(event), event)
	require.NoError(t, err)
	assert.Nil(t, repo.created, "matching action inside dedup window should be skipped even when latest action differs")
}

func TestConsumer_PublishAuditLogCreated_OldEntry_NotSkipped(t *testing.T) {
	t.Parallel()

	entityType := "source"
	entityID := testutil.MustDeterministicUUID("consumer-dedup-old-entity")
	action := "create"

	repo := &stubAuditLogRepo{
		listByEntityResult: []*entities.AuditLog{
			{
				EntityType: entityType,
				EntityID:   entityID,
				Action:     action,
				CreatedAt:  time.Now().UTC().Add(-time.Minute), // outside dedup window
			},
		},
	}
	// Streaming-disabled fast-path: repo.Create autocommit, no tx envelope.
	consumer, mock := newStreamingConsumer(t, repo)

	actor := "admin"
	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-dedup-old-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-dedup-old-tenant"),
		EntityType: entityType,
		EntityID:   entityID,
		Action:     action,
		Actor:      &actor,
		Changes:    map[string]any{"name": "new-source"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	err := consumer.PublishAuditLogCreated(auditDispatchContext(event), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created, "old entry outside dedup window should not be skipped")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsumer_PublishAuditLogCreated_ListByEntityError_ContinuesNormally(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{
		listByEntityErr: errors.New("database connection lost"),
	}
	// Streaming-disabled fast-path: repo.Create autocommit, no tx envelope.
	consumer, mock := newStreamingConsumer(t, repo)

	actor := "system"
	fixedTime := testutil.FixedTime()
	event := &sharedDomain.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("consumer-dedup-error-unique"),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("consumer-dedup-error-tenant"),
		EntityType: "match_rule",
		EntityID:   testutil.MustDeterministicUUID("consumer-dedup-error-entity"),
		Action:     "create",
		Actor:      &actor,
		Changes:    map[string]any{"key": "value"},
		OccurredAt: fixedTime,
		Timestamp:  fixedTime,
	}

	logger := &capturingAuditLogger{}
	ctx := libCommons.ContextWithLogger(auditDispatchContext(event), logger)

	err := consumer.PublishAuditLogCreated(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created, "dedup check failure should not prevent audit log creation")
	assert.Contains(t, logger.joined(), "dedup check failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

type capturingAuditLogger struct {
	mu       sync.Mutex
	messages []string
}

func (logger *capturingAuditLogger) Log(_ context.Context, _ libLog.Level, msg string, _ ...libLog.Field) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	logger.messages = append(logger.messages, msg)
}

//nolint:ireturn
func (logger *capturingAuditLogger) With(_ ...libLog.Field) libLog.Logger { return logger }

//nolint:ireturn
func (logger *capturingAuditLogger) WithGroup(_ string) libLog.Logger { return logger }

func (*capturingAuditLogger) Enabled(_ libLog.Level) bool { return true }

func (*capturingAuditLogger) Sync(_ context.Context) error { return nil }

func (logger *capturingAuditLogger) joined() string {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	return strings.Join(logger.messages, "\n")
}

func auditDispatchContext(event *sharedDomain.AuditLogCreatedEvent) context.Context {
	if event == nil {
		return context.Background()
	}

	return tmcore.ContextWithTenantID(context.Background(), event.TenantID.String())
}
