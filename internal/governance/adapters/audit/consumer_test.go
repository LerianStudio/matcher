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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
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
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)
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
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

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

	err = consumer.PublishAuditLogCreated(context.Background(), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.Equal(t, event.EntityType, repo.created.EntityType)
	assert.Equal(t, event.EntityID, repo.created.EntityID)
	assert.Equal(t, event.Action, repo.created.Action)
	assert.Equal(t, event.TenantID, repo.created.TenantID)
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
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

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

	err = consumer.PublishAuditLogCreated(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist audit log")
}

func TestConsumer_PublishAuditLogCreated_WithoutActor(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{}
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

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

	err = consumer.PublishAuditLogCreated(context.Background(), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.Nil(t, repo.created.ActorID)
}

func TestConsumer_PublishAuditLogCreated_WithNilChanges(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{}
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

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

	err = consumer.PublishAuditLogCreated(context.Background(), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
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
		err = consumer.PublishAuditLogCreated(context.Background(), event)
	})

	require.NoError(t, err)
	assert.Nil(t, repo.created, "duplicate delivery should be skipped — no new audit log created")
}

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
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

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

	err = consumer.PublishAuditLogCreated(context.Background(), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created, "different action should not be skipped")
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

	err = consumer.PublishAuditLogCreated(context.Background(), event)
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
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

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

	err = consumer.PublishAuditLogCreated(context.Background(), event)
	require.NoError(t, err)
	require.NotNil(t, repo.created, "old entry outside dedup window should not be skipped")
}

func TestConsumer_PublishAuditLogCreated_ListByEntityError_ContinuesNormally(t *testing.T) {
	t.Parallel()

	repo := &stubAuditLogRepo{
		listByEntityErr: errors.New("database connection lost"),
	}
	consumer, err := NewConsumer(repo)
	require.NoError(t, err)

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
	ctx := libCommons.ContextWithLogger(context.Background(), logger)

	err = consumer.PublishAuditLogCreated(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created, "dedup check failure should not prevent audit log creation")
	assert.Contains(t, logger.joined(), "dedup check failed")
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
