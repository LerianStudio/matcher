//go:build unit

package audit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errOutboxRepoFailure is a sentinel error for outbox repository failures in tests.
var errOutboxRepoFailure = errors.New("outbox repo failure")

// Compile-time interface compliance check.
var _ sharedPorts.OutboxRepository = (*stubOutboxRepo)(nil)

type stubOutboxRepo struct {
	created *shared.OutboxEvent
	err     error
}

func (s *stubOutboxRepo) Create(
	_ context.Context,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	if s.err != nil {
		return nil, s.err
	}

	s.created = event

	return event, nil
}

func (s *stubOutboxRepo) CreateWithTx(
	ctx context.Context,
	_ sharedPorts.Tx,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return s.Create(ctx, event)
}

func (s *stubOutboxRepo) ListPending(
	_ context.Context,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (s *stubOutboxRepo) ListPendingByType(
	_ context.Context,
	_ string,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (s *stubOutboxRepo) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (s *stubOutboxRepo) GetByID(_ context.Context, _ uuid.UUID) (*shared.OutboxEvent, error) {
	return nil, nil
}

func (s *stubOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (s *stubOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ int) error {
	return nil
}

func (s *stubOutboxRepo) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (s *stubOutboxRepo) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (s *stubOutboxRepo) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (s *stubOutboxRepo) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func TestNewOutboxPublisher_Success(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)
	require.NotNil(t, publisher)
}

func TestNewOutboxPublisher_NilRepo(t *testing.T) {
	t.Parallel()

	publisher, err := NewOutboxPublisher(nil)
	require.ErrorIs(t, err, ErrNilOutboxRepository)
	require.Nil(t, publisher)
}

func TestOutboxPublisher_Publish_Success(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	event := ports.AuditEvent{
		EntityType: "context",
		EntityID:   uuid.New(),
		Action:     "create",
		Actor:      "user-123",
		OccurredAt: time.Now().UTC(),
		Changes:    map[string]any{"name": "test-context"},
	}

	err = publisher.Publish(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.Equal(t, "governance.audit_log_created", repo.created.EventType)
	assert.Equal(t, event.EntityID, repo.created.AggregateID)
}

func TestOutboxPublisher_Publish_NilPublisher(t *testing.T) {
	t.Parallel()

	var publisher *OutboxPublisher

	err := publisher.Publish(context.Background(), ports.AuditEvent{})
	require.ErrorIs(t, err, ErrNilOutboxRepository)
}

func TestOutboxPublisher_Publish_InvalidTenantID(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	event := ports.AuditEvent{
		EntityType: "context",
		EntityID:   uuid.New(),
		Action:     "create",
		Actor:      "user-123",
		OccurredAt: time.Now().UTC(),
		Changes:    map[string]any{"name": "test-context"},
	}

	err = publisher.Publish(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tenant id")
	require.Nil(t, repo.created)
}

func TestOutboxPublisher_Publish_RepoError(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{err: errOutboxRepoFailure}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	event := ports.AuditEvent{
		EntityType: "context",
		EntityID:   uuid.New(),
		Action:     "create",
		Actor:      "user-123",
		OccurredAt: time.Now().UTC(),
		Changes:    map[string]any{"name": "test-context"},
	}

	err = publisher.Publish(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist outbox event")
}

func TestOutboxPublisher_Publish_WithoutActor(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	event := ports.AuditEvent{
		EntityType: "source",
		EntityID:   uuid.New(),
		Action:     "update",
		Actor:      "",
		OccurredAt: time.Now().UTC(),
		Changes:    map[string]any{"status": "active"},
	}

	err = publisher.Publish(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.NotEmpty(t, repo.created.Payload)
}

func TestOutboxPublisher_Publish_WithNilChanges(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	event := ports.AuditEvent{
		EntityType: "field_map",
		EntityID:   uuid.New(),
		Action:     "delete",
		Actor:      "admin",
		OccurredAt: time.Now().UTC(),
		Changes:    nil,
	}

	err = publisher.Publish(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
}

// TestOutboxPublisher_Publish_OversizedChangesTruncated verifies that when the
// Changes map serializes above the 900 KiB cap the publisher swaps it for a
// truncation marker instead of failing the triggering business operation.
func TestOutboxPublisher_Publish_OversizedChangesTruncated(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// Build a ~1.5 MiB payload by repeating a large string entry.
	oversizedValue := strings.Repeat("A", 1024*1024+512*1024)
	event := ports.AuditEvent{
		EntityType: "context",
		EntityID:   uuid.New(),
		Action:     "update",
		Actor:      "admin",
		OccurredAt: time.Now().UTC(),
		Changes:    map[string]any{"huge_field": oversizedValue},
	}

	err = publisher.Publish(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)

	// Truncation marker should be present in the payload; the original huge
	// value should not be persisted.
	assert.Contains(t, string(repo.created.Payload), `"_truncated":true`)
	assert.NotContains(t, string(repo.created.Payload), "AAAAAAAAAAAAAAAA")
}
