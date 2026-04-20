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
// serialized AuditLogCreatedEvent envelope exceeds the broker's per-event cap
// the publisher swaps Changes for a truncation marker and re-marshals, rather
// than failing the triggering business operation.
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
	// value should not be persisted, and the final payload must fit under the
	// broker cap.
	assert.Contains(t, string(repo.created.Payload), `"_truncated":true`)
	assert.NotContains(t, string(repo.created.Payload), "AAAAAAAAAAAAAAAA")
	assert.LessOrEqual(t, len(repo.created.Payload), shared.DefaultOutboxMaxPayloadBytes,
		"post-truncation payload must fit under broker cap")
}

// TestOutboxPublisher_Publish_ExactlyAtCap verifies that a payload at or just
// under the cap is published without truncation (boundary inclusive of cap).
// Uses a binary search over Changes value length to land exactly at the cap,
// accounting for JSON encoding overhead (quotes, escapes, key names).
//
// The envelope Timestamp is pinned to a fixed instant via WithClock so the
// serialized envelope width is deterministic across the binary search and the
// verification Publish call. Zero nanoseconds are used to avoid any RFC3339Nano
// trailing-zero trimming jitter if the serialization format ever changes.
func TestOutboxPublisher_Publish_ExactlyAtCap(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo, WithClock(func() time.Time { return fixedNow }))
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	entityID := uuid.New()
	// Binary search for the largest Changes value length that lands the
	// payload at or below the literal broker cap.
	lo, hi := 0, shared.DefaultOutboxMaxPayloadBytes
	for lo < hi {
		mid := (lo + hi + 1) / 2

		repo.created = nil
		event := ports.AuditEvent{
			EntityType: "context",
			EntityID:   entityID,
			Action:     "update",
			Actor:      "admin",
			OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
			Changes:    map[string]any{"field": strings.Repeat("x", mid)},
		}

		err = publisher.Publish(ctx, event)
		require.NoError(t, err)

		if len(repo.created.Payload) <= shared.DefaultOutboxMaxPayloadBytes &&
			!strings.Contains(string(repo.created.Payload), `"_truncated":true`) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	require.Positive(t, lo, "binary search should find a positive value length")

	repo.created = nil
	atCapEvent := ports.AuditEvent{
		EntityType: "context",
		EntityID:   entityID,
		Action:     "update",
		Actor:      "admin",
		OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Changes:    map[string]any{"field": strings.Repeat("x", lo)},
	}

	err = publisher.Publish(ctx, atCapEvent)
	require.NoError(t, err)
	require.NotNil(t, repo.created)

	assert.NotContains(t, string(repo.created.Payload), `"_truncated":true`)
	assert.LessOrEqual(t, len(repo.created.Payload), shared.DefaultOutboxMaxPayloadBytes)
}

// TestOutboxPublisher_Publish_OneByteOverCap verifies the strict ">" boundary:
// a payload above the cap MUST be truncated. Uses a Changes value ~50 KiB
// above the cap for robustness against future envelope changes while still
// exercising the strict inequality.
func TestOutboxPublisher_Publish_OneByteOverCap(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	event := ports.AuditEvent{
		EntityType: "context",
		EntityID:   uuid.New(),
		Action:     "update",
		Actor:      "admin",
		OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Changes: map[string]any{
			"field": strings.Repeat("x", shared.DefaultOutboxMaxPayloadBytes+(50*1024)),
		},
	}

	err = publisher.Publish(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)

	assert.Contains(t, string(repo.created.Payload), `"_truncated":true`)
	assert.LessOrEqual(t, len(repo.created.Payload), shared.DefaultOutboxMaxPayloadBytes)
}

// TestOutboxPublisher_Publish_UTF8NearBoundary verifies that a multi-byte
// UTF-8 string whose byte length crosses the cap triggers truncation even
// though its rune count would be under the cap.
func TestOutboxPublisher_Publish_UTF8NearBoundary(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// "日" is three UTF-8 bytes; 400000 runes is ~1.14 MiB — above the cap.
	multibyte := strings.Repeat("日", 400000)
	event := ports.AuditEvent{
		EntityType: "context",
		EntityID:   uuid.New(),
		Action:     "update",
		Actor:      "admin",
		OccurredAt: time.Now().UTC(),
		Changes:    map[string]any{"field": multibyte},
	}

	err = publisher.Publish(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)

	assert.Contains(t, string(repo.created.Payload), `"_truncated":true`)
	assert.LessOrEqual(t, len(repo.created.Payload), shared.DefaultOutboxMaxPayloadBytes)
}

// TestOutboxPublisher_Publish_MarshalFailure verifies that a Changes map
// containing a type json.Marshal cannot encode surfaces a wrapped error
// rather than silently dropping the event.
func TestOutboxPublisher_Publish_MarshalFailure(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// Channels are not JSON-marshalable; json.Marshal returns
	// *json.UnsupportedTypeError for this Changes shape.
	event := ports.AuditEvent{
		EntityType: "context",
		EntityID:   uuid.New(),
		Action:     "update",
		Actor:      "admin",
		OccurredAt: time.Now().UTC(),
		Changes:    map[string]any{"field": make(chan int)},
	}

	err = publisher.Publish(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal audit event")
	require.Nil(t, repo.created)
}
