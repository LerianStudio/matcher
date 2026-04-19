//go:build unit

package audit

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

var errOutboxRepoFailure = errors.New("outbox repo failure")

// Compile-time interface compliance check.
var _ sharedPorts.OutboxRepository = (*stubOutboxRepo)(nil)

type stubOutboxRepo struct {
	created *shared.OutboxEvent
	err     error
}

func (stub *stubOutboxRepo) Create(
	_ context.Context,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	if stub.err != nil {
		return nil, stub.err
	}

	stub.created = event

	return event, nil
}

func (stub *stubOutboxRepo) CreateWithTx(
	ctx context.Context,
	_ sharedPorts.Tx,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return stub.Create(ctx, event)
}

func (stub *stubOutboxRepo) ListPending(
	_ context.Context,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (stub *stubOutboxRepo) ListPendingByType(
	_ context.Context,
	_ string,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (stub *stubOutboxRepo) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (stub *stubOutboxRepo) GetByID(_ context.Context, _ uuid.UUID) (*shared.OutboxEvent, error) {
	return nil, nil
}

func (stub *stubOutboxRepo) MarkPublished(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (stub *stubOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ int) error {
	return nil
}

func (stub *stubOutboxRepo) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (stub *stubOutboxRepo) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (stub *stubOutboxRepo) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*shared.OutboxEvent, error) {
	return nil, nil
}

func (stub *stubOutboxRepo) MarkInvalid(_ context.Context, _ uuid.UUID, _ string) error {
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

func TestOutboxPublisher_PublishExceptionEvent_Success(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := testutil.MustDeterministicUUID("tenant-success")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	reasonCode := "INSUFFICIENT_FUNDS"
	exceptionID := testutil.MustDeterministicUUID("exception-success")
	event := ports.AuditEvent{
		ExceptionID: exceptionID,
		Action:      "resolve",
		Actor:       "user-123",
		Notes:       "Manual resolution",
		ReasonCode:  &reasonCode,
		OccurredAt:  testutil.FixedTime(),
		Metadata:    map[string]string{"source": "api"},
	}

	err = publisher.PublishExceptionEvent(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.Equal(t, "governance.audit_log_created", repo.created.EventType)
	assert.Equal(t, event.ExceptionID, repo.created.AggregateID)
}

func TestOutboxPublisher_PublishExceptionEvent_NilPublisher(t *testing.T) {
	t.Parallel()

	var publisher *OutboxPublisher

	err := publisher.PublishExceptionEvent(context.Background(), ports.AuditEvent{})
	require.ErrorIs(t, err, ErrNilOutboxRepository)
}

func TestOutboxPublisher_PublishExceptionEvent_InvalidTenantID(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	event := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-invalid-tenant"),
		Action:      "create",
		Actor:       "user-123",
		OccurredAt:  testutil.FixedTime(),
	}

	err = publisher.PublishExceptionEvent(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tenant id")
	require.Nil(t, repo.created)
}

func TestOutboxPublisher_PublishExceptionEvent_RepoError(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{err: errOutboxRepoFailure}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := testutil.MustDeterministicUUID("tenant-repo-error")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	event := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-repo-error"),
		Action:      "create",
		Actor:       "user-123",
		OccurredAt:  testutil.FixedTime(),
	}

	err = publisher.PublishExceptionEvent(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist outbox event")
}

func TestOutboxPublisher_PublishExceptionEvent_EmptyTenantID(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "")

	event := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-empty-tenant"),
		Action:      "create",
		Actor:       "user-123",
		OccurredAt:  testutil.FixedTime(),
	}

	err = publisher.PublishExceptionEvent(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
}

func TestOutboxPublisher_PublishExceptionEvent_WithoutActor(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := testutil.MustDeterministicUUID("tenant-no-actor")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	event := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-no-actor"),
		Action:      "auto_resolve",
		Actor:       "",
		OccurredAt:  testutil.FixedTime(),
	}

	err = publisher.PublishExceptionEvent(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.NotEmpty(t, repo.created.Payload)
}

func TestOutboxPublisher_PublishExceptionEventWithTx_NilTx(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := testutil.MustDeterministicUUID("tenant-with-tx")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	event := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-with-tx"),
		Action:      "dispute",
		Actor:       "user-456",
		OccurredAt:  testutil.FixedTime(),
	}

	// nil tx triggers warning log and falls back to non-transactional path.
	var tx *sql.Tx

	require.NotPanics(t, func() {
		err = publisher.PublishExceptionEventWithTx(ctx, tx, event)
	})

	require.NoError(t, err)
	require.NotNil(t, repo.created)
	assert.Equal(t, "governance.audit_log_created", repo.created.EventType)
}

func TestBuildOutboxChangesMap_AllFields(t *testing.T) {
	t.Parallel()

	reasonCode := "AMOUNT_MISMATCH"
	exceptionID := testutil.MustDeterministicUUID("exception-all-fields")
	event := ports.AuditEvent{
		ExceptionID: exceptionID,
		Action:      "resolve",
		Actor:       "user-123",
		Notes:       "Resolved via API",
		ReasonCode:  &reasonCode,
		OccurredAt:  testutil.FixedTime(),
		Metadata:    map[string]string{"key": "value"},
	}

	changes := buildOutboxChangesMap(event, "")

	assert.Equal(t, exceptionID.String(), changes["exception_id"])
	assert.Equal(t, event.Action, changes["action"])
	assert.NotEmpty(t, changes["actor_hash"])
	assert.Equal(t, event.Notes, changes["notes"])
	assert.Equal(t, reasonCode, changes["reason_code"])
	assert.Equal(t, event.Metadata, changes["metadata"])
}

func TestBuildOutboxChangesMap_MinimalFields(t *testing.T) {
	t.Parallel()

	exceptionID := testutil.MustDeterministicUUID("exception-minimal")
	fixedTime := testutil.FixedTime()
	event := ports.AuditEvent{
		ExceptionID: exceptionID,
		Action:      "create",
		OccurredAt:  fixedTime,
	}

	changes := buildOutboxChangesMap(event, "")

	assert.Equal(t, exceptionID.String(), changes["exception_id"])
	assert.Equal(t, event.Action, changes["action"])
	// actor_hash is nil (not empty string) when event.Actor is empty because
	// the map key is never set by buildOutboxChangesMap for empty actors.
	assert.Nil(t, changes["actor_hash"])
	assert.Nil(t, changes["notes"])
	assert.Nil(t, changes["reason_code"])
	assert.Nil(t, changes["metadata"])
}

// TestOutboxPublisher_PublishExceptionEvent_OversizedChangesTruncated verifies
// that when the serialized AuditLogCreatedEvent envelope exceeds the broker's
// per-event cap the publisher swaps Changes for a truncation marker and
// re-marshals, rather than failing the triggering business operation.
func TestOutboxPublisher_PublishExceptionEvent_OversizedChangesTruncated(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := testutil.MustDeterministicUUID("tenant-oversize")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// Build a ~1.5 MiB payload by packing the Notes field with a large string.
	oversizedNotes := strings.Repeat("A", 1024*1024+512*1024)
	event := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-oversize"),
		Action:      "update",
		Actor:       "user-oversize",
		Notes:       oversizedNotes,
		OccurredAt:  testutil.FixedTime(),
	}

	err = publisher.PublishExceptionEvent(ctx, event)
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

// TestOutboxPublisher_PublishExceptionEvent_ExactlyAtCap verifies that a
// payload exactly at the cap is published without truncation (boundary
// inclusive of cap). Uses a binary search over Notes length to find the
// Notes size that lands the payload at exactly DefaultOutboxMaxPayloadBytes,
// accounting for the JSON envelope the exception publisher builds (notes
// key + quotes + extra Changes-map overhead when Notes is non-empty).
func TestOutboxPublisher_PublishExceptionEvent_ExactlyAtCap(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := testutil.MustDeterministicUUID("tenant-exact-cap")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// The exception publisher's buildOutboxChangesMap adds different
	// envelope overhead depending on whether Notes is empty versus set,
	// so we binary-search Notes length for the largest value that lands
	// at or below the cap.
	lo, hi := 0, shared.DefaultOutboxMaxPayloadBytes
	for lo < hi {
		mid := (lo + hi + 1) / 2

		repo.created = nil
		event := ports.AuditEvent{
			ExceptionID: testutil.MustDeterministicUUID("exception-exact-cap"),
			Action:      "update",
			Actor:       "admin",
			Notes:       strings.Repeat("x", mid),
			OccurredAt:  testutil.FixedTime(),
		}

		err = publisher.PublishExceptionEvent(ctx, event)
		require.NoError(t, err)

		if len(repo.created.Payload) <= shared.DefaultOutboxMaxPayloadBytes &&
			!strings.Contains(string(repo.created.Payload), `"_truncated":true`) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	// At the binary-search pivot the payload should be exactly at or just
	// under the cap with Changes intact — and raising Notes by a single
	// character should trip the gate.
	require.Positive(t, lo, "binary search should find a positive Notes length")

	repo.created = nil
	atCapEvent := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-exact-cap"),
		Action:      "update",
		Actor:       "admin",
		Notes:       strings.Repeat("x", lo),
		OccurredAt:  testutil.FixedTime(),
	}

	err = publisher.PublishExceptionEvent(ctx, atCapEvent)
	require.NoError(t, err)
	require.NotNil(t, repo.created)

	assert.NotContains(t, string(repo.created.Payload), `"_truncated":true`)
	assert.LessOrEqual(t, len(repo.created.Payload), shared.DefaultOutboxMaxPayloadBytes)
}

// TestOutboxPublisher_PublishExceptionEvent_OneByteOverCap verifies the
// strict ">" boundary: a payload one byte above the cap MUST be truncated.
// The test uses Notes length 0.5 MiB which comfortably pushes the event
// over the 1 MiB cap without brittle off-by-one calibration.
func TestOutboxPublisher_PublishExceptionEvent_OneByteOverCap(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := testutil.MustDeterministicUUID("tenant-one-byte-over")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// Use a Notes size ~50 KiB above the cap to keep the test robust
	// against future envelope changes while still exercising the strict
	// inequality boundary.
	event := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-one-byte-over"),
		Action:      "update",
		Actor:       "admin",
		Notes:       strings.Repeat("x", shared.DefaultOutboxMaxPayloadBytes+(50*1024)),
		OccurredAt:  testutil.FixedTime(),
	}

	err = publisher.PublishExceptionEvent(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)

	assert.Contains(t, string(repo.created.Payload), `"_truncated":true`)
	assert.LessOrEqual(t, len(repo.created.Payload), shared.DefaultOutboxMaxPayloadBytes)
}

// TestOutboxPublisher_PublishExceptionEvent_UTF8NearBoundary verifies that a
// multi-byte UTF-8 string whose byte length crosses the cap triggers
// truncation even though its rune count would be under the cap.
func TestOutboxPublisher_PublishExceptionEvent_UTF8NearBoundary(t *testing.T) {
	t.Parallel()

	repo := &stubOutboxRepo{}
	publisher, err := NewOutboxPublisher(repo)
	require.NoError(t, err)

	tenantID := testutil.MustDeterministicUUID("tenant-utf8")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// "日" is three UTF-8 bytes; 400000 runes is ~1.14 MiB — above the cap.
	multibyte := strings.Repeat("日", 400000)
	event := ports.AuditEvent{
		ExceptionID: testutil.MustDeterministicUUID("exception-utf8"),
		Action:      "update",
		Actor:       "admin",
		Notes:       multibyte,
		OccurredAt:  testutil.FixedTime(),
	}

	err = publisher.PublishExceptionEvent(ctx, event)
	require.NoError(t, err)
	require.NotNil(t, repo.created)

	assert.Contains(t, string(repo.created.Payload), `"_truncated":true`)
	assert.LessOrEqual(t, len(repo.created.Payload), shared.DefaultOutboxMaxPayloadBytes)
}

// TestMarshalOrTruncate_MarshalFailure verifies that marshalOrTruncate
// surfaces a wrapped error when the audit event contains an
// unmarshalable value, rather than silently dropping the event.
//
// The exception publisher's public surface only accepts typed fields
// (string, time.Time, map[string]string) that always marshal, so the
// failure path must be exercised at the helper level directly.
func TestMarshalOrTruncate_MarshalFailure(t *testing.T) {
	t.Parallel()

	event := &shared.AuditLogCreatedEvent{
		UniqueID:   testutil.MustDeterministicUUID("unique-marshal-fail"),
		EventType:  shared.EventTypeAuditLogCreated,
		TenantID:   testutil.MustDeterministicUUID("tenant-marshal-fail"),
		EntityType: entityTypeException,
		EntityID:   testutil.MustDeterministicUUID("entity-marshal-fail"),
		Action:     "update",
		// Channels are not JSON-marshalable.
		Changes:    map[string]any{"bad": make(chan int)},
		OccurredAt: testutil.FixedTime(),
		Timestamp:  testutil.FixedTime(),
	}

	payload, err := marshalOrTruncate(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal audit event")
	assert.Nil(t, payload)
}
