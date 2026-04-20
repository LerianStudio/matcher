//go:build integration

package matching

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	outboxEntities "github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/bootstrap"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestOutboxDispatcher_DispatchOnce_PublishesPendingEvents verifies that
// a valid pending outbox event is published by the dispatcher.
func TestOutboxDispatcher_DispatchOnce_PublishesPendingEvents(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := integration.NewTestOutboxRepository(t, h.Connection)

		payload := validMatchConfirmedPayload(h.Seed.TenantID)
		event, err := outboxEntities.NewOutboxEvent(
			ctx,
			shared.EventTypeMatchConfirmed,
			payload.MatchID,
			mustJSON(t, payload),
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, event)
		require.NoError(t, err)

		cap := &capturePublishers{}
		dispatcher := newDispatcher(t, h, cap)

		processed := dispatcher.DispatchOnce(ctx)
		require.Equal(t, 1, processed)
		require.Equal(t, 1, cap.matchConfirmed)
		require.NotNil(t, cap.last)
		require.Equal(t, shared.EventTypeMatchConfirmed, cap.last.EventType)
	})
}

// TestOutboxDispatcher_RetriesFailedEvents verifies that a FAILED event
// with an old updated_at timestamp is retried and published.
func TestOutboxDispatcher_RetriesFailedEvents(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := integration.NewTestOutboxRepository(t, h.Connection)

		payload := validMatchConfirmedPayload(h.Seed.TenantID)
		event, err := outboxEntities.NewOutboxEvent(
			ctx,
			shared.EventTypeMatchConfirmed,
			payload.MatchID,
			mustJSON(t, payload),
		)
		require.NoError(t, err)
		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		// Move to PROCESSING (canonical outbox requires this before MarkFailed)
		_, err = repo.ListPending(ctx, 10)
		require.NoError(t, err)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "transient network error", 3))

		// Backdate updated_at beyond retry window
		backdateOutboxEvent(t, ctx, h, created.ID, 10*time.Minute)

		cap := &capturePublishers{}
		dispatcher := newDispatcher(t, h, cap, outboxEntities.WithRetryWindow(1*time.Second))

		processed := dispatcher.DispatchOnce(ctx)
		require.GreaterOrEqual(t, processed, 1)
		require.GreaterOrEqual(t, cap.matchConfirmed, 1, "Failed event should be retried and published")

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusPublished, fetched.Status)
	})
}

// TestOutboxDispatcher_ResetsStuckProcessing verifies that a PROCESSING event
// stuck beyond the processing timeout is reset and successfully published.
func TestOutboxDispatcher_ResetsStuckProcessing(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := integration.NewTestOutboxRepository(t, h.Connection)

		payload := validMatchConfirmedPayload(h.Seed.TenantID)
		event, err := outboxEntities.NewOutboxEvent(
			ctx,
			shared.EventTypeMatchConfirmed,
			payload.MatchID,
			mustJSON(t, payload),
		)
		require.NoError(t, err)
		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		// ListPending atomically moves the event to PROCESSING
		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Len(t, pending, 1)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusProcessing, fetched.Status)

		// Simulate stuck processing by backdating updated_at
		backdateOutboxEvent(t, ctx, h, created.ID, 15*time.Minute)

		cap := &capturePublishers{}
		dispatcher := newDispatcher(t, h, cap, outboxEntities.WithProcessingTimeout(1*time.Second))

		processed := dispatcher.DispatchOnce(ctx)
		require.GreaterOrEqual(t, processed, 1)
		require.GreaterOrEqual(t, cap.matchConfirmed, 1,
			"Stuck processing event should be reset and published",
		)

		fetched2, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusPublished, fetched2.Status)
	})
}

// TestOutboxDispatcher_MarksInvalidOnBadPayload verifies that an event with
// invalid payload is classified as non-retryable and marked INVALID.
func TestOutboxDispatcher_MarksInvalidOnBadPayload(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := integration.NewTestOutboxRepository(t, h.Connection)

		// Syntactically valid JSON but missing required fields.
		// The handler's json.Unmarshal succeeds but produces a zero-valued
		// MatchConfirmedEvent with no TenantID — which the capture handler
		// still records. We use a custom retry classifier to mark any handler
		// error as non-retryable.
		badPayload := []byte(`{"broken":true}`)
		event, err := outboxEntities.NewOutboxEvent(
			ctx,
			shared.EventTypeMatchConfirmed,
			uuid.New(),
			badPayload,
		)
		require.NoError(t, err)
		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		// Classify all json.SyntaxError as non-retryable so the dispatcher
		// marks the event INVALID instead of retrying indefinitely.
		classifier := outboxEntities.RetryClassifierFunc(func(err error) bool {
			return err != nil // any handler error is non-retryable in this test
		})

		cap := &capturePublishers{}
		dispatcher := newDispatcher(t, h, cap,
			outboxEntities.WithRetryClassifier(classifier),
		)

		processed := dispatcher.DispatchOnce(ctx)
		require.Equal(t, 1, processed)
		require.Equal(t, 0, cap.matchConfirmed,
			"Bad payload should not produce a successful publish",
		)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusInvalid, fetched.Status)
		require.NotEmpty(t, fetched.LastError)
	})
}

// TestOutboxDispatcher_SkipsRecentlyFailedEvents verifies that a FAILED event
// with a recent updated_at timestamp is NOT retried within the retry window.
func TestOutboxDispatcher_SkipsRecentlyFailedEvents(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := integration.NewTestOutboxRepository(t, h.Connection)

		payload := validMatchConfirmedPayload(h.Seed.TenantID)
		event, err := outboxEntities.NewOutboxEvent(
			ctx,
			shared.EventTypeMatchConfirmed,
			payload.MatchID,
			mustJSON(t, payload),
		)
		require.NoError(t, err)
		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		// Move to PROCESSING (canonical outbox requires this before MarkFailed)
		_, err = repo.ListPending(ctx, 10)
		require.NoError(t, err)

		// Mark as failed — updated_at is automatically set to now
		require.NoError(t, repo.MarkFailed(ctx, created.ID, "transient error", 3))

		cap := &capturePublishers{}
		dispatcher := newDispatcher(t, h, cap, outboxEntities.WithRetryWindow(10*time.Minute))

		processed := dispatcher.DispatchOnce(ctx)
		require.Equal(t, 0, processed)
		require.Equal(t, 0, cap.matchConfirmed,
			"Recently failed event should not be retried",
		)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusFailed, fetched.Status)
	})
}

func TestOutboxDispatcher_DispatchOnce_MultiTenantDBBacked(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := integration.NewTestOutboxRepository(t, h.Connection)

		tenantA := uuid.New()
		tenantB := uuid.New()
		require.NoError(t, ensureTenantOutboxSchema(t, h, tenantA.String()))
		require.NoError(t, ensureTenantOutboxSchema(t, h, tenantB.String()))

		ctxA := outboxEntities.ContextWithTenantID(context.Background(), tenantA.String())
		ctxB := outboxEntities.ContextWithTenantID(context.Background(), tenantB.String())

		payloadA := validMatchConfirmedPayload(tenantA)
		eventA, err := outboxEntities.NewOutboxEvent(ctxA, shared.EventTypeMatchConfirmed, payloadA.MatchID, mustJSON(t, payloadA))
		require.NoError(t, err)
		createdA, err := repo.Create(ctxA, eventA)
		require.NoError(t, err)

		payloadB := validMatchConfirmedPayload(tenantB)
		eventB, err := outboxEntities.NewOutboxEvent(ctxB, shared.EventTypeMatchConfirmed, payloadB.MatchID, mustJSON(t, payloadB))
		require.NoError(t, err)
		createdB, err := repo.Create(ctxB, eventB)
		require.NoError(t, err)

		cap := &capturePublishers{}
		dispatcher := newDispatcher(t, h, cap)

		processedA := dispatcher.DispatchOnce(ctxA)
		processedB := dispatcher.DispatchOnce(ctxB)
		require.Equal(t, 1, processedA)
		require.Equal(t, 1, processedB)
		require.Equal(t, 2, cap.matchConfirmed)
		require.ElementsMatch(t, []uuid.UUID{tenantA, tenantB}, cap.tenantIDs)

		fetchedA, err := repo.GetByID(ctxA, createdA.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusPublished, fetchedA.Status)

		fetchedB, err := repo.GetByID(ctxB, createdB.ID)
		require.NoError(t, err)
		require.Equal(t, outboxEntities.OutboxStatusPublished, fetchedB.Status)
	})
}

// allEventCapture records calls to every publisher interface the outbox
// dispatcher can invoke, indexed by concrete event type. Unlike the
// matching-focused capturePublishers in helpers_test.go, this stub also
// satisfies sharedPorts.IngestionEventPublisher and
// shared.AuditEventPublisher — required for the 5-event-type parameterized
// test below.
type allEventCapture struct {
	matchConfirmed     []*shared.MatchConfirmedEvent
	matchUnmatched     []*shared.MatchUnmatchedEvent
	ingestionCompleted []*shared.IngestionCompletedEvent
	ingestionFailed    []*shared.IngestionFailedEvent
	auditLogCreated    []*shared.AuditLogCreatedEvent
}

func (c *allEventCapture) PublishMatchConfirmed(_ context.Context, event *shared.MatchConfirmedEvent) error {
	c.matchConfirmed = append(c.matchConfirmed, event)
	return nil
}

func (c *allEventCapture) PublishMatchUnmatched(_ context.Context, event *shared.MatchUnmatchedEvent) error {
	c.matchUnmatched = append(c.matchUnmatched, event)
	return nil
}

func (c *allEventCapture) PublishIngestionCompleted(_ context.Context, event *shared.IngestionCompletedEvent) error {
	c.ingestionCompleted = append(c.ingestionCompleted, event)
	return nil
}

func (c *allEventCapture) PublishIngestionFailed(_ context.Context, event *shared.IngestionFailedEvent) error {
	c.ingestionFailed = append(c.ingestionFailed, event)
	return nil
}

func (c *allEventCapture) PublishAuditLogCreated(_ context.Context, event *shared.AuditLogCreatedEvent) error {
	c.auditLogCreated = append(c.auditLogCreated, event)
	return nil
}

var (
	_ shared.MatchEventPublisher          = (*allEventCapture)(nil)
	_ sharedPorts.IngestionEventPublisher = (*allEventCapture)(nil)
	_ shared.AuditEventPublisher          = (*allEventCapture)(nil)
)

// newAllEventDispatcher wires a dispatcher with a capture that records
// every event-type publisher. Used by tests that need to assert a
// specific event type was delivered without also having to stub the
// other four no-op publishers inline.
func newAllEventDispatcher(
	t *testing.T,
	h *integration.TestHarness,
	cap *allEventCapture,
	opts ...outboxEntities.DispatcherOption,
) *outboxEntities.Dispatcher {
	t.Helper()

	repo := integration.NewTestOutboxRepository(t, h.Connection)
	registry := outboxEntities.NewHandlerRegistry()
	err := bootstrap.RegisterOutboxHandlers(registry, cap, cap, cap)
	require.NoError(t, err)

	dispatcher, err := outboxEntities.NewDispatcher(
		repo,
		registry,
		nil,
		noop.NewTracerProvider().Tracer("tests.integration.outbox.all_events"),
		opts...,
	)
	require.NoError(t, err)

	return dispatcher
}

// TestOutboxDispatcher_DispatchOnce_AllEventTypes parameterizes the
// single-happy-path test (which previously covered only
// EventTypeMatchConfirmed) over every event type registered via
// bootstrap.RegisterOutboxHandlers. A regression that drops a handler
// from the registry — or one that swaps publishers for the wrong event
// type — fails here rather than leaking into production silently.
func TestOutboxDispatcher_DispatchOnce_AllEventTypes(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		tenantID := h.Seed.TenantID

		cases := []struct {
			name      string
			eventType string
			buildID   uuid.UUID // the event's idempotency ID, used for NewOutboxEvent
			buildJSON func() []byte
			assert    func(t *testing.T, cap *allEventCapture)
		}{
			{
				name:      "match_confirmed",
				eventType: shared.EventTypeMatchConfirmed,
				buildID:   uuid.New(),
				buildJSON: func() []byte {
					p := validMatchConfirmedPayload(tenantID)
					return mustJSON(t, p)
				},
				assert: func(t *testing.T, cap *allEventCapture) {
					require.Len(t, cap.matchConfirmed, 1, "match_confirmed must produce exactly one publish call")
					assert.Equal(t, shared.EventTypeMatchConfirmed, cap.matchConfirmed[0].EventType)
					// No cross-publisher leakage.
					assert.Empty(t, cap.matchUnmatched)
					assert.Empty(t, cap.ingestionCompleted)
					assert.Empty(t, cap.ingestionFailed)
					assert.Empty(t, cap.auditLogCreated)
				},
			},
			{
				name:      "match_unmatched",
				eventType: shared.EventTypeMatchUnmatched,
				buildID:   uuid.New(),
				buildJSON: func() []byte {
					p := shared.MatchUnmatchedEvent{
						EventType:      shared.EventTypeMatchUnmatched,
						TenantID:       tenantID,
						ContextID:      uuid.New(),
						RunID:          uuid.New(),
						MatchID:        uuid.New(),
						RuleID:         uuid.New(),
						TransactionIDs: []uuid.UUID{uuid.New()},
						Reason:         "superseded",
						Timestamp:      time.Now().UTC(),
					}
					return mustJSON(t, p)
				},
				assert: func(t *testing.T, cap *allEventCapture) {
					require.Len(t, cap.matchUnmatched, 1)
					assert.Equal(t, shared.EventTypeMatchUnmatched, cap.matchUnmatched[0].EventType)
					assert.Equal(t, "superseded", cap.matchUnmatched[0].Reason)
					assert.Empty(t, cap.matchConfirmed)
					assert.Empty(t, cap.ingestionCompleted)
					assert.Empty(t, cap.ingestionFailed)
					assert.Empty(t, cap.auditLogCreated)
				},
			},
			{
				name:      "ingestion_completed",
				eventType: shared.EventTypeIngestionCompleted,
				buildID:   uuid.New(),
				buildJSON: func() []byte {
					p := shared.IngestionCompletedEvent{
						EventType:   shared.EventTypeIngestionCompleted,
						JobID:       uuid.New(),
						ContextID:   uuid.New(),
						SourceID:    uuid.New(),
						TotalRows:   100,
						CompletedAt: time.Now().UTC(),
						Timestamp:   time.Now().UTC(),
					}
					return mustJSON(t, p)
				},
				assert: func(t *testing.T, cap *allEventCapture) {
					require.Len(t, cap.ingestionCompleted, 1)
					assert.Equal(t, shared.EventTypeIngestionCompleted, cap.ingestionCompleted[0].EventType)
					assert.Equal(t, 100, cap.ingestionCompleted[0].TotalRows)
					assert.Empty(t, cap.matchConfirmed)
					assert.Empty(t, cap.matchUnmatched)
					assert.Empty(t, cap.ingestionFailed)
					assert.Empty(t, cap.auditLogCreated)
				},
			},
			{
				name:      "ingestion_failed",
				eventType: shared.EventTypeIngestionFailed,
				buildID:   uuid.New(),
				buildJSON: func() []byte {
					p := shared.IngestionFailedEvent{
						EventType: shared.EventTypeIngestionFailed,
						JobID:     uuid.New(),
						ContextID: uuid.New(),
						SourceID:  uuid.New(),
						Error:     "parse error: invalid column",
						Timestamp: time.Now().UTC(),
					}
					return mustJSON(t, p)
				},
				assert: func(t *testing.T, cap *allEventCapture) {
					require.Len(t, cap.ingestionFailed, 1)
					assert.Equal(t, shared.EventTypeIngestionFailed, cap.ingestionFailed[0].EventType)
					assert.Equal(t, "parse error: invalid column", cap.ingestionFailed[0].Error)
					assert.Empty(t, cap.matchConfirmed)
					assert.Empty(t, cap.matchUnmatched)
					assert.Empty(t, cap.ingestionCompleted)
					assert.Empty(t, cap.auditLogCreated)
				},
			},
			{
				name:      "audit_log_created",
				eventType: shared.EventTypeAuditLogCreated,
				buildID:   uuid.New(),
				buildJSON: func() []byte {
					p := shared.AuditLogCreatedEvent{
						UniqueID:   uuid.New(),
						EventType:  shared.EventTypeAuditLogCreated,
						TenantID:   tenantID,
						EntityType: "reconciliation_context",
						EntityID:   uuid.New(),
						Action:     "CREATE",
						OccurredAt: time.Now().UTC(),
						Timestamp:  time.Now().UTC(),
					}
					return mustJSON(t, p)
				},
				assert: func(t *testing.T, cap *allEventCapture) {
					require.Len(t, cap.auditLogCreated, 1)
					assert.Equal(t, shared.EventTypeAuditLogCreated, cap.auditLogCreated[0].EventType)
					assert.Equal(t, "CREATE", cap.auditLogCreated[0].Action)
					assert.Empty(t, cap.matchConfirmed)
					assert.Empty(t, cap.matchUnmatched)
					assert.Empty(t, cap.ingestionCompleted)
					assert.Empty(t, cap.ingestionFailed)
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				repo := integration.NewTestOutboxRepository(t, h.Connection)

				event, err := outboxEntities.NewOutboxEvent(
					ctx,
					tc.eventType,
					tc.buildID,
					tc.buildJSON(),
				)
				require.NoError(t, err)

				_, err = repo.Create(ctx, event)
				require.NoError(t, err)

				cap := &allEventCapture{}
				dispatcher := newAllEventDispatcher(t, h, cap)

				processed := dispatcher.DispatchOnce(ctx)
				require.GreaterOrEqual(t, processed, 1, "dispatcher must process the pending event")

				tc.assert(t, cap)
			})
		}
	})
}

// TestOutboxDispatcher_ProductionClassifier_MarksInvalidOnMalformedJSON
// asserts that when the dispatcher is wired with the ACTUAL production
// classifier (bootstrap.IsNonRetryableOutboxError), structurally-invalid
// payloads (ones that json.Unmarshal cannot decode into the target event
// type) are classified as non-retryable and marked INVALID. This differs
// from TestOutboxDispatcher_MarksInvalidOnBadPayload above, which uses
// a bespoke "any-error-is-non-retryable" classifier for demonstration
// purposes — production's classifier only marks INVALID for specific
// sentinel errors, and a regression that drops one of those sentinels
// would make the dispatcher retry corrupt payloads forever.
//
// The payload here is syntactically valid JSON — required because both
// the JSONB column and NewOutboxEvent's json.Valid guard reject truly
// malformed bytes at insert time — but structurally wrong for the
// target event struct. json.Unmarshal returns *json.UnmarshalTypeError
// in the publishMatchConfirmed handler, which wraps it with
// errInvalidPayload (the sentinel in nonRetryableErrors that drives
// the INVALID classification). This exercises the same classifier
// code path the original truncated-JSON scenario did.
func TestOutboxDispatcher_ProductionClassifier_MarksInvalidOnMalformedJSON(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := integration.NewTestOutboxRepository(t, h.Connection)

		// Valid JSON, wrong shape: a numeric array cannot be decoded into
		// MatchConfirmedEvent, so json.Unmarshal returns *json.UnmarshalTypeError,
		// which bootstrap wraps with errInvalidPayload before bubbling. That
		// sentinel is in nonRetryableErrors, so the production classifier
		// classifies it non-retryable.
		corrupt := []byte(`[1,2,3]`)

		event, err := outboxEntities.NewOutboxEvent(
			ctx,
			shared.EventTypeMatchConfirmed,
			uuid.New(),
			corrupt,
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		cap := &allEventCapture{}
		dispatcher := newAllEventDispatcher(t, h, cap,
			outboxEntities.WithRetryClassifier(outboxEntities.RetryClassifierFunc(bootstrap.IsNonRetryableOutboxError)),
		)

		processed := dispatcher.DispatchOnce(ctx)
		require.Equal(t, 1, processed)
		assert.Empty(t, cap.matchConfirmed, "corrupt payload must not produce a successful publish")

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, outboxEntities.OutboxStatusInvalid, fetched.Status,
			"production classifier must mark structurally-invalid payload as INVALID, not FAILED/PENDING")
		assert.NotEmpty(t, fetched.LastError, "INVALID event must carry the last error for diagnosis")
	})
}

// --- Helpers ---

func validMatchConfirmedPayload(tenantID uuid.UUID) shared.MatchConfirmedEvent {
	return shared.MatchConfirmedEvent{
		EventType:      shared.EventTypeMatchConfirmed,
		TenantID:       tenantID,
		ContextID:      uuid.New(),
		RunID:          uuid.New(),
		MatchID:        uuid.New(),
		RuleID:         uuid.New(),
		TransactionIDs: []uuid.UUID{uuid.New(), uuid.New()},
		Confidence:     100,
		ConfirmedAt:    time.Now().UTC(),
		Timestamp:      time.Now().UTC(),
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()

	data, err := json.Marshal(v)
	require.NoError(t, err)

	return data
}

func backdateOutboxEvent(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	eventID uuid.UUID,
	age time.Duration,
) {
	t.Helper()

	pastTime := time.Now().UTC().Add(-age)

	_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx,
			"UPDATE outbox_events SET updated_at = $1 WHERE id = $2",
			pastTime, eventID,
		)

		return struct{}{}, execErr
	})
	require.NoError(t, err)
}

func ensureTenantOutboxSchema(t *testing.T, h *integration.TestHarness, tenantID string) error {
	t.Helper()

	resolver, err := h.Connection.Resolver(context.Background())
	if err != nil {
		return err
	}

	quotedID := auth.QuoteIdentifier(tenantID)
	queries := []string{
		"CREATE SCHEMA IF NOT EXISTS " + quotedID,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.outbox_events (LIKE public.outbox_events INCLUDING ALL)", quotedID),
	}

	for _, query := range queries {
		if _, err := resolver.ExecContext(context.Background(), query); err != nil {
			return err
		}
	}

	return nil
}
