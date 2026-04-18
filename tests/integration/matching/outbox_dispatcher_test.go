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
	"github.com/stretchr/testify/require"

	outboxEntities "github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/LerianStudio/matcher/internal/auth"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
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
