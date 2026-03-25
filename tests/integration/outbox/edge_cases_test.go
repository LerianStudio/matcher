//go:build integration

package outbox

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	outboxRepo "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/outbox"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestOutboxRepository_MultipleEventsOrdering(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		for i := 0; i < 5; i++ {
			event, err := entities.NewOutboxEvent(
				ctx,
				"test.event."+string(rune('A'+i)),
				uuid.New(),
				[]byte(`{"order":`+string(rune('0'+i))+`}`),
			)
			require.NoError(t, err)
			_, err = repo.Create(ctx, event)
			require.NoError(t, err)
			time.Sleep(10 * time.Millisecond)
		}

		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Len(t, pending, 5)

		for i := 0; i < len(pending)-1; i++ {
			require.True(
				t,
				pending[i].CreatedAt.Before(pending[i+1].CreatedAt) ||
					pending[i].CreatedAt.Equal(pending[i+1].CreatedAt),
				"Events should be ordered by created_at ASC",
			)
		}
	})
}

func TestOutboxRepository_LargePayload(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		data := strings.Repeat("a", 100*1024)
		payloadMap := map[string]string{"data": data}
		largePayload, err := json.Marshal(payloadMap)
		require.NoError(t, err)

		event, err := entities.NewOutboxEvent(ctx, "large.payload.event", uuid.New(), largePayload)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)

		var expected, actual map[string]string
		require.NoError(t, json.Unmarshal(largePayload, &expected))
		require.NoError(t, json.Unmarshal(fetched.Payload, &actual))
		require.Equal(t, expected, actual)
	})
}

func TestOutboxRepository_RetryAfterFailure(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"retry.event",
			uuid.New(),
			[]byte(`{"retry":true}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "first failure", 3))

		firstFetch, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusFailed, firstFetch.Status)
		require.Equal(t, 1, firstFetch.Attempts)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "second failure", 3))

		secondFetch, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusFailed, secondFetch.Status)
		require.Equal(t, 2, secondFetch.Attempts)
		require.Contains(t, secondFetch.LastError, "second failure")
	})
}

func TestOutboxRepository_ProcessingStatusIsNotPending(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(ctx, "processing.test", uuid.New(), []byte(`{}`))
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		pending1, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Len(t, pending1, 1)
		require.Equal(t, created.ID, pending1[0].ID)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusProcessing, fetched.Status)

		pending2, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Empty(t, pending2)
	})
}

func TestOutboxRepository_ConcurrentListPending(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		for i := 0; i < 10; i++ {
			event, err := entities.NewOutboxEvent(
				ctx,
				"concurrent.event."+string(rune('0'+i)),
				uuid.New(),
				[]byte(`{}`),
			)
			require.NoError(t, err)
			_, err = repo.Create(ctx, event)
			require.NoError(t, err)
		}

		pending1, err := repo.ListPending(ctx, 5)
		require.NoError(t, err)
		require.Len(t, pending1, 5)

		pending2, err := repo.ListPending(ctx, 5)
		require.NoError(t, err)
		require.Len(t, pending2, 5)

		for _, p1 := range pending1 {
			for _, p2 := range pending2 {
				require.NotEqual(t, p1.ID, p2.ID, "ListPending should use FOR UPDATE SKIP LOCKED")
			}
		}
	})
}

func TestOutboxRepository_EventTypes(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		eventTypes := []string{
			"ingestion.job.started",
			"ingestion.job.completed",
			"ingestion.job.failed",
			"matching.run.started",
			"matching.run.completed",
			"matching.group.created",
			"exception.created",
		}

		for _, eventType := range eventTypes {
			event, err := entities.NewOutboxEvent(
				ctx,
				eventType,
				uuid.New(),
				[]byte(`{"type":"`+eventType+`"}`),
			)
			require.NoError(t, err)

			created, err := repo.Create(ctx, event)
			require.NoError(t, err)
			require.Equal(t, eventType, created.EventType)
		}

		pending, err := repo.ListPending(ctx, 20)
		require.NoError(t, err)
		require.Len(t, pending, len(eventTypes))
	})
}

func TestOutboxRepository_PublishedEventNotInPending(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(ctx, "publish.test", uuid.New(), []byte(`{}`))
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		pending1, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Len(t, pending1, 1)

		require.NoError(t, repo.MarkPublished(ctx, created.ID, time.Now().UTC()))

		pending2, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Empty(t, pending2)

		published, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusPublished, published.Status)
		require.NotNil(t, published.PublishedAt)
	})
}

func TestOutboxRepository_UnicodePayload(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		unicodePayload := []byte(`{"message":"日本語 - Émojis 🎉 - Кириллица - العربية","status":"ok"}`)
		event, err := entities.NewOutboxEvent(ctx, "unicode.event", uuid.New(), unicodePayload)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)

		var expected, actual map[string]any
		require.NoError(t, json.Unmarshal(unicodePayload, &expected))
		require.NoError(t, json.Unmarshal(fetched.Payload, &actual))
		require.Equal(t, expected, actual)
	})
}

func TestOutboxRepository_ResetStuckProcessing(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"stuck.event",
			uuid.New(),
			[]byte(`{"stuck":true}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusPending, created.Status)

		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, created.ID, pending[0].ID)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusProcessing, fetched.Status)

		db, err := h.Connection.Resolver(ctx)
		require.NoError(t, err)
		primaryDBs := db.PrimaryDBs()
		require.NotEmpty(t, primaryDBs)

		pastTime := time.Now().UTC().Add(-1 * time.Hour)
		_, err = primaryDBs[0].ExecContext(
			ctx,
			"UPDATE outbox_events SET updated_at = $1 WHERE id = $2",
			pastTime,
			created.ID,
		)
		require.NoError(t, err)

		processingBefore := time.Now().UTC().Add(-30 * time.Minute)
		maxAttempts := 10

		recovered, err := repo.ResetStuckProcessing(ctx, 10, processingBefore, maxAttempts)
		require.NoError(t, err)
		require.Len(t, recovered, 1)
		require.Equal(t, created.ID, recovered[0].ID)
		require.Equal(t, entities.OutboxStatusProcessing, recovered[0].Status)

		fetchedAfterReset, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusProcessing, fetchedAfterReset.Status)
	})
}

func TestOutboxRepository_ResetStuckProcessing_RespectsMaxAttempts(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"maxed.out.event",
			uuid.New(),
			[]byte(`{"maxed":true}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		_, err = repo.ListPending(ctx, 10)
		require.NoError(t, err)

		db, err := h.Connection.Resolver(ctx)
		require.NoError(t, err)
		primaryDBs := db.PrimaryDBs()
		require.NotEmpty(t, primaryDBs)

		pastTime := time.Now().UTC().Add(-1 * time.Hour)
		maxAttempts := 3
		_, err = primaryDBs[0].ExecContext(
			ctx,
			"UPDATE outbox_events SET updated_at = $1, attempts = $2 WHERE id = $3",
			pastTime,
			maxAttempts,
			created.ID,
		)
		require.NoError(t, err)

		processingBefore := time.Now().UTC().Add(-30 * time.Minute)

		recovered, err := repo.ResetStuckProcessing(ctx, 10, processingBefore, maxAttempts)
		require.NoError(t, err)
		require.Empty(t, recovered, "Event at max attempts should NOT be recovered")

		fetchedAfter, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusProcessing, fetchedAfter.Status)
	})
}

func TestOutboxRepository_MarkInvalid(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"invalid.event",
			uuid.New(),
			[]byte(`{"invalid":true}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		errMsg := "payload validation failed: missing required field"
		require.NoError(t, repo.MarkInvalid(ctx, created.ID, errMsg))

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusInvalid, fetched.Status)
		require.Contains(t, fetched.LastError, errMsg)

		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Empty(t, pending, "Invalid events should not appear in pending")
	})
}
