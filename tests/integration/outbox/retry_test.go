//go:build integration

package outbox

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	outboxRepo "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/outbox"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestOutboxRepository_ResetForRetry_NoEligibleEvents(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		events, err := repo.ResetForRetry(ctx, 10, time.Now().UTC(), 3)
		require.NoError(t, err)
		require.Empty(t, events)
	})
}

func TestOutboxRepository_ResetForRetry_RespectsMaxAttempts(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(ctx, "test.retry", uuid.New(), []byte(`{"test":"data"}`))
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "first failure", 5))
		require.NoError(t, repo.MarkFailed(ctx, created.ID, "second failure", 5))
		require.NoError(t, repo.MarkFailed(ctx, created.ID, "third failure", 5))

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, 3, fetched.Attempts)

		events, err := repo.ResetForRetry(ctx, 10, time.Now().UTC().Add(time.Minute), 3)
		require.NoError(t, err)
		require.Empty(t, events)

		events, err = repo.ResetForRetry(ctx, 10, time.Now().UTC().Add(time.Minute), 5)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.Equal(t, entities.OutboxStatusProcessing, events[0].Status)
	})
}

func TestOutboxRepository_ResetForRetry_RespectsFailedBeforeCutoff(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"test.retry.cutoff",
			uuid.New(),
			[]byte(`{"test":"data"}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "failure", 3))

		pastCutoff := time.Now().UTC().Add(-time.Hour)
		events, err := repo.ResetForRetry(ctx, 10, pastCutoff, 3)
		require.NoError(t, err)
		require.Empty(t, events)

		futureCutoff := time.Now().UTC().Add(time.Minute)
		events, err = repo.ResetForRetry(ctx, 10, futureCutoff, 3)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.Equal(t, entities.OutboxStatusProcessing, events[0].Status)
	})
}

func TestOutboxRepository_ResetForRetry_SetsStatusToProcessing(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"test.retry.status",
			uuid.New(),
			[]byte(`{"test":"data"}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "failure", 3))

		beforeReset, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusFailed, beforeReset.Status)

		events, err := repo.ResetForRetry(ctx, 10, time.Now().UTC().Add(time.Minute), 3)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.Equal(t, entities.OutboxStatusProcessing, events[0].Status)

		afterReset, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusProcessing, afterReset.Status)
		require.True(
			t,
			afterReset.UpdatedAt.After(beforeReset.UpdatedAt) ||
				afterReset.UpdatedAt.Equal(beforeReset.UpdatedAt),
		)
	})
}

func TestOutboxRepository_ListFailedForRetry(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"test.list.failed",
			uuid.New(),
			[]byte(`{"test":"data"}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "failure", 3))

		events, err := repo.ListFailedForRetry(ctx, 10, time.Now().UTC().Add(time.Minute), 3)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.Equal(t, created.ID, events[0].ID)
		require.Equal(t, entities.OutboxStatusFailed, events[0].Status)
	})
}

func TestOutboxRepository_ResetForRetry_EdgeCases(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		_, err := repo.ResetForRetry(ctx, 0, time.Now().UTC(), 3)
		require.Error(t, err)

		_, err = repo.ResetForRetry(ctx, 10, time.Now().UTC(), 0)
		require.Error(t, err)

		_, err = repo.ListFailedForRetry(ctx, 0, time.Now().UTC(), 3)
		require.Error(t, err)

		_, err = repo.ListFailedForRetry(ctx, 10, time.Now().UTC(), 0)
		require.Error(t, err)
	})
}
