//go:build integration

package outbox

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	outboxRepo "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/outbox"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestOutboxRepository_CreateAndListPending(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"ingestion.completed",
			uuid.New(),
			[]byte(`{"status":"ok"}`),
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
	})
}

func TestOutboxRepository_MarkPublished(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"ingestion.completed",
			uuid.New(),
			[]byte(`{"status":"ok"}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		publishedAt := time.Now().UTC()
		require.NoError(t, repo.MarkPublished(ctx, created.ID, publishedAt))

		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Empty(t, pending)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusPublished, fetched.Status)
		require.NotNil(t, fetched.PublishedAt)
		require.True(t, fetched.PublishedAt.Equal(publishedAt))
	})
}

func TestOutboxRepository_MarkFailed(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		event, err := entities.NewOutboxEvent(
			ctx,
			"ingestion.failed",
			uuid.New(),
			[]byte(`{"status":"failed"}`),
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, event)
		require.NoError(t, err)

		require.NoError(t, repo.MarkFailed(ctx, created.ID, "boom", 3))

		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Empty(t, pending)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, entities.OutboxStatusFailed, fetched.Status)
		require.Contains(t, fetched.LastError, "boom")
	})
}

func TestOutboxRepository_CreateWithTx_CommitAndRollback(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		db, err := h.Connection.Resolver(ctx)
		require.NoError(t, err)

		primaryDBs := db.PrimaryDBs()
		require.NotEmpty(t, primaryDBs)

		tx, err := primaryDBs[0].BeginTx(ctx, nil)
		require.NoError(t, err)

		event, err := entities.NewOutboxEvent(
			ctx,
			"ingestion.tx",
			uuid.New(),
			[]byte(`{"status":"tx"}`),
		)
		require.NoError(t, err)

		created, err := repo.CreateWithTx(ctx, tx, event)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.NotNil(t, created)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)

		txRollback, err := primaryDBs[0].BeginTx(ctx, nil)
		require.NoError(t, err)

		eventRollback, err := entities.NewOutboxEvent(
			ctx,
			"ingestion.rollback",
			uuid.New(),
			[]byte(`{"status":"rollback"}`),
		)
		require.NoError(t, err)

		createdRollback, err := repo.CreateWithTx(ctx, txRollback, eventRollback)
		require.NoError(t, err)
		require.NoError(t, txRollback.Rollback())
		require.NotNil(t, createdRollback)

		fetchedRollback, err := repo.GetByID(ctx, createdRollback.ID)
		require.Error(t, err)
		require.Nil(t, fetchedRollback)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestOutboxRepository_ListPending_Empty(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := outboxRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		pending, err := repo.ListPending(ctx, 10)
		require.NoError(t, err)
		require.Empty(t, pending)
	})
}
