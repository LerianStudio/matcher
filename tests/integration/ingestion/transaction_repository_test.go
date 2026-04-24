//go:build integration

package ingestion

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	jobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	txRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Ingestion_TransactionRepository_CreateAndFindByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := txRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"ref-001",
			decimal.NewFromInt(100),
			"USD",
			time.Now().UTC(),
			"payment",
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete

		created, err := repo.Create(ctx, tx)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)

		fetched, err := repo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.ExternalID, fetched.ExternalID)
		require.True(t, created.Amount.Equal(fetched.Amount))
		require.Equal(t, created.Currency, fetched.Currency)
	})
}

func TestIntegration_Ingestion_TransactionRepository_ListUnmatchedByContext(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := txRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 4, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		matchedTx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"matched-1",
			decimal.NewFromInt(10),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		matchedTx.ExtractionStatus = shared.ExtractionStatusComplete
		matchedTx.Status = shared.TransactionStatusMatched
		_, err = repo.Create(ctx, matchedTx)
		require.NoError(t, err)

		unmatchedTx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"unmatched-1",
			decimal.NewFromInt(11),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		unmatchedTx.ExtractionStatus = shared.ExtractionStatusComplete
		unmatchedTx.Status = shared.TransactionStatusUnmatched
		_, err = repo.Create(ctx, unmatchedTx)
		require.NoError(t, err)

		pendingTx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"pending-1",
			decimal.NewFromFloat(11.50),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		pendingTx.ExtractionStatus = shared.ExtractionStatusComplete
		pendingTx.Status = shared.TransactionStatusPendingReview
		_, err = repo.Create(ctx, pendingTx)
		require.NoError(t, err)

		incompleteTx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"incomplete-1",
			decimal.NewFromInt(12),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		incompleteTx.ExtractionStatus = shared.ExtractionStatusPending
		incompleteTx.Status = shared.TransactionStatusUnmatched
		_, err = repo.Create(ctx, incompleteTx)
		require.NoError(t, err)

		results, err := repo.ListUnmatchedByContext(ctx, h.Seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, unmatchedTx.ExternalID, results[0].ExternalID)

		start := time.Now().Add(24 * time.Hour)
		results, err = repo.ListUnmatchedByContext(ctx, h.Seed.ContextID, &start, nil, 10, 0)
		require.NoError(t, err)
		require.Empty(t, results)
	})
}

func TestIntegration_Ingestion_TransactionRepository_MarkMatched(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := txRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 2, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx1, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"mark-match-1",
			decimal.NewFromInt(20),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		created1, err := repo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"mark-match-2",
			decimal.NewFromInt(21),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		created2, err := repo.Create(ctx, tx2)
		require.NoError(t, err)

		err = repo.MarkMatched(ctx, h.Seed.ContextID, []uuid.UUID{created1.ID, created2.ID})
		require.NoError(t, err)

		updated1, err := repo.FindByID(ctx, created1.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusMatched, updated1.Status)

		updated2, err := repo.FindByID(ctx, created2.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusMatched, updated2.Status)
	})
}

func TestIntegration_Ingestion_TransactionRepository_MarkPendingReview(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := txRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 2, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx1, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"review-1",
			decimal.NewFromInt(20),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		created1, err := repo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"review-2",
			decimal.NewFromInt(21),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		created2, err := repo.Create(ctx, tx2)
		require.NoError(t, err)

		err = repo.MarkPendingReview(ctx, h.Seed.ContextID, []uuid.UUID{created1.ID, created2.ID})
		require.NoError(t, err)

		updated1, err := repo.FindByID(ctx, created1.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusPendingReview, updated1.Status)

		updated2, err := repo.FindByID(ctx, created2.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusPendingReview, updated2.Status)
	})
}

func newCompletedJob(t *testing.T, h *integration.TestHarness) *entities.IngestionJob {
	t.Helper()

	jRepo := jobRepo.NewRepository(h.Provider())
	ctx := h.Ctx()

	job, err := entities.NewIngestionJob(ctx, h.Seed.ContextID, h.Seed.SourceID, "test.csv", 100)
	require.NoError(t, err)
	job.Status = value_objects.JobStatusCompleted

	created, err := jRepo.Create(ctx, job)
	require.NoError(t, err)

	return created
}
