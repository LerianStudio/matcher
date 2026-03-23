//go:build integration

package ingestion

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	jobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	txRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestJobRepository_ForeignKeyConstraint(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentSourceID := uuid.New()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			nonExistentSourceID,
			"orphan.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		_, err = repo.Create(ctx, job)
		require.Error(t, err)
	})
}

func TestTransactionRepository_ForeignKeyConstraint(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentJobID := uuid.New()

		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			nonExistentJobID,
			h.Seed.SourceID,
			"orphan-tx",
			decimal.NewFromInt(100),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete

		_, err = repo.Create(ctx, tx)
		require.Error(t, err)
	})
}

func TestTransactionRepository_DuplicateExternalID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(ctx, h.Seed.ContextID, h.Seed.SourceID, "dup.csv", 100)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx1, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"DUP-001",
			decimal.NewFromInt(100),
			"USD",
			time.Now().UTC(),
			"first",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"DUP-001",
			decimal.NewFromInt(200),
			"USD",
			time.Now().UTC(),
			"second",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx2)
		require.Error(t, err)
	})
}

func TestTransactionRepository_LargeAmount(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"large.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 1, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		largeAmount := decimal.NewFromFloat(999999999999.99)
		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"LARGE-001",
			largeAmount,
			"USD",
			time.Now().UTC(),
			"large amount",
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete

		created, err := tRepo.Create(ctx, tx)
		require.NoError(t, err)

		fetched, err := tRepo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.True(t, fetched.Amount.Equal(largeAmount))
	})
}

func TestTransactionRepository_SmallAmount(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"small.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 1, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		smallAmount := decimal.NewFromFloat(0.0001)
		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"SMALL-001",
			smallAmount,
			"USD",
			time.Now().UTC(),
			"small amount",
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete

		created, err := tRepo.Create(ctx, tx)
		require.NoError(t, err)

		fetched, err := tRepo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.True(t, fetched.Amount.Equal(smallAmount))
	})
}

func TestTransactionRepository_NegativeAmount(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"negative.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 1, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		negativeAmount := decimal.NewFromFloat(-500.00)
		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"NEG-001",
			negativeAmount,
			"USD",
			time.Now().UTC(),
			"refund",
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete

		created, err := tRepo.Create(ctx, tx)
		require.NoError(t, err)

		fetched, err := tRepo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.True(t, fetched.Amount.Equal(negativeAmount))
	})
}

func TestTransactionRepository_UnicodeDescription(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"unicode.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 1, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		unicodeDesc := "Payment from 日本語 - Ümlauts - Émojis 🎉 - Кириллица"
		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"UNICODE-001",
			decimal.NewFromInt(100),
			"JPY",
			time.Now().UTC(),
			unicodeDesc,
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete

		created, err := tRepo.Create(ctx, tx)
		require.NoError(t, err)

		fetched, err := tRepo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, unicodeDesc, fetched.Description)
	})
}

func TestTransactionRepository_MetadataJSON(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"metadata.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 1, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		metadata := map[string]any{
			"source_system":      "SAP",
			"batch_id":           "BATCH-2024-001",
			"processing_order":   42,
			"validated":          true,
			"validation_details": map[string]any{"rule": "R001", "passed": true},
		}

		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"META-001",
			decimal.NewFromInt(100),
			"USD",
			time.Now().UTC(),
			"with metadata",
			metadata,
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete

		created, err := tRepo.Create(ctx, tx)
		require.NoError(t, err)

		fetched, err := tRepo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.NotEmpty(t, fetched.Metadata)
	})
}

func TestTransactionRepository_MarkMatched_EmptyList(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		err := tRepo.MarkMatched(ctx, h.Seed.ContextID, []uuid.UUID{})
		require.NoError(t, err)
	})
}

func TestTransactionRepository_MarkPendingReview_EmptyList(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		err := tRepo.MarkPendingReview(ctx, h.Seed.ContextID, []uuid.UUID{})
		require.NoError(t, err)
	})
}

func TestTransactionRepository_MarkMatched_NonExistentIDs(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentIDs := []uuid.UUID{uuid.New(), uuid.New()}
		err := tRepo.MarkMatched(ctx, h.Seed.ContextID, nonExistentIDs)
		require.NoError(t, err)
	})
}

func TestJobRepository_CursorPagination(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		var createdIDs []uuid.UUID
		for i := 0; i < 5; i++ {
			job, err := entities.NewIngestionJob(
				ctx,
				h.Seed.ContextID,
				h.Seed.SourceID,
				"cursor"+string(rune('A'+i))+".csv",
				int64(i*100),
			)
			require.NoError(t, err)
			require.NoError(t, job.Start(ctx))
			job.ID = uuid.MustParse("00000000-0000-0000-0001-00000000000" + string(rune('0'+i)))
			created, err := repo.Create(ctx, job)
			require.NoError(t, err)
			createdIDs = append(createdIDs, created.ID)
		}

		jobs1, pagination1, err := repo.FindByContextID(
			ctx,
			h.Seed.ContextID,
			repositories.CursorFilter{Limit: 2, SortBy: "id"},
		)
		require.NoError(t, err)
		require.Len(t, jobs1, 2)
		require.NotEmpty(t, pagination1.Next)
		require.Empty(t, pagination1.Prev)

		jobs2, pagination2, err := repo.FindByContextID(
			ctx,
			h.Seed.ContextID,
			repositories.CursorFilter{Limit: 2, SortBy: "id", Cursor: pagination1.Next},
		)
		require.NoError(t, err)
		require.Len(t, jobs2, 2)
		require.NotEmpty(t, pagination2.Next)
		require.NotEmpty(t, pagination2.Prev)

		for _, j1 := range jobs1 {
			for _, j2 := range jobs2 {
				require.NotEqual(t, j1.ID, j2.ID, "Jobs should not overlap between pages")
			}
		}
	})
}

func TestJobRepository_UpdateWithTx(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"tx_update.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		var createdJob *entities.IngestionJob

		err = repo.WithTx(ctx, func(tx *sql.Tx) error {
			var txErr error
			createdJob, txErr = repo.CreateWithTx(ctx, tx, job)
			if txErr != nil {
				return txErr
			}

			require.NoError(t, createdJob.Complete(ctx, 50, 5))
			_, txErr = repo.UpdateWithTx(ctx, tx, createdJob)

			return txErr
		})
		require.NoError(t, err)

		fetched, err := repo.FindByID(ctx, createdJob.ID)
		require.NoError(t, err)
		require.Equal(t, 50, fetched.Metadata.TotalRows)
		require.Equal(t, 5, fetched.Metadata.FailedRows)
	})
}

func TestTransactionRepository_FindByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		_, err := repo.FindByID(ctx, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestTransactionRepository_ListUnmatchedByContext_EmptyResult(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentContextID := uuid.New()
		results, err := repo.ListUnmatchedByContext(ctx, nonExistentContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Empty(t, results)
	})
}
