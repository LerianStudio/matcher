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

func TestIngestionFlow_CompleteJobWithTransactions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"bank_statement.csv",
			5000,
		)
		require.NoError(t, err)

		require.NoError(t, job.Start(ctx))
		require.Equal(t, value_objects.JobStatusProcessing, job.Status)

		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, createdJob.ID)

		transactions := []struct {
			externalID  string
			amount      decimal.Decimal
			currency    string
			description string
		}{
			{"TXN-001", decimal.NewFromFloat(100.50), "USD", "Wire transfer from ABC Corp"},
			{"TXN-002", decimal.NewFromFloat(250.00), "USD", "Payment for invoice #12345"},
			{"TXN-003", decimal.NewFromFloat(75.25), "EUR", "International payment"},
			{"TXN-004", decimal.NewFromFloat(1000.00), "USD", "Bulk payment batch 1"},
			{"TXN-005", decimal.NewFromFloat(50.00), "GBP", "Refund processing"},
		}

		var createdTxIDs []uuid.UUID

		for _, txData := range transactions {
			tx, err := shared.NewTransaction(
				createdJob.ID,
				h.Seed.SourceID,
				txData.externalID,
				txData.amount,
				txData.currency,
				time.Now().UTC(),
				txData.description,
				map[string]any{"batch": "test"},
			)
			require.NoError(t, err)
			tx.ExtractionStatus = shared.ExtractionStatusComplete

			created, err := tRepo.Create(ctx, tx)
			require.NoError(t, err)
			createdTxIDs = append(createdTxIDs, created.ID)
		}

		require.NoError(t, createdJob.Complete(ctx, len(transactions), 0))
		updatedJob, err := jRepo.Update(ctx, createdJob)
		require.NoError(t, err)
		require.Equal(t, value_objects.JobStatusCompleted, updatedJob.Status)
		require.Equal(t, len(transactions), updatedJob.Metadata.TotalRows)
		require.Equal(t, 0, updatedJob.Metadata.FailedRows)
		require.NotNil(t, updatedJob.CompletedAt)

		unmatched, err := tRepo.ListUnmatchedByContext(ctx, h.Seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, len(transactions))

		err = tRepo.MarkMatched(
			ctx,
			h.Seed.ContextID,
			[]uuid.UUID{createdTxIDs[0], createdTxIDs[1]},
		)
		require.NoError(t, err)

		unmatched, err = tRepo.ListUnmatchedByContext(ctx, h.Seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, len(transactions)-2)

		err = tRepo.MarkPendingReview(ctx, h.Seed.ContextID, []uuid.UUID{createdTxIDs[2]})
		require.NoError(t, err)

		unmatched, err = tRepo.ListUnmatchedByContext(ctx, h.Seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, len(transactions)-3)
	})
}

func TestIngestionFlow_JobWithPartialFailures(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"mixed_results.csv",
			1000,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx1, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"GOOD-001",
			decimal.NewFromFloat(100.00),
			"USD",
			time.Now().UTC(),
			"Valid",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"GOOD-002",
			decimal.NewFromFloat(200.00),
			"USD",
			time.Now().UTC(),
			"Valid",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx2)
		require.NoError(t, err)

		tx3, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"BAD-001",
			decimal.NewFromFloat(0),
			"XXX",
			time.Now().UTC(),
			"Invalid",
			map[string]any{},
		)
		require.NoError(t, err)
		tx3.ExtractionStatus = shared.ExtractionStatusFailed
		_, err = tRepo.Create(ctx, tx3)
		require.NoError(t, err)

		require.NoError(t, createdJob.Complete(ctx, 3, 1))
		updatedJob, err := jRepo.Update(ctx, createdJob)
		require.NoError(t, err)

		require.Equal(t, value_objects.JobStatusCompleted, updatedJob.Status)
		require.Equal(t, 3, updatedJob.Metadata.TotalRows)
		require.Equal(t, 1, updatedJob.Metadata.FailedRows)
	})
}

func TestIngestionFlow_JobFailed(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"corrupted.csv",
			0,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))

		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		failureReason := "File format not recognized: corrupted header"
		require.NoError(t, createdJob.Fail(ctx, failureReason))
		updatedJob, err := jRepo.Update(ctx, createdJob)
		require.NoError(t, err)

		require.Equal(t, value_objects.JobStatusFailed, updatedJob.Status)
		require.NotNil(t, updatedJob.CompletedAt)
	})
}

func TestIngestionFlow_MultipleJobsSameContext(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job1, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"batch1.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job1.Start(ctx))
		createdJob1, err := jRepo.Create(ctx, job1)
		require.NoError(t, err)

		job2, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"batch2.csv",
			200,
		)
		require.NoError(t, err)
		require.NoError(t, job2.Start(ctx))
		createdJob2, err := jRepo.Create(ctx, job2)
		require.NoError(t, err)

		tx1, err := shared.NewTransaction(
			createdJob1.ID,
			h.Seed.SourceID,
			"B1-001",
			decimal.NewFromFloat(50.00),
			"USD",
			time.Now().UTC(),
			"Batch 1",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			createdJob2.ID,
			h.Seed.SourceID,
			"B2-001",
			decimal.NewFromFloat(75.00),
			"USD",
			time.Now().UTC(),
			"Batch 2",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx2)
		require.NoError(t, err)

		require.NoError(t, createdJob1.Complete(ctx, 1, 0))
		_, err = jRepo.Update(ctx, createdJob1)
		require.NoError(t, err)

		require.NoError(t, createdJob2.Complete(ctx, 1, 0))
		_, err = jRepo.Update(ctx, createdJob2)
		require.NoError(t, err)

		unmatched, err := tRepo.ListUnmatchedByContext(ctx, h.Seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, 2)
	})
}

func TestIngestionFlow_TransactionDateFiltering(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		jRepo := jobRepo.NewRepository(h.Provider())
		tRepo := txRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		job, err := entities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"dated.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 3, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		now := time.Now().UTC()
		yesterday := now.Add(-24 * time.Hour)
		lastWeek := now.Add(-7 * 24 * time.Hour)

		tx1, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"OLD-001",
			decimal.NewFromFloat(100.00),
			"USD",
			lastWeek,
			"Old tx",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"RECENT-001",
			decimal.NewFromFloat(200.00),
			"USD",
			yesterday,
			"Recent tx",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx2)
		require.NoError(t, err)

		tx3, err := shared.NewTransaction(
			createdJob.ID,
			h.Seed.SourceID,
			"TODAY-001",
			decimal.NewFromFloat(300.00),
			"USD",
			now,
			"Today tx",
			map[string]any{},
		)
		require.NoError(t, err)
		tx3.ExtractionStatus = shared.ExtractionStatusComplete
		_, err = tRepo.Create(ctx, tx3)
		require.NoError(t, err)

		allUnmatched, err := tRepo.ListUnmatchedByContext(ctx, h.Seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, allUnmatched, 3)

		startDate := yesterday.Add(-1 * time.Hour)
		recentUnmatched, err := tRepo.ListUnmatchedByContext(
			ctx,
			h.Seed.ContextID,
			&startDate,
			nil,
			10,
			0,
		)
		require.NoError(t, err)
		require.Len(t, recentUnmatched, 2)

		endDate := yesterday.Add(1 * time.Hour)
		olderUnmatched, err := tRepo.ListUnmatchedByContext(
			ctx,
			h.Seed.ContextID,
			nil,
			&endDate,
			10,
			0,
		)
		require.NoError(t, err)
		require.Len(t, olderUnmatched, 2)
	})
}
