//go:build integration

package matching

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	jobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	txRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	exceptionRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/exception_creator"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Matching_ExceptionCreatorRepository_CreateExceptions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := exceptionRepo.NewRepository(h.Provider())
		runRepo := matchRunRepo.NewRepository(h.Provider())
		transactionRepo := txRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		job, err := ingestionEntities.NewIngestionJob(
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
			"exc-ref-1",
			decimal.NewFromInt(100),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx1.ExtractionStatus = shared.ExtractionStatusComplete
		tx1.Status = shared.TransactionStatusUnmatched
		created1, err := transactionRepo.Create(ctx, tx1)
		require.NoError(t, err)

		tx2, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"exc-ref-2",
			decimal.NewFromInt(200),
			"USD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx2.ExtractionStatus = shared.ExtractionStatusComplete
		tx2.Status = shared.TransactionStatusUnmatched
		created2, err := transactionRepo.Create(ctx, tx2)
		require.NoError(t, err)

		run, err := matchingEntities.NewMatchRun(
			ctx,
			h.Seed.ContextID,
			matchingVO.MatchRunModeCommit,
		)
		require.NoError(t, err)
		createdRun, err := runRepo.Create(ctx, run)
		require.NoError(t, err)

		inputs := []matchingPorts.ExceptionTransactionInput{
			buildExceptionInputFromTx(t, created1, "", "No matching bank transaction"),
			buildExceptionInputFromTx(t, created2, "", "Currency mismatch"),
		}

		err = repo.CreateExceptions(ctx, h.Seed.ContextID, createdRun.ID, inputs, nil)
		require.NoError(t, err)
	})
}

func TestIntegration_Matching_ExceptionCreatorRepository_CreateExceptions_EmptyList(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := exceptionRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		err := repo.CreateExceptions(
			ctx,
			h.Seed.ContextID,
			uuid.New(),
			[]matchingPorts.ExceptionTransactionInput{},
			nil,
		)
		require.NoError(t, err)
	})
}

func TestIntegration_Matching_ExceptionCreatorRepository_CreateExceptions_NilReasons(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := exceptionRepo.NewRepository(h.Provider())
		transactionRepo := txRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 1, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"exc-nil-reason",
			decimal.NewFromInt(50),
			"EUR",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete
		tx.Status = shared.TransactionStatusUnmatched
		created, err := transactionRepo.Create(ctx, tx)
		require.NoError(t, err)

		inputs := []matchingPorts.ExceptionTransactionInput{
			buildExceptionInputFromTx(t, created, "", ""),
		}

		err = repo.CreateExceptions(ctx, h.Seed.ContextID, uuid.New(), inputs, nil)
		require.NoError(t, err)
	})
}

func TestIntegration_Matching_ExceptionCreatorRepository_CreateExceptions_Idempotent(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := exceptionRepo.NewRepository(h.Provider())
		transactionRepo := txRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 1, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"exc-idempotent",
			decimal.NewFromInt(75),
			"GBP",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete
		tx.Status = shared.TransactionStatusUnmatched
		created, err := transactionRepo.Create(ctx, tx)
		require.NoError(t, err)

		inputs := []matchingPorts.ExceptionTransactionInput{
			buildExceptionInputFromTx(t, created, "", ""),
		}

		err = repo.CreateExceptions(ctx, h.Seed.ContextID, uuid.New(), inputs, nil)
		require.NoError(t, err)

		err = repo.CreateExceptions(ctx, h.Seed.ContextID, uuid.New(), inputs, nil)
		require.NoError(t, err)
	})
}

func TestIntegration_Matching_ExceptionCreatorRepository_CreateExceptions_SkipsNilUUIDs(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := exceptionRepo.NewRepository(h.Provider())
		transactionRepo := txRepo.NewRepository(h.Provider())
		jRepo := jobRepo.NewRepository(h.Provider())

		ctx := h.Ctx()

		job, err := ingestionEntities.NewIngestionJob(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			"file.csv",
			100,
		)
		require.NoError(t, err)
		require.NoError(t, job.Start(ctx))
		require.NoError(t, job.Complete(ctx, 1, 0))
		createdJob, err := jRepo.Create(ctx, job)
		require.NoError(t, err)

		tx, err := shared.NewTransaction(
			ctx,
			h.Seed.TenantID,
			createdJob.ID,
			h.Seed.SourceID,
			"exc-skip-nil",
			decimal.NewFromInt(25),
			"CAD",
			time.Now().UTC(),
			"desc",
			map[string]any{},
		)
		require.NoError(t, err)
		tx.ExtractionStatus = shared.ExtractionStatusComplete
		tx.Status = shared.TransactionStatusUnmatched
		created, err := transactionRepo.Create(ctx, tx)
		require.NoError(t, err)

		inputs := []matchingPorts.ExceptionTransactionInput{
			{TransactionID: uuid.Nil},
			buildExceptionInputFromTx(t, created, "", ""),
			{TransactionID: uuid.Nil},
		}

		err = repo.CreateExceptions(ctx, h.Seed.ContextID, uuid.New(), inputs, nil)
		require.NoError(t, err)
	})
}
