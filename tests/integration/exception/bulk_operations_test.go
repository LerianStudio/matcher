//go:build integration

package exception

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	exceptionCommand "github.com/LerianStudio/matcher/internal/exception/services/command"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestBulkAssign_AssignsMultipleExceptions(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "bulk-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 3)

		// Create 3 OPEN exceptions backed by distinct transactions.
		var exceptionIDs []uuid.UUID
		for i := range 3 {
			tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
				"BULK-ASSIGN-"+uuid.New().String()[:8],
				decimal.NewFromFloat(100.00+float64(i)),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
				exceptionVO.ExceptionSeverityLow, "UNMATCHED: no counterparty")

			exceptionIDs = append(exceptionIDs, exc.ID)
		}

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		result, err := uc.BulkAssign(ctx, exceptionCommand.BulkAssignInput{
			ExceptionIDs: exceptionIDs,
			Assignee:     "operator-A",
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Succeeded, 3, "all 3 exceptions should succeed")
		require.Empty(t, result.Failed, "no failures expected")

		// Verify each exception is now ASSIGNED with correct assignee.
		exceptionRepo := wired.ExceptionRepo
		for _, id := range exceptionIDs {
			loaded, findErr := exceptionRepo.FindByID(ctx, id)
			require.NoError(t, findErr)
			require.Equal(t, exceptionVO.ExceptionStatusAssigned, loaded.Status,
				"exception %s should be ASSIGNED", id)
			require.NotNil(t, loaded.AssignedTo)
			require.Equal(t, "operator-A", *loaded.AssignedTo)
		}
	})
}

func TestBulkAssign_PartialInvalidIDs(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "bulk-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 2)

		// Create 2 valid OPEN exceptions.
		var validIDs []uuid.UUID
		for i := range 2 {
			tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
				"BULK-PARTIAL-"+uuid.New().String()[:8],
				decimal.NewFromFloat(200.00+float64(i)),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
				exceptionVO.ExceptionSeverityMedium, "UNMATCHED: partial test")

			validIDs = append(validIDs, exc.ID)
		}

		// Mix in a non-existent ID.
		nonExistentID := uuid.New()
		mixedIDs := []uuid.UUID{validIDs[0], nonExistentID, validIDs[1]}

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		// BulkAssign uses partial-success semantics: valid items succeed, invalid ones fail.
		result, err := uc.BulkAssign(ctx, exceptionCommand.BulkAssignInput{
			ExceptionIDs: mixedIDs,
			Assignee:     "operator-B",
		})
		require.NoError(t, err, "bulk assign returns nil error even with partial failures")
		require.NotNil(t, result)
		require.Len(t, result.Succeeded, 2, "2 valid exceptions should succeed")
		require.Len(t, result.Failed, 1, "1 non-existent exception should fail")
		require.Equal(t, nonExistentID, result.Failed[0].ExceptionID)
		require.NotEmpty(t, result.Failed[0].Error)

		// Verify the valid exceptions were assigned.
		exceptionRepo := wired.ExceptionRepo
		for _, id := range validIDs {
			loaded, findErr := exceptionRepo.FindByID(ctx, id)
			require.NoError(t, findErr)
			require.Equal(t, exceptionVO.ExceptionStatusAssigned, loaded.Status)
			require.NotNil(t, loaded.AssignedTo)
			require.Equal(t, "operator-B", *loaded.AssignedTo)
		}
	})
}

func TestBulkResolve_ResolvesAll(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "resolver-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 3)

		// Create 3 OPEN exceptions.
		var exceptionIDs []uuid.UUID
		for i := range 3 {
			tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
				"BULK-RESOLVE-"+uuid.New().String()[:8],
				decimal.NewFromFloat(300.00+float64(i)),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
				exceptionVO.ExceptionSeverityHigh, "UNMATCHED: needs resolution")

			exceptionIDs = append(exceptionIDs, exc.ID)
		}

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		result, err := uc.BulkResolve(ctx, exceptionCommand.BulkResolveInput{
			ExceptionIDs: exceptionIDs,
			Resolution:   "Cleared via batch review",
			Reason:       "End-of-day reconciliation sweep",
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Succeeded, 3, "all 3 exceptions should be resolved")
		require.Empty(t, result.Failed)

		// Verify each exception is now RESOLVED with the provided resolution notes.
		exceptionRepo := wired.ExceptionRepo
		for _, id := range exceptionIDs {
			loaded, findErr := exceptionRepo.FindByID(ctx, id)
			require.NoError(t, findErr)
			require.Equal(t, exceptionVO.ExceptionStatusResolved, loaded.Status,
				"exception %s should be RESOLVED", id)
			require.NotNil(t, loaded.ResolutionNotes)
			require.Equal(t, "Cleared via batch review", *loaded.ResolutionNotes)

			// Verify audit outbox event was created for each exception.
			outboxCount := countOutboxEvents(t, ctx, h.Connection, id)
			require.GreaterOrEqual(t, outboxCount, 1,
				"at least one outbox audit event expected for exception %s", id)
		}
	})
}

func TestBulkResolve_SkipsAlreadyResolved(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "resolver-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 3)

		// Create 3 OPEN exceptions.
		var exceptionIDs []uuid.UUID
		for i := range 3 {
			tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
				"BULK-SKIP-"+uuid.New().String()[:8],
				decimal.NewFromFloat(400.00+float64(i)),
				"USD",
			)

			exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
				exceptionVO.ExceptionSeverityMedium, "UNMATCHED: skip test")

			exceptionIDs = append(exceptionIDs, exc.ID)
		}

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		// Resolve the first exception individually before the bulk operation.
		firstResult, err := uc.BulkResolve(ctx, exceptionCommand.BulkResolveInput{
			ExceptionIDs: []uuid.UUID{exceptionIDs[0]},
			Resolution:   "Pre-resolved individually",
			Reason:       "Manual review",
		})
		require.NoError(t, err)
		require.Len(t, firstResult.Succeeded, 1)

		// Now bulk-resolve all 3 — the already-resolved one should land in Failed.
		result, err := uc.BulkResolve(ctx, exceptionCommand.BulkResolveInput{
			ExceptionIDs: exceptionIDs,
			Resolution:   "Batch resolution round 2",
			Reason:       "Sweep",
		})
		require.NoError(t, err, "bulk resolve returns nil error even with partial failures")
		require.NotNil(t, result)

		// The already-resolved exception (index 0) should fail the transition validation.
		require.Len(t, result.Succeeded, 2,
			"2 OPEN exceptions should resolve successfully")
		require.Len(t, result.Failed, 1,
			"1 already-resolved exception should be in the failed list")
		require.Equal(t, exceptionIDs[0], result.Failed[0].ExceptionID,
			"the pre-resolved exception should be the one that failed")
		require.NotEmpty(t, result.Failed[0].Error,
			"failure entry should describe the transition error")

		// Verify the other 2 are now RESOLVED.
		exceptionRepo := wired.ExceptionRepo
		for _, id := range exceptionIDs[1:] {
			loaded, findErr := exceptionRepo.FindByID(ctx, id)
			require.NoError(t, findErr)
			require.Equal(t, exceptionVO.ExceptionStatusResolved, loaded.Status)
		}
	})
}

func TestBulkAssign_EmptyList(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "bulk-operator")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		result, err := uc.BulkAssign(ctx, exceptionCommand.BulkAssignInput{
			ExceptionIDs: []uuid.UUID{},
			Assignee:     "operator-C",
		})
		require.Error(t, err, "empty ID list should return an error")
		require.ErrorIs(t, err, exceptionCommand.ErrBulkEmptyIDs)
		require.Nil(t, result)
	})
}
