//go:build integration

package exception

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	exceptionEntities "github.com/LerianStudio/matcher/internal/exception/domain/entities"
	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	exceptionCommand "github.com/LerianStudio/matcher/internal/exception/services/command"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestForceMatch_ResolvesExceptionAndCreatesAuditLog(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "test-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"FORCE-MATCH-"+uuid.New().String()[:8], decimal.NewFromFloat(500.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			exceptionVO.ExceptionSeverityLow, "UNMATCHED: no counterparty found")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		resolved, err := uc.ForceMatch(ctx, exceptionCommand.ForceMatchCommand{
			ExceptionID:    exc.ID,
			OverrideReason: "POLICY_EXCEPTION",
			Notes:          "Approved by supervisor for known settlement delay",
		})
		require.NoError(t, err)
		require.NotNil(t, resolved)
		require.Equal(t, exceptionVO.ExceptionStatusResolved, resolved.Status)
		require.NotNil(t, resolved.ResolutionType)
		require.Equal(t, "FORCE_MATCH", *resolved.ResolutionType)
		require.NotNil(t, resolved.ResolutionReason)
		require.Equal(t, "POLICY_EXCEPTION", *resolved.ResolutionReason)
		require.NotNil(t, resolved.ResolutionNotes)
		require.Contains(t, *resolved.ResolutionNotes, "Approved by supervisor")

		// Verify outbox audit event was created atomically
		outboxCount := countOutboxEvents(t, ctx, h.Connection, exc.ID)
		require.GreaterOrEqual(t, outboxCount, 1, "at least one outbox audit event expected")
	})
}

func TestForceMatch_RequiresOverrideReason(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "test-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"FORCE-REASON-"+uuid.New().String()[:8], decimal.NewFromFloat(100.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			exceptionVO.ExceptionSeverityLow, "UNMATCHED")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		// Empty override reason should fail
		_, err := uc.ForceMatch(ctx, exceptionCommand.ForceMatchCommand{
			ExceptionID:    exc.ID,
			OverrideReason: "",
			Notes:          "Some notes",
		})
		require.Error(t, err)
		require.ErrorIs(t, err, exceptionVO.ErrInvalidOverrideReason)
	})
}

func TestForceMatch_RequiresNotes(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "test-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"FORCE-NOTES-"+uuid.New().String()[:8], decimal.NewFromFloat(100.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			exceptionVO.ExceptionSeverityLow, "UNMATCHED")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		// Empty notes should fail
		_, err := uc.ForceMatch(ctx, exceptionCommand.ForceMatchCommand{
			ExceptionID:    exc.ID,
			OverrideReason: "OPS_APPROVAL",
			Notes:          "",
		})
		require.Error(t, err)
		require.ErrorIs(t, err, exceptionEntities.ErrResolutionNotesRequired)
	})
}

func TestForceMatch_NonExistentException(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "test-operator")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		_, err := uc.ForceMatch(ctx, exceptionCommand.ForceMatchCommand{
			ExceptionID:    uuid.New(), // does not exist
			OverrideReason: "POLICY_EXCEPTION",
			Notes:          "Testing non-existent",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "find exception")
	})
}

func TestAdjustEntry_ResolvesExceptionWithAuditTrail(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "test-adjuster")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"ADJUST-ENTRY-"+uuid.New().String()[:8], decimal.NewFromFloat(250.00), "EUR")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			exceptionVO.ExceptionSeverityMedium, "UNMATCHED: amount discrepancy")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		resolved, err := uc.AdjustEntry(ctx, exceptionCommand.AdjustEntryCommand{
			ExceptionID: exc.ID,
			ReasonCode:  "AMOUNT_CORRECTION",
			Notes:       "Adjusted for rounding difference in EUR settlement",
			Amount:      decimal.NewFromFloat(5.50),
			Currency:    "EUR",
			EffectiveAt: time.Now().UTC(),
		})
		require.NoError(t, err)
		require.NotNil(t, resolved)
		require.Equal(t, exceptionVO.ExceptionStatusResolved, resolved.Status)
		require.NotNil(t, resolved.ResolutionType)
		require.Equal(t, "ADJUST_ENTRY", *resolved.ResolutionType)
		require.NotNil(t, resolved.ResolutionReason)
		require.Equal(t, "AMOUNT_CORRECTION", *resolved.ResolutionReason)

		// Verify outbox audit event was created atomically
		outboxCount := countOutboxEvents(t, ctx, h.Connection, exc.ID)
		require.GreaterOrEqual(t, outboxCount, 1, "audit event expected in outbox")
	})
}

func TestAdjustEntry_ValidReasonCodes(t *testing.T) {
	validReasons := []string{
		"AMOUNT_CORRECTION",
		"CURRENCY_CORRECTION",
		"DATE_CORRECTION",
		"OTHER",
	}

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "test-adjuster")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, len(validReasons))

		for _, reasonCode := range validReasons {
			t.Run(reasonCode, func(t *testing.T) {
				tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
					"REASON-"+reasonCode+"-"+uuid.New().String()[:8],
					decimal.NewFromFloat(100.00), "USD")

				exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
					exceptionVO.ExceptionSeverityLow, "UNMATCHED")

				uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

				resolved, err := uc.AdjustEntry(ctx, exceptionCommand.AdjustEntryCommand{
					ExceptionID: exc.ID,
					ReasonCode:  reasonCode,
					Notes:       "Testing reason code " + reasonCode,
					Amount:      decimal.NewFromFloat(10.00),
					Currency:    "USD",
					EffectiveAt: time.Now().UTC(),
				})
				require.NoError(t, err)
				require.Equal(t, exceptionVO.ExceptionStatusResolved, resolved.Status)
				require.NotNil(t, resolved.ResolutionReason)
				require.Equal(t, reasonCode, *resolved.ResolutionReason)
			})
		}
	})
}

func TestAdjustEntry_ZeroAmountRejected(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "test-adjuster")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"ZERO-AMT-"+uuid.New().String()[:8], decimal.NewFromFloat(100.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			exceptionVO.ExceptionSeverityLow, "UNMATCHED")

		uc, _ := wireExceptionUseCase(t, h, &noopResolutionExecutor{})

		_, err := uc.AdjustEntry(ctx, exceptionCommand.AdjustEntryCommand{
			ExceptionID: exc.ID,
			ReasonCode:  "AMOUNT_CORRECTION",
			Notes:       "This should fail",
			Amount:      decimal.Zero,
			Currency:    "USD",
			EffectiveAt: time.Now().UTC(),
		})
		require.Error(t, err)
		require.ErrorIs(t, err, exceptionCommand.ErrZeroAdjustmentAmount)
	})
}

func TestAdjustEntry_ExecutorFailureRollsBack(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "test-adjuster")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"FAIL-EXEC-"+uuid.New().String()[:8], decimal.NewFromFloat(100.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			exceptionVO.ExceptionSeverityLow, "UNMATCHED")

		executorErr := errors.New("matching context unavailable")
		uc, _ := wireExceptionUseCase(t, h, &failingResolutionExecutor{err: executorErr})

		_, err := uc.AdjustEntry(ctx, exceptionCommand.AdjustEntryCommand{
			ExceptionID: exc.ID,
			ReasonCode:  "AMOUNT_CORRECTION",
			Notes:       "Should fail due to executor",
			Amount:      decimal.NewFromFloat(10.00),
			Currency:    "USD",
			EffectiveAt: time.Now().UTC(),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "adjust entry executor")

		// Exception should remain OPEN (not resolved) since executor failed before DB update
		reloaded, found, findErr := findExceptionByTransactionID(t, ctx, h.Connection, tx.ID)
		require.NoError(t, findErr)
		require.True(t, found)
		require.Equal(t, exceptionVO.ExceptionStatusOpen, reloaded.Status,
			"exception must remain OPEN when executor fails")
	})
}
