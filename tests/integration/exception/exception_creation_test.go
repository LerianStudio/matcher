//go:build integration

package exception

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Exception_IntegrationExceptionFromUnmatched(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 3)

		ledgerTx1 := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"MATCH-REF-001", decimal.NewFromFloat(100.00), "USD")
		ledgerTx2 := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"LEDGER-ORPHAN-001", decimal.NewFromFloat(200.00), "EUR")
		ledgerTx3 := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"LEDGER-ORPHAN-002", decimal.NewFromFloat(300.00), "GBP")

		bankJob := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.NonLedgerSourceID, 1)
		bankTx := createTransaction(t, ctx, wired.TxRepo, bankJob.ID, seed.NonLedgerSourceID,
			"MATCH-REF-001", decimal.NewFromFloat(100.00), "USD")

		result, _, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, result.Status)

		matchedTx1, err := wired.TxRepo.FindByID(ctx, ledgerTx1.ID)
		require.NoError(t, err)

		matchedBankTx, err := wired.TxRepo.FindByID(ctx, bankTx.ID)
		require.NoError(t, err)

		require.Equal(t, "MATCHED", string(matchedTx1.Status))
		require.Equal(t, "MATCHED", string(matchedBankTx.Status))

		exc2, found, err := findExceptionByTransactionID(t, ctx, h.Connection, ledgerTx2.ID)
		require.NoError(t, err)
		require.True(t, found, "exception should be created for orphan ledgerTx2")
		require.Equal(t, exceptionVO.ExceptionStatusOpen, exc2.Status)
		// Per PRD AC-002: MEDIUM requires amount >= 1000 OR age >= 24h
		// Amount 200.00 EUR is below threshold and age < 24h, so severity is LOW
		require.Equal(t, sharedexception.ExceptionSeverityLow, exc2.Severity)

		exc3, found, err := findExceptionByTransactionID(t, ctx, h.Connection, ledgerTx3.ID)
		require.NoError(t, err)
		require.True(t, found, "exception should be created for orphan ledgerTx3")
		require.Equal(t, exceptionVO.ExceptionStatusOpen, exc3.Status)
		// Per PRD AC-002: MEDIUM requires amount >= 1000 OR age >= 24h
		// Amount 300.00 GBP is below threshold and age < 24h, so severity is LOW
		require.Equal(t, sharedexception.ExceptionSeverityLow, exc3.Severity)

		_, foundMatched, err := findExceptionByTransactionID(t, ctx, h.Connection, ledgerTx1.ID)
		require.NoError(t, err)
		require.False(t, foundMatched, "no exception should be created for matched transaction")

		exceptionCount := countInt(t, ctx, h.Connection, "SELECT count(*) FROM exceptions")
		require.GreaterOrEqual(t, exceptionCount, 2)
	})
}

func TestIntegration_Exception_IntegrationExceptionFromUnmatched_WithReason(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)

		orphanTx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"ORPHAN-WITH-REASON", decimal.NewFromFloat(500.00), "JPY")

		result, _, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, result.Status)

		exc, found, err := findExceptionByTransactionID(t, ctx, h.Connection, orphanTx.ID)
		require.NoError(t, err)
		require.True(t, found, "exception should be created for orphan transaction")
		require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status)
		require.NotNil(t, exc.Reason, "exception reason must be set for unmatched transaction")
		require.NotEmpty(t, *exc.Reason, "exception reason must not be empty")
		require.Contains(
			t,
			*exc.Reason,
			"UNMATCHED",
			"exception reason must indicate unmatched status",
		)
	})
}

func TestIntegration_Exception_IntegrationExceptionFromUnmatched_Idempotent(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)

		orphanTx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"ORPHAN-IDEMPOTENT-"+uuid.New().String()[:8], decimal.NewFromFloat(150.00), "CHF")

		result1, _, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, result1.Status)

		exc1, found, err := findExceptionByTransactionID(t, ctx, h.Connection, orphanTx.ID)
		require.NoError(t, err)
		require.True(t, found)
		originalID := exc1.ID

		result2, _, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, result2.Status)

		exc2, found, err := findExceptionByTransactionID(t, ctx, h.Connection, orphanTx.ID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, originalID, exc2.ID, "exception should be the same (idempotent upsert)")
	})
}

// TestIntegration_Exception_IntegrationExceptionSeverityBoundaryConditions tests the PRD AC-002 thresholds:
// - MEDIUM severity: amount >= 1000 OR age >= 24h
// - LOW severity: otherwise
func TestIntegration_Exception_IntegrationExceptionSeverityBoundaryConditions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 3)

		t.Run("amount_exactly_1000_is_MEDIUM", func(t *testing.T) {
			tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
				"BOUNDARY-1000-"+uuid.New().String()[:8], decimal.NewFromFloat(1000.00), "USD")

			result, _, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
				TenantID:  h.Seed.TenantID,
				ContextID: seed.ContextID,
				Mode:      matchingVO.MatchRunModeCommit,
			})
			require.NoError(t, err)
			require.Equal(t, matchingVO.MatchRunStatusCompleted, result.Status)

			exc, found, err := findExceptionByTransactionID(t, ctx, h.Connection, tx.ID)
			require.NoError(t, err)
			require.True(t, found, "exception should be created for orphan transaction")
			require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status)
			require.Equal(
				t,
				sharedexception.ExceptionSeverityMedium,
				exc.Severity,
				"amount exactly 1000 should be MEDIUM severity per PRD AC-002",
			)
		})

		t.Run("amount_999.99_is_LOW", func(t *testing.T) {
			tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
				"BOUNDARY-999-"+uuid.New().String()[:8], decimal.NewFromFloat(999.99), "USD")

			result, _, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
				TenantID:  h.Seed.TenantID,
				ContextID: seed.ContextID,
				Mode:      matchingVO.MatchRunModeCommit,
			})
			require.NoError(t, err)
			require.Equal(t, matchingVO.MatchRunStatusCompleted, result.Status)

			exc, found, err := findExceptionByTransactionID(t, ctx, h.Connection, tx.ID)
			require.NoError(t, err)
			require.True(t, found, "exception should be created for orphan transaction")
			require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status)
			require.Equal(
				t,
				sharedexception.ExceptionSeverityLow,
				exc.Severity,
				"amount 999.99 should be LOW severity per PRD AC-002",
			)
		})

		t.Run("transaction_aged_24h_is_MEDIUM", func(t *testing.T) {
			tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
				"BOUNDARY-24H-"+uuid.New().String()[:8], decimal.NewFromFloat(100.00), "USD")

			resolver, resolverErr := h.Connection.Resolver(ctx)
			require.NoError(t, resolverErr, "failed to get db resolver")
			primaryDBs := resolver.PrimaryDBs()
			require.NotEmpty(t, primaryDBs, "no primary databases available")

			_, err := primaryDBs[0].ExecContext(ctx, `
				UPDATE transactions
				SET date = date - INTERVAL '24 hours'
				WHERE id = $1
			`, tx.ID.String())
			require.NoError(t, err)

			result, _, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
				TenantID:  h.Seed.TenantID,
				ContextID: seed.ContextID,
				Mode:      matchingVO.MatchRunModeCommit,
			})
			require.NoError(t, err)
			require.Equal(t, matchingVO.MatchRunStatusCompleted, result.Status)

			exc, found, err := findExceptionByTransactionID(t, ctx, h.Connection, tx.ID)
			require.NoError(t, err)
			require.True(t, found, "exception should be created for orphan transaction aged 24h")
			require.Equal(t, exceptionVO.ExceptionStatusOpen, exc.Status)
			require.Equal(
				t,
				sharedexception.ExceptionSeverityMedium,
				exc.Severity,
				"transaction aged 24h should be MEDIUM severity per PRD AC-002",
			)
		})
	})
}

func TestIntegration_Exception_IntegrationExceptionFromUnmatched_NoExceptionsForFullMatch(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		now := time.Now().UTC().Format(time.DateOnly)

		ledgerCSV := buildCSV("FULL-MATCH-001", "100.00", "USD", now, "Full match ledger")
		bankCSV := buildCSV("FULL-MATCH-001", "100.00", "USD", now, "Full match bank")

		ledgerJob, err := wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"ledger.csv",
			int64(len(ledgerCSV)),
			"csv",
			strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)
		require.NotNil(t, ledgerJob)

		bankJob, err := wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.NonLedgerSourceID,
			"bank.csv",
			int64(len(bankCSV)),
			"csv",
			strings.NewReader(bankCSV),
		)
		require.NoError(t, err)
		require.NotNil(t, bankJob)

		result, _, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.Equal(t, matchingVO.MatchRunStatusCompleted, result.Status)

		exceptionCountBefore := countInt(
			t,
			ctx,
			h.Connection,
			"SELECT count(*) FROM exceptions WHERE transaction_id IN (SELECT id FROM transactions WHERE ingestion_job_id IN ($1, $2))",
			ledgerJob.ID.String(),
			bankJob.ID.String(),
		)

		require.Equal(
			t,
			0,
			exceptionCountBefore,
			"no exceptions should be created when all transactions match",
		)
	})
}
