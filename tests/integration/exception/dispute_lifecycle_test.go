//go:build integration

package exception

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	exceptionCommand "github.com/LerianStudio/matcher/internal/exception/services/command"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Exception_Dispute_OpenAndClose_WonWithAuditTrail(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "dispute-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"DISPUTE-WON-"+uuid.New().String()[:8], decimal.NewFromFloat(500.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityMedium, "UNMATCHED: suspected bank error")

		disputeUC, _ := wireDisputeUseCase(t, h)

		// Open the dispute
		opened, err := disputeUC.OpenDispute(ctx, exceptionCommand.OpenDisputeCommand{
			ExceptionID: exc.ID,
			Category:    "BANK_FEE_ERROR",
			Description: "Bank applied incorrect fee on settlement",
		})
		require.NoError(t, err)
		require.NotNil(t, opened)
		require.Equal(t, exc.ID, opened.ExceptionID)
		require.Equal(t, "OPEN", opened.State.String())
		require.Equal(t, "BANK_FEE_ERROR", opened.Category.String())

		// Close the dispute as WON
		won, err := disputeUC.CloseDispute(ctx, exceptionCommand.CloseDisputeCommand{
			DisputeID:  opened.ID,
			Resolution: "Bank confirmed fee error and issued refund",
			Won:        true,
		})
		require.NoError(t, err)
		require.NotNil(t, won)
		require.Equal(t, "WON", won.State.String())
		require.NotNil(t, won.Resolution)
		require.Contains(t, *won.Resolution, "Bank confirmed")

		// Verify outbox audit events for both DISPUTE_OPENED and DISPUTE_WON
		outboxCount := countOutboxEvents(t, ctx, h.Connection, exc.ID)
		require.GreaterOrEqual(t, outboxCount, 2, "expected DISPUTE_OPENED + DISPUTE_WON audit events")
	})
}

func TestIntegration_Exception_Dispute_OpenAndClose_LostWithAuditTrail(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "dispute-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"DISPUTE-LOST-"+uuid.New().String()[:8], decimal.NewFromFloat(300.00), "GBP")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityLow, "UNMATCHED: no counterparty")

		disputeUC, _ := wireDisputeUseCase(t, h)

		opened, err := disputeUC.OpenDispute(ctx, exceptionCommand.OpenDisputeCommand{
			ExceptionID: exc.ID,
			Category:    "UNRECOGNIZED_CHARGE",
			Description: "Transaction does not match any known settlement",
		})
		require.NoError(t, err)
		require.NotNil(t, opened)

		// Close as LOST
		lost, err := disputeUC.CloseDispute(ctx, exceptionCommand.CloseDisputeCommand{
			DisputeID:  opened.ID,
			Resolution: "Investigation confirmed charge is valid",
			Won:        false,
		})
		require.NoError(t, err)
		require.NotNil(t, lost)
		require.Equal(t, "LOST", lost.State.String())
		require.NotNil(t, lost.Resolution)
		require.Contains(t, *lost.Resolution, "Investigation confirmed")

		// Verify outbox audit events for both DISPUTE_OPENED and DISPUTE_LOST
		outboxCount := countOutboxEvents(t, ctx, h.Connection, exc.ID)
		require.GreaterOrEqual(t, outboxCount, 2, "expected DISPUTE_OPENED + DISPUTE_LOST audit events")
	})
}

func TestIntegration_Exception_Dispute_SubmitEvidence(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "evidence-submitter")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"EVIDENCE-"+uuid.New().String()[:8], decimal.NewFromFloat(750.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityMedium, "UNMATCHED")

		disputeUC, _ := wireDisputeUseCase(t, h)

		opened, err := disputeUC.OpenDispute(ctx, exceptionCommand.OpenDisputeCommand{
			ExceptionID: exc.ID,
			Category:    "DUPLICATE_TRANSACTION",
			Description: "Suspected duplicate charge from processor",
		})
		require.NoError(t, err)

		fileURL := "https://evidence.example.com/doc-001.pdf"
		updated, err := disputeUC.SubmitEvidence(ctx, exceptionCommand.SubmitEvidenceCommand{
			DisputeID: opened.ID,
			Comment:   "Bank statement showing duplicate on line 42",
			FileURL:   &fileURL,
		})
		require.NoError(t, err)
		require.NotNil(t, updated)
		require.Equal(t, "OPEN", updated.State.String(), "should remain OPEN after evidence")
		require.Len(t, updated.Evidence, 1, "should have exactly one evidence entry")
		require.Equal(t, "Bank statement showing duplicate on line 42", updated.Evidence[0].Comment)

		// Verify outbox audit events (DISPUTE_OPENED + EVIDENCE_SUBMITTED)
		outboxCount := countOutboxEvents(t, ctx, h.Connection, exc.ID)
		require.GreaterOrEqual(t, outboxCount, 2)
	})
}

func TestIntegration_Exception_Dispute_MultipleEvidenceSubmissions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "evidence-submitter")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"MULTI-EV-"+uuid.New().String()[:8], decimal.NewFromFloat(200.00), "CHF")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityLow, "UNMATCHED")

		disputeUC, _ := wireDisputeUseCase(t, h)

		opened, err := disputeUC.OpenDispute(ctx, exceptionCommand.OpenDisputeCommand{
			ExceptionID: exc.ID,
			Category:    "OTHER",
			Description: "Investigating discrepancy",
		})
		require.NoError(t, err)

		// Submit first evidence
		_, err = disputeUC.SubmitEvidence(ctx, exceptionCommand.SubmitEvidenceCommand{
			DisputeID: opened.ID,
			Comment:   "Initial bank statement screenshot",
		})
		require.NoError(t, err)

		// Submit second evidence
		fileURL := "https://evidence.example.com/doc-002.pdf"
		updated, err := disputeUC.SubmitEvidence(ctx, exceptionCommand.SubmitEvidenceCommand{
			DisputeID: opened.ID,
			Comment:   "Processor response confirming duplicate",
			FileURL:   &fileURL,
		})
		require.NoError(t, err)
		require.Len(t, updated.Evidence, 2, "should accumulate evidence entries")
		require.Equal(t, "Initial bank statement screenshot", updated.Evidence[0].Comment)
		require.Equal(t, "Processor response confirming duplicate", updated.Evidence[1].Comment)
	})
}

func TestIntegration_Exception_Dispute_OpenOnNonExistentException(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "dispute-operator")

		disputeUC, _ := wireDisputeUseCase(t, h)

		_, err := disputeUC.OpenDispute(ctx, exceptionCommand.OpenDisputeCommand{
			ExceptionID: uuid.New(), // does not exist
			Category:    "BANK_FEE_ERROR",
			Description: "This should fail",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "find exception")
	})
}

func TestIntegration_Exception_Dispute_CloseNonExistentDispute(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "dispute-operator")

		disputeUC, _ := wireDisputeUseCase(t, h)

		_, err := disputeUC.CloseDispute(ctx, exceptionCommand.CloseDisputeCommand{
			DisputeID:  uuid.New(), // does not exist
			Resolution: "This should fail",
			Won:        true,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "find dispute")
	})
}

func TestIntegration_Exception_Dispute_FullLifecycleWithEvidence(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtxWithActor(t, h, "lifecycle-operator")
		seed := seedTestConfig(t, h)
		wired := wireServices(t, h)

		jRepo := ingestionJobRepo.NewRepository(wired.Provider)
		job := createIngestionJob(t, ctx, jRepo, seed.ContextID, seed.LedgerSourceID, 1)
		tx := createTransaction(t, ctx, wired.TxRepo, job.ID, seed.LedgerSourceID,
			"LIFECYCLE-"+uuid.New().String()[:8], decimal.NewFromFloat(1500.00), "USD")

		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID,
			sharedexception.ExceptionSeverityMedium, "UNMATCHED: high value discrepancy")

		disputeUC, _ := wireDisputeUseCase(t, h)

		// Step 1: Open dispute
		opened, err := disputeUC.OpenDispute(ctx, exceptionCommand.OpenDisputeCommand{
			ExceptionID: exc.ID,
			Category:    "BANK_FEE_ERROR",
			Description: "High-value settlement discrepancy",
		})
		require.NoError(t, err)
		require.Equal(t, "OPEN", opened.State.String())

		// Step 2: Submit evidence
		_, err = disputeUC.SubmitEvidence(ctx, exceptionCommand.SubmitEvidenceCommand{
			DisputeID: opened.ID,
			Comment:   "Bank statement provided by client",
		})
		require.NoError(t, err)

		// Step 3: Submit more evidence
		fileURL := "https://evidence.example.com/bank-response.pdf"
		_, err = disputeUC.SubmitEvidence(ctx, exceptionCommand.SubmitEvidenceCommand{
			DisputeID: opened.ID,
			Comment:   "Bank response letter",
			FileURL:   &fileURL,
		})
		require.NoError(t, err)

		// Step 4: Close as won
		won, err := disputeUC.CloseDispute(ctx, exceptionCommand.CloseDisputeCommand{
			DisputeID:  opened.ID,
			Resolution: "Bank acknowledged error, refund issued",
			Won:        true,
		})
		require.NoError(t, err)
		require.Equal(t, "WON", won.State.String())
		require.Len(t, won.Evidence, 2, "all evidence should be preserved after close")

		// Verify complete audit trail:
		// DISPUTE_OPENED + EVIDENCE_SUBMITTED + EVIDENCE_SUBMITTED + DISPUTE_WON = 4
		outboxCount := countOutboxEvents(t, ctx, h.Connection, exc.ID)
		require.GreaterOrEqual(t, outboxCount, 4,
			"expected audit events for open + 2 evidence + close")
	})
}
