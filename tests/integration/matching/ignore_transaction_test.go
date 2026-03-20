//go:build integration

package matching

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestIgnoreTransaction_MarksAsIgnored verifies that an unmatched transaction
// can be ignored and its status changes to IGNORED.
func TestIgnoreTransaction_MarksAsIgnored(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		csv := buildCSV("IGN-MARK-001", "50.00", "USD", "2026-01-05", "to-be-ignored")
		_, err := wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"ignore-mark.csv",
			int64(len(csv)),
			"csv",
			strings.NewReader(csv),
		)
		require.NoError(t, err)

		tx, err := wired.TxRepo.FindBySourceAndExternalID(ctx, seed.LedgerSourceID, "IGN-MARK-001")
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Equal(t, shared.TransactionStatusUnmatched, tx.Status)

		updated, err := wired.IngestionUC.IgnoreTransaction(ctx, ingestionCommand.IgnoreTransactionInput{
			TransactionID: tx.ID,
			ContextID:     seed.ContextID,
			Reason:        "not relevant for reconciliation",
		})
		require.NoError(t, err)
		require.NotNil(t, updated)
		require.Equal(t, shared.TransactionStatusIgnored, updated.Status)

		fetched, err := wired.TxRepo.FindByID(ctx, tx.ID)
		require.NoError(t, err)
		require.Equal(t, shared.TransactionStatusIgnored, fetched.Status)
	})
}

// TestIgnoreTransaction_ExcludedFromMatching verifies that an ignored transaction
// does not participate in matching. Without its counterpart, no match groups form.
func TestIgnoreTransaction_ExcludedFromMatching(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		ledgerCSV := buildCSV("IGN-EXCL-002", "75.00", "USD", "2026-01-10", "ledger-ignore")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"ledger-ignore.csv", int64(len(ledgerCSV)), "csv",
			strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("ign-excl-002", "75.00", "USD", "2026-01-10", "bank-ignore")
		_, err = wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.NonLedgerSourceID,
			"bank-ignore.csv", int64(len(bankCSV)), "csv",
			strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		ledgerTx, err := wired.TxRepo.FindBySourceAndExternalID(ctx, seed.LedgerSourceID, "IGN-EXCL-002")
		require.NoError(t, err)
		require.NotNil(t, ledgerTx)

		_, err = wired.IngestionUC.IgnoreTransaction(ctx, ingestionCommand.IgnoreTransactionInput{
			TransactionID: ledgerTx.ID,
			ContextID:     seed.ContextID,
			Reason:        "excluding from matching",
		})
		require.NoError(t, err)

		_, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.Empty(t, groups, "Ignored transaction should not participate in matching")
	})
}

// TestIgnoreTransaction_AlreadyMatchedRejected verifies that a transaction
// in MATCHED status cannot be ignored.
func TestIgnoreTransaction_AlreadyMatchedRejected(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		ledgerCSV := buildCSV("IGN-MTCH-003", "200.00", "USD", "2026-01-15", "ledger-matched")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"ledger-matched.csv", int64(len(ledgerCSV)), "csv",
			strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("ign-mtch-003", "200.00", "USD", "2026-01-15", "bank-matched")
		_, err = wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.NonLedgerSourceID,
			"bank-matched.csv", int64(len(bankCSV)), "csv",
			strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		_, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.Len(t, groups, 1)

		ledgerTx, err := wired.TxRepo.FindBySourceAndExternalID(ctx, seed.LedgerSourceID, "IGN-MTCH-003")
		require.NoError(t, err)
		require.NotNil(t, ledgerTx)
		require.Equal(t, shared.TransactionStatusMatched, ledgerTx.Status)

		_, err = wired.IngestionUC.IgnoreTransaction(ctx, ingestionCommand.IgnoreTransactionInput{
			TransactionID: ledgerTx.ID,
			ContextID:     seed.ContextID,
			Reason:        "should fail",
		})
		require.Error(t, err)
		require.ErrorIs(t, err, ingestionCommand.ErrTransactionNotIgnorable)
	})
}

// TestIgnoreTransaction_NonExistentRejected verifies that attempting to ignore
// a transaction that does not exist returns ErrTransactionNotFound.
func TestIgnoreTransaction_NonExistentRejected(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		_, err := wired.IngestionUC.IgnoreTransaction(ctx, ingestionCommand.IgnoreTransactionInput{
			TransactionID: uuid.New(),
			ContextID:     seed.ContextID,
			Reason:        "non-existent transaction",
		})
		require.Error(t, err)
		require.ErrorIs(t, err, ingestionCommand.ErrTransactionNotFound)
	})
}
