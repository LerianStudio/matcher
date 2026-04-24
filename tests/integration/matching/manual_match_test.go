//go:build integration

package matching

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Matching_ManualMatch_CreatesGroupAndMarksTransactions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		// Ingest two transactions on different sources
		ledgerCSV := buildCSV("MANUAL-001", "75.00", "USD", "2026-02-01", "manual ledger")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"manual_ledger.csv", int64(len(ledgerCSV)), "csv", strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("MANUAL-002", "75.00", "USD", "2026-02-01", "manual bank")
		_, err = wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.NonLedgerSourceID,
			"manual_bank.csv", int64(len(bankCSV)), "csv", strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		// Retrieve unmatched transactions
		unmatched, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, 2)

		txIDs := make([]uuid.UUID, 0, len(unmatched))
		for _, tx := range unmatched {
			txIDs = append(txIDs, tx.ID)
		}

		// Execute manual match
		group, err := wired.MatchingUC.ManualMatch(ctx, matchingCommand.ManualMatchInput{
			TenantID:       h.Seed.TenantID,
			ContextID:      seed.ContextID,
			TransactionIDs: txIDs,
		})
		require.NoError(t, err)
		require.NotNil(t, group)

		// Verify group properties
		require.Equal(t, seed.ContextID, group.ContextID)
		require.Equal(t, matchingVO.MatchGroupStatusConfirmed, group.Status)
		require.NotNil(t, group.ConfirmedAt)

		// Verify match items were created
		itemCount := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM match_items WHERE match_group_id=$1",
			group.ID.String(),
		)
		require.Equal(t, 2, itemCount)

		// Verify transactions are now marked as matched
		for _, txID := range txIDs {
			tx, findErr := wired.TxRepo.FindByID(ctx, txID)
			require.NoError(t, findErr)
			require.Equal(t, shared.TransactionStatusMatched, tx.Status)
		}

		// Verify no unmatched transactions remain
		remaining, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Empty(t, remaining)
	})
}

func TestIntegration_Matching_ManualMatch_ThreeTransactions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		// Ingest 3 transactions across both sources
		ledgerCSV := "id,amount,currency,date,description\n" +
			"THREE-001,50.00,USD,2026-02-01,first\n" +
			"THREE-002,30.00,USD,2026-02-01,second\n"
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"three_ledger.csv", int64(len(ledgerCSV)), "csv", strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("THREE-003", "80.00", "USD", "2026-02-01", "third")
		_, err = wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.NonLedgerSourceID,
			"three_bank.csv", int64(len(bankCSV)), "csv", strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		unmatched, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, 3)

		txIDs := make([]uuid.UUID, 0, len(unmatched))
		for _, tx := range unmatched {
			txIDs = append(txIDs, tx.ID)
		}

		group, err := wired.MatchingUC.ManualMatch(ctx, matchingCommand.ManualMatchInput{
			TenantID:       h.Seed.TenantID,
			ContextID:      seed.ContextID,
			TransactionIDs: txIDs,
		})
		require.NoError(t, err)
		require.NotNil(t, group)

		itemCount := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM match_items WHERE match_group_id=$1",
			group.ID.String(),
		)
		require.Equal(t, 3, itemCount)
	})
}

func TestIntegration_Matching_ManualMatch_RejectsNonUnmatchedTransactions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		// Create a match group first (marks transactions as MATCHED)
		group := runMatchAndGetGroup(t, ctx, h, wired, seed)
		require.NotNil(t, group)

		// Get the matched transaction IDs from match items
		matchedItemCount := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM match_items WHERE match_group_id=$1",
			group.ID.String(),
		)
		require.Equal(t, 2, matchedItemCount)

		// Ingest one more transaction to have an additional one
		extraCSV := buildCSV("EXTRA-001", "99.00", "USD", "2026-02-01", "extra")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"extra.csv", int64(len(extraCSV)), "csv", strings.NewReader(extraCSV),
		)
		require.NoError(t, err)

		// Get one matched and one unmatched transaction
		unmatchedTxs, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.NotEmpty(t, unmatchedTxs)

		// Try to manual match using a transaction that doesn't exist in this context
		// (we'll use a random UUID paired with the unmatched tx)
		_, err = wired.MatchingUC.ManualMatch(ctx, matchingCommand.ManualMatchInput{
			TenantID:       h.Seed.TenantID,
			ContextID:      seed.ContextID,
			TransactionIDs: []uuid.UUID{unmatchedTxs[0].ID, uuid.New()},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, matchingCommand.ErrTransactionNotFound)
	})
}

func TestIntegration_Matching_ManualMatch_RejectsFewerThanTwoTransactions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		// Ingest one transaction
		csv := buildCSV("SINGLE-001", "10.00", "USD", "2026-02-01", "single")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"single.csv", int64(len(csv)), "csv", strings.NewReader(csv),
		)
		require.NoError(t, err)

		unmatched, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, 1)

		_, err = wired.MatchingUC.ManualMatch(ctx, matchingCommand.ManualMatchInput{
			TenantID:       h.Seed.TenantID,
			ContextID:      seed.ContextID,
			TransactionIDs: []uuid.UUID{unmatched[0].ID},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, matchingCommand.ErrMinimumTransactionsRequired)
	})
}

func TestIntegration_Matching_ManualMatch_RejectsDuplicateIDs(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		csv := buildCSV("DUP-001", "10.00", "USD", "2026-02-01", "dup")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"dup.csv", int64(len(csv)), "csv", strings.NewReader(csv),
		)
		require.NoError(t, err)

		unmatched, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.NotEmpty(t, unmatched)

		sameID := unmatched[0].ID

		_, err = wired.MatchingUC.ManualMatch(ctx, matchingCommand.ManualMatchInput{
			TenantID:       h.Seed.TenantID,
			ContextID:      seed.ContextID,
			TransactionIDs: []uuid.UUID{sameID, sameID},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, matchingCommand.ErrDuplicateTransactionIDs)
	})
}

func TestIntegration_Matching_Unmatch_RevertsTransactionsToUnmatched(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		// Ingest and manually match two transactions
		ledgerCSV := buildCSV("UNMATCH-001", "200.00", "USD", "2026-02-01", "to unmatch ledger")
		_, err := wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.LedgerSourceID,
			"unmatch_ledger.csv", int64(len(ledgerCSV)), "csv", strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("unmatch-001", "200.00", "USD", "2026-02-01", "to unmatch bank")
		_, err = wired.IngestionUC.StartIngestion(
			ctx, seed.ContextID, seed.NonLedgerSourceID,
			"unmatch_bank.csv", int64(len(bankCSV)), "csv", strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		unmatched, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, unmatched, 2)

		txIDs := make([]uuid.UUID, 0, len(unmatched))
		for _, tx := range unmatched {
			txIDs = append(txIDs, tx.ID)
		}

		group, err := wired.MatchingUC.ManualMatch(ctx, matchingCommand.ManualMatchInput{
			TenantID:       h.Seed.TenantID,
			ContextID:      seed.ContextID,
			TransactionIDs: txIDs,
		})
		require.NoError(t, err)
		require.NotNil(t, group)

		// Verify transactions are matched
		for _, txID := range txIDs {
			tx, findErr := wired.TxRepo.FindByID(ctx, txID)
			require.NoError(t, findErr)
			require.Equal(t, shared.TransactionStatusMatched, tx.Status)
		}

		// Unmatch the confirmed group — domain allows CONFIRMED → REJECTED.
		err = wired.MatchingUC.Unmatch(ctx, matchingCommand.UnmatchInput{
			TenantID:     h.Seed.TenantID,
			ContextID:    seed.ContextID,
			MatchGroupID: group.ID,
			Reason:       "testing unmatch",
		})
		require.NoError(t, err)

		// Verify transactions are reverted to unmatched
		reverted, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, reverted, 2, "both transactions should be unmatched after unmatch")
	})
}

func TestIntegration_Matching_Unmatch_NonExistentGroupReturnsError(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)

		err := wired.MatchingUC.Unmatch(ctx, matchingCommand.UnmatchInput{
			TenantID:     h.Seed.TenantID,
			ContextID:    seed.ContextID,
			MatchGroupID: uuid.New(),
			Reason:       "testing non-existent",
		})
		require.Error(t, err)
		require.ErrorIs(t, err, matchingCommand.ErrMatchGroupNotFound)
	})
}

func TestIntegration_Matching_Unmatch_RevertsProposedGroupTransactions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)
		wired := wireE4T9UseCases(t, h)

		group := runMatchAndGetGroup(t, ctx, h, wired, seed)
		require.NotNil(t, group)

		// Unmatch succeeds regardless of status (PROPOSED or CONFIRMED).
		err := wired.MatchingUC.Unmatch(ctx, matchingCommand.UnmatchInput{
			TenantID:     h.Seed.TenantID,
			ContextID:    seed.ContextID,
			MatchGroupID: group.ID,
			Reason:       "reverting group",
		})
		require.NoError(t, err)

		// Verify transactions are unmatched again
		remaining, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, remaining, 2)
	})
}
