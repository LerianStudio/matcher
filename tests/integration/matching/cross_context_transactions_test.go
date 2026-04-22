//go:build integration

package matching

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	sharedCross "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/tests/integration"
)

// buildMultiRowCSV constructs a CSV payload with a header and N data rows.
// Each row is: externalID, amount, currency, date, description.
func buildMultiRowCSV(rows [][]string) string {
	var sb strings.Builder

	sb.WriteString("id,amount,currency,date,description\n")

	for _, r := range rows {
		sb.WriteString(strings.Join(r, ","))
		sb.WriteByte('\n')
	}

	return sb.String()
}

// newCrossAdapter creates a TransactionRepositoryAdapter wired to the harness.
func newCrossAdapter(
	t *testing.T,
	h *integration.TestHarness,
) *sharedCross.TransactionRepositoryAdapter {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)
	txBaseRepo := ingestionTxRepo.NewRepository(provider)

	adapter, err := sharedCross.NewTransactionRepositoryAdapterFromRepo(provider, txBaseRepo)
	require.NoError(t, err)

	return adapter
}

// ingestCSVAndCollectIDs ingests a CSV into a source, then retrieves all
// unmatched transactions for the context and filters them by source to
// collect the generated UUIDs.
func ingestCSVAndCollectIDs(
	t *testing.T,
	ctx context.Context,
	wired e4t9Wired,
	seed e4t9Seed,
	sourceID uuid.UUID,
	filename, csvPayload string,
) []uuid.UUID {
	t.Helper()

	_, err := wired.IngestionUC.StartIngestion(
		ctx,
		seed.ContextID,
		sourceID,
		filename,
		int64(len(csvPayload)),
		"csv",
		strings.NewReader(csvPayload),
	)
	require.NoError(t, err)

	// Retrieve all unmatched transactions for the context to collect IDs.
	txns, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 100, 0)
	require.NoError(t, err)

	// Filter to only transactions belonging to the specified source.
	ids := make([]uuid.UUID, 0, len(txns))
	for _, txn := range txns {
		if txn.SourceID == sourceID {
			ids = append(ids, txn.ID)
		}
	}

	return ids
}

// queryTransactionStatus reads the status of a single transaction via a
// tenant-scoped direct SQL query, independent of the adapter under test.
func queryTransactionStatus(
	t *testing.T,
	ctx context.Context,
	conn *integration.TestHarness,
	txID uuid.UUID,
) shared.TransactionStatus {
	t.Helper()

	status, err := pgcommon.WithTenantTx(ctx, conn.Connection, func(tx *sql.Tx) (shared.TransactionStatus, error) {
		var raw string
		if err := tx.QueryRowContext(ctx, "SELECT status FROM transactions WHERE id = $1", txID.String()).Scan(&raw); err != nil {
			return "", fmt.Errorf("scan transaction status: %w", err)
		}

		return shared.TransactionStatus(raw), nil
	})
	require.NoError(t, err)

	return status
}

// TestCrossContextTx_FindByContextAndIDs verifies that all ingested transactions
// are returned when queried with their exact IDs through the cross-context adapter.
func TestCrossContextTx_FindByContextAndIDs(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)

		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)
		adapter := newCrossAdapter(t, h)

		csv := buildMultiRowCSV([][]string{
			{"FIND-001", "100.00", "USD", "2026-01-15", "test1"},
			{"FIND-002", "200.00", "USD", "2026-01-15", "test2"},
			{"FIND-003", "300.00", "USD", "2026-01-15", "test3"},
		})

		txIDs := ingestCSVAndCollectIDs(t, ctx, wired, seed, seed.LedgerSourceID, "find_all.csv", csv)
		require.Len(t, txIDs, 3, "expected 3 transactions after ingestion")

		// Query through the cross-context adapter.
		found, err := adapter.FindByContextAndIDs(ctx, seed.ContextID, txIDs)
		require.NoError(t, err)
		require.Len(t, found, 3)

		// Build a set from the returned IDs for order-independent comparison.
		returnedIDs := make(map[uuid.UUID]struct{}, len(found))
		for _, txn := range found {
			returnedIDs[txn.ID] = struct{}{}
		}

		for _, id := range txIDs {
			_, exists := returnedIDs[id]
			require.True(t, exists, "expected transaction %s in results", id)
		}
	})
}

// TestCrossContextTx_FindByContextAndIDs_PartialMatch verifies that when the
// query includes a non-existent ID alongside real ones, only the existing
// transactions are returned without error.
func TestCrossContextTx_FindByContextAndIDs_PartialMatch(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)

		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)
		adapter := newCrossAdapter(t, h)

		csv := buildMultiRowCSV([][]string{
			{"PARTIAL-001", "100.00", "USD", "2026-01-15", "partial-a"},
			{"PARTIAL-002", "200.00", "USD", "2026-01-15", "partial-b"},
		})

		txIDs := ingestCSVAndCollectIDs(t, ctx, wired, seed, seed.LedgerSourceID, "partial.csv", csv)
		require.Len(t, txIDs, 2, "expected 2 transactions after ingestion")

		// Add a random (non-existent) ID to the query.
		queryIDs := make([]uuid.UUID, 0, len(txIDs)+1)
		queryIDs = append(queryIDs, txIDs...)
		queryIDs = append(queryIDs, uuid.New())

		found, err := adapter.FindByContextAndIDs(ctx, seed.ContextID, queryIDs)
		require.NoError(t, err)
		require.Len(t, found, 2, "non-existent ID should be silently excluded")

		returnedIDs := make(map[uuid.UUID]struct{}, len(found))
		for _, txn := range found {
			returnedIDs[txn.ID] = struct{}{}
		}

		for _, id := range txIDs {
			_, exists := returnedIDs[id]
			require.True(t, exists, "expected transaction %s in results", id)
		}
	})
}

// TestCrossContextTx_FindByContextAndIDs_EmptyList verifies that querying with
// an empty ID list returns an empty result without error.
func TestCrossContextTx_FindByContextAndIDs_EmptyList(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)

		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		adapter := newCrossAdapter(t, h)

		// Query with an empty slice — should return empty, not error.
		found, err := adapter.FindByContextAndIDs(ctx, seed.ContextID, []uuid.UUID{})
		require.NoError(t, err)
		require.Empty(t, found)
	})
}

// TestCrossContextTx_MarkMatched verifies that MarkMatched transitions
// transactions from UNMATCHED to MATCHED, and that subsequent queries
// through the adapter reflect the new status.
func TestCrossContextTx_MarkMatched(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)

		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)
		adapter := newCrossAdapter(t, h)

		csv := buildMultiRowCSV([][]string{
			{"MARK-001", "150.00", "USD", "2026-01-15", "mark-a"},
			{"MARK-002", "250.00", "USD", "2026-01-15", "mark-b"},
		})

		txIDs := ingestCSVAndCollectIDs(t, ctx, wired, seed, seed.LedgerSourceID, "mark_matched.csv", csv)
		require.Len(t, txIDs, 2)

		// Pre-condition: transactions start as UNMATCHED.
		for _, id := range txIDs {
			require.Equal(t, shared.TransactionStatusUnmatched, queryTransactionStatus(t, ctx, h, id))
		}

		// Act: mark matched through the cross-context adapter.
		err := adapter.WithTx(ctx, func(tx matchingRepos.Tx) error {
			return adapter.MarkMatchedWithTx(ctx, tx, seed.ContextID, txIDs)
		})
		require.NoError(t, err)

		// Assert: status is now MATCHED.
		for _, id := range txIDs {
			require.Equal(t, shared.TransactionStatusMatched, queryTransactionStatus(t, ctx, h, id))
		}

		// Double-check via FindByContextAndIDs that the returned objects also
		// carry the updated status.
		found, err := adapter.FindByContextAndIDs(ctx, seed.ContextID, txIDs)
		require.NoError(t, err)
		require.Len(t, found, 2)

		for _, txn := range found {
			require.Equal(t, shared.TransactionStatusMatched, txn.Status,
				"transaction %s should be MATCHED", txn.ID)
		}
	})
}

// TestCrossContextTx_MarkUnmatched_Rollback verifies the status lifecycle:
// UNMATCHED → MATCHED (via MarkMatched) → UNMATCHED (via MarkUnmatched).
func TestCrossContextTx_MarkUnmatched_Rollback(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)

		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)
		adapter := newCrossAdapter(t, h)

		csv := buildMultiRowCSV([][]string{
			{"ROLLBACK-001", "120.00", "USD", "2026-01-15", "rollback-a"},
			{"ROLLBACK-002", "180.00", "USD", "2026-01-15", "rollback-b"},
		})

		txIDs := ingestCSVAndCollectIDs(t, ctx, wired, seed, seed.LedgerSourceID, "rollback.csv", csv)
		require.Len(t, txIDs, 2)

		// Step 1: Transition to MATCHED.
		err := adapter.WithTx(ctx, func(tx matchingRepos.Tx) error {
			return adapter.MarkMatchedWithTx(ctx, tx, seed.ContextID, txIDs)
		})
		require.NoError(t, err)

		for _, id := range txIDs {
			require.Equal(t, shared.TransactionStatusMatched, queryTransactionStatus(t, ctx, h, id))
		}

		// Step 2: Revert to UNMATCHED.
		err = adapter.WithTx(ctx, func(tx matchingRepos.Tx) error {
			return adapter.MarkUnmatchedWithTx(ctx, tx, seed.ContextID, txIDs)
		})
		require.NoError(t, err)

		for _, id := range txIDs {
			require.Equal(t, shared.TransactionStatusUnmatched, queryTransactionStatus(t, ctx, h, id),
				"transaction %s should be reverted to UNMATCHED", id)
		}

		// Step 3: Verify they reappear in the unmatched listing.
		unmatched, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 100, 0)
		require.NoError(t, err)

		unmatchedIDs := make(map[uuid.UUID]struct{}, len(unmatched))
		for _, txn := range unmatched {
			unmatchedIDs[txn.ID] = struct{}{}
		}

		for _, id := range txIDs {
			_, exists := unmatchedIDs[id]
			require.True(t, exists, "transaction %s should be in unmatched list after rollback", id)
		}
	})
}

// TestCrossContextTx_WithTx_Transactional verifies that WithTx wraps multiple
// operations atomically — either all succeed or none persist.
func TestCrossContextTx_WithTx_Transactional(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)

		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		wired := wireE4T9UseCases(t, h)
		adapter := newCrossAdapter(t, h)

		csv := buildMultiRowCSV([][]string{
			{"WITHTX-001", "100.00", "USD", "2026-01-15", "tx-a"},
			{"WITHTX-002", "200.00", "USD", "2026-01-15", "tx-b"},
		})

		txIDs := ingestCSVAndCollectIDs(t, ctx, wired, seed, seed.LedgerSourceID, "with_tx.csv", csv)
		require.Len(t, txIDs, 2)

		// Successful transactional operation: mark both as matched atomically.
		err := adapter.WithTx(ctx, func(tx matchingRepos.Tx) error {
			if markErr := adapter.MarkMatchedWithTx(ctx, tx, seed.ContextID, txIDs); markErr != nil {
				return fmt.Errorf("mark matched in tx: %w", markErr)
			}

			return nil
		})
		require.NoError(t, err)

		// Both should be MATCHED after committed transaction.
		for _, id := range txIDs {
			require.Equal(t, shared.TransactionStatusMatched, queryTransactionStatus(t, ctx, h, id),
				"transaction %s should be MATCHED after successful WithTx", id)
		}

		// Revert them to UNMATCHED for the rollback test.
		err = adapter.WithTx(ctx, func(tx matchingRepos.Tx) error {
			return adapter.MarkUnmatchedWithTx(ctx, tx, seed.ContextID, txIDs)
		})
		require.NoError(t, err)

		for _, id := range txIDs {
			require.Equal(t, shared.TransactionStatusUnmatched, queryTransactionStatus(t, ctx, h, id))
		}

		// Failed transactional operation: mark matched, then return error to
		// trigger rollback. Transactions should stay UNMATCHED.
		err = adapter.WithTx(ctx, func(tx matchingRepos.Tx) error {
			if markErr := adapter.MarkMatchedWithTx(ctx, tx, seed.ContextID, txIDs); markErr != nil {
				return fmt.Errorf("mark matched in tx: %w", markErr)
			}

			return errForceRollback
		})
		require.Error(t, err)

		// Status must remain UNMATCHED because the transaction was rolled back.
		for _, id := range txIDs {
			require.Equal(t, shared.TransactionStatusUnmatched, queryTransactionStatus(t, ctx, h, id),
				"transaction %s should remain UNMATCHED after rolled-back WithTx", id)
		}
	})
}

// errForceRollback is a sentinel error used to intentionally abort a
// transactional callback and verify rollback behaviour.
var errForceRollback = &forceRollbackError{msg: "intentional rollback"}

type forceRollbackError struct{ msg string }

func (e *forceRollbackError) Error() string { return e.msg }
