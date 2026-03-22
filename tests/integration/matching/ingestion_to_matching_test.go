//go:build integration

package matching

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegrationE4T9_IngestionToMatching_PersistsArtifactsAndUpdatesTransactions(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)

		wired := wireE4T9UseCases(t, h)

		ledgerCSV := buildCSV("REF-001", "100.00", "USD", "2026-01-01", "ledger")
		_, err := wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"ledger.csv",
			int64(len(ledgerCSV)),
			"csv",
			strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("ref-001", "100.00", "USD", "2026-01-01", "bank")
		_, err = wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.NonLedgerSourceID,
			"bank.csv",
			int64(len(bankCSV)),
			"csv",
			strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		candidates, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, candidates, 2)

		run, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotNil(t, run)
		require.Equal(t, matchingVO.MatchRunModeCommit, run.Mode)
		require.Len(t, groups, 1)

		require.Equal(t, 1, countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM match_groups WHERE context_id=$1 AND run_id=$2",
			seed.ContextID.String(), run.ID.String(),
		))
		require.Equal(t, 2, countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM match_items WHERE match_group_id=$1",
			groups[0].ID.String(),
		))

		remaining, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Empty(t, remaining)
	})
}

// TestIntegrationE4T9_ConfiguredSourceSidesDriveMatching verifies that
// matching uses configured source sides rather than runtime direction hints.
func TestIntegrationE4T9_ConfiguredSourceSidesDriveMatching(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)

		wired := wireE4T9UseCases(t, h)

		// Ingest matching transactions on both sources.
		ledgerCSV := buildCSV("DIR-001", "75.00", "USD", "2026-02-01", "directed-ledger")
		_, err := wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"directed_ledger.csv",
			int64(len(ledgerCSV)),
			"csv",
			strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("dir-001", "75.00", "USD", "2026-02-01", "directed-bank")
		_, err = wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.NonLedgerSourceID,
			"directed_bank.csv",
			int64(len(bankCSV)),
			"csv",
			strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		// Verify both transactions are unmatched before running.
		candidates, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, candidates, 2, "both transactions should be unmatched before directed match")

		// Run matching using the configured source sides.
		run, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.NotNil(t, run, "match run should be returned")
		require.Equal(t, matchingVO.MatchRunModeCommit, run.Mode)
		require.Len(t, groups, 1, "directed mode should still produce one match group")

		// Verify persistence: 1 group, 2 items.
		require.Equal(t, 1, countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM match_groups WHERE context_id=$1 AND run_id=$2",
			seed.ContextID.String(), run.ID.String(),
		))
		require.Equal(t, 2, countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM match_items WHERE match_group_id=$1",
			groups[0].ID.String(),
		))

		// Verify all transactions are now matched.
		remaining, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Empty(t, remaining, "all transactions should be matched after directed run")
	})
}
