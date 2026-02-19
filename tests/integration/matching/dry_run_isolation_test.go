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

func TestIntegrationE4T9_DryRunIsolation(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)

		wired := wireE4T9UseCases(t, h)

		ledgerCSV := buildCSV("REF-DRY", "42.00", "USD", "2026-01-02", "ledger")
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

		bankCSV := buildCSV("ref-dry", "42.00", "USD", "2026-01-02", "bank")
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

		before, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, before, 2)

		dryRun, dryGroups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:        h.Seed.TenantID,
			ContextID:       seed.ContextID,
			Mode:            matchingVO.MatchRunModeDryRun,
			PrimarySourceID: nil,
		})
		require.NoError(t, err)
		require.NotNil(t, dryRun)
		require.Equal(t, matchingVO.MatchRunModeDryRun, dryRun.Mode)
		require.NotEmpty(t, dryGroups)

		require.Equal(
			t,
			1,
			countInt(
				t,
				ctx,
				h.Connection,
				"SELECT count(*) FROM match_runs WHERE context_id=$1",
				seed.ContextID.String(),
			),
		)
		require.Equal(
			t,
			0,
			countInt(
				t,
				ctx,
				h.Connection,
				"SELECT count(*) FROM match_groups WHERE context_id=$1",
				seed.ContextID.String(),
			),
		)
		require.Equal(t, 0, countInt(t, ctx, h.Connection, "SELECT count(*) FROM match_items"))

		afterDry, err := wired.TxRepo.ListUnmatchedByContext(ctx, seed.ContextID, nil, nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, afterDry, 2)

		commitRun, commitGroups, err := wired.MatchingUC.RunMatch(
			ctx,
			matchingCommand.RunMatchInput{
				TenantID:        h.Seed.TenantID,
				ContextID:       seed.ContextID,
				Mode:            matchingVO.MatchRunModeCommit,
				PrimarySourceID: nil,
			},
		)
		require.NoError(t, err)
		require.NotNil(t, commitRun)
		require.Equal(t, matchingVO.MatchRunModeCommit, commitRun.Mode)
		require.Len(t, commitGroups, 1)

		require.Equal(
			t,
			2,
			countInt(
				t,
				ctx,
				h.Connection,
				"SELECT count(*) FROM match_runs WHERE context_id=$1",
				seed.ContextID.String(),
			),
		)
		require.Equal(
			t,
			1,
			countInt(
				t,
				ctx,
				h.Connection,
				"SELECT count(*) FROM match_groups WHERE context_id=$1",
				seed.ContextID.String(),
			),
		)

		afterCommit, err := wired.TxRepo.ListUnmatchedByContext(
			ctx,
			seed.ContextID,
			nil,
			nil,
			10,
			0,
		)
		require.NoError(t, err)
		require.Empty(t, afterCommit)
	})
}
