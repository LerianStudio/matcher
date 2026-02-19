//go:build e2e

package journeys

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestFullReconciliation_OneToOneExactMatch tests the complete reconciliation flow
// from configuration through ingestion to matching with exact matches.
func TestFullReconciliation_OneToOneExactMatch(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			// Step 1: Create reconciliation context
			tc.Logf("Step 1: Creating reconciliation context")
			reconciliationContext := f.Context.NewContext().
				WithName("full-recon").
				OneToOne().
				MustCreate(ctx)

			// Step 2: Create sources (ledger and bank)
			tc.Logf("Step 2: Creating sources")
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			// Step 3: Create field maps
			tc.Logf("Step 3: Creating field maps")
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Step 4: Create match rule
			tc.Logf("Step 4: Creating match rule")
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			// Step 5: Upload ledger transactions
			tc.Logf("Step 5: Uploading ledger transactions")
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("001", "100.00", "USD", "2026-01-15", "subscription").
				AddRow("002", "250.50", "USD", "2026-01-16", "consulting").
				AddRow("003", "75.00", "EUR", "2026-01-17", "supplies").
				Build()

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			tc.Logf("Ledger job created: %s", ledgerJob.ID)

			// Wait for ledger ingestion to complete
			err = e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, ledgerJob.ID)
			require.NoError(t, err, "ledger ingestion should complete")

			// Step 6: Upload bank transactions (matching)
			tc.Logf("Step 6: Uploading bank transactions")
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("001", "100.00", "USD", "2026-01-15", "wire transfer").
				AddRow("002", "250.50", "USD", "2026-01-16", "ach deposit").
				AddRow("003", "75.00", "EUR", "2026-01-17", "sepa transfer").
				Build()

			bankJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank.csv",
				bankCSV,
			)
			require.NoError(t, err)
			tc.Logf("Bank job created: %s", bankJob.ID)

			// Wait for bank ingestion to complete
			err = e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID)
			require.NoError(t, err, "bank ingestion should complete")

			// Step 7: Trigger matching
			tc.Logf("Step 7: Triggering matching")
			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)
			tc.Logf("Match run started: %s", matchResp.RunID)

			// Wait for matching to complete
			err = e2e.WaitForMatchRunComplete(
				ctx,
				tc,
				client,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err, "matching should complete")

			// Step 8: Verify results
			tc.Logf("Step 8: Verifying match results")
			matchRun, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun.Status)

			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			tc.Logf("Found %d match groups", len(groups))

			// We expect all 3 transactions to match
			require.GreaterOrEqual(t, len(groups), 3, "should have at least 3 match groups")

			tc.Logf("✓ Full reconciliation completed successfully")
		},
	)
}

// TestFullReconciliation_DryRunThenCommit tests the dry run followed by commit workflow.
func TestFullReconciliation_DryRunThenCommit(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			// Setup: Create context, sources, field maps, and rules
			reconciliationContext := f.Context.NewContext().WithName("dry-run-test").MustCreate(ctx)

			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			// Upload matching transactions
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DRY-001", "500.00", "USD", "2026-01-20", "test tx").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DRY-001", "500.00", "USD", "2026-01-20", "test tx").
				Build()

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, ledgerJob.ID),
			)

			bankJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank.csv",
				bankCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID),
			)

			// Step 1: Run dry run
			tc.Logf("Running DRY_RUN matching")
			dryRunResp, err := client.Matching.RunMatchDryRun(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)

			err = e2e.WaitForMatchRunComplete(
				ctx,
				tc,
				client,
				reconciliationContext.ID,
				dryRunResp.RunID,
			)
			require.NoError(t, err)

			dryRunResult, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				dryRunResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", dryRunResult.Status)
			tc.Logf("Dry run completed: %s", dryRunResp.RunID)

			// Step 2: Run commit
			tc.Logf("Running COMMIT matching")
			commitResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)

			err = e2e.WaitForMatchRunComplete(
				ctx,
				tc,
				client,
				reconciliationContext.ID,
				commitResp.RunID,
			)
			require.NoError(t, err)

			commitResult, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				commitResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", commitResult.Status)
			tc.Logf("Commit completed: %s", commitResp.RunID)

			tc.Logf("✓ Dry run then commit workflow completed successfully")
		},
	)
}
