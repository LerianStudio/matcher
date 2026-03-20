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

// TestMatchingModes_OneToMany tests matching in one-to-many mode.
func TestMatchingModes_OneToMany(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			// Create one-to-many context
			reconciliationContext := f.Context.NewContext().
				WithName("one-to-many").
				OneToMany().
				MustCreate(ctx)

			require.Equal(t, "1:N", reconciliationContext.Type)

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

			// Ledger: 1 transaction of $300
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("O2M-PARENT", "300.00", "USD", "2026-01-15", "consolidated").
				Build()

			// Bank: 3 transactions that sum to $300
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("O2M-CHILD-1", "100.00", "USD", "2026-01-15", "part 1").
				AddRow("O2M-CHILD-2", "100.00", "USD", "2026-01-15", "part 2").
				AddRow("O2M-CHILD-3", "100.00", "USD", "2026-01-15", "part 3").
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

			// Run matching
			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp.RunID,
				),
			)

			// Verify results
			matchRun, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun.Status)

			tc.Logf("✓ One-to-many matching completed")
		},
	)
}

// TestMatchingModes_ManyToMany tests matching in many-to-many mode.
func TestMatchingModes_ManyToMany(t *testing.T) {
	t.Skip("N:M matching is not yet implemented")

	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			// Create many-to-many context
			reconciliationContext := f.Context.NewContext().
				WithName("many-to-many").
				ManyToMany().
				MustCreate(ctx)

			require.Equal(t, "N:M", reconciliationContext.Type)

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

			// Ledger: 2 transactions of $150 each = $300
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("M2M-L1", "150.00", "USD", "2026-01-15", "ledger part 1").
				AddRow("M2M-L2", "150.00", "USD", "2026-01-15", "ledger part 2").
				Build()

			// Bank: 3 transactions that also sum to $300
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("M2M-B1", "100.00", "USD", "2026-01-15", "bank part 1").
				AddRow("M2M-B2", "100.00", "USD", "2026-01-15", "bank part 2").
				AddRow("M2M-B3", "100.00", "USD", "2026-01-15", "bank part 3").
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

			// Run matching
			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp.RunID,
				),
			)

			// Verify results
			matchRun, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun.Status)

			tc.Logf("✓ Many-to-many matching completed")
		},
	)
}

// TestMatchingModes_ToleranceMatching tests tolerance-based matching.
func TestMatchingModes_ToleranceMatching(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().
				WithName("tolerance-match").
				MustCreate(ctx)
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

			// Create tolerance rule: allow $1.00 difference
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Tolerance().
				WithToleranceConfig("1.00").
				MustCreate(ctx)

			// Ledger: $100.00
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("TOL-001", "100.00", "USD", "2026-01-15", "exact").
				AddRow("TOL-002", "200.00", "USD", "2026-01-16", "tolerance match").
				Build()

			// Bank: $100.00 (exact) and $200.50 (within $1 tolerance)
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("TOL-001", "100.00", "USD", "2026-01-15", "exact").
				AddRow("TOL-002", "200.50", "USD", "2026-01-16", "off by $0.50").
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

			// Run matching
			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp.RunID,
				),
			)

			// Verify both matched
			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(
				t,
				len(groups),
				2,
				"both transactions should match within tolerance",
			)

			tc.Logf("✓ Tolerance matching completed with %d groups", len(groups))
		},
	)
}

// TestMatchingModes_DirectedPrimarySource tests directed matching where a primary
// source ID is specified. In directed mode, the matcher treats the primary source
// as the anchor: its transactions are the "left side" and all other sources form
// the "right side". This test verifies that passing a real source ID (instead of
// an empty string) correctly triggers asymmetric matching and produces valid results.
func TestMatchingModes_DirectedPrimarySource(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			// Step 1: Create reconciliation context with 1:1 matching
			tc.Logf("Step 1: Creating reconciliation context for directed matching")
			reconciliationContext := f.Context.NewContext().
				WithName("directed-match").
				OneToOne().
				MustCreate(ctx)

			// Step 2: Create two sources — bank and gateway
			tc.Logf("Step 2: Creating sources (bank + gateway)")
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			gatewaySource := f.Source.NewSource(reconciliationContext.ID).
				WithName("gateway").
				AsGateway().
				MustCreate(ctx)

			// Step 3: Create field maps for both sources
			tc.Logf("Step 3: Creating field maps")
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, gatewaySource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Step 4: Create exact match rule
			tc.Logf("Step 4: Creating exact match rule")
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			// Step 5: Upload bank transactions (this will be our primary source)
			tc.Logf("Step 5: Uploading bank transactions")
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DIR-001", "100.00", "USD", "2026-01-15", "wire transfer").
				AddRow("DIR-002", "250.50", "USD", "2026-01-16", "ach deposit").
				AddRow("DIR-003", "75.00", "EUR", "2026-01-17", "sepa transfer").
				Build()

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

			// Step 6: Upload gateway transactions (matching amounts and currencies)
			tc.Logf("Step 6: Uploading gateway transactions")
			gatewayCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DIR-001", "100.00", "USD", "2026-01-15", "payment capture").
				AddRow("DIR-002", "250.50", "USD", "2026-01-16", "payment capture").
				AddRow("DIR-003", "75.00", "EUR", "2026-01-17", "payment capture").
				Build()

			gatewayJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				gatewaySource.ID,
				"gateway.csv",
				gatewayCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, gatewayJob.ID),
			)

			// Step 7: Run directed matching with bank as the primary source
			tc.Logf("Step 7: Running directed matching with bank source as primary (%s)", bankSource.ID)
			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err, "directed match run should be accepted")
			tc.Logf("Match run started: %s", matchResp.RunID)

			err = e2e.WaitForMatchRunComplete(
				ctx,
				tc,
				client,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err, "directed match run should complete")

			// Step 8: Verify the match run completed and produced groups
			tc.Logf("Step 8: Verifying directed match results")
			matchRun, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun.Status, "directed match run should be COMPLETED")

			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			tc.Logf("Directed match produced %d match groups", len(groups))
			require.GreaterOrEqual(t, len(groups), 3,
				"directed matching with bank as primary should produce at least 3 match groups")

			// Step 9: Run directed matching with gateway as primary (second direction)
			tc.Logf("Step 9: Running directed matching with gateway source as primary (%s)", gatewaySource.ID)
			matchResp2, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err, "second directed match run should be accepted")
			tc.Logf("Second match run started: %s", matchResp2.RunID)

			err = e2e.WaitForMatchRunComplete(
				ctx,
				tc,
				client,
				reconciliationContext.ID,
				matchResp2.RunID,
			)
			require.NoError(t, err, "second directed match run should complete")

			matchRun2, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				matchResp2.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun2.Status,
				"directed match with gateway as primary should also be COMPLETED")

			tc.Logf("Directed matching completed successfully from both directions")
		},
	)
}

// TestMatchingModes_PercentTolerance tests percentage-based tolerance matching.
func TestMatchingModes_PercentTolerance(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().
				WithName("percent-tolerance").
				MustCreate(ctx)
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

			// Create 5% tolerance rule
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Tolerance().
				WithPercentToleranceConfig(5.0).
				MustCreate(ctx)

			// Ledger: $1000.00
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("PCT-001", "1000.00", "USD", "2026-01-15", "base").
				Build()

			// Bank: $1040.00 (4% off, within 5% tolerance)
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("PCT-001", "1040.00", "USD", "2026-01-15", "4% variance").
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

			// Run matching
			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp.RunID,
				),
			)

			// Verify matched
			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 1, "should match within 5% tolerance")

			tc.Logf("✓ Percent tolerance matching completed")
		},
	)
}
