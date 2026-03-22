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

// TestMatchRunHistory_MultipleRuns tests running matching multiple times on same context.
func TestMatchRunHistory_MultipleRuns(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("multi-run").MustCreate(ctx)
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

			runIDs := make([]string, 0, 3)

			for i := 1; i <= 3; i++ {
				ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
					AddRow("MR-"+string(rune('A'-1+i))+"-001", "100.00", "USD", "2026-01-15", "batch").
					Build()
				bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
					AddRow("MR-"+string(rune('A'-1+i))+"-001", "100.00", "USD", "2026-01-15", "batch").
					Build()

				ledgerJob, err := client.Ingestion.UploadCSV(
					ctx,
					reconciliationContext.ID,
					ledgerSource.ID,
					"l.csv",
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
					"b.csv",
					bankCSV,
				)
				require.NoError(t, err)
				require.NoError(
					t,
					e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID),
				)

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

				runIDs = append(runIDs, matchResp.RunID)
				tc.Logf("Run %d completed: %s", i, matchResp.RunID)
			}

			runs, err := client.Matching.ListMatchRuns(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(runs), 3, "should have at least 3 runs")

			for _, runID := range runIDs {
				run, err := client.Matching.GetMatchRun(ctx, reconciliationContext.ID, runID)
				require.NoError(t, err)
				require.Equal(t, "COMPLETED", run.Status)
			}

			tc.Logf("✓ Multiple runs completed: %d runs tracked", len(runs))
		},
	)
}

// TestMatchRunHistory_DryRunDoesNotPersist tests that dry runs don't persist matches.
func TestMatchRunHistory_DryRunDoesNotPersist(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("dry-persist").MustCreate(ctx)
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

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DRY-001", "100.00", "USD", "2026-01-15", "test").
				Build()

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"l.csv",
				csv,
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
				"b.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID),
			)

			dryRunResp, err := client.Matching.RunMatchDryRun(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					dryRunResp.RunID,
				),
			)

			dryRun, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				dryRunResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "DRY_RUN", dryRun.Mode)

			commitResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					commitResp.RunID,
				),
			)

			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				commitResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(
				t,
				len(groups),
				1,
				"commit should still find matches after dry run",
			)

			tc.Logf("✓ Dry run did not consume transactions")
		},
	)
}

// TestMatchRunHistory_RunModes tests different run modes are tracked correctly.
func TestMatchRunHistory_RunModes(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("run-modes").MustCreate(ctx)
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

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MODE-001", "100.00", "USD", "2026-01-15", "test").
				Build()

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"l.csv",
				csv,
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
				"b.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID),
			)

			dryRunResp, err := client.Matching.RunMatchDryRun(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					dryRunResp.RunID,
				),
			)

			dryRun, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				dryRunResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "DRY_RUN", dryRun.Mode)

			commitResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					commitResp.RunID,
				),
			)

			commitRun, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				commitResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMMIT", commitRun.Mode)

			runs, err := client.Matching.ListMatchRuns(ctx, reconciliationContext.ID)
			require.NoError(t, err)

			modes := make(map[string]int)
			for _, run := range runs {
				modes[run.Mode]++
			}
			require.GreaterOrEqual(t, modes["DRY_RUN"], 1, "should have at least one dry run")
			require.GreaterOrEqual(t, modes["COMMIT"], 1, "should have at least one commit run")

			tc.Logf("✓ Run modes tracked: %v", modes)
		},
	)
}

// TestMatchRunHistory_GetRunDetails tests retrieving detailed run information.
func TestMatchRunHistory_GetRunDetails(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("run-details").MustCreate(ctx)
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

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DET-001", "100.00", "USD", "2026-01-15", "details test").
				AddRow("DET-002", "200.00", "USD", "2026-01-16", "details test").
				Build()

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"l.csv",
				csv,
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
				"b.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID),
			)

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

			run, err := client.Matching.GetMatchRun(ctx, reconciliationContext.ID, matchResp.RunID)
			require.NoError(t, err)
			require.Equal(t, matchResp.RunID, run.ID)
			require.Equal(t, reconciliationContext.ID, run.ContextID)
			require.Equal(t, "COMMIT", run.Mode)
			require.Equal(t, "COMPLETED", run.Status)
			require.False(t, run.StartedAt.IsZero(), "should have started_at")
			require.False(t, run.CompletedAt.IsZero(), "should have completed_at")

			tc.Logf("✓ Run details: ID=%s, Status=%s, Mode=%s", run.ID, run.Status, run.Mode)
		},
	)
}
