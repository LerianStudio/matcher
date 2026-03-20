//go:build e2e

package journeys

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestIdempotency_DuplicateIngestion tests that duplicate file uploads are handled correctly.
func TestIdempotency_DuplicateIngestion(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("idem-ingest").MustCreate(ctx)
			source := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, source.ID).
				WithStandardMapping().
				MustCreate(ctx)

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("IDEM-001", "100.00", "USD", "2026-01-15", "test").
				AddRow("IDEM-002", "200.00", "USD", "2026-01-16", "test").
				Build()

			// First upload
			job1, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				source.ID,
				"data.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, job1.ID),
			)
			tc.Logf("First upload completed: job=%s", job1.ID)

			// Second upload with same content
			job2, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				source.ID,
				"data.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, job2.ID),
			)
			tc.Logf("Second upload completed: job=%s", job2.ID)

			// Jobs should be different (each upload creates new job)
			require.NotEqual(t, job1.ID, job2.ID, "each upload should create a new job")

			// List all jobs
			jobs, err := client.Ingestion.ListJobsByContext(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(jobs), 2, "should have at least 2 jobs")

			tc.Logf("✓ Duplicate ingestion handled: %d jobs created", len(jobs))
		},
	)
}

// TestIdempotency_MultipleMatchRuns tests multiple match runs on same data.
func TestIdempotency_MultipleMatchRuns(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("idem-match").MustCreate(ctx)
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
				AddRow("MULTI-001", "100.00", "USD", "2026-01-15", "test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MULTI-001", "100.00", "USD", "2026-01-15", "test").
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

			// First match run
			matchResp1, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp1.RunID,
				),
			)
			tc.Logf("First match run completed: %s", matchResp1.RunID)

			groups1, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp1.RunID,
			)
			require.NoError(t, err)

			// Second match run (should work, may or may not find new matches)
			matchResp2, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp2.RunID,
				),
			)
			tc.Logf("Second match run completed: %s", matchResp2.RunID)

			// Runs should be different
			require.NotEqual(
				t,
				matchResp1.RunID,
				matchResp2.RunID,
				"each run should have unique ID",
			)

			groups2, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp2.RunID,
			)
			require.NoError(t, err)

			tc.Logf("First run: %d groups, Second run: %d groups", len(groups1), len(groups2))
			tc.Logf("✓ Multiple match runs completed successfully")
		},
	)
}

// TestIdempotency_ConcurrentDryRuns tests concurrent dry runs don't interfere.
func TestIdempotency_ConcurrentDryRuns(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().
				WithName("concurrent-dry").
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
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CONC-001", "100.00", "USD", "2026-01-15", "test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CONC-001", "100.00", "USD", "2026-01-15", "test").
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

			// Start multiple dry runs (they should not commit)
			dryRun1, err := client.Matching.RunMatchDryRun(ctx, reconciliationContext.ID)
			require.NoError(t, err)

			dryRun2, err := client.Matching.RunMatchDryRun(ctx, reconciliationContext.ID)
			require.NoError(t, err)

			// Wait for both
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					dryRun1.RunID,
				),
			)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					dryRun2.RunID,
				),
			)

			// Both should complete independently
			run1, err := client.Matching.GetMatchRun(ctx, reconciliationContext.ID, dryRun1.RunID)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", run1.Status)

			run2, err := client.Matching.GetMatchRun(ctx, reconciliationContext.ID, dryRun2.RunID)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", run2.Status)

			tc.Logf("✓ Concurrent dry runs completed: %s, %s", dryRun1.RunID, dryRun2.RunID)
		},
	)
}

// TestIdempotency_ContextRecreation tests recreating context with same name.
func TestIdempotency_ContextRecreation(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		contextName := tc.UniqueName("recreate-test")

		// Create first context
		created1, err := apiClient.Configuration.CreateContext(ctx, client.CreateContextRequest{
			Name:     contextName,
			Type:     "1:1",
			Interval: "0 0 * * *",
		})
		require.NoError(t, err)
		tc.RegisterCleanup(func() error {
			return apiClient.Configuration.DeleteContext(context.Background(), created1.ID)
		})
		tc.Logf("Created first context: %s", created1.ID)

		// Delete it
		err = apiClient.Configuration.DeleteContext(ctx, created1.ID)
		require.NoError(t, err)

		// Create second context with same name
		created2, err := apiClient.Configuration.CreateContext(ctx, client.CreateContextRequest{
			Name:     contextName,
			Type:     "1:1",
			Interval: "0 0 * * *",
		})
		require.NoError(t, err)
		tc.RegisterCleanup(func() error {
			return apiClient.Configuration.DeleteContext(context.Background(), created2.ID)
		})
		tc.Logf("Created second context: %s", created2.ID)

		// IDs should be different
		require.NotEqual(t, created1.ID, created2.ID, "recreated context should have new ID")

		tc.Logf("✓ Context recreation with same name works")
	})
}
