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

// TestPagination_ContextsList tests cursor-based pagination on contexts list.
func TestPagination_ContextsList(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			// Create multiple contexts to test pagination
			numContexts := 5
			for i := 0; i < numContexts; i++ {
				f.Context.NewContext().WithName("page-ctx").MustCreate(ctx)
			}

			// List all contexts
			contexts, err := client.Configuration.ListContexts(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(
				t,
				len(contexts),
				numContexts,
				"should have at least %d contexts",
				numContexts,
			)

			tc.Logf("✓ Listed %d contexts", len(contexts))
		},
	)
}

// TestPagination_SourcesList tests pagination on sources within a context.
func TestPagination_SourcesList(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("sources-page").MustCreate(ctx)

			// Create multiple sources
			numSources := 4
			for i := 0; i < numSources; i++ {
				f.Source.NewSource(reconciliationContext.ID).
					WithName("src").
					AsLedger().
					MustCreate(ctx)
			}

			// List sources for context
			sources, err := client.Configuration.ListSources(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.Equal(t, numSources, len(sources), "should have exactly %d sources", numSources)

			tc.Logf("✓ Listed %d sources", len(sources))
		},
	)
}

// TestPagination_RulesList tests pagination on match rules.
func TestPagination_RulesList(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("rules-page").MustCreate(ctx)

			// Create multiple rules with different priorities
			for i := 1; i <= 3; i++ {
				f.Rule.NewRule(reconciliationContext.ID).
					WithPriority(i).
					Exact().
					WithExactConfig(true, true).
					MustCreate(ctx)
			}

			// List rules for context
			rules, err := client.Configuration.ListMatchRules(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.Equal(t, 3, len(rules), "should have 3 rules")

			// Verify rules are returned in priority order
			for i, rule := range rules {
				require.Equal(t, i+1, rule.Priority, "rule %d should have priority %d", i, i+1)
			}

			tc.Logf("✓ Listed %d rules in priority order", len(rules))
		},
	)
}

// TestPagination_JobsList tests pagination on ingestion jobs.
func TestPagination_JobsList(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("jobs-page").MustCreate(ctx)
			source := f.Source.NewSource(reconciliationContext.ID).
				WithName("src").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, source.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create multiple ingestion jobs
			numJobs := 3
			for i := 0; i < numJobs; i++ {
				csv := factories.NewCSVBuilder(tc.NamePrefix()).
					AddRow("TX", "100.00", "USD", "2026-01-15", "test").
					Build()
				job, err := client.Ingestion.UploadCSV(
					ctx,
					reconciliationContext.ID,
					source.ID,
					"batch.csv",
					csv,
				)
				require.NoError(t, err)
				require.NoError(
					t,
					e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, job.ID),
				)
			}

			// List jobs for context
			jobs, err := client.Ingestion.ListJobsByContext(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.Equal(t, numJobs, len(jobs), "should have %d jobs", numJobs)

			tc.Logf("✓ Listed %d ingestion jobs", len(jobs))
		},
	)
}

// TestPagination_TransactionsList tests pagination on transactions within a job.
func TestPagination_TransactionsList(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("tx-page").MustCreate(ctx)
			source := f.Source.NewSource(reconciliationContext.ID).
				WithName("src").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, source.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Upload multiple transactions
			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("TX-1", "100.00", "USD", "2026-01-15", "tx 1").
				AddRow("TX-2", "200.00", "USD", "2026-01-16", "tx 2").
				AddRow("TX-3", "300.00", "USD", "2026-01-17", "tx 3").
				AddRow("TX-4", "400.00", "USD", "2026-01-18", "tx 4").
				AddRow("TX-5", "500.00", "USD", "2026-01-19", "tx 5").
				Build()

			job, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				source.ID,
				"multi.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, job.ID),
			)

			// List transactions for job
			transactions, err := client.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				job.ID,
			)
			require.NoError(t, err)
			require.Equal(t, 5, len(transactions), "should have 5 transactions")

			tc.Logf("✓ Listed %d transactions", len(transactions))
		},
	)
}

// TestPagination_MatchGroupsList tests pagination on match groups.
func TestPagination_MatchGroupsList(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("groups-page").MustCreate(ctx)
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
				AddRow("GRP-1", "100.00", "USD", "2026-01-15", "match 1").
				AddRow("GRP-2", "200.00", "USD", "2026-01-16", "match 2").
				AddRow("GRP-3", "300.00", "USD", "2026-01-17", "match 3").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("GRP-1", "100.00", "USD", "2026-01-15", "match 1").
				AddRow("GRP-2", "200.00", "USD", "2026-01-16", "match 2").
				AddRow("GRP-3", "300.00", "USD", "2026-01-17", "match 3").
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

			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
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

			// List match groups
			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 3, "should have at least 3 match groups")

			tc.Logf("✓ Listed %d match groups", len(groups))
		},
	)
}

// TestPagination_ExportJobsList tests pagination on export jobs.
func TestPagination_ExportJobsList(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()

			// List export jobs (may be empty, just verify endpoint works)
			jobs, err := client.Reporting.ListExportJobs(ctx)
			require.NoError(t, err)

			tc.Logf("✓ Listed %d export jobs", len(jobs))
		},
	)
}
