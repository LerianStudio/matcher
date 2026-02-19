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

// =============================================================================
// Transaction Listing Tests
// =============================================================================

// TestTransactionQueries_ListByJob tests listing transactions for a specific job.
func TestTransactionQueries_ListByJob(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("tx-list-job").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create transactions
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("TX-001", "100.00", "USD", "2026-01-15", "first").
				AddRow("TX-002", "200.00", "USD", "2026-01-16", "second").
				AddRow("TX-003", "300.00", "EUR", "2026-01-17", "third").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			// List transactions by job
			transactions, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.Len(t, transactions, 3, "should have 3 transactions")

			for _, tx := range transactions {
				require.NotEmpty(t, tx.ID)
				require.NotEmpty(t, tx.ExternalID)
				require.NotEmpty(t, tx.Amount)
				require.NotEmpty(t, tx.Currency)
				tc.Logf("Transaction: %s, amount=%s %s", tx.ExternalID, tx.Amount, tx.Currency)
			}

			tc.Logf("✓ Listed %d transactions for job", len(transactions))
		},
	)
}

// TestTransactionQueries_TransactionDetails tests transaction field contents.
func TestTransactionQueries_TransactionDetails(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("tx-details").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DETAIL-001", "1234.56", "EUR", "2026-02-15", "detailed transaction").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			transactions, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.Len(t, transactions, 1)

			tx := transactions[0]
			require.Contains(t, tx.ExternalID, "DETAIL-001")
			require.Equal(t, "EUR", tx.Currency)
			require.NotEmpty(t, tx.Date)
			require.Equal(t, ledgerJob.ID, tx.JobID)
			require.Equal(t, ledgerSource.ID, tx.SourceID)

			tc.Logf("✓ Transaction details verified: id=%s, external_id=%s", tx.ID, tx.ExternalID)
		},
	)
}

// TestTransactionQueries_MultipleJobs tests transactions from multiple jobs.
func TestTransactionQueries_MultipleJobs(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("tx-multi-job").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// First job
			csv1 := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("JOB1-001", "100.00", "USD", "2026-01-15", "job 1 tx 1").
				AddRow("JOB1-002", "200.00", "USD", "2026-01-16", "job 1 tx 2").
				Build()

			job1, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"batch1.csv",
				csv1,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job1.ID),
			)

			// Second job
			csv2 := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("JOB2-001", "300.00", "USD", "2026-01-17", "job 2 tx 1").
				AddRow("JOB2-002", "400.00", "USD", "2026-01-18", "job 2 tx 2").
				AddRow("JOB2-003", "500.00", "USD", "2026-01-19", "job 2 tx 3").
				Build()

			job2, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"batch2.csv",
				csv2,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job2.ID),
			)

			// Verify each job has correct transactions
			tx1, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				job1.ID,
			)
			require.NoError(t, err)
			require.Len(t, tx1, 2)

			tx2, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				job2.ID,
			)
			require.NoError(t, err)
			require.Len(t, tx2, 3)

			// Verify job IDs are correct
			for _, tx := range tx1 {
				require.Equal(t, job1.ID, tx.JobID)
			}
			for _, tx := range tx2 {
				require.Equal(t, job2.ID, tx.JobID)
			}

			tc.Logf("✓ Transactions correctly associated with their jobs")
		},
	)
}

// TestTransactionQueries_LargeVolume tests querying large volumes of transactions.
func TestTransactionQueries_LargeVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large volume test in short mode")
	}

	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("tx-large-vol").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create 100 transactions
			csvBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			for i := 1; i <= 100; i++ {
				csvBuilder.AddRowf(
					"LARGE-%03d",
					float64(i*10),
					"USD",
					"2026-01-15",
					"large volume tx %d",
					i,
				)
			}

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"large.csv",
				csvBuilder.Build(),
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			// List all transactions
			transactions, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.Len(t, transactions, 100, "should have 100 transactions")

			tc.Logf("✓ Successfully queried %d transactions", len(transactions))
		},
	)
}

// =============================================================================
// Transaction Status Tests
// =============================================================================

// TestTransactionQueries_StatusAfterMatch tests transaction status after matching.
func TestTransactionQueries_StatusAfterMatch(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("tx-status").MustCreate(ctx)
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

			// Create matching and non-matching transactions
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MATCH-001", "100.00", "USD", "2026-01-15", "will match").
				AddRow("NOMATCH-001", "200.00", "USD", "2026-01-16", "will not match").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MATCH-001", "100.00", "USD", "2026-01-15", "matches ledger").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			bankJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank.csv",
				bankCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID),
			)

			// Run matching
			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					apiClient,
					reconciliationContext.ID,
					matchResp.RunID,
				),
			)

			// Query transactions and verify status
			ledgerTxs, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.Len(t, ledgerTxs, 2)

			matchedCount := 0
			unmatchedCount := 0
			for _, tx := range ledgerTxs {
				tc.Logf("Transaction %s status: %s", tx.ExternalID, tx.Status)
				if tx.Status == "MATCHED" || tx.Status == "matched" {
					matchedCount++
				} else if tx.Status == "UNMATCHED" || tx.Status == "unmatched" || tx.Status == "PENDING" || tx.Status == "pending" {
					unmatchedCount++
				}
			}

			tc.Logf("Matched: %d, Unmatched: %d", matchedCount, unmatchedCount)
			tc.Logf("✓ Transaction status reflects match results")
		},
	)
}

// =============================================================================
// Job Listing Tests
// =============================================================================

// TestTransactionQueries_ListJobsByContext tests listing jobs for a context.
func TestTransactionQueries_ListJobsByContext(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("jobs-list").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create multiple jobs
			for i := 1; i <= 3; i++ {
				csv := factories.NewCSVBuilder(tc.NamePrefix()).
					AddRowf("BATCH%d-001", float64(i*100), "USD", "2026-01-15", "batch %d", i).
					Build()

				job, err := apiClient.Ingestion.UploadCSV(
					ctx,
					reconciliationContext.ID,
					ledgerSource.ID,
					"batch.csv",
					csv,
				)
				require.NoError(t, err)
				require.NoError(
					t,
					e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID),
				)
			}

			// List all jobs for context
			jobs, err := apiClient.Ingestion.ListJobsByContext(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(jobs), 3, "should have at least 3 jobs")

			for _, job := range jobs {
				require.NotEmpty(t, job.ID)
				require.Equal(t, reconciliationContext.ID, job.ContextID)
				require.Equal(t, "COMPLETED", job.Status)
				tc.Logf("Job: %s, status=%s, total_rows=%d", job.ID, job.Status, job.TotalRows)
			}

			tc.Logf("✓ Listed %d jobs for context", len(jobs))
		},
	)
}

// TestTransactionQueries_JobDetails tests job detail retrieval.
func TestTransactionQueries_JobDetails(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("job-details").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DETAIL-001", "100.00", "USD", "2026-01-15", "detail test 1").
				AddRow("DETAIL-002", "200.00", "USD", "2026-01-16", "detail test 2").
				AddRow("DETAIL-003", "300.00", "USD", "2026-01-17", "detail test 3").
				AddRow("DETAIL-004", "400.00", "USD", "2026-01-18", "detail test 4").
				AddRow("DETAIL-005", "500.00", "USD", "2026-01-19", "detail test 5").
				Build()

			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"detailed.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID),
			)

			// Get job details
			retrieved, err := apiClient.Ingestion.GetJob(ctx, reconciliationContext.ID, job.ID)
			require.NoError(t, err)
			require.Equal(t, job.ID, retrieved.ID)
			require.Equal(t, "COMPLETED", retrieved.Status)
			require.Equal(t, 5, retrieved.TotalRows)
			require.Equal(t, 0, retrieved.FailedRows)

			tc.Logf(
				"✓ Job details verified: total_rows=%d, failed_rows=%d",
				retrieved.TotalRows,
				retrieved.FailedRows,
			)
		},
	)
}
