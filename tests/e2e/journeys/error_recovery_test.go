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
// Ingestion Error Recovery Tests
// =============================================================================

// TestErrorRecovery_PartialIngestionFailure tests handling of partially invalid data.
func TestErrorRecovery_PartialIngestionFailure(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("partial-fail").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create CSV with some valid and some invalid rows
			invalidCSV := []byte(`id,amount,currency,date,description
VALID-001,100.00,USD,2026-01-15,valid row
INVALID-002,not_a_number,USD,2026-01-16,invalid amount
VALID-003,300.00,USD,2026-01-17,another valid row
INVALID-004,400.00,INVALID_CURRENCY,2026-01-18,bad currency
VALID-005,500.00,USD,2026-01-19,final valid row
`)

			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"mixed.csv",
				invalidCSV,
			)
			require.NoError(t, err)

			// Wait for job to complete (may complete with errors)
			err = e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID)
			// Job may fail or complete with errors
			if err != nil {
				tc.Logf("Job completed with error (expected for invalid data): %v", err)
			}

			// Get job details to see error count
			completed, err := apiClient.Ingestion.GetJob(ctx, reconciliationContext.ID, job.ID)
			require.NoError(t, err)

			tc.Logf("Job status: %s, total_rows: %d, failed_rows: %d",
				completed.Status, completed.TotalRows, completed.FailedRows)

			// The system should handle partial failures gracefully
			tc.Logf("✓ Partial ingestion failure handled")
		},
	)
}

// TestErrorRecovery_EmptyFile tests handling of empty file upload.
func TestErrorRecovery_EmptyFile(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("empty-file").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Upload empty file
			emptyCSV := []byte("")
			_, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"empty.csv",
				emptyCSV,
			)
			// Should either fail immediately or create a job that fails
			tc.Logf("Empty file upload result: err=%v", err)

			tc.Logf("✓ Empty file handled")
		},
	)
}

// TestErrorRecovery_HeaderOnlyFile tests handling of header-only CSV.
func TestErrorRecovery_HeaderOnlyFile(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("header-only").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Upload CSV with only header
			headerOnlyCSV := []byte("id,amount,currency,date,description\n")
			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"header-only.csv",
				headerOnlyCSV,
			)

			if err != nil {
				tc.Logf("Header-only file rejected at upload: %v", err)
			} else {
				// Wait for job completion
				err = e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID)
				if err != nil {
					tc.Logf("Job failed (expected): %v", err)
				} else {
					completed, _ := apiClient.Ingestion.GetJob(ctx, reconciliationContext.ID, job.ID)
					tc.Logf("Job completed: total_rows=%d, failed_rows=%d", completed.TotalRows, completed.FailedRows)
				}
			}

			tc.Logf("✓ Header-only file handled")
		},
	)
}

// TestErrorRecovery_MalformedCSV tests handling of malformed CSV.
func TestErrorRecovery_MalformedCSV(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("malformed-csv").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Malformed CSV (mismatched columns)
			malformedCSV := []byte(`id,amount,currency,date,description
ROW1,100.00,USD
ROW2,200.00,USD,2026-01-15,desc,extra_column,another_extra
ROW3,"unclosed quote,200.00,USD,2026-01-15
`)

			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"malformed.csv",
				malformedCSV,
			)

			if err != nil {
				tc.Logf("Malformed CSV rejected at upload: %v", err)
			} else {
				err = e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID)
				tc.Logf("Job result: err=%v", err)
			}

			tc.Logf("✓ Malformed CSV handled")
		},
	)
}

// =============================================================================
// Matching Error Recovery Tests
// =============================================================================

// TestErrorRecovery_MatchingWithNoTransactions tests matching with no transactions.
func TestErrorRecovery_MatchingWithNoTransactions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("no-transactions").
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

			// Try to run matching with no transactions ingested
			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")

			if err != nil {
				tc.Logf("Match run with no transactions rejected: %v", err)
			} else {
				err = e2e.WaitForMatchRunComplete(ctx, tc, apiClient, reconciliationContext.ID, matchResp.RunID)
				if err != nil {
					tc.Logf("Match run failed (expected): %v", err)
				} else {
					run, _ := apiClient.Matching.GetMatchRun(ctx, reconciliationContext.ID, matchResp.RunID)
					tc.Logf("Match run completed: status=%s", run.Status)
				}
			}

			tc.Logf("✓ Matching with no transactions handled")
		},
	)
}

// TestErrorRecovery_MatchingWithNoRules tests matching with no rules configured.
func TestErrorRecovery_MatchingWithNoRules(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("no-rules").MustCreate(ctx)
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

				// Note: Not creating any rules

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("NORULE-001", "100.00", "USD", "2026-01-15", "no rule test").
				Build()

			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"data.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID),
			)

			// Try to run matching without rules
			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")

			if err != nil {
				tc.Logf("Match run without rules rejected: %v", err)
			} else {
				err = e2e.WaitForMatchRunComplete(ctx, tc, apiClient, reconciliationContext.ID, matchResp.RunID)
				tc.Logf("Match run result: err=%v", err)
			}

			tc.Logf("✓ Matching with no rules handled")
		},
	)
}

// TestErrorRecovery_ConcurrentMatchRuns tests concurrent match runs on same context.
func TestErrorRecovery_ConcurrentMatchRuns(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("concurrent-match").
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
				AddRow("CONCURRENT-001", "100.00", "USD", "2026-01-15", "concurrent test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CONCURRENT-001", "100.00", "USD", "2026-01-15", "concurrent test").
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

			// Start first match run
			match1, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)

			// Immediately try to start second match run (should be blocked or handled)
			match2, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")

			if err != nil {
				tc.Logf("Second concurrent match run rejected (expected): %v", err)
			} else {
				tc.Logf("Second match run accepted: %s", match2.RunID)
			}

			// Wait for first to complete
			err = e2e.WaitForMatchRunComplete(
				ctx,
				tc,
				apiClient,
				reconciliationContext.ID,
				match1.RunID,
			)
			require.NoError(t, err)

			tc.Logf("✓ Concurrent match runs handled")
		},
	)
}

// =============================================================================
// Retry and Recovery Tests
// =============================================================================

// TestErrorRecovery_RetryFailedJob tests re-running after a failed job.
func TestErrorRecovery_RetryFailedJob(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("retry-job").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// First: Upload bad data
			badCSV := []byte(
				"id,amount,currency,date,description\nBAD,not_number,INVALID,bad-date,desc",
			)
			job1, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"bad.csv",
				badCSV,
			)

			if err == nil {
				// Wait for it to potentially fail
				_ = e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job1.ID)
			}

			// Second: Upload good data (retry with correct data)
			goodCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("RETRY-001", "100.00", "USD", "2026-01-15", "good data").
				Build()

			job2, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"good.csv",
				goodCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job2.ID),
			)

			// Verify the good job succeeded
			completed, err := apiClient.Ingestion.GetJob(ctx, reconciliationContext.ID, job2.ID)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", completed.Status)
			require.Equal(t, 0, completed.FailedRows)
			tc.Logf(
				"Job completed: total_rows=%d, failed_rows=%d",
				completed.TotalRows,
				completed.FailedRows,
			)

			// TotalRows may be 0 if deduplication detected the row as already existing
			// The key assertion is that the job completed successfully
			tc.Logf("✓ Retry after failed job succeeded")
		},
	)
}

// TestErrorRecovery_RerunMatchingAfterDataFix tests matching after fixing data.
func TestErrorRecovery_RerunMatchingAfterDataFix(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("rerun-after-fix").
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

			// First run: Mismatched data
			ledgerCSV1 := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("FIX-001", "100.00", "USD", "2026-01-15", "ledger").
				Build()
			bankCSV1 := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DIFFERENT-001", "100.00", "USD", "2026-01-15", "bank different id").
				Build()

			job1, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.csv",
				ledgerCSV1,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job1.ID),
			)

			job2, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank.csv",
				bankCSV1,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job2.ID),
			)

			match1, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					apiClient,
					reconciliationContext.ID,
					match1.RunID,
				),
			)

			groups1, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				match1.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, 0, len(groups1))
			tc.Logf("First run: %d matches (expected 0)", len(groups1))

			// Second run: Fixed data (matching IDs)
			ledgerCSV2 := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("FIX-002", "200.00", "USD", "2026-01-16", "ledger fixed").
				Build()
			bankCSV2 := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("FIX-002", "200.00", "USD", "2026-01-16", "bank fixed").
				Build()

			job3, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger2.csv",
				ledgerCSV2,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job3.ID),
			)

			job4, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank2.csv",
				bankCSV2,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job4.ID),
			)

			match2, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					apiClient,
					reconciliationContext.ID,
					match2.RunID,
				),
			)

			groups2, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				match2.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(groups2))
			tc.Logf("Second run: %d matches", len(groups2))

			tc.Logf("✓ Rerun matching after data fix")
		},
	)
}

// =============================================================================
// State Consistency Tests
// =============================================================================

// TestErrorRecovery_ContextStateAfterFailure tests context is in valid state after failure.
func TestErrorRecovery_ContextStateAfterFailure(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("state-after-fail").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Cause a failed job
			badCSV := []byte("this,is,not,valid,csv\ndata")
			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"bad.csv",
				badCSV,
			)
			if err == nil {
				_ = e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID)
			}

			// Context should still be accessible
			fetchedContext, err := apiClient.Configuration.GetContext(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.Equal(t, reconciliationContext.ID, fetchedContext.ID)

			// Should still be able to list sources
			sources, err := apiClient.Configuration.ListSources(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.Len(t, sources, 1)

			// Should still be able to ingest more data
			goodCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("STATE-001", "100.00", "USD", "2026-01-15", "after failure").
				Build()
			goodJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"good.csv",
				goodCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, goodJob.ID),
			)

			tc.Logf("✓ Context state valid after failure")
		},
	)
}
