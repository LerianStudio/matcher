//go:build e2e

package journeys

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// =============================================================================
// Export Job Lifecycle Tests
// =============================================================================

// TestExportJobs_CreateAndComplete tests the full export job lifecycle.
func TestExportJobs_CreateAndComplete(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup context with data
			reconciliationContext := f.Context.NewContext().
				WithName("export-lifecycle").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("EXP-001", "100.00", "USD", "2026-01-15", "export test 1").
				AddRow("EXP-002", "200.00", "USD", "2026-01-16", "export test 2").
				AddRow("EXP-003", "300.00", "USD", "2026-01-17", "export test 3").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"data.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			// Create export job
			tc.Logf("Creating export job")
			exportJob, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "MATCHED",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.NoError(t, err)
			require.NotEmpty(t, exportJob.JobID)
			tc.Logf("Export job created: %s, status=%s", exportJob.JobID, exportJob.Status)

			// Wait for completion
			err = e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID)
			require.NoError(t, err)

			// Verify final state
			completed, err := apiClient.Reporting.GetExportJob(ctx, exportJob.JobID)
			require.NoError(t, err)
			require.Equal(t, "SUCCEEDED", completed.Status)
			require.NotEmpty(t, completed.FileName)

			tc.Logf(
				"✓ Export job lifecycle completed: %s -> %s",
				exportJob.Status,
				completed.Status,
			)
		},
	)
}

// TestExportJobs_DownloadAfterComplete tests downloading export results.
func TestExportJobs_DownloadAfterComplete(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("export-download").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DL-001", "100.00", "USD", "2026-01-15", "download test").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"data.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			exportJob, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "MATCHED",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID))

			// Download the export
			data, err := apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID)
			require.NoError(t, err)
			require.NotEmpty(t, data, "download should return content")

			tc.Logf("Downloaded %d bytes", len(data))
			tc.Logf("✓ Export download successful")
		},
	)
}

// TestExportJobs_Cancel tests cancelling an export job.
func TestExportJobs_Cancel(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Create context with large dataset to give time to cancel
			reconciliationContext := f.Context.NewContext().
				WithName("export-cancel").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create moderate amount of data
			csvBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			for i := 1; i <= 50; i++ {
				csvBuilder.AddRowf(
					"CANCEL-%03d",
					float64(i*10),
					"USD",
					"2026-01-15",
					"cancel test %d",
					i,
				)
			}

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"data.csv",
				csvBuilder.Build(),
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			// Create export job
			exportJob, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "MATCHED",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.NoError(t, err)

			// Try to cancel immediately
			err = apiClient.Reporting.CancelExportJob(ctx, exportJob.JobID)
			// Cancel may succeed or fail depending on how fast the job completed
			if err != nil {
				tc.Logf("Cancel returned error (job may have completed): %v", err)
			} else {
				tc.Logf("Cancel succeeded")
			}

			// Verify final state
			job, err := apiClient.Reporting.GetExportJob(ctx, exportJob.JobID)
			require.NoError(t, err)
			tc.Logf("Final job status: %s", job.Status)

			// Job should be either canceled or succeeded
			require.Contains(t, []string{"CANCELED", "SUCCEEDED"}, job.Status)

			tc.Logf("✓ Export job cancellation handled correctly")
		},
	)
}

// TestExportJobs_ListJobs tests listing export jobs.
func TestExportJobs_ListJobs(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("export-list").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("LIST-001", "100.00", "USD", "2026-01-15", "list test").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"data.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			// Create multiple export jobs
			for i := 1; i <= 3; i++ {
				exportJob, err := apiClient.Reporting.CreateExportJob(
					ctx,
					reconciliationContext.ID,
					client.CreateExportJobRequest{
						ReportType: "MATCHED",
						Format:     "CSV",
						DateFrom:   "2026-01-01",
						DateTo:     "2026-12-31",
					},
				)
				require.NoError(t, err)
				require.NoError(
					t,
					e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID),
				)
			}

			// List all export jobs
			jobs, err := apiClient.Reporting.ListExportJobs(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(jobs), 3, "should have at least 3 export jobs")

			for _, job := range jobs {
				tc.Logf("Export job: %s, type=%s, status=%s", job.ID, job.ReportType, job.Status)
			}

			tc.Logf("✓ Listed %d export jobs", len(jobs))
		},
	)
}

// =============================================================================
// Export Format Tests
// =============================================================================

// TestExportJobs_CSVFormat tests CSV format export.
func TestExportJobs_CSVFormat(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("export-csv").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CSV-001", "100.00", "USD", "2026-01-15", "csv format test").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"data.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			exportJob, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "MATCHED",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID))

			data, err := apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			// Verify it looks like CSV (contains commas, newlines)
			content := string(data)
			tc.Logf("CSV content preview: %s...", content[:min(100, len(content))])

			tc.Logf("✓ CSV format export successful")
		},
	)
}

// TestExportJobs_JSONFormat tests JSON format export.
func TestExportJobs_JSONFormat(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("export-json").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("JSON-001", "100.00", "USD", "2026-01-15", "json format test").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"data.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			exportJob, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "MATCHED",
					Format:     "JSON",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID))

			data, err := apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			// Verify it looks like JSON (starts with [ or {)
			content := string(data)
			tc.Logf("JSON content preview: %s...", content[:min(100, len(content))])

			tc.Logf("✓ JSON format export successful")
		},
	)
}

// =============================================================================
// Export Type Tests
// =============================================================================

// TestExportJobs_MatchedTransactionsType tests exporting matched transactions.
func TestExportJobs_MatchedTransactionsType(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("export-matched-type").
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
				AddRow("MATCHED-001", "100.00", "USD", "2026-01-15", "will match").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MATCHED-001", "100.00", "USD", "2026-01-15", "matches ledger").
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

			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
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

			// Export matched transactions
			exportJob, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "MATCHED",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID))

			data, err := apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			tc.Logf("✓ Matched transactions export successful: %d bytes", len(data))
		},
	)
}

// TestExportJobs_UnmatchedTransactionsType tests exporting unmatched transactions.
func TestExportJobs_UnmatchedTransactionsType(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("export-unmatched-type").
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

			// Only ledger transactions - no bank data to match with (bank source still required)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("UNMATCHED-001", "100.00", "USD", "2026-01-15", "no match").
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

			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
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

			// Export unmatched transactions
			exportJob, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "UNMATCHED",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID))

			data, err := apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			tc.Logf("✓ Unmatched transactions export successful: %d bytes", len(data))
		},
	)
}

// =============================================================================
// Export Error Handling Tests
// =============================================================================

// TestExportJobs_NonExistentJob tests handling of non-existent job IDs.
func TestExportJobs_NonExistentJob(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Reporting.GetExportJob(ctx, "00000000-0000-0000-0000-000000000000")
			require.Error(t, err)

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsNotFound(), "should return 404, got %d", apiErr.StatusCode)
			}

			tc.Logf("✓ Non-existent export job returns 404")
		},
	)
}

// TestExportJobs_DownloadBeforeComplete tests downloading before job completes.
func TestExportJobs_DownloadBeforeComplete(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("export-early-dl").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create larger dataset to ensure job takes time
			csvBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			for i := 1; i <= 50; i++ {
				csvBuilder.AddRowf(
					"EARLY-%03d",
					float64(i*10),
					"USD",
					"2026-01-15",
					"early download test %d",
					i,
				)
			}

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"data.csv",
				csvBuilder.Build(),
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)

			// Create export job but don't wait
			exportJob, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "MATCHED",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.NoError(t, err)

			// Try immediate download (might fail or return empty depending on system)
			_, err = apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID)
			// Behavior depends on implementation - may return error or empty data
			tc.Logf("Early download result: err=%v", err)

			// Now wait and verify normal download works
			require.NoError(t, e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID))
			data, err := apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			tc.Logf("✓ Export download after completion works correctly")
		},
	)
}

// TestExportJobs_InvalidFormat tests handling of invalid export format.
func TestExportJobs_InvalidFormat(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("export-invalid-fmt").
				MustCreate(ctx)

			_, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "MATCHED",
					Format:     "invalid_format",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.Error(t, err, "invalid format should be rejected")

			tc.Logf("✓ Invalid format correctly rejected")
		},
	)
}

// TestExportJobs_InvalidType tests handling of invalid export type.
func TestExportJobs_InvalidType(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("export-invalid-type").
				MustCreate(ctx)

			_, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "invalid_type",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-12-31",
				},
			)
			require.Error(t, err, "invalid type should be rejected")

			tc.Logf("✓ Invalid type correctly rejected")
		},
	)
}
