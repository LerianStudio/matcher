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

// TestExport_SyncMatchedReport tests synchronous export of matched transactions.
func TestExport_SyncMatchedReport(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup: Create context with matched transactions
			reconciliationContext := f.Context.NewContext().
				WithName("export-matched").
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

			// Ingest matching transactions
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("EXP-001", "100.00", "USD", "2026-01-15", "export test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("EXP-001", "100.00", "USD", "2026-01-15", "export test").
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

			// Export matched report
			tc.Logf("Exporting matched transactions report")
			reportData, err := apiClient.Reporting.ExportMatchedReport(
				ctx,
				reconciliationContext.ID,
				"2026-01-01",
				"2026-01-31",
			)
			require.NoError(t, err)
			require.NotEmpty(t, reportData, "matched report should have content")
			tc.Logf("✓ Matched report exported: %d bytes", len(reportData))
		},
	)
}

// TestExport_SyncUnmatchedReport tests synchronous export of unmatched transactions.
func TestExport_SyncUnmatchedReport(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup: Create context with unmatched transactions
			reconciliationContext := f.Context.NewContext().
				WithName("export-unmatched").
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

			// Ingest NON-matching transactions (different IDs)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("LEDGER-ONLY-001", "100.00", "USD", "2026-01-15", "unmatched").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BANK-ONLY-001", "200.00", "USD", "2026-01-15", "unmatched").
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

			// Run matching (will not match)
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

			// Export unmatched report
			tc.Logf("Exporting unmatched transactions report")
			reportData, err := apiClient.Reporting.ExportUnmatchedReport(
				ctx,
				reconciliationContext.ID,
				"2026-01-01",
				"2026-01-31",
			)
			require.NoError(t, err)
			require.NotEmpty(t, reportData, "unmatched report should have content")
			tc.Logf("✓ Unmatched report exported: %d bytes", len(reportData))
		},
	)
}

// TestExport_AsyncExportJob tests the async export job workflow.
func TestExport_AsyncExportJob(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup context with data
			reconciliationContext := f.Context.NewContext().WithName("async-export").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ASYNC-001", "100.00", "USD", "2026-01-15", "async export test").
				AddRow("ASYNC-002", "200.00", "USD", "2026-01-16", "async export test").
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

			// Create async export job
			tc.Logf("Creating async export job")
			createResp, err := apiClient.Reporting.CreateExportJob(
				ctx,
				reconciliationContext.ID,
				client.CreateExportJobRequest{
					ReportType: "UNMATCHED",
					Format:     "CSV",
					DateFrom:   "2026-01-01",
					DateTo:     "2026-01-31",
				},
			)
			require.NoError(t, err)
			require.NotEmpty(t, createResp.JobID)
			tc.Logf("Export job created: %s", createResp.JobID)

			// Wait for export job to complete
			err = e2e.WaitForExportJobComplete(ctx, tc, apiClient, createResp.JobID)
			require.NoError(t, err)

			// Verify job status
			completedJob, err := apiClient.Reporting.GetExportJob(ctx, createResp.JobID)
			require.NoError(t, err)
			require.Equal(t, "SUCCEEDED", completedJob.Status)

			// Download the export
			exportData, err := apiClient.Reporting.DownloadExportJob(ctx, createResp.JobID)
			require.NoError(t, err)
			require.NotEmpty(t, exportData, "export should have content")
			tc.Logf("✓ Async export completed: %d bytes", len(exportData))
		},
	)
}

// TestExport_DashboardAggregates tests the dashboard statistics endpoints.
func TestExport_DashboardAggregates(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup context with matched and unmatched transactions
			reconciliationContext := f.Context.NewContext().
				WithName("dashboard-test").
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

			// Ingest mixed: 2 matching, 1 non-matching
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DASH-001", "100.00", "USD", "2026-01-15", "match").
				AddRow("DASH-002", "200.00", "USD", "2026-01-16", "match").
				AddRow("DASH-003", "300.00", "USD", "2026-01-17", "no match").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DASH-001", "100.00", "USD", "2026-01-15", "match").
				AddRow("DASH-002", "200.00", "USD", "2026-01-16", "match").
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

			// Test dashboard aggregates
			dateFrom := "2026-01-01"
			dateTo := "2026-01-31"
			tc.Logf("Fetching dashboard aggregates")
			dashboard, err := apiClient.Reporting.GetDashboardAggregates(
				ctx,
				reconciliationContext.ID,
				dateFrom,
				dateTo,
			)
			require.NoError(t, err)
			require.NotNil(t, dashboard)
			require.NotNil(t, dashboard.Volume)
			require.NotNil(t, dashboard.MatchRate)
			tc.Logf("Dashboard: total=%d matched=%d unmatched=%d rate=%.2f%%",
				dashboard.Volume.TotalTransactions, dashboard.Volume.MatchedTransactions,
				dashboard.Volume.UnmatchedCount, dashboard.MatchRate.MatchRate*100)

			// Test volume stats
			volume, err := apiClient.Reporting.GetVolumeStats(
				ctx,
				reconciliationContext.ID,
				dateFrom,
				dateTo,
			)
			require.NoError(t, err)
			require.NotNil(t, volume)
			tc.Logf("Volume stats: total=%s matched=%s", volume.TotalVolume, volume.MatchedVolume)

			// Test match rate stats
			matchRate, err := apiClient.Reporting.GetMatchRateStats(
				ctx,
				reconciliationContext.ID,
				dateFrom,
				dateTo,
			)
			require.NoError(t, err)
			require.NotNil(t, matchRate)
			tc.Logf("Match rate: %.2f%%", matchRate.MatchRate*100)

			tc.Logf("✓ Dashboard aggregates retrieved successfully")
		},
	)
}
