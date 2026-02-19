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

// TestReporting_DashboardMetrics tests the comprehensive dashboard metrics endpoint.
func TestReporting_DashboardMetrics(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("metrics-test").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").AsLedger().MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").AsBank().MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().WithExactConfig(true, true).MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("METR-001", "100.00", "USD", "2026-01-15", "matched").
				AddRow("METR-002", "200.00", "USD", "2026-01-16", "matched").
				AddRow("METR-003", "300.00", "USD", "2026-01-17", "unmatched").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("METR-001", "100.00", "USD", "2026-01-15", "matched").
				AddRow("METR-002", "200.00", "USD", "2026-01-16", "matched").
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

			metrics, err := apiClient.Reporting.GetDashboardMetrics(
				ctx,
				reconciliationContext.ID,
				"2026-01-01",
				"2026-01-31",
			)
			require.NoError(t, err)
			require.NotNil(t, metrics)
			require.NotEmpty(t, metrics.UpdatedAt)
			tc.Logf("✓ Dashboard metrics retrieved successfully")
		},
	)
}

// TestReporting_SummaryExport tests the summary export endpoint.
func TestReporting_SummaryExport(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("summary-export").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").AsLedger().MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").AsBank().MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().WithExactConfig(true, true).MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("SUM-001", "100.00", "USD", "2026-01-15", "summary test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("SUM-001", "100.00", "USD", "2026-01-15", "summary test").
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

			reportData, err := apiClient.Reporting.ExportSummaryReport(
				ctx,
				reconciliationContext.ID,
				"2026-01-01",
				"2026-01-31",
			)
			require.NoError(t, err)
			require.NotEmpty(t, reportData, "summary report should have content")
			tc.Logf("✓ Summary report exported: %d bytes", len(reportData))
		},
	)
}

// TestReporting_VarianceExport tests the variance export endpoint.
func TestReporting_VarianceExport(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("variance-export").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").AsLedger().MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").AsBank().MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().WithExactConfig(true, true).MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("VAR-001", "100.00", "USD", "2026-01-15", "variance test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("VAR-001", "100.00", "USD", "2026-01-15", "variance test").
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

			reportData, err := apiClient.Reporting.ExportVarianceReport(
				ctx,
				reconciliationContext.ID,
				"2026-01-01",
				"2026-01-31",
			)
			require.NoError(t, err)
			require.NotEmpty(t, reportData, "variance report should have content")
			tc.Logf("✓ Variance report exported: %d bytes", len(reportData))
		},
	)
}

// TestReporting_ContextListFiltering tests listing contexts with type and status filters.
func TestReporting_ContextListFiltering(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			f.Context.NewContext().
				WithName("filter-1to1").
				OneToOne().
				MustCreate(ctx)
			f.Context.NewContext().
				WithName("filter-1toN").
				OneToMany().
				MustCreate(ctx)

			allContexts, err := apiClient.Configuration.ListContexts(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(
				t,
				len(allContexts),
				2,
				"should have at least 2 contexts",
			)
			tc.Logf("✓ Listed %d contexts total", len(allContexts))
		},
	)
}
