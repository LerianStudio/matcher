//go:build e2e

package journeys

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// reportDateRange returns a (dateFrom, dateTo) pair that spans 89 days back from today.
// The fixture data in setupMatchedPipeline is seeded relative to the current time,
// so this range stays valid over time while remaining inside the API's 90-day limit.
func reportDateRange() (string, string) {
	now := time.Now().UTC()
	return now.AddDate(0, 0, -89).Format("2006-01-02"), now.Format("2006-01-02")
}

func setFeeMetadataForE2EContextTransactions(
	t *testing.T,
	tc *e2e.TestContext,
	contextID string,
	amount string,
	currency string,
) {
	t.Helper()

	db, err := sql.Open("pgx", tc.Config().PostgresDSN())
	require.NoError(t, err)
	defer db.Close()

	payload := fmt.Sprintf(`{"fee":{"amount":"%s","currency":"%s"}}`, amount, currency)
	_, err = db.ExecContext(
		context.Background(),
		`UPDATE public.transactions
		 SET metadata = $1::jsonb,
		     updated_at = NOW()
		 WHERE ingestion_job_id IN (
		     SELECT id FROM public.ingestion_jobs WHERE context_id = $2
		 )`,
		payload,
		contextID,
	)
	require.NoError(t, err)
}

// setupMatchedPipeline creates a full reconciliation pipeline with matched and unmatched
// transactions. It returns the context ID for use in report queries.
// The pipeline creates:
//   - 2 sources (ledger + bank) with standard field maps
//   - 1 exact match rule (currency + amount)
//   - Ledger CSV with both matching and non-matching transactions
//   - Bank CSV with only the matching subset
//   - A committed match run
func setupMatchedPipeline(
	t *testing.T,
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	name string,
	matchedCount int,
	unmatchedCount int,
) string {
	t.Helper()

	f := factories.New(tc, apiClient)

	reconciliationContext := f.Context.NewContext().
		WithName(name).
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

	// Build ledger CSV: matched rows + unmatched rows
	matchedDate := time.Now().UTC().AddDate(0, 0, -2).Format("2006-01-02")
	unmatchedDate := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")

	ledgerBuilder := factories.NewCSVBuilder(tc.NamePrefix())
	for i := 1; i <= matchedCount; i++ {
		ledgerBuilder.AddRow(
			fmt.Sprintf("RPM-%03d", i),
			fmt.Sprintf("%d.00", 100*i),
			"USD",
			matchedDate,
			fmt.Sprintf("matched tx %d", i),
		)
	}
	for i := 1; i <= unmatchedCount; i++ {
		ledgerBuilder.AddRow(
			fmt.Sprintf("RPU-%03d", i),
			fmt.Sprintf("%d.50", 100*i),
			"USD",
			unmatchedDate,
			fmt.Sprintf("unmatched tx %d", i),
		)
	}
	ledgerCSV := ledgerBuilder.Build()

	// Build bank CSV: only the matching rows
	bankBuilder := factories.NewCSVBuilder(tc.NamePrefix())
	for i := 1; i <= matchedCount; i++ {
		bankBuilder.AddRow(
			fmt.Sprintf("RPM-%03d", i),
			fmt.Sprintf("%d.00", 100*i),
			"USD",
			matchedDate,
			fmt.Sprintf("matched tx %d", i),
		)
	}
	bankCSV := bankBuilder.Build()

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

	return reconciliationContext.ID
}

func setupVariancePipeline(
	t *testing.T,
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	name string,
) string {
	t.Helper()

	f := factories.New(tc, apiClient)

	reconciliationContext := f.Context.NewContext().
		WithName(name).
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

	schedule := f.FeeSchedule.NewFeeSchedule().
		WithName("variance-fee-schedule").
		WithFlatFee("flat-fee", 1, "10.00").
		MustCreate(ctx)
	f.FeeRule.NewFeeRule(reconciliationContext.ID).
		Any().
		WithName("variance-fee-rule").
		WithFeeScheduleID(schedule.ID).
		WithPriority(1).
		MustCreate(ctx)

	matchedDate := time.Now().UTC().AddDate(0, 0, -2).Format("2006-01-02")

	ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
		AddRow("VRP-001", "250.00", "USD", matchedDate, "variance payment").
		Build()
	ledgerJob, err := apiClient.Ingestion.UploadCSV(
		ctx,
		reconciliationContext.ID,
		ledgerSource.ID,
		"variance_ledger.csv",
		ledgerCSV,
	)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID))

	bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
		AddRow("vrp-001", "250.00", "USD", matchedDate, "variance payment").
		Build()
	bankJob, err := apiClient.Ingestion.UploadCSV(
		ctx,
		reconciliationContext.ID,
		bankSource.ID,
		"variance_bank.csv",
		bankCSV,
	)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID))

	setFeeMetadataForE2EContextTransactions(t, tc, reconciliationContext.ID, "12.00", "USD")

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

	return reconciliationContext.ID
}

// TestReporting_GetMatchedReport verifies that the matched report endpoint returns
// items with valid fields after a successful match run.
func TestReporting_GetMatchedReport(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			contextID := setupMatchedPipeline(t, ctx, tc, apiClient, "rpt-matched", 3, 1)

			dateFrom, dateTo := reportDateRange()
			report, err := apiClient.Reporting.GetMatchedReport(ctx, contextID, map[string]string{
				"date_from": dateFrom,
				"date_to":   dateTo,
			})
			require.NoError(t, err)
			require.NotNil(t, report)
			require.NotEmpty(t, report.Items, "matched report should have items")

			for i, item := range report.Items {
				require.NotEmpty(t, item.TransactionID, "item[%d] TransactionID", i)
				require.NotEmpty(t, item.MatchGroupID, "item[%d] MatchGroupID", i)
				require.NotEmpty(t, item.SourceID, "item[%d] SourceID", i)
				require.NotEmpty(t, item.Amount, "item[%d] Amount", i)
				require.NotEmpty(t, item.Currency, "item[%d] Currency", i)
			}
			tc.Logf("Matched report returned %d items", len(report.Items))
		},
	)
}

// TestReporting_GetUnmatchedReport verifies that the unmatched report endpoint
// returns the non-matching transactions that were ingested but not matched.
func TestReporting_GetUnmatchedReport(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			contextID := setupMatchedPipeline(t, ctx, tc, apiClient, "rpt-unmatched", 2, 2)

			dateFrom, dateTo := reportDateRange()
			report, err := apiClient.Reporting.GetUnmatchedReport(ctx, contextID, map[string]string{
				"date_from": dateFrom,
				"date_to":   dateTo,
			})
			require.NoError(t, err)
			require.NotNil(t, report)
			require.NotEmpty(t, report.Items, "unmatched report should have items")

			for i, item := range report.Items {
				require.NotEmpty(t, item.TransactionID, "item[%d] TransactionID", i)
				require.NotEmpty(t, item.SourceID, "item[%d] SourceID", i)
				require.NotEmpty(t, item.Amount, "item[%d] Amount", i)
				require.NotEmpty(t, item.Currency, "item[%d] Currency", i)
			}
			tc.Logf("Unmatched report returned %d items", len(report.Items))
		},
	)
}

// TestReporting_GetSummaryReport verifies that the summary report endpoint returns
// aggregate counts and amounts after matching has run.
func TestReporting_GetSummaryReport(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			contextID := setupMatchedPipeline(t, ctx, tc, apiClient, "rpt-summary", 3, 1)

			dateFrom, dateTo := reportDateRange()
			summary, err := apiClient.Reporting.GetSummaryReport(ctx, contextID, map[string]string{
				"date_from": dateFrom,
				"date_to":   dateTo,
			})
			require.NoError(t, err)
			require.NotNil(t, summary)
			require.Greater(t, summary.MatchedCount, 0, "matched count should be > 0")
			require.Equal(t, 1, summary.UnmatchedCount, "summary should report the seeded unmatched transaction count")
			require.NotEmpty(t, summary.TotalAmount, "total amount should be non-empty")
			require.NotEmpty(t, summary.MatchedAmount, "matched amount should be non-empty")
			require.NotEmpty(t, summary.UnmatchedAmount, "unmatched amount should be non-empty")
			tc.Logf(
				"Summary: matched=%d, unmatched=%d, total=%s, matched_amt=%s, unmatched_amt=%s",
				summary.MatchedCount,
				summary.UnmatchedCount,
				summary.TotalAmount,
				summary.MatchedAmount,
				summary.UnmatchedAmount,
			)
		},
	)
}

// TestReporting_GetVarianceReport verifies that the variance report endpoint returns
// a fee-enabled response with at least one variance row.
func TestReporting_GetVarianceReport(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			contextID := setupVariancePipeline(t, ctx, tc, apiClient, "rpt-variance")

			dateFrom, dateTo := reportDateRange()
			report, err := apiClient.Reporting.GetVarianceReport(ctx, contextID, map[string]string{
				"date_from": dateFrom,
				"date_to":   dateTo,
			})
			require.NoError(t, err)
			require.NotNil(t, report)
			require.NotEmpty(t, report.Items, "variance report should have at least one item")
			for _, item := range report.Items {
				require.NotEmpty(t, item.FeeScheduleName)
			}
			tc.Logf("Variance report returned %d items", len(report.Items))
		},
	)
}

// TestReporting_GetMatchedReport_Pagination verifies cursor-based pagination on the
// matched report endpoint. It fetches a first page with limit=2, then follows the
// next cursor to retrieve the second page.
func TestReporting_GetMatchedReport_Pagination(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			// Create enough matched pairs for pagination (6 matched = 12 items across 2 sources).
			contextID := setupMatchedPipeline(t, ctx, tc, apiClient, "rpt-paginated", 6, 0)

			// First page: limit=2
			dateFrom, dateTo := reportDateRange()
			page1, err := apiClient.Reporting.GetMatchedReport(ctx, contextID, map[string]string{
				"date_from": dateFrom,
				"date_to":   dateTo,
				"limit":     "2",
			})
			require.NoError(t, err)
			require.NotNil(t, page1)
			require.Len(t, page1.Items, 2, "first page should respect limit exactly when enough data exists")
			require.NotEmpty(t, page1.Pagination.Next, "first page must expose a next cursor when more data exists")
			tc.Logf("Page 1: %d items, next cursor: %q", len(page1.Items), page1.Pagination.Next)

			page2, err := apiClient.Reporting.GetMatchedReport(ctx, contextID, map[string]string{
				"date_from": dateFrom,
				"date_to":   dateTo,
				"limit":     "2",
				"cursor":    page1.Pagination.Next,
			})
			require.NoError(t, err)
			require.NotNil(t, page2)
			require.Len(t, page2.Items, 2, "second page should also respect limit when more data exists")
			tc.Logf("Page 2: %d items, next cursor: %q", len(page2.Items), page2.Pagination.Next)

			// Pages should not overlap.
			page1IDs := make(map[string]struct{})
			for _, item := range page1.Items {
				page1IDs[item.SourceID+"|"+item.TransactionID] = struct{}{}
			}
			for _, item := range page2.Items {
				key := item.SourceID + "|" + item.TransactionID
				_, exists := page1IDs[key]
				require.False(
					t,
					exists,
					"page 2 should not contain items from page 1 (duplicate: %s)",
					key,
				)
			}
		},
	)
}

// TestReporting_GetMatchedReport_EmptyContext verifies that querying the matched
// report for a context with no ingested data returns an empty result without error.
func TestReporting_GetMatchedReport_EmptyContext(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			emptyCtx := f.Context.NewContext().
				WithName("rpt-empty-matched").
				MustCreate(ctx)

			dateFrom, dateTo := reportDateRange()
			report, err := apiClient.Reporting.GetMatchedReport(ctx, emptyCtx.ID, map[string]string{
				"date_from": dateFrom,
				"date_to":   dateTo,
			})
			require.NoError(t, err)
			require.NotNil(t, report)
			require.Empty(t, report.Items, "matched report for empty context should have no items")
			tc.Logf("Empty context matched report: %d items (expected 0)", len(report.Items))
		},
	)
}

// TestReporting_GetMatchedReport_InvalidCursor verifies that malformed cursor values
// are rejected with a 400 response.
func TestReporting_GetMatchedReport_InvalidCursor(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			emptyCtx := f.Context.NewContext().
				WithName("rpt-invalid-cursor").
				MustCreate(ctx)

			dateFrom, dateTo := reportDateRange()
			_, err := apiClient.Reporting.GetMatchedReport(ctx, emptyCtx.ID, map[string]string{
				"date_from": dateFrom,
				"date_to":   dateTo,
				"cursor":    "not-a-valid-cursor",
			})
			require.Error(t, err)

			var apiErr *client.APIError
			require.ErrorAs(t, err, &apiErr)
			require.True(t, apiErr.IsBadRequest(), "invalid cursor should return 400, got %d", apiErr.StatusCode)
		},
	)
}

// TestReporting_CountUnmatched verifies that the unmatched count endpoint returns
// a count >= 1 when there are unmatched transactions.
func TestReporting_CountUnmatched(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			contextID := setupMatchedPipeline(t, ctx, tc, apiClient, "rpt-count-unmatched", 2, 3)

			dateFrom, dateTo := reportDateRange()
			countResp, err := apiClient.Reporting.CountUnmatched(
				ctx,
				contextID,
				dateFrom,
				dateTo,
			)
			require.NoError(t, err)
			require.NotNil(t, countResp)
			require.Equal(t, 3, countResp.Count, "count should match the seeded unmatched transaction total")
			tc.Logf("Unmatched count: %d", countResp.Count)
		},
	)
}

// TestReporting_CountUnmatched_EmptyContext verifies that counting unmatched
// transactions in a context with no data returns zero.
func TestReporting_CountUnmatched_EmptyContext(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			emptyCtx := f.Context.NewContext().
				WithName("rpt-empty-count").
				MustCreate(ctx)

			dateFrom, dateTo := reportDateRange()
			countResp, err := apiClient.Reporting.CountUnmatched(
				ctx,
				emptyCtx.ID,
				dateFrom,
				dateTo,
			)
			require.NoError(t, err)
			require.NotNil(t, countResp)
			require.Equal(t, 0, countResp.Count, "empty context should have 0 unmatched")
			tc.Logf("Empty context unmatched count: %d (expected 0)", countResp.Count)
		},
	)
}
