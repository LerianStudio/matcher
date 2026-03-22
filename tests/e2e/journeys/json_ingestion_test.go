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

// TestJSONIngestion_BasicFlow tests JSON file ingestion as an alternative to CSV.
func TestJSONIngestion_BasicFlow(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			// Setup: Create context, source, field map, and rule
			reconciliationContext := f.Context.NewContext().
				WithName("json-ingestion").
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

			// Create JSON content
			ledgerJSON := factories.NewJSONBuilder(tc.NamePrefix()).
				AddRow("JSON-001", "100.00", "USD", "2026-01-15", "json ledger tx").
				AddRow("JSON-002", "250.50", "USD", "2026-01-16", "json ledger tx 2").
				Build()

			bankJSON := factories.NewJSONBuilder(tc.NamePrefix()).
				AddRow("JSON-001", "100.00", "USD", "2026-01-15", "json bank tx").
				AddRow("JSON-002", "250.50", "USD", "2026-01-16", "json bank tx 2").
				Build()

			// Upload JSON files
			ledgerJob, err := client.Ingestion.UploadJSON(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.json",
				ledgerJSON,
			)
			require.NoError(t, err)
			tc.Logf("Ledger JSON job created: %s", ledgerJob.ID)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, ledgerJob.ID),
			)

			bankJob, err := client.Ingestion.UploadJSON(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank.json",
				bankJSON,
			)
			require.NoError(t, err)
			tc.Logf("Bank JSON job created: %s", bankJob.ID)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID),
			)

			// Verify jobs completed with correct counts
			ledgerJobStatus, err := client.Ingestion.GetJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", ledgerJobStatus.Status)
			require.Equal(t, 2, ledgerJobStatus.TotalRows)

			bankJobStatus, err := client.Ingestion.GetJob(ctx, reconciliationContext.ID, bankJob.ID)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", bankJobStatus.Status)
			require.Equal(t, 2, bankJobStatus.TotalRows)

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

			// Verify match results
			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 2, "both JSON transactions should match")

			tc.Logf("✓ JSON ingestion completed with %d matches", len(groups))
		},
	)
}

// TestJSONIngestion_EmptyArray tests that empty JSON arrays are rejected with 400.
func TestJSONIngestion_EmptyArray(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("json-empty").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Upload empty JSON array - should be rejected
			emptyJSON := []byte("[]")
			_, err := client.Ingestion.UploadJSON(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"empty.json",
				emptyJSON,
			)
			require.Error(t, err, "empty JSON array should be rejected")

			// Verify we get a 400 Bad Request
			require.Contains(t, err.Error(), "400", "should return 400 for empty file")

			tc.Logf("✓ Empty JSON array correctly rejected with 400")
		},
	)
}

// TestJSONIngestion_MixedFormats tests using both JSON and CSV in the same context.
func TestJSONIngestion_MixedFormats(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("mixed-format").MustCreate(ctx)
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

			// Ledger: JSON format
			ledgerJSON := factories.NewJSONBuilder(tc.NamePrefix()).
				AddRow("MIX-001", "500.00", "USD", "2026-01-20", "json source").
				Build()

			// Bank: CSV format
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MIX-001", "500.00", "USD", "2026-01-20", "csv source").
				Build()

			// Upload mixed formats
			ledgerJob, err := client.Ingestion.UploadJSON(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.json",
				ledgerJSON,
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

			// Verify match works across formats
			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 1, "should match JSON to CSV")

			tc.Logf("✓ Mixed format ingestion (JSON + CSV) completed successfully")
		},
	)
}
