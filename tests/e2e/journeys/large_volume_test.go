//go:build e2e

package journeys

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestLargeVolume_ThousandTransactions tests ingestion and matching with 1000+ transactions.
func TestLargeVolume_ThousandTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large volume test in short mode")
	}

	e2e.RunE2EWithTimeout(
		t,
		5*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("large-volume").MustCreate(ctx)
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

			numTransactions := 1000
			tc.Logf("Generating %d transactions...", numTransactions)

			ledgerBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			bankBuilder := factories.NewCSVBuilder(tc.NamePrefix())

			for i := 0; i < numTransactions; i++ {
				id := fmt.Sprintf("LV-%04d", i)
				amount := fmt.Sprintf("%d.00", 100+i%100)
				date := fmt.Sprintf("2026-01-%02d", (i%28)+1)
				ledgerBuilder.AddRow(id, amount, "USD", date, "large volume tx")
				bankBuilder.AddRow(id, amount, "USD", date, "large volume tx")
			}

			ledgerCSV := ledgerBuilder.Build()
			bankCSV := bankBuilder.Build()

			tc.Logf("Uploading ledger CSV (%d bytes)...", len(ledgerCSV))
			start := time.Now()
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
			tc.Logf("Ledger ingestion completed in %v", time.Since(start))

			tc.Logf("Uploading bank CSV (%d bytes)...", len(bankCSV))
			start = time.Now()
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
			tc.Logf("Bank ingestion completed in %v", time.Since(start))

			tc.Logf("Running matching...")
			start = time.Now()
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
			tc.Logf("Matching completed in %v", time.Since(start))

			matchRun, err := client.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun.Status)

			tc.Logf("✓ Large volume test completed: %d transactions processed", numTransactions)
		},
	)
}

// TestLargeVolume_HundredTransactions tests ingestion with 100 transactions (faster variant).
func TestLargeVolume_HundredTransactions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().
				WithName("medium-volume").
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

			numTransactions := 100
			ledgerBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			bankBuilder := factories.NewCSVBuilder(tc.NamePrefix())

			for i := 0; i < numTransactions; i++ {
				id := fmt.Sprintf("MV-%03d", i)
				amount := fmt.Sprintf("%d.50", 50+i)
				date := fmt.Sprintf("2026-01-%02d", (i%28)+1)
				ledgerBuilder.AddRow(id, amount, "USD", date, "medium volume")
				bankBuilder.AddRow(id, amount, "USD", date, "medium volume")
			}

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"l.csv",
				ledgerBuilder.Build(),
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
				bankBuilder.Build(),
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

			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), numTransactions, "all transactions should match")

			tc.Logf(
				"✓ Medium volume test: %d matches from %d transactions",
				len(groups),
				numTransactions,
			)
		},
	)
}

// TestLargeVolume_MixedCurrencies tests large volume with multiple currencies.
func TestLargeVolume_MixedCurrencies(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().
				WithName("multi-currency").
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

			currencies := []string{"USD", "EUR", "GBP", "BRL", "JPY"}
			txPerCurrency := 20
			ledgerBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			bankBuilder := factories.NewCSVBuilder(tc.NamePrefix())

			for _, currency := range currencies {
				for i := 0; i < txPerCurrency; i++ {
					id := fmt.Sprintf("MC-%s-%02d", currency, i)
					amount := fmt.Sprintf("%d.00", 100+i)
					date := "2026-01-15"
					ledgerBuilder.AddRow(id, amount, currency, date, "multi-currency")
					bankBuilder.AddRow(id, amount, currency, date, "multi-currency")
				}
			}

			ledgerJob, err := client.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"l.csv",
				ledgerBuilder.Build(),
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
				bankBuilder.Build(),
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

			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			expectedMatches := len(currencies) * txPerCurrency
			require.GreaterOrEqual(t, len(groups), expectedMatches, "all transactions should match")

			tc.Logf(
				"✓ Multi-currency test: %d matches across %d currencies",
				len(groups),
				len(currencies),
			)
		},
	)
}
