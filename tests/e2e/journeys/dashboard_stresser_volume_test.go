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

// TestDashboardStresser_HighVolume creates ~500k transactions for dashboard stress testing.
// Run with: E2E_KEEP_DATA=1 go test -tags e2e -v -run TestDashboardStresser_HighVolume -count=1 ./tests/e2e/journeys/...
// Data will be preserved after test completion for dashboard viewing.
func TestDashboardStresser_HighVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high volume dashboard test in short mode")
	}

	// Use 30 minute timeout for high volume
	e2e.RunE2EWithTimeout(
		t,
		30*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			cfg := HighVolumeDashboardConfig()
			rng := seededRand(cfg.Seed)

			// Create a custom test context that skips cleanup if E2E_KEEP_DATA is set
			if shouldSkipCleanup() {
				tc.Logf("⚠️  E2E_KEEP_DATA is set - cleanup will be SKIPPED")
				tc.Logf("⚠️  Data will persist in database after test completion")
			}

			tc.Logf("=== HIGH VOLUME DASHBOARD STRESSER ===")
			tc.Logf("Seed: %d (deterministic)", cfg.Seed)
			totalTx := cfg.PerfectMatchCount*2 +
				cfg.ToleranceMatchCount*2 +
				cfg.DateLagMatchCount*2 +
				cfg.UnmatchedCount +
				cfg.MultiSourceCount*3
			tc.Logf("Total transactions to generate: %d (~%dk)", totalTx, totalTx/1000)

			// Create context with fixed name for easy identification
			f := factories.New(tc, apiClient)
			reconciliationContext := createContextWithoutCleanup(
				ctx,
				f,
				"dashboard-stress-5k",
				shouldSkipCleanup(),
			)
			tc.Logf(
				"Context created: %s (ID: %s)",
				reconciliationContext.Name,
				reconciliationContext.ID,
			)

			// Create sources
			tc.Logf("\nCreating data sources...")
			ledgerSource := createSourceWithoutCleanup(
				ctx,
				f,
				reconciliationContext.ID,
				"ledger",
				"LEDGER",
				"LEFT",
				shouldSkipCleanup(),
			)
			bankSource := createSourceWithoutCleanup(
				ctx,
				f,
				reconciliationContext.ID,
				"bank",
				"BANK",
				"RIGHT",
				shouldSkipCleanup(),
			)
			gatewaySource := createSourceWithoutCleanup(
				ctx,
				f,
				reconciliationContext.ID,
				"gateway",
				"GATEWAY",
				"RIGHT",
				shouldSkipCleanup(),
			)

			// Create field maps (no cleanup needed - cascade deleted with context)
			tc.Logf("Creating field mappings...")
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, gatewaySource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create rules
			tc.Logf("Creating matching rules...")
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(2).
				Tolerance().
				WithToleranceConfig(cfg.ToleranceAmount).
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(3).
				DateLag().
				WithDateLagConfig(cfg.DateLagMinDays, cfg.DateLagMaxDays, "ABS", true).
				MustCreate(ctx)

			// Generate transaction data
			tc.Logf("\nGenerating %d transactions...", totalTx)
			baseDate := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			txGen := newTransactionGenerator(tc.NamePrefix(), rng, cfg, baseDate)

			ledgerBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			bankBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			gatewayBuilder := factories.NewCSVBuilder(tc.NamePrefix())

			// Generate in batches with progress logging
			tc.Logf("  Generating %d perfect match pairs...", cfg.PerfectMatchCount)
			for i := 0; i < cfg.PerfectMatchCount; i++ {
				tx := txGen.perfectMatch(i)
				ledgerBuilder.AddRowRaw(
					tx.ledgerID,
					tx.amount,
					tx.currency,
					tx.date,
					tx.description,
				)
				bankBuilder.AddRowRaw(tx.bankID, tx.amount, tx.currency, tx.date, tx.description)
				if i > 0 && i%50000 == 0 {
					tc.Logf("    Progress: %d/%d perfect matches", i, cfg.PerfectMatchCount)
				}
			}

			tc.Logf("  Generating %d tolerance match pairs...", cfg.ToleranceMatchCount)
			for i := 0; i < cfg.ToleranceMatchCount; i++ {
				tx := txGen.toleranceMatch(i)
				ledgerBuilder.AddRowRaw(
					tx.ledgerID,
					tx.ledgerAmount,
					tx.currency,
					tx.date,
					tx.description,
				)
				bankBuilder.AddRowRaw(
					tx.bankID,
					tx.bankAmount,
					tx.currency,
					tx.date,
					tx.description,
				)
			}

			tc.Logf("  Generating %d date lag match pairs...", cfg.DateLagMatchCount)
			for i := 0; i < cfg.DateLagMatchCount; i++ {
				tx := txGen.dateLagMatch(i)
				ledgerBuilder.AddRowRaw(
					tx.ledgerID,
					tx.amount,
					tx.currency,
					tx.ledgerDate,
					tx.description,
				)
				bankBuilder.AddRowRaw(
					tx.bankID,
					tx.amount,
					tx.currency,
					tx.bankDate,
					tx.description,
				)
			}

			tc.Logf("  Generating %d unmatched transactions...", cfg.UnmatchedCount)
			for i := 0; i < cfg.UnmatchedCount; i++ {
				tx := txGen.unmatched(i)
				if tx.isLedgerOnly {
					ledgerBuilder.AddRowRaw(tx.id, tx.amount, tx.currency, tx.date, tx.description)
				} else {
					bankBuilder.AddRowRaw(tx.id, tx.amount, tx.currency, tx.date, tx.description)
				}
			}

			tc.Logf("  Generating %d multi-source transactions...", cfg.MultiSourceCount)
			for i := 0; i < cfg.MultiSourceCount; i++ {
				tx := txGen.multiSource(i)
				ledgerBuilder.AddRowRaw(
					tx.ledgerID,
					tx.amount,
					tx.currency,
					tx.date,
					tx.description,
				)
				bankBuilder.AddRowRaw(tx.bankID, tx.amount, tx.currency, tx.date, tx.description)
				gatewayBuilder.AddRowRaw(
					tx.gatewayID,
					tx.amount,
					tx.currency,
					tx.date,
					tx.description,
				)
			}

			ledgerCSV := ledgerBuilder.Build()
			bankCSV := bankBuilder.Build()
			gatewayCSV := gatewayBuilder.Build()

			tc.Logf("Generated CSVs: ledger=%.2fMB, bank=%.2fMB, gateway=%.2fMB",
				float64(len(ledgerCSV))/1024/1024,
				float64(len(bankCSV))/1024/1024,
				float64(len(gatewayCSV))/1024/1024)

			// Ingest transactions
			tc.Logf("\nIngesting transactions...")

			tc.Logf("  Uploading ledger transactions...")
			start := time.Now()
			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger-stress.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)
			tc.Logf("  Ledger ingestion complete (took %v)", time.Since(start))

			tc.Logf("  Uploading bank transactions...")
			start = time.Now()
			bankJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank-stress.csv",
				bankCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID),
			)
			tc.Logf("  Bank ingestion complete (took %v)", time.Since(start))

			tc.Logf("  Uploading gateway transactions...")
			start = time.Now()
			gatewayJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				gatewaySource.ID,
				"gateway-stress.csv",
				gatewayCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, gatewayJob.ID),
			)
			tc.Logf("  Gateway ingestion complete (took %v)", time.Since(start))

			// Run matching
			tc.Logf("\nRunning matching...")
			start = time.Now()
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
			tc.Logf("Matching complete (took %v)", time.Since(start))

			// Get results
			matchRun, err := apiClient.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun.Status)

			// Collect match groups for later phases
			matchGroups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			tc.Logf("  Match groups found: %d", len(matchGroups))

			// Query dashboard (max 90 days allowed)
			tc.Logf("\nQuerying dashboard data...")
			dateFrom := baseDate.Format("2006-01-02")
			maxDays := cfg.DateRangeDays
			if maxDays > 89 {
				maxDays = 89
			}
			dateTo := baseDate.AddDate(0, 0, maxDays).Format("2006-01-02")
			dashboard, err := apiClient.Reporting.GetDashboardAggregates(
				ctx,
				reconciliationContext.ID,
				dateFrom,
				dateTo,
			)
			require.NoError(t, err)

			runDashboardStresserHighVolumeEnrichment(
				t,
				tc,
				apiClient,
				reconciliationContext.ID,
				reconciliationContext.Name,
				ledgerSource.ID,
				ledgerJob.ID,
				bankJob.ID,
				ledgerCSV,
				dateFrom,
				dateTo,
				dashboard,
				matchGroups,
			)
		},
	)
}
