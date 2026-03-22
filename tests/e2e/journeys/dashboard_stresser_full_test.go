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

// TestDashboardStresser_FullJourney creates a rich dataset for dashboard visualization.
// It exercises the complete flow: context, sources, mappings, rules, ingestion, matching.
// After running, the dashboard for this context will be FULL of data.
func TestDashboardStresser_FullJourney(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dashboard stresser in short mode")
	}

	e2e.RunE2EWithTimeout(
		t,
		10*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			cfg := DefaultDashboardStresserConfig()
			rng := seededRand(cfg.Seed)
			f := factories.New(tc, apiClient)

			tc.Logf("=== DASHBOARD STRESSER START ===")
			tc.Logf("Seed: %d (deterministic)", cfg.Seed)
			tc.Logf("Total transactions to generate: %d",
				cfg.PerfectMatchCount*2+
					cfg.ToleranceMatchCount*2+
					cfg.DateLagMatchCount*2+
					cfg.UnmatchedCount+
					cfg.MultiSourceCount*3)

			// ============================================================
			// STEP 1: Create Reconciliation Context
			// ============================================================
			tc.Logf("\n[STEP 1/9] Creating reconciliation context...")
			reconciliationContext := f.Context.NewContext().
				WithName("dashboard-stresser").
				WithDescription("E2E dashboard stresser - full data journey").
				OneToOne().
				MustCreate(ctx)
			tc.Logf("Context created: %s", reconciliationContext.ID)

			// ============================================================
			// STEP 2: Create Multiple Sources
			// ============================================================
			tc.Logf("\n[STEP 2/9] Creating data sources...")
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger-primary").
				AsLedger().
				MustCreate(ctx)

			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank-statement").
				AsBank().
				MustCreate(ctx)

			gatewaySource := f.Source.NewSource(reconciliationContext.ID).
				WithName("payment-gateway").
				AsGateway().
				MustCreate(ctx)

			tc.Logf("Sources created: ledger=%s, bank=%s, gateway=%s",
				ledgerSource.ID, bankSource.ID, gatewaySource.ID)

			// ============================================================
			// STEP 3: Create Field Mappings
			// ============================================================
			tc.Logf("\n[STEP 3/9] Creating field mappings...")
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, gatewaySource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			tc.Logf("Field mappings created for all sources")

			// ============================================================
			// STEP 4: Create Matching Rules (priority order)
			// ============================================================
			tc.Logf("\n[STEP 4/9] Creating matching rules...")

			// Rule 1: Exact match (highest priority)
			exactRule := f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)
			tc.Logf("Created exact match rule: %s (priority 1)", exactRule.ID)

			// Rule 2: Tolerance match
			toleranceRule := f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(2).
				Tolerance().
				WithToleranceConfig(cfg.ToleranceAmount).
				MustCreate(ctx)
			tc.Logf(
				"Created tolerance rule: %s (priority 2, tolerance=%s)",
				toleranceRule.ID,
				cfg.ToleranceAmount,
			)

			// Rule 3: Date lag match (ABS = absolute/bi-directional date difference)
			dateLagRule := f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(3).
				DateLag().
				WithDateLagConfig(cfg.DateLagMinDays, cfg.DateLagMaxDays, "ABS", true).
				MustCreate(ctx)
			tc.Logf(
				"Created date lag rule: %s (priority 3, %d-%d days)",
				dateLagRule.ID,
				cfg.DateLagMinDays,
				cfg.DateLagMaxDays,
			)

			// ============================================================
			// STEP 5: Generate Transaction Data
			// ============================================================
			tc.Logf("\n[STEP 5/9] Generating transaction data...")
			baseDate := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			txGen := newTransactionGenerator(tc.NamePrefix(), rng, cfg, baseDate)

			// Build CSVs for each source
			ledgerBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			bankBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			gatewayBuilder := factories.NewCSVBuilder(tc.NamePrefix())

			// 5a: Perfect matches (ledger + bank with identical data)
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
			}

			// 5b: Tolerance matches (amounts differ within tolerance)
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

			// 5c: Date lag matches (same amount, different dates)
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

			// 5d: Unmatched (ledger-only or bank-only)
			tc.Logf("  Generating %d unmatched transactions...", cfg.UnmatchedCount)
			for i := 0; i < cfg.UnmatchedCount; i++ {
				tx := txGen.unmatched(i)
				if tx.isLedgerOnly {
					ledgerBuilder.AddRowRaw(tx.id, tx.amount, tx.currency, tx.date, tx.description)
				} else {
					bankBuilder.AddRowRaw(tx.id, tx.amount, tx.currency, tx.date, tx.description)
				}
			}

			// 5e: Multi-source (transactions across ledger, bank, AND gateway)
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

			tc.Logf("Generated CSVs: ledger=%d bytes, bank=%d bytes, gateway=%d bytes",
				len(ledgerCSV), len(bankCSV), len(gatewayCSV))

			// ============================================================
			// STEP 6: Ingest Transactions
			// ============================================================
			tc.Logf("\n[STEP 6/9] Ingesting transactions...")

			// Upload ledger
			tc.Logf("  Uploading ledger transactions...")
			start := time.Now()
			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger-stresser.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
			)
			tc.Logf("  Ledger ingestion complete: %s (took %v)", ledgerJob.ID, time.Since(start))

			// Upload bank
			tc.Logf("  Uploading bank transactions...")
			start = time.Now()
			bankJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				bankSource.ID,
				"bank-stresser.csv",
				bankCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID),
			)
			tc.Logf("  Bank ingestion complete: %s (took %v)", bankJob.ID, time.Since(start))

			// Upload gateway
			tc.Logf("  Uploading gateway transactions...")
			start = time.Now()
			gatewayJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				gatewaySource.ID,
				"gateway-stresser.csv",
				gatewayCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, gatewayJob.ID),
			)
			tc.Logf("  Gateway ingestion complete: %s (took %v)", gatewayJob.ID, time.Since(start))

			// ============================================================
			// STEP 7: Run Multiple Matching Jobs
			// ============================================================
			tc.Logf("\n[STEP 7/9] Running matching jobs...")

			// First: Dry run to preview
			tc.Logf("  Running DRY_RUN matching...")
			start = time.Now()
			dryRunResp, err := apiClient.Matching.RunMatchDryRun(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					apiClient,
					reconciliationContext.ID,
					dryRunResp.RunID,
				),
			)
			dryRunResult, err := apiClient.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				dryRunResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", dryRunResult.Status)
			tc.Logf("  Dry run complete: %s (took %v)", dryRunResp.RunID, time.Since(start))

			// Second: Commit matching
			tc.Logf("  Running COMMIT matching...")
			start = time.Now()
			commitResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					apiClient,
					reconciliationContext.ID,
					commitResp.RunID,
				),
			)
			commitResult, err := apiClient.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				commitResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", commitResult.Status)
			tc.Logf("  Commit complete: %s (took %v)", commitResp.RunID, time.Since(start))

			// ============================================================
			// STEP 8: Retrieve and Verify Match Results
			// ============================================================
			tc.Logf("\n[STEP 8/9] Retrieving match results...")

			matchGroups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				commitResp.RunID,
			)
			require.NoError(t, err)
			tc.Logf("  Total match groups: %d", len(matchGroups))

			// Get match run history
			matchRuns, err := apiClient.Matching.ListMatchRuns(ctx, reconciliationContext.ID)
			require.NoError(t, err)
			tc.Logf("  Match run history: %d runs", len(matchRuns))

			// ============================================================
			// STEP 9: Query Dashboard Data
			// ============================================================
			tc.Logf("\n[STEP 9/9] Querying dashboard data...")

			// Use date range that covers our transactions
			dateFrom := baseDate.Format("2006-01-02")
			dateTo := baseDate.AddDate(0, 0, cfg.DateRangeDays+10).Format("2006-01-02")

			dashboard, err := apiClient.Reporting.GetDashboardAggregates(
				ctx,
				reconciliationContext.ID,
				dateFrom,
				dateTo,
			)
			require.NoError(t, err)
			require.NotNil(t, dashboard)

			tc.Logf("\n=== DASHBOARD SUMMARY ===")
			tc.Logf("Context ID: %s", reconciliationContext.ID)
			tc.Logf("Context Name: %s", reconciliationContext.Name)

			if dashboard.Volume != nil {
				tc.Logf("\nVolume Stats:")
				tc.Logf("  Total Transactions: %d", dashboard.Volume.TotalTransactions)
				tc.Logf("  Matched Transactions: %d", dashboard.Volume.MatchedTransactions)
				tc.Logf("  Unmatched Count: %d", dashboard.Volume.UnmatchedCount)
				tc.Logf("  Total Amount: %s", dashboard.Volume.TotalAmount)
				tc.Logf("  Matched Amount: %s", dashboard.Volume.MatchedAmount)
				tc.Logf("  Unmatched Amount: %s", dashboard.Volume.UnmatchedAmount)
			}

			if dashboard.MatchRate != nil {
				tc.Logf("\nMatch Rate Stats:")
				tc.Logf("  Match Rate: %.2f%%", dashboard.MatchRate.MatchRate)
				tc.Logf("  Match Rate by Amount: %.2f%%", dashboard.MatchRate.MatchRateAmount)
				tc.Logf("  Total Count: %d", dashboard.MatchRate.TotalCount)
				tc.Logf("  Matched Count: %d", dashboard.MatchRate.MatchedCount)
				tc.Logf("  Unmatched Count: %d", dashboard.MatchRate.UnmatchedCount)
			}

			if dashboard.SLA != nil {
				tc.Logf("\nSLA Stats:")
				tc.Logf("  Total Exceptions: %d", dashboard.SLA.TotalExceptions)
				tc.Logf("  Resolved On Time: %d", dashboard.SLA.ResolvedOnTime)
				tc.Logf("  Resolved Late: %d", dashboard.SLA.ResolvedLate)
				tc.Logf("  Pending Within SLA: %d", dashboard.SLA.PendingWithinSLA)
				tc.Logf("  Pending Overdue: %d", dashboard.SLA.PendingOverdue)
				tc.Logf("  SLA Compliance Rate: %.2f%%", dashboard.SLA.SLAComplianceRate)
			}

			tc.Logf("\n=== MATCH GROUPS BY RULE ===")
			ruleStats := analyzeMatchGroups(matchGroups)
			for ruleID, count := range ruleStats {
				tc.Logf("  Rule %s: %d groups", ruleID, count)
			}

			// H20: Tighten assertions using deterministic seed (42).
			//
			// Transaction generation is seeded, so counts are reproducible:
			//   - 200 perfect-match pairs  → 200 exact-rule groups (guaranteed)
			//   -  50 tolerance-match pairs → up to 50 tolerance-rule groups
			//   -  30 date-lag-match pairs  → up to 30 date-lag-rule groups
			//   - 100 unmatched             → 0 groups (single-sided by design)
			//   -  20 multi-source triples  → extra matches via ledger↔bank exact rule
			//
			// Lower bound: all 200 perfect-match pairs share the same external ID
			// on both sides, so the exact rule (priority 1) will always match them.
			// Multi-source triples also share the external ID across ledger+bank,
			// so they add another ~20 exact-match groups.
			expectedMinGroups := cfg.PerfectMatchCount + cfg.MultiSourceCount // 200 + 20 = 220
			require.GreaterOrEqual(t, len(matchGroups), expectedMinGroups,
				"seeded(42) full run: expected >= %d groups from %d perfect + %d multi-source pairs",
				expectedMinGroups, cfg.PerfectMatchCount, cfg.MultiSourceCount,
			)

			// Upper bound: total matchable pairs across all three rules.
			maxPossibleGroups := cfg.PerfectMatchCount + cfg.ToleranceMatchCount +
				cfg.DateLagMatchCount + cfg.MultiSourceCount // 200+50+30+20 = 300
			require.LessOrEqual(t, len(matchGroups), maxPossibleGroups,
				"match groups should not exceed total matchable pairs (%d)", maxPossibleGroups,
			)

			// Also verify the dashboard volume stats are populated with reasonable counts.
			if dashboard.Volume != nil {
				// Total ingested = perfect*2 + tolerance*2 + dateLag*2 + unmatched + multi*3
				expectedIngested := cfg.PerfectMatchCount*2 + cfg.ToleranceMatchCount*2 +
					cfg.DateLagMatchCount*2 + cfg.UnmatchedCount + cfg.MultiSourceCount*3
				require.Greater(t, dashboard.Volume.TotalTransactions, 0,
					"dashboard should report > 0 total transactions")
				require.LessOrEqual(t, dashboard.Volume.TotalTransactions, expectedIngested+10,
					"dashboard total should not wildly exceed expected ingested count (%d)",
					expectedIngested,
				)
			}

			if dashboard.MatchRate != nil {
				require.Greater(t, dashboard.MatchRate.MatchedCount, 0,
					"dashboard should report > 0 matched transactions")
				require.GreaterOrEqual(t, dashboard.MatchRate.MatchRate, 30.0,
					"match rate should be at least 30%% given 200+ matchable pairs vs 100 unmatched",
				)
			}

			tc.Logf("\n✓ Dashboard stresser completed successfully!")
			tc.Logf("  Match groups: %d (expected range [%d, %d])", len(matchGroups), expectedMinGroups, maxPossibleGroups)
			tc.Logf("  Context ID for dashboard viewing: %s", reconciliationContext.ID)
		},
	)
}
