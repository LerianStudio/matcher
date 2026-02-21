//go:build e2e

package journeys

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// shouldSkipCleanup returns true if E2E_KEEP_DATA env var is set.
func shouldSkipCleanup() bool {
	return os.Getenv("E2E_KEEP_DATA") != ""
}

// cleanupContextChildren deletes all children of a context in the correct dependency order:
// field maps → rules → sources. All errors are logged as warnings but do not halt the cascade,
// ensuring best-effort cleanup even when individual deletions fail.
func cleanupContextChildren(
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	contextID string,
) {
	// 1. Delete field maps (via sources)
	sources, err := apiClient.Configuration.ListSources(ctx, contextID)
	if err != nil {
		tc.Logf("  warn: failed to list sources for cleanup: %v", err)
	}

	if sources != nil {
		for _, src := range sources {
			fm, fmErr := apiClient.Configuration.GetFieldMapBySource(ctx, contextID, src.ID)
			if fmErr == nil && fm != nil && fm.ID != "" {
				if delErr := apiClient.Configuration.DeleteFieldMap(ctx, fm.ID); delErr != nil {
					tc.Logf("  warn: failed to delete field map %s: %v", fm.ID, delErr)
				}
			}
		}
	}

	// 2. Delete rules
	rules, rulesErr := apiClient.Configuration.ListMatchRules(ctx, contextID)
	if rulesErr != nil {
		tc.Logf("  warn: failed to list rules for cleanup: %v", rulesErr)
	}

	if rules != nil {
		for _, r := range rules {
			if delErr := apiClient.Configuration.DeleteMatchRule(ctx, contextID, r.ID); delErr != nil {
				tc.Logf("  warn: failed to delete rule %s: %v", r.ID, delErr)
			}
		}
	}

	// 3. Delete sources (after field maps are removed)
	if sources != nil {
		for _, src := range sources {
			if delErr := apiClient.Configuration.DeleteSource(ctx, contextID, src.ID); delErr != nil {
				tc.Logf("  warn: failed to delete source %s: %v", src.ID, delErr)
			}
		}
	}
}

// deleteClonedContext deletes a cloned context and all its children in the correct order:
// field maps → rules → sources → context.
func deleteClonedContext(
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	contextID string,
	endpointFailures map[string]string,
	enrichedEndpoints *int,
) {
	cleanupContextChildren(ctx, tc, apiClient, contextID)

	if err := apiClient.Configuration.DeleteContext(ctx, contextID); err != nil {
		if endpointFailures != nil {
			endpointFailures["DeleteContext"] = err.Error()
		}
	} else {
		tc.Logf("  DeleteContext: cleaned up clone %s", contextID)

		if enrichedEndpoints != nil {
			*enrichedEndpoints++
		}
	}
}

// DashboardStresserConfig controls the dashboard stresser behavior.
type DashboardStresserConfig struct {
	Seed int64

	// Transaction counts
	PerfectMatchCount   int // Transactions that will match exactly
	ToleranceMatchCount int // Transactions that will match with tolerance
	DateLagMatchCount   int // Transactions that will match with date lag
	UnmatchedCount      int // Transactions that will NOT match (ledger only or bank only)
	MultiSourceCount    int // Transactions across 3+ sources

	// Rule configuration
	ToleranceAmount  string
	PercentTolerance float64
	DateLagMinDays   int
	DateLagMaxDays   int

	// Currencies to use
	Currencies []string

	// Date range (days from "base date")
	DateRangeDays int
}

// DefaultDashboardStresserConfig returns a balanced configuration for dashboard testing.
func DefaultDashboardStresserConfig() DashboardStresserConfig {
	return DashboardStresserConfig{
		Seed:                42, // Deterministic by default
		PerfectMatchCount:   200,
		ToleranceMatchCount: 50,
		DateLagMatchCount:   30,
		UnmatchedCount:      100,
		MultiSourceCount:    20,
		ToleranceAmount:     "5.00",
		PercentTolerance:    2.0,
		DateLagMinDays:      1,
		DateLagMaxDays:      3,
		Currencies:          []string{"USD", "EUR", "GBP", "BRL", "JPY"},
		DateRangeDays:       30,
	}
}

// HighVolumeDashboardConfig returns a configuration for ~5k transactions.
// Sized to work within the matching engine's candidate limit (5000 per side).
// Total: 1.5k*2 + 300*2 + 100*2 + 1k + 100*3 = ~5k transactions
func HighVolumeDashboardConfig() DashboardStresserConfig {
	return DashboardStresserConfig{
		Seed:                42,
		PerfectMatchCount:   1500, // 3k transactions (1.5k pairs)
		ToleranceMatchCount: 300,  // 600 transactions
		DateLagMatchCount:   100,  // 200 transactions
		UnmatchedCount:      1000, // 1k single-sided
		MultiSourceCount:    100,  // 300 transactions (3 sources)
		ToleranceAmount:     "5.00",
		PercentTolerance:    2.0,
		DateLagMinDays:      1,
		DateLagMaxDays:      3,
		Currencies:          []string{"USD", "EUR", "GBP", "BRL", "JPY", "CAD", "AUD", "CHF"},
		DateRangeDays:       90,
	}
}

// seededRand creates a deterministic random generator.
func seededRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

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
			dryRunResp, err := apiClient.Matching.RunMatchDryRun(ctx, reconciliationContext.ID, "")
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
			commitResp, err := apiClient.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
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

			// Assertions
			require.Greater(t, len(matchGroups), 0, "should have some matches")
			tc.Logf("\n✓ Dashboard stresser completed successfully!")
			tc.Logf("  Context ID for dashboard viewing: %s", reconciliationContext.ID)
		},
	)
}

// knownFailures documents endpoints expected to fail and why.
// Entries prefixed with "env:" are environment-dependent (no infra configured).
// Entries prefixed with "bug:" are tracked backend issues to fix.
// When a known failure starts passing, the test will remind you to remove it.
var knownFailures = map[string]string{
	"ForceMatch":         "bug: MatchingGateway/ResolutionExecutor nil in bootstrap",
	"BulkDispatch":       "bug: UUID validation issue",
	"DispatchToExternal": "env: no external dispatch target configured",
}

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
				shouldSkipCleanup(),
			)
			bankSource := createSourceWithoutCleanup(
				ctx,
				f,
				reconciliationContext.ID,
				"bank",
				"BANK",
				shouldSkipCleanup(),
			)
			gatewaySource := createSourceWithoutCleanup(
				ctx,
				f,
				reconciliationContext.ID,
				"gateway",
				"GATEWAY",
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

			// ============================================================
			// ENRICHED API COVERAGE PHASES (Steps 10-22)
			// All non-critical: assert.NoError + log warnings
			// ============================================================

			// Tracking variables for enriched summary
			var (
				enrichedEndpoints    int
				scheduleCreated      bool
				clonedContextID      string
				feeScheduleSimulated bool
				exceptionsFound      int
				commentsAdded        int
				commentsDeleted      int
				disputesOpened       int
				disputesClosed       int
				bulkAssigned         int
				bulkResolved         int
				bulkDispatched       int
				forceMatched         int
				adjustedEntries      int
				dispatched           int
				syncReports          int
				asyncJobs            int
				auditLogsFound       int
				searchResultsFound   int
			)

			// endpointFailures collects all endpoint errors for categorized evaluation at the end.
			endpointFailures := make(map[string]string)

			// ============================================================
			// STEP 10: Configuration Verification
			// ============================================================
			tc.Logf("\n[STEP 10/22] Configuration verification...")

			if contexts, err := apiClient.Configuration.ListContexts(ctx); err != nil {
				endpointFailures["ListContexts"] = err.Error()
			} else {
				tc.Logf("  ListContexts: %d contexts", len(contexts))
				enrichedEndpoints++
			}

			if ctxDetail, err := apiClient.Configuration.GetContext(ctx, reconciliationContext.ID); err != nil {
				endpointFailures["GetContext"] = err.Error()
			} else {
				tc.Logf("  GetContext: %s (%s)", ctxDetail.Name, ctxDetail.ID)
				enrichedEndpoints++
			}

			if _, err := apiClient.Configuration.UpdateContext(ctx, reconciliationContext.ID, client.UpdateContextRequest{
				Description: strPtr("Enriched stresser - updated description"),
			}); err != nil {
				endpointFailures["UpdateContext"] = err.Error()
			} else {
				tc.Logf("  UpdateContext: description updated")
				enrichedEndpoints++
			}

			if sources, err := apiClient.Configuration.ListSources(ctx, reconciliationContext.ID); err != nil {
				endpointFailures["ListSources"] = err.Error()
			} else {
				tc.Logf("  ListSources: %d sources", len(sources))
				enrichedEndpoints++
			}

			if src, err := apiClient.Configuration.GetSource(ctx, reconciliationContext.ID, ledgerSource.ID); err != nil {
				endpointFailures["GetSource"] = err.Error()
			} else {
				tc.Logf("  GetSource: %s (%s)", src.Name, src.Type)
				enrichedEndpoints++
			}

			if rules, err := apiClient.Configuration.ListMatchRules(ctx, reconciliationContext.ID); err != nil {
				endpointFailures["ListMatchRules"] = err.Error()
			} else {
				tc.Logf("  ListMatchRules: %d rules", len(rules))
				enrichedEndpoints++

				// Reorder rules (reverse then restore)
				if len(rules) >= 2 {
					ruleIDs := make([]string, len(rules))
					for i, r := range rules {
						ruleIDs[i] = r.ID
					}
					if err := apiClient.Configuration.ReorderMatchRules(ctx, reconciliationContext.ID, client.ReorderMatchRulesRequest{
						RuleIDs: ruleIDs,
					}); err != nil {
						endpointFailures["ReorderMatchRules"] = err.Error()
					} else {
						tc.Logf("  ReorderMatchRules: reordered %d rules", len(ruleIDs))
						enrichedEndpoints++
					}
				}
			}

			if fm, err := apiClient.Configuration.GetFieldMapBySource(ctx, reconciliationContext.ID, ledgerSource.ID); err != nil {
				endpointFailures["GetFieldMapBySource"] = err.Error()
			} else {
				tc.Logf("  GetFieldMapBySource: %s", fm.ID)
				enrichedEndpoints++
			}

			// ============================================================
			// STEP 11: Schedule CRUD
			// ============================================================
			tc.Logf("\n[STEP 11/22] Schedule CRUD...")

			schedule, err := apiClient.Configuration.CreateSchedule(ctx, reconciliationContext.ID, client.CreateScheduleRequest{
				CronExpression: "0 0 * * *",
				Enabled:        true,
			})
			if err != nil {
				endpointFailures["CreateSchedule"] = err.Error()
			} else {
				scheduleCreated = true
				tc.Logf("  CreateSchedule: %s", schedule.ID)
				enrichedEndpoints++

				if got, err := apiClient.Configuration.GetSchedule(ctx, reconciliationContext.ID, schedule.ID); err != nil {
					endpointFailures["GetSchedule"] = err.Error()
				} else {
					tc.Logf("  GetSchedule: cron=%s enabled=%v", got.CronExpression, got.Enabled)
					enrichedEndpoints++
				}

				if list, err := apiClient.Configuration.ListSchedules(ctx, reconciliationContext.ID); err != nil {
					endpointFailures["ListSchedules"] = err.Error()
				} else {
					tc.Logf("  ListSchedules: %d schedules", len(list))
					enrichedEndpoints++
				}

				disabled := false
				if _, err := apiClient.Configuration.UpdateSchedule(ctx, reconciliationContext.ID, schedule.ID, client.UpdateScheduleRequest{
					Enabled: &disabled,
				}); err != nil {
					endpointFailures["UpdateSchedule"] = err.Error()
				} else {
					tc.Logf("  UpdateSchedule: disabled schedule")
					enrichedEndpoints++
				}

				if err := apiClient.Configuration.DeleteSchedule(ctx, reconciliationContext.ID, schedule.ID); err != nil {
					endpointFailures["DeleteSchedule"] = err.Error()
				} else {
					tc.Logf("  DeleteSchedule: removed schedule")
					enrichedEndpoints++
				}
			}

			// ============================================================
			// STEP 12: Ingestion Enrichment
			// ============================================================
			tc.Logf("\n[STEP 12/22] Ingestion enrichment...")

			if jobs, err := apiClient.Ingestion.ListJobsByContext(ctx, reconciliationContext.ID); err != nil {
				endpointFailures["ListJobsByContext"] = err.Error()
			} else {
				tc.Logf("  ListJobsByContext: %d jobs", len(jobs))
				enrichedEndpoints++
			}

			// Collect some transactions from the first job for later use
			var allTransactions []client.Transaction
			if txns, err := apiClient.Ingestion.ListTransactionsByJob(ctx, reconciliationContext.ID, ledgerJob.ID); err != nil {
				endpointFailures["ListTransactionsByJob"] = err.Error()
			} else {
				allTransactions = txns
				tc.Logf("  ListTransactionsByJob: %d transactions from ledger job", len(txns))
				enrichedEndpoints++
			}

			// Preview a file
			if preview, err := apiClient.Ingestion.PreviewFile(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"preview-sample.csv",
				ledgerCSV,
				"csv",
				5,
			); err != nil {
				endpointFailures["PreviewFile"] = err.Error()
			} else {
				tc.Logf("  PreviewFile: %d columns, %d rows, format=%s", len(preview.Columns), preview.RowCount, preview.Format)
				enrichedEndpoints++
			}

			// Search transactions
			if searchResp, err := apiClient.Ingestion.SearchTransactions(ctx, reconciliationContext.ID, client.SearchTransactionsParams{
				Currency: "USD",
				Limit:    10,
			}); err != nil {
				endpointFailures["SearchTransactions"] = err.Error()
			} else {
				searchResultsFound = searchResp.Total
				tc.Logf("  SearchTransactions: %d results (total: %d)", len(searchResp.Items), searchResp.Total)
				enrichedEndpoints++
			}

			// Ignore a transaction (pick last unmatched one if available)
			if len(allTransactions) > 0 {
				// Find an unmatched transaction
				var txToIgnore *client.Transaction
				for i := len(allTransactions) - 1; i >= 0; i-- {
					if allTransactions[i].Status == "UNMATCHED" {
						txToIgnore = &allTransactions[i]
						break
					}
				}
				if txToIgnore != nil {
					if _, err := apiClient.Ingestion.IgnoreTransaction(ctx, reconciliationContext.ID, txToIgnore.ID, client.IgnoreTransactionRequest{
						Reason: "stresser test - ignore for coverage",
					}); err != nil {
						endpointFailures["IgnoreTransaction"] = err.Error()
					} else {
						tc.Logf("  IgnoreTransaction: %s", txToIgnore.ID)
						enrichedEndpoints++
					}
				}
			}

			// ============================================================
			// STEP 13: Matching Operations
			// ============================================================
			tc.Logf("\n[STEP 13/22] Matching operations...")

			// List match runs
			if runs, err := apiClient.Matching.ListMatchRuns(ctx, reconciliationContext.ID); err != nil {
				endpointFailures["ListMatchRuns"] = err.Error()
			} else {
				tc.Logf("  ListMatchRuns: %d runs", len(runs))
				enrichedEndpoints++
			}

			// Manual match: pick 1 unmatched transaction from ledger + 1 from bank
			// ManualMatch requires transactions from at least 2 different sources.
			{
				var ledgerTxID, bankTxID string
				for _, tx := range allTransactions {
					if tx.Status == "UNMATCHED" && ledgerTxID == "" {
						ledgerTxID = tx.ID
						break
					}
				}

				bankTxns, bankTxErr := apiClient.Ingestion.ListTransactionsByJob(ctx, reconciliationContext.ID, bankJob.ID)
				if bankTxErr != nil {
					endpointFailures["ListTransactionsByJob_bank"] = bankTxErr.Error()
				} else {
					for _, tx := range bankTxns {
						if tx.Status == "UNMATCHED" && bankTxID == "" {
							bankTxID = tx.ID
							break
						}
					}
				}

				if ledgerTxID != "" && bankTxID != "" {
					if resp, err := apiClient.Matching.ManualMatch(ctx, reconciliationContext.ID, client.ManualMatchRequest{
						TransactionIDs: []string{ledgerTxID, bankTxID},
						Notes:          "stresser manual match test",
					}); err != nil {
						endpointFailures["ManualMatch"] = err.Error()
					} else {
						tc.Logf("  ManualMatch: group %s with %d items", resp.MatchGroup.ID, len(resp.MatchGroup.Items))
						enrichedEndpoints++
					}
				}
			}

			// Create an adjustment
			if len(allTransactions) > 0 {
				if _, err := apiClient.Matching.CreateAdjustment(ctx, reconciliationContext.ID, client.CreateAdjustmentRequest{
					Type:          "ROUNDING",
					Amount:        "1.50",
					Currency:      "USD",
					Direction:     "CREDIT",
					Reason:        "stresser_test",
					Description:   "Stresser adjustment for API coverage",
					TransactionID: allTransactions[0].ID,
				}); err != nil {
					endpointFailures["CreateAdjustment"] = err.Error()
				} else {
					tc.Logf("  CreateAdjustment: created for transaction %s", allTransactions[0].ID)
					enrichedEndpoints++
				}
			}

			// Unmatch a group (pick the last one to minimize impact)
			if len(matchGroups) > 0 {
				lastGroup := matchGroups[len(matchGroups)-1]
				if err := apiClient.Matching.UnmatchGroup(ctx, reconciliationContext.ID, lastGroup.ID, client.UnmatchRequest{
					Reason: "stresser unmatch test",
				}); err != nil {
					endpointFailures["UnmatchGroup"] = err.Error()
				} else {
					tc.Logf("  UnmatchGroup: unmatched group %s", lastGroup.ID)
					enrichedEndpoints++
				}
			}

			// ============================================================
			// STEP 14: Exception Lifecycle
			// ============================================================
			tc.Logf("\n[STEP 14/22] Exception lifecycle...")

			var exceptionIDs []string

			// List all exceptions
			if allExc, err := apiClient.Exception.ListExceptions(ctx, client.ExceptionListFilter{Limit: 100}); err != nil {
				endpointFailures["ListExceptions"] = err.Error()
			} else {
				tc.Logf("  ListExceptions: %d exceptions", len(allExc.Items))
				enrichedEndpoints++
			}

			// List open exceptions
			openExceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			if err != nil {
				endpointFailures["ListOpenExceptions"] = err.Error()
			} else {
				tc.Logf("  ListOpenExceptions: %d open exceptions", len(openExceptions.Items))
				enrichedEndpoints++
				for _, exc := range openExceptions.Items {
					exceptionIDs = append(exceptionIDs, exc.ID)
				}
				exceptionsFound = len(openExceptions.Items)
			}

			// Get details of first exception
			if len(exceptionIDs) > 0 {
				if exc, err := apiClient.Exception.GetException(ctx, exceptionIDs[0]); err != nil {
					endpointFailures["GetException"] = err.Error()
				} else {
					tc.Logf("  GetException: %s severity=%s status=%s", exc.ID, exc.Severity, exc.Status)
					enrichedEndpoints++
				}
			}

			// Force match the first exception
			if len(exceptionIDs) > 0 {
				if _, err := apiClient.Exception.ForceMatch(ctx, exceptionIDs[0], client.ForceMatchRequest{
					OverrideReason: "stresser test",
					Notes:          "Force matching for API coverage",
				}); err != nil {
					endpointFailures["ForceMatch"] = err.Error()
				} else {
					forceMatched++
					tc.Logf("  ForceMatch: %s", exceptionIDs[0])
					enrichedEndpoints++
				}
			}

			// Adjust entry on second exception
			if len(exceptionIDs) > 1 {
				if _, err := apiClient.Exception.AdjustEntry(ctx, exceptionIDs[1], client.AdjustEntryRequest{
					ReasonCode:  "AMOUNT_CORRECTION",
					Notes:       "Stresser adjustment entry",
					Amount:      decimal.RequireFromString("0.50"),
					Currency:    "USD",
					EffectiveAt: time.Now().UTC(),
				}); err != nil {
					endpointFailures["AdjustEntry"] = err.Error()
				} else {
					adjustedEntries++
					tc.Logf("  AdjustEntry: %s", exceptionIDs[1])
					enrichedEndpoints++
				}
			}

			// Dispatch third exception to external system
			if len(exceptionIDs) > 2 {
				if _, err := apiClient.Exception.DispatchToExternal(ctx, exceptionIDs[2], client.DispatchRequest{
					TargetSystem: "jira",
					Queue:        "RECON-TEAM",
				}); err != nil {
					endpointFailures["DispatchToExternal"] = err.Error()
				} else {
					dispatched++
					tc.Logf("  DispatchToExternal: %s", exceptionIDs[2])
					enrichedEndpoints++
				}
			}

			// Get history for first exception
			if len(exceptionIDs) > 0 {
				if history, err := apiClient.Exception.GetExceptionHistory(ctx, exceptionIDs[0], "", 50); err != nil {
					endpointFailures["GetExceptionHistory"] = err.Error()
				} else {
					tc.Logf("  GetExceptionHistory: %d events", len(history.Items))
					enrichedEndpoints++
				}
			}

			// ============================================================
			// STEP 15: Exception Comments
			// ============================================================
			tc.Logf("\n[STEP 15/22] Exception comments...")

			if len(exceptionIDs) > 3 {
				excID := exceptionIDs[3]

				// Add a comment
				comment, err := apiClient.Exception.AddComment(ctx, excID, client.AddCommentRequest{
					Content: "Stresser test comment - investigating this exception",
				})
				if err != nil {
					endpointFailures["AddComment"] = err.Error()
				} else {
					commentsAdded++
					tc.Logf("  AddComment: %s on exception %s", comment.ID, excID)
					enrichedEndpoints++

					// List comments
					if comments, err := apiClient.Exception.ListComments(ctx, excID); err != nil {
						endpointFailures["ListComments"] = err.Error()
					} else {
						tc.Logf("  ListComments: %d comments", len(comments.Items))
						enrichedEndpoints++
					}

					// Delete the comment
					if err := apiClient.Exception.DeleteComment(ctx, excID, comment.ID); err != nil {
						endpointFailures["DeleteComment"] = err.Error()
					} else {
						commentsDeleted++
						tc.Logf("  DeleteComment: %s", comment.ID)
						enrichedEndpoints++
					}
				}
			} else {
				tc.Logf("  SKIP: Not enough exceptions for comments (need >3, have %d)", len(exceptionIDs))
			}

			// ============================================================
			// STEP 16: Bulk Exception Operations
			// ============================================================
			tc.Logf("\n[STEP 16/22] Bulk exception operations...")

			// Use remaining exception IDs for bulk operations (skip first 4 used above).
			// The >= 4 guard ensures we never reuse IDs consumed by Steps 14-15.
			bulkIDs := exceptionIDs
			if len(bulkIDs) >= 4 {
				bulkIDs = bulkIDs[4:]
			} else {
				bulkIDs = nil
			}

			if len(bulkIDs) >= 3 {
				bulkN := len(bulkIDs)
				third := bulkN / 3

				// Bulk assign first third
				assignBatch := bulkIDs[:min(third, 10)]
				if len(assignBatch) > 0 {
					if resp, err := apiClient.Exception.BulkAssign(ctx, client.BulkAssignRequest{
						ExceptionIDs: assignBatch,
						Assignee:     "stresser-test@example.com",
					}); err != nil {
						endpointFailures["BulkAssign"] = err.Error()
					} else {
						bulkAssigned = len(resp.Succeeded)
						tc.Logf("  BulkAssign: %d succeeded, %d failed", len(resp.Succeeded), len(resp.Failed))
						enrichedEndpoints++
					}
				}

				// Bulk resolve second third
				resolveBatch := bulkIDs[third:min(2*third, bulkN)]
				if len(resolveBatch) > 10 {
					resolveBatch = resolveBatch[:10]
				}
				if len(resolveBatch) > 0 {
					if resp, err := apiClient.Exception.BulkResolve(ctx, client.BulkResolveRequest{
						ExceptionIDs: resolveBatch,
						Resolution:   "ACCEPTED",
						Reason:       "Stresser bulk resolve test",
					}); err != nil {
						endpointFailures["BulkResolve"] = err.Error()
					} else {
						bulkResolved = len(resp.Succeeded)
						tc.Logf("  BulkResolve: %d succeeded, %d failed", len(resp.Succeeded), len(resp.Failed))
						enrichedEndpoints++
					}
				}

				// Bulk dispatch final third
				dispatchBatch := bulkIDs[2*third:]
				if len(dispatchBatch) > 10 {
					dispatchBatch = dispatchBatch[:10]
				}
				if len(dispatchBatch) > 0 {
					if resp, err := apiClient.Exception.BulkDispatch(ctx, client.BulkDispatchRequest{
						ExceptionIDs: dispatchBatch,
						TargetSystem: "jira",
						Queue:        "BULK-TEST",
					}); err != nil {
						endpointFailures["BulkDispatch"] = err.Error()
					} else {
						bulkDispatched = len(resp.Succeeded)
						tc.Logf("  BulkDispatch: %d succeeded, %d failed", len(resp.Succeeded), len(resp.Failed))
						enrichedEndpoints++
					}
				}
			} else {
				tc.Logf("  SKIP: Not enough exceptions for bulk ops (need >=3, have %d)", len(bulkIDs))
			}

			// ============================================================
			// STEP 17: Dispute Lifecycle
			// ============================================================
			tc.Logf("\n[STEP 17/22] Dispute lifecycle...")

			// Use exception IDs not touched by prior steps
			// Steps 14 used [0:4], Step 15 used [3], Step 16 used [4:~37]
			// Pick from index 50+ to ensure fresh OPEN exceptions
			var disputeExcIDs []string
			if len(exceptionIDs) > 52 {
				disputeExcIDs = exceptionIDs[50:52]
			} else if len(exceptionIDs) > 42 {
				disputeExcIDs = exceptionIDs[40:42]
			}

			var disputeIDs []string
			for i, excID := range disputeExcIDs {
				dispute, err := apiClient.Exception.OpenDispute(ctx, excID, client.OpenDisputeRequest{
					Category:    "AMOUNT_MISMATCH",
					Description: fmt.Sprintf("Stresser dispute #%d", i),
				})
				if err != nil {
					endpointFailures["OpenDispute"] = err.Error()
					continue
				}
				disputesOpened++
				disputeIDs = append(disputeIDs, dispute.ID)
				tc.Logf("  OpenDispute: %s on exception %s", dispute.ID, excID)
				enrichedEndpoints++

				// Submit evidence
				if _, err := apiClient.Exception.SubmitEvidence(ctx, dispute.ID, client.SubmitEvidenceRequest{
					Comment: fmt.Sprintf("Evidence for dispute #%d - bank statement attached", i),
				}); err != nil {
					endpointFailures["SubmitEvidence"] = err.Error()
				} else {
					tc.Logf("  SubmitEvidence: added to dispute %s", dispute.ID)
					enrichedEndpoints++
				}
			}

			// List disputes
			if disputeList, err := apiClient.Exception.ListDisputes(ctx); err != nil {
				endpointFailures["ListDisputes"] = err.Error()
			} else {
				tc.Logf("  ListDisputes: %d disputes", len(disputeList.Items))
				enrichedEndpoints++
			}

			// Get first dispute detail
			if len(disputeIDs) > 0 {
				if d, err := apiClient.Exception.GetDispute(ctx, disputeIDs[0]); err != nil {
					endpointFailures["GetDispute"] = err.Error()
				} else {
					tc.Logf("  GetDispute: %s state=%s", d.ID, d.State)
					enrichedEndpoints++
				}
			}

			// Close first dispute
			if len(disputeIDs) > 0 {
				won := true
				if _, err := apiClient.Exception.CloseDispute(ctx, disputeIDs[0], client.CloseDisputeRequest{
					Resolution: "Amount confirmed correct after review",
					Won:        &won,
				}); err != nil {
					endpointFailures["CloseDispute"] = err.Error()
				} else {
					disputesClosed++
					tc.Logf("  CloseDispute: %s (won)", disputeIDs[0])
					enrichedEndpoints++
				}
			}

			for i := 1; i < len(disputeIDs); i++ {
				lost := false
				if _, err := apiClient.Exception.CloseDispute(ctx, disputeIDs[i], client.CloseDisputeRequest{
					Resolution: "Auto-closed for test cleanup",
					Won:        &lost,
				}); err != nil {
					endpointFailures[fmt.Sprintf("CloseDispute_cleanup_%d_%s", i, disputeIDs[i])] = err.Error()
				} else {
					disputesClosed++
					tc.Logf("  CloseDispute: %s (lost, cleanup)", disputeIDs[i])
				}
			}

			// ============================================================
			// STEP 18: Reporting Deep Dive
			// ============================================================
			tc.Logf("\n[STEP 18/22] Reporting deep dive...")

			if vol, err := apiClient.Reporting.GetVolumeStats(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["GetVolumeStats"] = err.Error()
			} else {
				tc.Logf("  GetVolumeStats: period=%s, totalVolume=%s", vol.Period, vol.TotalVolume)
				enrichedEndpoints++
			}

			if mr, err := apiClient.Reporting.GetMatchRateStats(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["GetMatchRateStats"] = err.Error()
			} else {
				tc.Logf("  GetMatchRateStats: rate=%.2f%%", mr.MatchRate)
				enrichedEndpoints++
			}

			if sla, err := apiClient.Reporting.GetSLAStats(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["GetSLAStats"] = err.Error()
			} else {
				tc.Logf("  GetSLAStats: compliance=%.2f%%", sla.SLAComplianceRate)
				enrichedEndpoints++
			}

			if metrics, err := apiClient.Reporting.GetDashboardMetrics(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["GetDashboardMetrics"] = err.Error()
			} else {
				tc.Logf("  GetDashboardMetrics: updatedAt=%s", metrics.UpdatedAt)
				enrichedEndpoints++
			}

			if sb, err := apiClient.Reporting.GetSourceBreakdown(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["GetSourceBreakdown"] = err.Error()
			} else {
				tc.Logf("  GetSourceBreakdown: %d sources", len(sb.Items))
				enrichedEndpoints++
			}

			if ci, err := apiClient.Reporting.GetCashImpact(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["GetCashImpact"] = err.Error()
			} else {
				tc.Logf("  GetCashImpact: total=%s, %d currencies", ci.TotalUnreconciledAmount, len(ci.CurrencyExposures))
				enrichedEndpoints++
			}

			if txCount, err := apiClient.Reporting.CountTransactions(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["CountTransactions"] = err.Error()
			} else {
				tc.Logf("  CountTransactions: %d", txCount.Count)
				enrichedEndpoints++
			}

			if mCount, err := apiClient.Reporting.CountMatches(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["CountMatches"] = err.Error()
			} else {
				tc.Logf("  CountMatches: %d", mCount.Count)
				enrichedEndpoints++
			}

			if eCount, err := apiClient.Reporting.CountExceptions(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["CountExceptions"] = err.Error()
			} else {
				tc.Logf("  CountExceptions: %d", eCount.Count)
				enrichedEndpoints++
			}

			// ============================================================
			// STEP 19: Export Pipeline
			// ============================================================
			tc.Logf("\n[STEP 19/22] Export pipeline...")

			// Sync exports
			if data, err := apiClient.Reporting.ExportMatchedReport(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["ExportMatchedReport"] = err.Error()
			} else {
				syncReports++
				tc.Logf("  ExportMatchedReport: %d bytes", len(data))
				enrichedEndpoints++
			}

			if data, err := apiClient.Reporting.ExportUnmatchedReport(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["ExportUnmatchedReport"] = err.Error()
			} else {
				syncReports++
				tc.Logf("  ExportUnmatchedReport: %d bytes", len(data))
				enrichedEndpoints++
			}

			if data, err := apiClient.Reporting.ExportSummaryReport(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["ExportSummaryReport"] = err.Error()
			} else {
				syncReports++
				tc.Logf("  ExportSummaryReport: %d bytes", len(data))
				enrichedEndpoints++
			}

			if data, err := apiClient.Reporting.ExportVarianceReport(ctx, reconciliationContext.ID, dateFrom, dateTo); err != nil {
				endpointFailures["ExportVarianceReport"] = err.Error()
			} else {
				syncReports++
				tc.Logf("  ExportVarianceReport: %d bytes", len(data))
				enrichedEndpoints++
			}

			// Async export job
			if exportJob, err := apiClient.Reporting.CreateExportJob(ctx, reconciliationContext.ID, client.CreateExportJobRequest{
				ReportType: "MATCHED",
				Format:     "csv",
				DateFrom:   dateFrom,
				DateTo:     dateTo,
			}); err != nil {
				endpointFailures["CreateExportJob"] = err.Error()
			} else {
				asyncJobs++
				tc.Logf("  CreateExportJob: %s status=%s", exportJob.JobID, exportJob.Status)
				enrichedEndpoints++

				// Wait for completion and try download
				if err := e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID); err != nil {
					endpointFailures["WaitForExportJobComplete"] = err.Error()
				} else {
					if job, err := apiClient.Reporting.GetExportJob(ctx, exportJob.JobID); err != nil {
						endpointFailures["GetExportJob"] = err.Error()
					} else {
						tc.Logf("  GetExportJob: status=%s records=%d", job.Status, job.RecordsWritten)
						enrichedEndpoints++
					}

					if data, err := apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID); err != nil {
						endpointFailures["DownloadExportJob"] = err.Error()
					} else {
						tc.Logf("  DownloadExportJob: %d bytes", len(data))
						enrichedEndpoints++
					}
				}
			}

			// List export jobs
			if jobs, err := apiClient.Reporting.ListExportJobs(ctx); err != nil {
				endpointFailures["ListExportJobs"] = err.Error()
			} else {
				tc.Logf("  ListExportJobs: %d jobs", len(jobs))
				enrichedEndpoints++
			}

			// ============================================================
			// STEP 20: Governance Audit
			// ============================================================
			tc.Logf("\n[STEP 20/22] Governance audit...")

			// Wait for audit logs to propagate (async outbox)
			if logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "context", reconciliationContext.ID, 1); err != nil {
				endpointFailures["WaitForAuditLogs"] = err.Error()
			} else {
				auditLogsFound = len(logs)
				tc.Logf("  ListAuditLogsByEntity: %d logs for context", len(logs))
				enrichedEndpoints++

				// Get first audit log detail
				if len(logs) > 0 {
					if log, err := apiClient.Governance.GetAuditLog(ctx, logs[0].ID); err != nil {
						endpointFailures["GetAuditLog"] = err.Error()
					} else {
						tc.Logf("  GetAuditLog: %s action=%s entity=%s", log.ID, log.Action, log.EntityType)
						enrichedEndpoints++
					}
				}
			}

			// List by entity type
			if logs, err := apiClient.Governance.ListAuditLogsByEntityType(ctx, "context"); err != nil {
				endpointFailures["ListAuditLogsByEntityType"] = err.Error()
			} else {
				tc.Logf("  ListAuditLogsByEntityType: %d context logs", len(logs))
				enrichedEndpoints++
			}

			// List by action
			if logs, err := apiClient.Governance.ListAuditLogsByAction(ctx, "CREATE"); err != nil {
				endpointFailures["ListAuditLogsByAction"] = err.Error()
			} else {
				tc.Logf("  ListAuditLogsByAction(CREATE): %d logs", len(logs))
				enrichedEndpoints++
			}

			// List archives
			if archives, err := apiClient.Governance.ListArchives(ctx); err != nil {
				endpointFailures["ListArchives"] = err.Error()
			} else {
				tc.Logf("  ListArchives: %d archives", len(archives))
				enrichedEndpoints++
			}

			// ============================================================
			// STEP 21: Clone Context
			// ============================================================
			tc.Logf("\n[STEP 21/22] Clone context...")

			if cloneResp, err := apiClient.Configuration.CloneContext(ctx, reconciliationContext.ID, client.CloneContextRequest{
				Name: tc.UniqueName("stresser-clone"),
			}); err != nil {
				endpointFailures["CloneContext"] = err.Error()
			} else {
				clonedContextID = cloneResp.Context.ID
				tc.Logf("  CloneContext: %s (sources=%d, rules=%d, fieldMaps=%d)",
					cloneResp.Context.ID, cloneResp.SourcesCloned, cloneResp.RulesCloned, cloneResp.FieldMapsCloned)
				enrichedEndpoints++

				// Clean up cloned context (must delete children first: field maps → rules + sources → context)
				if !shouldSkipCleanup() {
					deleteClonedContext(ctx, tc, apiClient, clonedContextID, endpointFailures, &enrichedEndpoints)
				}
			}

			// ============================================================
			// STEP 22: Fee Schedule CRUD + Simulation
			// ============================================================
			tc.Logf("\n[STEP 22/22] Fee schedule CRUD + simulation...")
			feeScheduleName := tc.UniqueName("stresser-fee-schedule")

			feeSchedule, err := apiClient.FeeSchedule.CreateFeeSchedule(ctx, client.CreateFeeScheduleRequest{
				Name:             feeScheduleName,
				Currency:         "USD",
				ApplicationOrder: "PARALLEL",
				RoundingScale:    2,
				RoundingMode:     "HALF_UP",
				Items: []client.CreateFeeScheduleItemRequest{
					{
						Name:          "Processing Fee",
						Priority:      1,
						StructureType: "FLAT",
						Structure:     map[string]any{"amount": "2.50"},
					},
				},
			})
			if err != nil {
				endpointFailures["CreateFeeSchedule"] = err.Error()
			} else {
				tc.Logf("  CreateFeeSchedule: %s", feeSchedule.ID)
				enrichedEndpoints++

				if got, err := apiClient.FeeSchedule.GetFeeSchedule(ctx, feeSchedule.ID); err != nil {
					endpointFailures["GetFeeSchedule"] = err.Error()
				} else {
					tc.Logf("  GetFeeSchedule: %s currency=%s", got.Name, got.Currency)
					enrichedEndpoints++
				}

				if list, err := apiClient.FeeSchedule.ListFeeSchedules(ctx); err != nil {
					endpointFailures["ListFeeSchedules"] = err.Error()
				} else {
					tc.Logf("  ListFeeSchedules: %d schedules", len(list))
					enrichedEndpoints++
				}

				newName := tc.UniqueName("stresser-fee-updated")
				if _, err := apiClient.FeeSchedule.UpdateFeeSchedule(ctx, feeSchedule.ID, client.UpdateFeeScheduleRequest{
					Name: &newName,
				}); err != nil {
					endpointFailures["UpdateFeeSchedule"] = err.Error()
				} else {
					tc.Logf("  UpdateFeeSchedule: renamed to %s", newName)
					enrichedEndpoints++
				}

				if sim, err := apiClient.FeeSchedule.SimulateFeeSchedule(ctx, feeSchedule.ID, client.SimulateFeeRequest{
					GrossAmount: "1000.00",
					Currency:    "USD",
				}); err != nil {
					endpointFailures["SimulateFeeSchedule"] = err.Error()
				} else {
					feeScheduleSimulated = true
					tc.Logf("  SimulateFeeSchedule: gross=%s net=%s fee=%s", sim.GrossAmount, sim.NetAmount, sim.TotalFee)
					enrichedEndpoints++
				}

				if !shouldSkipCleanup() {
					if err := apiClient.FeeSchedule.DeleteFeeSchedule(ctx, feeSchedule.ID); err != nil {
						endpointFailures["DeleteFeeSchedule"] = err.Error()
					} else {
						tc.Logf("  DeleteFeeSchedule: removed %s", feeSchedule.ID)
						enrichedEndpoints++
					}
				} else {
					tc.Logf("  DeleteFeeSchedule: SKIPPED (E2E_KEEP_DATA set, preserving %s)", feeSchedule.ID)
				}
			}

			// ── Endpoint Failure Evaluation ──────────────────────────────────────
			tc.Logf("\n" + repeatStr("─", 60))
			tc.Logf("ENDPOINT FAILURE ANALYSIS")
			tc.Logf(repeatStr("─", 60))

			var unexpectedFailures []string
			var knownHits []string
			var fixedKnown []string

			for name, errMsg := range endpointFailures {
				if reason, known := knownFailures[name]; known {
					knownHits = append(knownHits, fmt.Sprintf("  KNOWN  %-30s → %s", name, reason))
				} else {
					unexpectedFailures = append(unexpectedFailures, fmt.Sprintf("  FAIL   %-30s → %s", name, errMsg))
				}
			}

			// Detect fixed known failures (were in allowlist but now pass)
			for name := range knownFailures {
				if _, failed := endpointFailures[name]; !failed {
					fixedKnown = append(fixedKnown, name)
				}
			}

			if len(knownHits) > 0 {
				tc.Logf("\nKnown failures (%d):", len(knownHits))
				for _, line := range knownHits {
					tc.Logf("%s", line)
				}
			}

			if len(fixedKnown) > 0 {
				tc.Logf("\nPreviously known failures now PASSING (%d) — remove from knownFailures:", len(fixedKnown))
				for _, name := range fixedKnown {
					tc.Logf("  FIXED  %s", name)
				}
			}

			if len(unexpectedFailures) > 0 {
				tc.Logf("\nUnexpected failures (%d):", len(unexpectedFailures))
				for _, line := range unexpectedFailures {
					tc.Logf("%s", line)
				}
			}

			tc.Logf("\nEndpoint results: %d passed, %d known failures, %d fixed, %d unexpected",
				enrichedEndpoints, len(knownHits), len(fixedKnown), len(unexpectedFailures))
			tc.Logf(repeatStr("─", 60))

			// Fail the test if any unexpected failures occurred
			if len(unexpectedFailures) > 0 {
				t.Errorf("%d unexpected endpoint failure(s) detected:\n%s",
					len(unexpectedFailures), strings.Join(unexpectedFailures, "\n"))
			}

			// ============================================================
			// ENRICHED SUMMARY
			// ============================================================
			tc.Logf("\n" + repeatStr("=", 60))
			tc.Logf("ENRICHED HIGH VOLUME DASHBOARD SUMMARY")
			tc.Logf(repeatStr("=", 60))
			tc.Logf("Context Name: %s", reconciliationContext.Name)
			tc.Logf("Context ID:   %s", reconciliationContext.ID)

			if dashboard.Volume != nil {
				tc.Logf("\nVolume Stats:")
				tc.Logf("  Total Transactions:   %d", dashboard.Volume.TotalTransactions)
				tc.Logf("  Matched Transactions: %d", dashboard.Volume.MatchedTransactions)
				tc.Logf("  Unmatched Count:      %d", dashboard.Volume.UnmatchedCount)
				tc.Logf("  Total Amount:         %s", dashboard.Volume.TotalAmount)
				tc.Logf("  Matched Amount:       %s", dashboard.Volume.MatchedAmount)
				tc.Logf("  Unmatched Amount:     %s", dashboard.Volume.UnmatchedAmount)
			}

			if dashboard.MatchRate != nil {
				tc.Logf("\nMatch Rate Stats:")
				tc.Logf("  Match Rate:           %.2f%%", dashboard.MatchRate.MatchRate)
				tc.Logf("  Match Rate by Amount: %.2f%%", dashboard.MatchRate.MatchRateAmount)
			}

			tc.Logf("\nEnriched API Coverage:")
			tc.Logf("  Enriched endpoints exercised: %d", enrichedEndpoints)
			tc.Logf("  Match groups:                 %d", len(matchGroups))
			tc.Logf("  Schedule created:             %v", scheduleCreated)
			tc.Logf("  Exceptions found:             %d", exceptionsFound)
			tc.Logf("  Force matched:                %d", forceMatched)
			tc.Logf("  Adjusted entries:             %d", adjustedEntries)
			tc.Logf("  Dispatched:                   %d", dispatched)
			tc.Logf("  Comments added/deleted:       %d/%d", commentsAdded, commentsDeleted)
			tc.Logf("  Bulk assigned/resolved/dispatched: %d/%d/%d", bulkAssigned, bulkResolved, bulkDispatched)
			tc.Logf("  Disputes opened/closed:       %d/%d", disputesOpened, disputesClosed)
			tc.Logf("  Sync reports:                 %d", syncReports)
			tc.Logf("  Async export jobs:            %d", asyncJobs)
			tc.Logf("  Audit logs found:             %d", auditLogsFound)
			tc.Logf("  Search results:               %d", searchResultsFound)
			tc.Logf("  Fee schedule simulated:       %v", feeScheduleSimulated)
			if clonedContextID != "" {
				tc.Logf("  Cloned context:               %s", clonedContextID)
			}

			tc.Logf("\n" + repeatStr("=", 60))
			if shouldSkipCleanup() {
				tc.Logf("Data preserved! View dashboard at context: %s", reconciliationContext.ID)
				tc.Logf(repeatStr("=", 60))
			} else {
				tc.Logf("Test completed (data will be cleaned up)")
				tc.Logf(repeatStr("=", 60))
			}
		},
	)
}

// createContextWithoutCleanup creates a context without registering cleanup when E2E_KEEP_DATA is set.
// The context is always activated regardless of skipCleanup, since downstream operations
// (ingestion, matching, reporting) require ACTIVE status.
func createContextWithoutCleanup(
	ctx context.Context,
	f *factories.Factories,
	name string,
	skipCleanup bool,
) *client.Context {
	builder := f.Context.NewContext().WithName(name).OneToOne()
	if skipCleanup {
		// Use direct API call without registering cleanup
		created, err := f.Context.Client().Configuration.CreateContext(
			ctx,
			client.CreateContextRequest{
				Name:     builder.GetRequest().Name,
				Type:     "1:1",
				Interval: "0 0 * * *",
			},
		)
		if err != nil {
			panic(err)
		}

		// Activate the context — required for ingestion/matching/reporting verifiers.
		activeStatus := "ACTIVE"
		activated, err := f.Context.Client().Configuration.UpdateContext(
			ctx, created.ID, client.UpdateContextRequest{Status: &activeStatus},
		)
		if err != nil {
			panic(fmt.Errorf("activate context %s: %w", created.ID, err))
		}

		return activated
	}
	return builder.MustCreate(ctx)
}

// Helper to create source without cleanup when E2E_KEEP_DATA is set
func createSourceWithoutCleanup(
	ctx context.Context,
	f *factories.Factories,
	contextID, name, sourceType string,
	skipCleanup bool,
) *client.Source {
	builder := f.Source.NewSource(contextID).WithName(name).WithType(sourceType)
	if skipCleanup {
		created, err := f.Source.Client().Configuration.CreateSource(
			ctx,
			contextID,
			client.CreateSourceRequest{
				Name: builder.GetRequest().Name,
				Type: sourceType,
			},
		)
		if err != nil {
			panic(err)
		}
		return created
	}
	return builder.MustCreate(ctx)
}

// repeatStr repeats a string n times
func repeatStr(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}

// TestDashboardStresser_QuickRun is a faster version with fewer transactions.
func TestDashboardStresser_QuickRun(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			cfg := DashboardStresserConfig{
				Seed:                123,
				PerfectMatchCount:   30,
				ToleranceMatchCount: 10,
				DateLagMatchCount:   5,
				UnmatchedCount:      15,
				MultiSourceCount:    5,
				ToleranceAmount:     "2.00",
				PercentTolerance:    1.0,
				DateLagMinDays:      1,
				DateLagMaxDays:      2,
				Currencies:          []string{"USD", "EUR"},
				DateRangeDays:       14,
			}
			rng := seededRand(cfg.Seed)
			f := factories.New(tc, apiClient)

			tc.Logf("Dashboard Stresser (Quick Run) - Seed: %d", cfg.Seed)

			// Create context
			reconciliationContext := f.Context.NewContext().
				WithName("dashboard-quick").
				OneToOne().
				MustCreate(ctx)

			// Create sources
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			// Create field maps
			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			// Create rules
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

			// Generate data
			baseDate := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			txGen := newTransactionGenerator(tc.NamePrefix(), rng, cfg, baseDate)

			ledgerBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			bankBuilder := factories.NewCSVBuilder(tc.NamePrefix())

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

			for i := 0; i < cfg.UnmatchedCount; i++ {
				tx := txGen.unmatched(i)
				if tx.isLedgerOnly {
					ledgerBuilder.AddRowRaw(tx.id, tx.amount, tx.currency, tx.date, tx.description)
				} else {
					bankBuilder.AddRowRaw(tx.id, tx.amount, tx.currency, tx.date, tx.description)
				}
			}

			// Ingest
			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"l.csv",
				ledgerBuilder.Build(),
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
				"b.csv",
				bankBuilder.Build(),
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, bankJob.ID),
			)

			// Match
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

			// Verify
			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)

			tc.Logf(
				"✓ Quick run completed: %d match groups, Context: %s",
				len(groups),
				reconciliationContext.ID,
			)
		},
	)
}

// ============================================================
// Transaction Generator
// ============================================================

type transactionGenerator struct {
	prefix   string
	rng      *rand.Rand
	cfg      DashboardStresserConfig
	baseDate time.Time
}

func newTransactionGenerator(
	prefix string,
	rng *rand.Rand,
	cfg DashboardStresserConfig,
	baseDate time.Time,
) *transactionGenerator {
	return &transactionGenerator{
		prefix:   prefix,
		rng:      rng,
		cfg:      cfg,
		baseDate: baseDate,
	}
}

type perfectMatchTx struct {
	ledgerID    string
	bankID      string
	amount      string
	currency    string
	date        string
	description string
}

func (g *transactionGenerator) perfectMatch(index int) perfectMatchTx {
	amount := 100.0 + g.rng.Float64()*9900.0 // $100 - $10,000
	currency := g.cfg.Currencies[g.rng.Intn(len(g.cfg.Currencies))]
	daysOffset := g.rng.Intn(g.cfg.DateRangeDays)
	date := g.baseDate.AddDate(0, 0, daysOffset).Format("2006-01-02")

	id := fmt.Sprintf("%s-PM-%04d", g.prefix, index)

	return perfectMatchTx{
		ledgerID:    id,
		bankID:      id,
		amount:      fmt.Sprintf("%.2f", amount),
		currency:    currency,
		date:        date,
		description: fmt.Sprintf("perfect match %d", index),
	}
}

type toleranceMatchTx struct {
	ledgerID     string
	bankID       string
	ledgerAmount string
	bankAmount   string
	currency     string
	date         string
	description  string
}

func (g *transactionGenerator) toleranceMatch(index int) toleranceMatchTx {
	amount := 100.0 + g.rng.Float64()*9900.0
	// Add variance within tolerance (up to configured amount)
	variance := g.rng.Float64() * 4.99 // Within $5 tolerance
	if g.rng.Intn(2) == 0 {
		variance = -variance
	}
	currency := g.cfg.Currencies[g.rng.Intn(len(g.cfg.Currencies))]
	daysOffset := g.rng.Intn(g.cfg.DateRangeDays)
	date := g.baseDate.AddDate(0, 0, daysOffset).Format("2006-01-02")

	id := fmt.Sprintf("%s-TM-%04d", g.prefix, index)

	return toleranceMatchTx{
		ledgerID:     id,
		bankID:       id,
		ledgerAmount: fmt.Sprintf("%.2f", amount),
		bankAmount:   fmt.Sprintf("%.2f", amount+variance),
		currency:     currency,
		date:         date,
		description:  fmt.Sprintf("tolerance match %d (variance: %.2f)", index, variance),
	}
}

type dateLagMatchTx struct {
	ledgerID    string
	bankID      string
	amount      string
	currency    string
	ledgerDate  string
	bankDate    string
	description string
}

func (g *transactionGenerator) dateLagMatch(index int) dateLagMatchTx {
	amount := 100.0 + g.rng.Float64()*9900.0
	currency := g.cfg.Currencies[g.rng.Intn(len(g.cfg.Currencies))]
	daysOffset := g.rng.Intn(g.cfg.DateRangeDays - g.cfg.DateLagMaxDays)
	ledgerDate := g.baseDate.AddDate(0, 0, daysOffset)
	// Bank date is 1-3 days later
	lagDays := g.cfg.DateLagMinDays + g.rng.Intn(g.cfg.DateLagMaxDays-g.cfg.DateLagMinDays+1)
	bankDate := ledgerDate.AddDate(0, 0, lagDays)

	id := fmt.Sprintf("%s-DL-%04d", g.prefix, index)

	return dateLagMatchTx{
		ledgerID:    id,
		bankID:      id,
		amount:      fmt.Sprintf("%.2f", amount),
		currency:    currency,
		ledgerDate:  ledgerDate.Format("2006-01-02"),
		bankDate:    bankDate.Format("2006-01-02"),
		description: fmt.Sprintf("date lag match %d (lag: %d days)", index, lagDays),
	}
}

type unmatchedTx struct {
	id           string
	amount       string
	currency     string
	date         string
	description  string
	isLedgerOnly bool
}

func (g *transactionGenerator) unmatched(index int) unmatchedTx {
	amount := 50.0 + g.rng.Float64()*5000.0
	currency := g.cfg.Currencies[g.rng.Intn(len(g.cfg.Currencies))]
	daysOffset := g.rng.Intn(g.cfg.DateRangeDays)
	date := g.baseDate.AddDate(0, 0, daysOffset).Format("2006-01-02")
	isLedgerOnly := g.rng.Intn(2) == 0

	var prefix string
	if isLedgerOnly {
		prefix = "UL" // Unmatched Ledger
	} else {
		prefix = "UB" // Unmatched Bank
	}

	return unmatchedTx{
		id:           fmt.Sprintf("%s-%s-%04d", g.prefix, prefix, index),
		amount:       fmt.Sprintf("%.2f", amount),
		currency:     currency,
		date:         date,
		description:  fmt.Sprintf("unmatched %s %d", prefix, index),
		isLedgerOnly: isLedgerOnly,
	}
}

type multiSourceTx struct {
	ledgerID    string
	bankID      string
	gatewayID   string
	amount      string
	currency    string
	date        string
	description string
}

func (g *transactionGenerator) multiSource(index int) multiSourceTx {
	amount := 500.0 + g.rng.Float64()*10000.0
	currency := g.cfg.Currencies[g.rng.Intn(len(g.cfg.Currencies))]
	daysOffset := g.rng.Intn(g.cfg.DateRangeDays)
	date := g.baseDate.AddDate(0, 0, daysOffset).Format("2006-01-02")

	id := fmt.Sprintf("%s-MS-%04d", g.prefix, index)

	return multiSourceTx{
		ledgerID:    id,
		bankID:      id,
		gatewayID:   id,
		amount:      fmt.Sprintf("%.2f", amount),
		currency:    currency,
		date:        date,
		description: fmt.Sprintf("multi-source %d", index),
	}
}

// ============================================================
// Helper Functions
// ============================================================

func analyzeMatchGroups(groups []client.MatchGroup) map[string]int {
	stats := make(map[string]int)
	for _, g := range groups {
		stats[g.RuleID]++
	}
	return stats
}

// strPtr returns a pointer to the given string.
func strPtr(s string) *string {
	return &s
}
