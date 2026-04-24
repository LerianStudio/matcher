//go:build e2e

package journeys

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// shouldSkipCleanup returns true if E2E_KEEP_DATA env var is set.
func shouldSkipCleanup() bool {
	return os.Getenv("E2E_KEEP_DATA") != ""
}

// cleanupContextChildren deletes all children of a context in the correct dependency order:
// field maps → fee rules → match rules → sources. All errors are logged as warnings but do not
// halt the cascade, ensuring best-effort cleanup even when individual deletions fail.
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

	// 2. Delete fee rules
	feeRules, feeRulesErr := apiClient.Configuration.ListFeeRules(ctx, contextID)
	if feeRulesErr != nil {
		tc.Logf("  warn: failed to list fee rules for cleanup: %v", feeRulesErr)
	}

	if feeRules != nil {
		for _, fr := range feeRules {
			if delErr := apiClient.Configuration.DeleteFeeRule(ctx, fr.ID); delErr != nil {
				tc.Logf("  warn: failed to delete fee rule %s: %v", fr.ID, delErr)
			}
		}
	}

	// 3. Delete match rules
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

	// 4. Delete sources (after field maps and rules are removed)
	if sources != nil {
		for _, src := range sources {
			if delErr := apiClient.Configuration.DeleteSource(ctx, contextID, src.ID); delErr != nil {
				tc.Logf("  warn: failed to delete source %s: %v", src.ID, delErr)
			}
		}
	}
}

// deleteClonedContext deletes a cloned context and all its children in the correct order:
// field maps → fee rules → match rules → sources → context.
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

// knownFailures documents endpoints expected to fail and why.
// Entries prefixed with "env:" are environment-dependent (no infra configured).
// Entries prefixed with "bug:" are tracked backend issues to fix.
// When a known failure starts passing, the test will remind you to remove it.
var knownFailures = map[string]string{
	"BulkDispatch":       "bug: UUID validation issue",
	"DispatchToExternal": "env: no external dispatch target configured",
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
	builder := f.Context.NewContext().WithName(name).OneToMany()
	if skipCleanup {
		// Use direct API call without registering cleanup
		created, err := f.Context.Client().Configuration.CreateContext(
			ctx,
			client.CreateContextRequest{
				Name:     builder.GetRequest().Name,
				Type:     "1:N",
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

// Helper to create source without cleanup when E2E_KEEP_DATA is set.
// When skipCleanup is true we call the API directly (no cleanup registration)
// but still resolve the side through the builder so preserved-data runs send
// a valid LEFT/RIGHT value.
//
// GetRequest() now calls resolveSide() internally, which advances the
// per-context counter and ensures auto-assignment works correctly even
// when Create() is bypassed.
func createSourceWithoutCleanup(
	ctx context.Context,
	f *factories.Factories,
	contextID, name, sourceType, side string,
	skipCleanup bool,
) *client.Source {
	builder := f.Source.NewSource(contextID).WithName(name).WithType(sourceType)
	if side != "" {
		if side == "LEFT" {
			builder = builder.Left()
		} else {
			builder = builder.Right()
		}
	}
	if skipCleanup {
		// GetRequest resolves the side and advances the counter, so the
		// factory stays consistent even when we bypass Create().
		req := builder.GetRequest()
		created, err := f.Source.Client().Configuration.CreateSource(
			ctx,
			contextID,
			client.CreateSourceRequest{
				Name: req.Name,
				Type: sourceType,
				Side: req.Side,
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
