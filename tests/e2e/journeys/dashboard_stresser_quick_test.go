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

			// Verify
			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)

			// H20: Tighten assertions beyond `> 0`.
			//
			// With seed=123 the RNG is deterministic. The test creates:
			//   - 30 perfect match pairs  → 30 match groups (exact rule, priority 1)
			//   - 10 tolerance match pairs → up to 10 match groups (tolerance rule, priority 2)
			//   - 15 unmatched (single-sided, no matches expected)
			//   -  5 multi-source pairs   → not generated in quick test
			//
			// Perfect matches always hit the exact rule, so we expect at least 30 groups.
			// Some tolerance pairs might coincidentally land in the exact rule too.
			// The combined minimum is 30 (perfect) + some tolerance matches.
			expectedMinGroups := cfg.PerfectMatchCount // 30 perfect pairs guaranteed
			require.GreaterOrEqual(t, len(groups), expectedMinGroups,
				"seeded(123) quick run should produce at least %d match groups from %d perfect + %d tolerance pairs",
				expectedMinGroups, cfg.PerfectMatchCount, cfg.ToleranceMatchCount,
			)

			// Upper bound sanity: we can't match more pairs than we created.
			maxPossibleGroups := cfg.PerfectMatchCount + cfg.ToleranceMatchCount
			require.LessOrEqual(t, len(groups), maxPossibleGroups,
				"match groups should not exceed total matchable pairs (%d)", maxPossibleGroups,
			)

			tc.Logf(
				"✓ Quick run completed: %d match groups (expected range [%d, %d]), Context: %s",
				len(groups),
				expectedMinGroups,
				maxPossibleGroups,
				reconciliationContext.ID,
			)
		},
	)
}
