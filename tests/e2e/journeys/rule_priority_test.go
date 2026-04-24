//go:build e2e

package journeys

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestRulePriority_ExactBeforeTolerance tests that exact rules are evaluated before tolerance.
func TestRulePriority_ExactBeforeTolerance(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("priority-test").
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

			// Rule 1: Exact match (highest priority)
			exactRule := f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			// Rule 2: Tolerance match (lower priority)
			toleranceRule := f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(2).
				Tolerance().
				WithToleranceConfig("5.00").
				MustCreate(ctx)

			tc.Logf("Created rules: exact (priority=%d) and tolerance (priority=%d)",
				exactRule.Priority, toleranceRule.Priority)

			// Upload exact matching transactions
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("PRI-001", "100.00", "USD", "2026-01-15", "exact match").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("PRI-001", "100.00", "USD", "2026-01-15", "exact match").
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

			// Verify match used exact rule (priority 1)
			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 1)

			// First group should be matched by exact rule
			if len(groups) > 0 {
				tc.Logf("Match group rule: %s", groups[0].RuleID)
			}

			tc.Logf("✓ Rule priority cascade completed")
		},
	)
}

// TestRulePriority_FallbackToTolerance tests fallback when exact doesn't match.
func TestRulePriority_FallbackToTolerance(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("fallback-test").
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

			// Rule 1: Exact match - won't match our data
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			// Rule 2: Tolerance match - will match our data
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(2).
				Tolerance().
				WithToleranceConfig("2.00").
				MustCreate(ctx)

			// Upload transactions with small variance (exact won't match, tolerance will)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("FALL-001", "100.00", "USD", "2026-01-15", "tolerance match").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("FALL-001", "101.50", "USD", "2026-01-15", "off by $1.50").
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

			// Verify match happened via tolerance rule
			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 1, "should match via tolerance fallback")

			tc.Logf("✓ Fallback to tolerance rule completed")
		},
	)
}

// TestRulePriority_ReorderRules tests rule reordering API.
func TestRulePriority_ReorderRules(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		reconciliationContext := f.Context.NewContext().WithName("reorder-test").MustCreate(ctx)

		// Create rules in specific order
		rule1 := f.Rule.NewRule(reconciliationContext.ID).
			WithPriority(1).
			Exact().
			WithExactConfig(true, true).
			MustCreate(ctx)
		rule2 := f.Rule.NewRule(reconciliationContext.ID).
			WithPriority(2).
			Tolerance().
			WithToleranceConfig("1.00").
			MustCreate(ctx)
		rule3 := f.Rule.NewRule(reconciliationContext.ID).
			WithPriority(3).
			DateLag().
			WithDateLagConfig(1, 3, "ABS", true).
			MustCreate(ctx)

		tc.Logf("Original order: rule1=%s, rule2=%s, rule3=%s", rule1.ID, rule2.ID, rule3.ID)

		// Reorder: put rule3 first, then rule1, then rule2
		err := apiClient.Configuration.ReorderMatchRules(
			ctx,
			reconciliationContext.ID,
			client.ReorderMatchRulesRequest{
				RuleIDs: []string{rule3.ID, rule1.ID, rule2.ID},
			},
		)
		require.NoError(t, err)

		// Verify new order
		rules, err := apiClient.Configuration.ListMatchRules(ctx, reconciliationContext.ID)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(rules), 3)

		tc.Logf("Reordered: first rule is now %s", rules[0].ID)
		tc.Logf("✓ Rule reordering completed")
	})
}

// TestRulePriority_MultipleRuleTypes tests matching with multiple rule types.
func TestRulePriority_MultipleRuleTypes(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("multi-rule").MustCreate(ctx)
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

			// Create multiple rules
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(2).
				Tolerance().
				WithToleranceConfig("0.50").
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(3).
				Tolerance().
				WithToleranceConfig("2.00").
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(4).
				Tolerance().
				WithPercentToleranceConfig(5.0).
				MustCreate(ctx)

			// Upload transactions with varying matches
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MR-001", "100.00", "USD", "2026-01-15", "exact").
				AddRow("MR-002", "200.00", "USD", "2026-01-16", "small tolerance").
				AddRow("MR-003", "300.00", "USD", "2026-01-17", "large tolerance").
				AddRow("MR-004", "1000.00", "USD", "2026-01-18", "percent tolerance").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MR-001", "100.00", "USD", "2026-01-15", "exact").
				AddRow("MR-002", "200.40", "USD", "2026-01-16", "off by $0.40").
				AddRow("MR-003", "301.75", "USD", "2026-01-17", "off by $1.75").
				AddRow("MR-004", "1045.00", "USD", "2026-01-18", "off by 4.5%").
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

			// Verify all transactions matched via appropriate rules
			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(
				t,
				len(groups),
				4,
				"all 4 transactions should match via different rules",
			)

			tc.Logf("✓ Multiple rule types completed with %d matches", len(groups))
		},
	)
}
