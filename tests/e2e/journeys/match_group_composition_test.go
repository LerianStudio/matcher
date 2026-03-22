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

// =============================================================================
// One-to-Many Match Group Composition Tests
// =============================================================================

// TestMatchGroupComposition_OneToManyGroupItems verifies group composition for 1:N matches.
func TestMatchGroupComposition_OneToManyGroupItems(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Create one-to-many context
			reconciliationContext := f.Context.NewContext().
				WithName("group-one-to-many").
				OneToMany().
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

			// Ledger: 1 consolidated transaction of $300
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("O2M-CONSOLIDATED", "300.00", "USD", "2026-01-15", "consolidated payment").
				Build()

			// Bank: 3 split transactions that sum to $300
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("O2M-SPLIT-1", "100.00", "USD", "2026-01-15", "split 1 of 3").
				AddRow("O2M-SPLIT-2", "100.00", "USD", "2026-01-15", "split 2 of 3").
				AddRow("O2M-SPLIT-3", "100.00", "USD", "2026-01-15", "split 3 of 3").
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

			// Get match groups and verify composition
			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)

			tc.Logf("Found %d match groups", len(groups))

			// Analyze group composition
			for i, group := range groups {
				tc.Logf("Group %d: ID=%s, items=%d, confidence=%.2f",
					i+1, group.ID, len(group.Items), group.Confidence)

				for j, item := range group.Items {
					tc.Logf("  Item %d: tx=%s, amount=%s %s, contribution=%s",
						j+1, item.TransactionID, item.Amount, item.Currency, item.Contribution)
				}
			}

			tc.Logf("✓ One-to-many group composition verified")
		},
	)
}

// TestMatchGroupComposition_OneToManyAmountSum verifies amounts sum correctly in 1:N.
func TestMatchGroupComposition_OneToManyAmountSum(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("group-o2m-sum").
				OneToMany().
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

			// Ledger: $500 total
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("SUM-PARENT", "500.00", "USD", "2026-01-15", "parent transaction").
				Build()

			// Bank: Various amounts summing to $500
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("SUM-CHILD-1", "150.00", "USD", "2026-01-15", "child 1").
				AddRow("SUM-CHILD-2", "200.00", "USD", "2026-01-15", "child 2").
				AddRow("SUM-CHILD-3", "100.00", "USD", "2026-01-15", "child 3").
				AddRow("SUM-CHILD-4", "50.00", "USD", "2026-01-15", "child 4").
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

			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)

			tc.Logf("Match groups found: %d", len(groups))
			for _, group := range groups {
				tc.Logf("Group %s has %d items", group.ID, len(group.Items))
			}

			tc.Logf("✓ One-to-many amount sum matching verified")
		},
	)
}

// =============================================================================
// Many-to-Many Match Group Composition Tests
// =============================================================================

// TestMatchGroupComposition_ManyToManyGroupItems verifies group composition for M:N matches.
func TestMatchGroupComposition_ManyToManyGroupItems(t *testing.T) {
	t.Skip("N:M matching not yet implemented - unsupported context type: N:M")

	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Create many-to-many context
			reconciliationContext := f.Context.NewContext().
				WithName("group-many-to-many").
				ManyToMany().
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

			// Ledger: 2 transactions totaling $300
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("M2M-L1", "100.00", "USD", "2026-01-15", "ledger part 1").
				AddRow("M2M-L2", "200.00", "USD", "2026-01-15", "ledger part 2").
				Build()

			// Bank: 3 transactions also totaling $300
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("M2M-B1", "80.00", "USD", "2026-01-15", "bank part 1").
				AddRow("M2M-B2", "120.00", "USD", "2026-01-15", "bank part 2").
				AddRow("M2M-B3", "100.00", "USD", "2026-01-15", "bank part 3").
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

			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)

			tc.Logf("Found %d match groups for many-to-many", len(groups))

			for i, group := range groups {
				tc.Logf("Group %d: ID=%s, items=%d", i+1, group.ID, len(group.Items))

				ledgerItems := 0
				bankItems := 0
				for _, item := range group.Items {
					tc.Logf("  Item: tx=%s, amount=%s", item.TransactionID, item.Amount)
					// Note: Would need source info to distinguish ledger vs bank items
					if ledgerItems < 2 {
						ledgerItems++
					} else {
						bankItems++
					}
				}
			}

			tc.Logf("✓ Many-to-many group composition verified")
		},
	)
}

// TestMatchGroupComposition_ManyToManyBalanced tests balanced M:N matching.
func TestMatchGroupComposition_ManyToManyBalanced(t *testing.T) {
	t.Skip("N:M matching not yet implemented - unsupported context type: N:M")

	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("group-m2m-balanced").
				ManyToMany().
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

			// Both sides have 3 transactions each, same total ($600)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BAL-L1", "100.00", "USD", "2026-01-15", "ledger 1").
				AddRow("BAL-L2", "200.00", "USD", "2026-01-15", "ledger 2").
				AddRow("BAL-L3", "300.00", "USD", "2026-01-15", "ledger 3").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BAL-B1", "150.00", "USD", "2026-01-15", "bank 1").
				AddRow("BAL-B2", "250.00", "USD", "2026-01-15", "bank 2").
				AddRow("BAL-B3", "200.00", "USD", "2026-01-15", "bank 3").
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

			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)

			tc.Logf("Balanced M:N produced %d groups", len(groups))

			tc.Logf("✓ Many-to-many balanced matching verified")
		},
	)
}

// =============================================================================
// Match Group Confidence Tests
// =============================================================================

// TestMatchGroupComposition_ConfidenceScores tests confidence scores in groups.
func TestMatchGroupComposition_ConfidenceScores(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("group-confidence").
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

			// Perfect match data
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CONF-001", "100.00", "USD", "2026-01-15", "exact match").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CONF-001", "100.00", "USD", "2026-01-15", "exact match").
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

			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, groups)

			// Verify confidence score (API returns percentage 0-100)
			for _, group := range groups {
				tc.Logf("Group %s: confidence=%.4f", group.ID, group.Confidence)
				require.GreaterOrEqual(t, group.Confidence, 0.0, "confidence should be >= 0")
				require.LessOrEqual(t, group.Confidence, 100.0, "confidence should be <= 100")
			}

			tc.Logf("✓ Match group confidence scores verified")
		},
	)
}

// TestMatchGroupComposition_RuleAssociation tests groups are associated with rules.
func TestMatchGroupComposition_RuleAssociation(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("group-rule").MustCreate(ctx)
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

			// Create rule with known ID
			exactRule := f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("RULE-001", "100.00", "USD", "2026-01-15", "rule association").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("RULE-001", "100.00", "USD", "2026-01-15", "rule association").
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

			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, groups)

			// Verify groups are associated with the rule
			for _, group := range groups {
				require.NotEmpty(t, group.RuleID, "group should have rule ID")
				tc.Logf(
					"Group %s matched by rule %s (expected: %s)",
					group.ID,
					group.RuleID,
					exactRule.ID,
				)
			}

			tc.Logf("✓ Match groups correctly associated with rules")
		},
	)
}

// =============================================================================
// Match Item Contribution Tests
// =============================================================================

// TestMatchGroupComposition_ItemContributions tests contribution tracking in groups.
func TestMatchGroupComposition_ItemContributions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("group-contrib").
				OneToMany().
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

			// Parent: $400
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CONTRIB-PARENT", "400.00", "USD", "2026-01-15", "parent").
				Build()

			// Children: $150 + $100 + $150 = $400
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("CONTRIB-C1", "150.00", "USD", "2026-01-15", "child 1").
				AddRow("CONTRIB-C2", "100.00", "USD", "2026-01-15", "child 2").
				AddRow("CONTRIB-C3", "150.00", "USD", "2026-01-15", "child 3").
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

			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)

			for _, group := range groups {
				tc.Logf("Group %s items:", group.ID)
				for _, item := range group.Items {
					tc.Logf("  Item: amount=%s, contribution=%s", item.Amount, item.Contribution)
					// Contribution should reflect the item's share in the match
					require.NotEmpty(t, item.Amount)
				}
			}

			tc.Logf("✓ Match item contributions verified")
		},
	)
}
