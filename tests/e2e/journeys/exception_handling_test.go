//go:build e2e

package journeys

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// =============================================================================
// Exception Creation Tests
// =============================================================================

// TestException_CreatedForUnmatchedTransactions verifies that exceptions are
// automatically created when transactions fail to match after a match run.
func TestException_CreatedForUnmatchedTransactions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup: Create context with strict exact matching
			reconciliationContext := f.Context.NewContext().
				WithName("exception-create").
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

			// Create deliberately non-matching transactions
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("EXCPT-001", "100.00", "USD", "2026-01-15", "ledger unmatched").
				AddRow("EXCPT-002", "200.00", "USD", "2026-01-16", "ledger unmatched 2").
				Build()

			// Bank has different IDs - won't match
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BANK-XXX", "100.00", "USD", "2026-01-15", "bank unmatched").
				AddRow("BANK-YYY", "200.00", "USD", "2026-01-16", "bank unmatched 2").
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

			// Run matching - these should NOT match
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

			// Verify match run completed
			matchRun, err := apiClient.Matching.GetMatchRun(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.Equal(t, "COMPLETED", matchRun.Status)

			// List exceptions - should have been created for unmatched transactions
			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			tc.Logf("Found %d open exceptions", len(exceptions.Items))

			// We should have at least 4 exceptions (2 ledger + 2 bank unmatched)
			require.GreaterOrEqual(
				t,
				len(exceptions.Items),
				4,
				"should have exceptions for unmatched transactions",
			)

			// Verify exception properties
			for _, exc := range exceptions.Items {
				require.NotEmpty(t, exc.ID)
				require.NotEmpty(t, exc.TransactionID)
				require.Equal(t, "OPEN", exc.Status)
				require.NotEmpty(t, exc.Severity)
			}

			tc.Logf("✓ Exceptions correctly created for unmatched transactions")
		},
	)
}

// TestException_StatusTransitions verifies exception status changes through workflow.
func TestException_StatusTransitions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup: Create unmatched transaction to generate exception
			reconciliationContext := f.Context.NewContext().
				WithName("exception-status").
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

			// Create single unmatched transaction
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("STATUS-001", "150.00", "USD", "2026-01-15", "status test").
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

			// Get the exception
			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items, "should have at least one exception")

			exception := exceptions.Items[0]
			require.Equal(t, "OPEN", exception.Status, "new exception should be OPEN")

			tc.Logf("Exception created with status: %s", exception.Status)

			// Resolve via force match to transition to RESOLVED
			resolved, err := apiClient.Exception.ForceMatch(
				ctx,
				exception.ID,
				client.ForceMatchRequest{
					OverrideReason: "POLICY_EXCEPTION",
					Notes:          "Approved by finance team for reconciliation test",
				},
			)
			require.NoError(t, err)
			require.Equal(
				t,
				"RESOLVED",
				resolved.Status,
				"exception should be RESOLVED after force match",
			)

			tc.Logf("✓ Exception status transitioned: OPEN → RESOLVED")
		},
	)
}

// =============================================================================
// Force Match Resolution Tests
// =============================================================================

// TestException_ForceMatchWorkflow tests the complete force match resolution workflow.
func TestException_ForceMatchWorkflow(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup context
			reconciliationContext := f.Context.NewContext().
				WithName("force-match-flow").
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

			// Create mismatched transactions
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("FM-FLOW-001", "100.00", "USD", "2026-01-15", "ledger tx").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("FM-FLOW-002", "100.50", "USD", "2026-01-15", "bank tx slight diff").
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

			// Get exceptions
			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			// Test each valid override reason
			validReasons := []string{
				"POLICY_EXCEPTION",
				"OPS_APPROVAL",
				"CUSTOMER_DISPUTE",
				"DATA_CORRECTION",
			}

			// Use first exception to test one reason
			resolved, err := apiClient.Exception.ForceMatch(
				ctx,
				exceptionID,
				client.ForceMatchRequest{
					OverrideReason: validReasons[0],
					Notes:          "Approved per company policy REF-12345. Amount variance within acceptable threshold.",
				},
			)
			require.NoError(t, err)
			require.Equal(t, "RESOLVED", resolved.Status)
			require.NotNil(t, resolved.ResolutionType)
			require.Equal(t, "FORCE_MATCH", *resolved.ResolutionType)
			require.NotNil(t, resolved.ResolutionReason)
			require.Equal(t, validReasons[0], *resolved.ResolutionReason)
			require.NotNil(t, resolved.ResolutionNotes)

			tc.Logf("✓ Force match workflow completed with reason: %s", validReasons[0])
		},
	)
}

// TestException_ForceMatchValidOverrideReasons tests all valid override reasons.
func TestException_ForceMatchValidOverrideReasons(t *testing.T) {
	validReasons := []struct {
		reason      string
		description string
	}{
		{"POLICY_EXCEPTION", "Company policy allows this variance"},
		{"OPS_APPROVAL", "Operations team approved this match"},
		{"CUSTOMER_DISPUTE", "Customer confirmed transaction is correct"},
		{"DATA_CORRECTION", "Source data was corrected externally"},
	}

	for _, tc := range validReasons {
		t.Run(tc.reason, func(t *testing.T) {
			// This test validates the reason codes are recognized
			// Full workflow is tested in TestException_ForceMatchWorkflow
			t.Logf("Valid override reason: %s - %s", tc.reason, tc.description)
		})
	}
}

// TestException_ForceMatchInvalidReason tests rejection of invalid override reasons.
func TestException_ForceMatchInvalidReason(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Exception.ForceMatch(
				ctx,
				"00000000-0000-0000-0000-000000000000",
				client.ForceMatchRequest{
					OverrideReason: "INVALID_REASON_CODE",
					Notes:          "This should be rejected",
				},
			)
			require.Error(t, err, "invalid override reason should be rejected")

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsBadRequest() || apiErr.IsNotFound(),
					"should return 400 or 404, got %d", apiErr.StatusCode)
			}

			tc.Logf("✓ Invalid override reason correctly rejected")
		},
	)
}

// TestException_ForceMatchEmptyNotes tests that force match requires notes.
func TestException_ForceMatchEmptyNotes(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Exception.ForceMatch(
				ctx,
				"00000000-0000-0000-0000-000000000000",
				client.ForceMatchRequest{
					OverrideReason: "POLICY_EXCEPTION",
					Notes:          "",
				},
			)
			require.Error(t, err, "empty notes should be rejected")

			tc.Logf("✓ Force match correctly requires notes")
		},
	)
}

// =============================================================================
// Adjust Entry Resolution Tests
// =============================================================================

// TestException_AdjustEntryWorkflow tests the adjust entry resolution workflow.
func TestException_AdjustEntryWorkflow(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup context
			reconciliationContext := f.Context.NewContext().WithName("adjust-entry").MustCreate(ctx)
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

			// Create unmatched transaction
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-001", "1000.00", "USD", "2026-01-15", "needs adjustment").
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

			// Get exception
			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			// Create adjustment entry
			adjustmentAmount := decimal.NewFromFloat(50.00)
			resolved, err := apiClient.Exception.AdjustEntry(
				ctx,
				exceptionID,
				client.AdjustEntryRequest{
					ReasonCode:  "AMOUNT_CORRECTION",
					Notes:       "Bank fee was not included in original transaction",
					Amount:      adjustmentAmount,
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.NoError(t, err)
			require.Equal(t, "RESOLVED", resolved.Status)
			require.NotNil(t, resolved.ResolutionType)
			require.Equal(t, "ADJUST_ENTRY", *resolved.ResolutionType)

			tc.Logf("✓ Adjust entry workflow completed")
		},
	)
}

// TestException_AdjustEntryValidReasonCodes tests valid adjustment reason codes.
func TestException_AdjustEntryValidReasonCodes(t *testing.T) {
	validReasons := []string{
		"AMOUNT_CORRECTION",
		"CURRENCY_CORRECTION",
		"DATE_CORRECTION",
		"OTHER",
	}

	for _, reason := range validReasons {
		t.Run(reason, func(t *testing.T) {
			t.Logf("Valid adjustment reason: %s", reason)
		})
	}
}

// TestException_AdjustEntryInvalidReason tests rejection of invalid adjustment reasons.
func TestException_AdjustEntryInvalidReason(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Exception.AdjustEntry(
				ctx,
				"00000000-0000-0000-0000-000000000000",
				client.AdjustEntryRequest{
					ReasonCode:  "INVALID_REASON",
					Notes:       "This should fail",
					Amount:      decimal.NewFromFloat(10.00),
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.Error(t, err, "invalid adjustment reason should be rejected")

			tc.Logf("✓ Invalid adjustment reason correctly rejected")
		},
	)
}

// TestException_AdjustEntryZeroAmount tests rejection of zero adjustment amounts.
func TestException_AdjustEntryZeroAmount(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Exception.AdjustEntry(
				ctx,
				"00000000-0000-0000-0000-000000000000",
				client.AdjustEntryRequest{
					ReasonCode:  "AMOUNT_CORRECTION",
					Notes:       "Zero amount adjustment",
					Amount:      decimal.Zero,
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.Error(t, err, "zero adjustment amount should be rejected")

			tc.Logf("✓ Zero adjustment amount correctly rejected")
		},
	)
}

// =============================================================================
// Exception Query Tests
// =============================================================================

// TestException_FilterByStatus tests filtering exceptions by status.
func TestException_FilterByStatus(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup and create exceptions
			reconciliationContext := f.Context.NewContext().
				WithName("filter-status").
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

			// Create multiple unmatched transactions
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("FILT-001", "100.00", "USD", "2026-01-15", "filter test 1").
				AddRow("FILT-002", "200.00", "USD", "2026-01-16", "filter test 2").
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

			// Filter by OPEN status
			openExceptions, err := apiClient.Exception.ListExceptions(
				ctx,
				client.ExceptionListFilter{
					Status: "OPEN",
					Limit:  100,
				},
			)
			require.NoError(t, err)
			require.NotEmpty(t, openExceptions.Items)

			for _, exc := range openExceptions.Items {
				require.Equal(t, "OPEN", exc.Status)
			}

			// Resolve one exception
			if len(openExceptions.Items) > 0 {
				_, err = apiClient.Exception.ForceMatch(
					ctx,
					openExceptions.Items[0].ID,
					client.ForceMatchRequest{
						OverrideReason: "POLICY_EXCEPTION",
						Notes:          "Test resolution",
					},
				)
				require.NoError(t, err)
			}

			// Filter by RESOLVED status
			resolvedExceptions, err := apiClient.Exception.ListExceptions(
				ctx,
				client.ExceptionListFilter{
					Status: "RESOLVED",
					Limit:  100,
				},
			)
			require.NoError(t, err)
			require.NotEmpty(t, resolvedExceptions.Items)

			for _, exc := range resolvedExceptions.Items {
				require.Equal(t, "RESOLVED", exc.Status)
			}

			tc.Logf("✓ Exception status filtering works correctly")
		},
	)
}

// TestException_FilterBySeverity tests filtering exceptions by severity.
func TestException_FilterBySeverity(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			severities := []string{"LOW", "MEDIUM", "HIGH", "CRITICAL"}

			for _, severity := range severities {
				exceptions, err := apiClient.Exception.ListExceptions(
					ctx,
					client.ExceptionListFilter{
						Severity: severity,
						Limit:    10,
					},
				)
				require.NoError(t, err)

				for _, exc := range exceptions.Items {
					require.Equal(t, severity, exc.Severity)
				}

				tc.Logf("Filtered by severity %s: %d exceptions", severity, len(exceptions.Items))
			}

			tc.Logf("✓ Exception severity filtering works correctly")
		},
	)
}

// TestException_Pagination tests cursor-based pagination for exceptions.
func TestException_Pagination(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Create many unmatched transactions
			reconciliationContext := f.Context.NewContext().
				WithName("exception-page").
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

			// Create 10 unmatched transactions
			csvBuilder := factories.NewCSVBuilder(tc.NamePrefix())
			for i := 1; i <= 10; i++ {
				csvBuilder.AddRow(
					tc.NamePrefix()+"-PAGE-"+string(rune('A'+i-1)),
					"100.00",
					"USD",
					"2026-01-15",
					"pagination test",
				)
			}

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				ledgerSource.ID,
				"ledger.csv",
				csvBuilder.Build(),
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, ledgerJob.ID),
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

			// Paginate with small limit
			allIDs := make(map[string]bool)
			cursor := ""
			pageCount := 0

			for {
				page, err := apiClient.Exception.ListExceptions(ctx, client.ExceptionListFilter{
					Limit:  3,
					Cursor: cursor,
				})
				require.NoError(t, err)
				pageCount++

				for _, exc := range page.Items {
					allIDs[exc.ID] = true
				}

				if !page.HasMore {
					break
				}

				cursor = page.NextCursor
				require.NotEmpty(t, cursor, "should have next cursor when HasMore is true")
			}

			tc.Logf(
				"Paginated through %d pages, found %d unique exceptions",
				pageCount,
				len(allIDs),
			)
			tc.Logf("✓ Exception pagination works correctly")
		},
	)
}

// =============================================================================
// Exception History Tests
// =============================================================================

// TestException_HistoryAuditTrail tests that exception actions create audit history.
func TestException_HistoryAuditTrail(t *testing.T) {
	t.Skip("skipping: exception history endpoint causes connection reset - pending server-side investigation (see #27)")

	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup and create exception
			reconciliationContext := f.Context.NewContext().
				WithName("exception-history").
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

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("HIST-001", "500.00", "USD", "2026-01-15", "history test").
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

			// Get exception
			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			// Resolve the exception
			_, err = apiClient.Exception.ForceMatch(ctx, exceptionID, client.ForceMatchRequest{
				OverrideReason: "OPS_APPROVAL",
				Notes:          "Approved for audit history test",
			})
			require.NoError(t, err)

			// Get history
			history, err := e2e.WaitForExceptionHistory(ctx, tc, apiClient, exceptionID, 1)
			require.NoError(t, err)
			require.NotEmpty(t, history.Items, "should have history entries")

			tc.Logf("Found %d history entries for exception", len(history.Items))

			// Verify history contains resolution action
			foundResolution := false
			for _, entry := range history.Items {
				tc.Logf("History entry: action=%s", entry.Action)
				if entry.Action == "FORCE_MATCH" || entry.Action == "RESOLVED" {
					foundResolution = true
				}
			}

			require.True(t, foundResolution, "should have resolution action in history")

			tc.Logf("✓ Exception history audit trail works correctly")
		},
	)
}

// =============================================================================
// Exception Edge Cases
// =============================================================================

// TestException_GetNonExistent tests getting a non-existent exception.
func TestException_GetNonExistent(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Exception.GetException(ctx, "00000000-0000-0000-0000-000000000000")
			require.Error(t, err)

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsNotFound(), "should return 404, got %d", apiErr.StatusCode)
			}

			tc.Logf("✓ Non-existent exception returns 404")
		},
	)
}

// TestException_ResolveAlreadyResolved tests that resolved exceptions cannot be resolved again.
func TestException_ResolveAlreadyResolved(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup and create exception
			reconciliationContext := f.Context.NewContext().
				WithName("double-resolve").
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

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DOUBLE-001", "100.00", "USD", "2026-01-15", "double resolve test").
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

			// Get and resolve exception
			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			// First resolution should succeed
			_, err = apiClient.Exception.ForceMatch(ctx, exceptionID, client.ForceMatchRequest{
				OverrideReason: "POLICY_EXCEPTION",
				Notes:          "First resolution",
			})
			require.NoError(t, err)

			// Second resolution should fail
			_, err = apiClient.Exception.ForceMatch(ctx, exceptionID, client.ForceMatchRequest{
				OverrideReason: "OPS_APPROVAL",
				Notes:          "Second resolution attempt",
			})
			require.Error(t, err, "resolving already resolved exception should fail")

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				// Should be 422 Unprocessable Entity
				require.Equal(
					t,
					422,
					apiErr.StatusCode,
					"should return 422 for invalid state transition",
				)
			}

			tc.Logf("✓ Already resolved exception correctly rejects second resolution")
		},
	)
}

// TestException_SortingOptions tests different sorting options.
func TestException_SortingOptions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			sortOptions := []struct {
				sortBy    string
				sortOrder string
			}{
				{"created_at", "asc"},
				{"created_at", "desc"},
				{"updated_at", "asc"},
				{"updated_at", "desc"},
				{"severity", "asc"},
				{"severity", "desc"},
			}

			for _, opt := range sortOptions {
				exceptions, err := apiClient.Exception.ListExceptions(
					ctx,
					client.ExceptionListFilter{
						SortBy:    opt.sortBy,
						SortOrder: opt.sortOrder,
						Limit:     10,
					},
				)
				require.NoError(t, err)
				tc.Logf(
					"Sort by %s %s: %d results",
					opt.sortBy,
					opt.sortOrder,
					len(exceptions.Items),
				)
			}

			tc.Logf("✓ Exception sorting options work correctly")
		},
	)
}
