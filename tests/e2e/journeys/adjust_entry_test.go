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
// Adjust Entry Resolution Tests
// =============================================================================

// TestAdjustEntry_ResolveWithAmountCorrection tests resolving an exception via amount adjustment.
func TestAdjustEntry_ResolveWithAmountCorrection(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup: Create context with strict matching to generate exceptions
			reconciliationContext := f.Context.NewContext().
				WithName("adjust-amount").
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

			// Create mismatched transactions with amount difference
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-AMT-001", "100.00", "USD", "2026-01-15", "ledger amount").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-AMT-001", "105.00", "USD", "2026-01-15", "bank different amount").
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

			// Run matching - will not match due to amount difference
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

			// Get exception for unmatched transaction
			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items, "should have exception for unmatched transaction")

			exceptionID := exceptions.Items[0].ID
			tc.Logf("Found exception: %s", exceptionID)

			// Resolve via adjust entry
			adjustmentAmount := decimal.NewFromFloat(5.00)
			resolved, err := apiClient.Exception.AdjustEntry(
				ctx,
				exceptionID,
				client.AdjustEntryRequest{
					ReasonCode:  "AMOUNT_CORRECTION",
					Notes:       "Correcting $5.00 difference between ledger and bank",
					Amount:      adjustmentAmount,
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.NoError(t, err)
			require.Equal(t, "RESOLVED", resolved.Status)
			require.NotNil(t, resolved.ResolutionType)
			require.Equal(t, "ADJUST_ENTRY", *resolved.ResolutionType)

			tc.Logf("✓ Exception resolved via amount adjustment")
		},
	)
}

// TestAdjustEntry_ResolveWithCurrencyCorrection tests resolving via currency adjustment.
func TestAdjustEntry_ResolveWithCurrencyCorrection(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("adjust-currency").
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

			// Create unmatched transaction (only on ledger, no bank transactions to match)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-CUR-001", "100.00", "EUR", "2026-01-15", "currency correction test").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			resolved, err := apiClient.Exception.AdjustEntry(
				ctx,
				exceptionID,
				client.AdjustEntryRequest{
					ReasonCode:  "CURRENCY_CORRECTION",
					Notes:       "Currency was recorded incorrectly, should have been USD",
					Amount:      decimal.NewFromFloat(100.00),
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.NoError(t, err)
			require.Equal(t, "RESOLVED", resolved.Status)

			tc.Logf("✓ Exception resolved via currency correction")
		},
	)
}

// TestAdjustEntry_ResolveWithDateCorrection tests resolving via date adjustment.
func TestAdjustEntry_ResolveWithDateCorrection(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("adjust-date").MustCreate(ctx)
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

			// Create unmatched transaction (only on ledger, no bank transactions to match)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-DATE-001", "100.00", "USD", "2026-01-15", "date correction test").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			correctEffectiveDate := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)
			resolved, err := apiClient.Exception.AdjustEntry(
				ctx,
				exceptionID,
				client.AdjustEntryRequest{
					ReasonCode:  "DATE_CORRECTION",
					Notes:       "Transaction date was recorded one day early",
					Amount:      decimal.NewFromFloat(100.00), // Use original transaction amount
					Currency:    "USD",
					EffectiveAt: correctEffectiveDate,
				},
			)
			require.NoError(t, err)
			require.Equal(t, "RESOLVED", resolved.Status)

			tc.Logf("✓ Exception resolved via date correction")
		},
	)
}

// TestAdjustEntry_ResolveWithOtherReason tests resolving via general adjustment.
func TestAdjustEntry_ResolveWithOtherReason(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("adjust-other").MustCreate(ctx)
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

			// Create unmatched transaction (only on ledger, no bank transactions to match)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-OTHER-001", "100.00", "USD", "2026-01-15", "other adjustment test").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			resolved, err := apiClient.Exception.AdjustEntry(
				ctx,
				exceptionID,
				client.AdjustEntryRequest{
					ReasonCode:  "OTHER",
					Notes:       "General adjustment for reconciliation purposes per finance team request",
					Amount:      decimal.NewFromFloat(50.00),
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.NoError(t, err)
			require.Equal(t, "RESOLVED", resolved.Status)

			tc.Logf("✓ Exception resolved via OTHER reason adjustment")
		},
	)
}

// =============================================================================
// Adjust Entry Validation Tests
// =============================================================================

// TestAdjustEntry_InvalidReasonCode tests rejection of invalid reason codes.
func TestAdjustEntry_InvalidReasonCode(t *testing.T) {
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
					Notes:       "This should be rejected",
					Amount:      decimal.NewFromFloat(100.00),
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.Error(t, err, "invalid reason code should be rejected")

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsBadRequest() || apiErr.IsNotFound(),
					"should return 400 or 404, got %d", apiErr.StatusCode)
			}

			tc.Logf("✓ Invalid reason code correctly rejected")
		},
	)
}

// TestAdjustEntry_MissingNotes tests that notes are required.
func TestAdjustEntry_MissingNotes(t *testing.T) {
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
					Notes:       "", // Empty notes
					Amount:      decimal.NewFromFloat(100.00),
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.Error(t, err, "empty notes should be rejected")

			tc.Logf("✓ Empty notes correctly rejected")
		},
	)
}

// TestAdjustEntry_InvalidCurrency tests rejection of invalid currency codes.
func TestAdjustEntry_InvalidCurrency(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Exception.AdjustEntry(
				ctx,
				"00000000-0000-0000-0000-000000000000",
				client.AdjustEntryRequest{
					ReasonCode:  "CURRENCY_CORRECTION",
					Notes:       "Testing invalid currency",
					Amount:      decimal.NewFromFloat(100.00),
					Currency:    "INVALID",
					EffectiveAt: time.Now(),
				},
			)
			require.Error(t, err, "invalid currency should be rejected")

			tc.Logf("✓ Invalid currency correctly rejected")
		},
	)
}

// TestAdjustEntry_NonExistentException tests handling of non-existent exception.
func TestAdjustEntry_NonExistentException(t *testing.T) {
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
					Notes:       "Testing non-existent exception",
					Amount:      decimal.NewFromFloat(100.00),
					Currency:    "USD",
					EffectiveAt: time.Now(),
				},
			)
			require.Error(t, err)

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsNotFound() || apiErr.IsBadRequest(),
					"should return 404 or 400, got %d", apiErr.StatusCode)
			}

			tc.Logf("✓ Non-existent exception correctly returns error")
		},
	)
}

// TestAdjustEntry_AlreadyResolvedException tests that resolved exceptions cannot be adjusted.
func TestAdjustEntry_AlreadyResolvedException(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("adjust-already-resolved").
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

			// Create unmatched transaction (only on ledger, no bank transactions to match)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-RESOLVED-001", "100.00", "USD", "2026-01-15", "already resolved test").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			// First resolution should succeed
			_, err = apiClient.Exception.AdjustEntry(ctx, exceptionID, client.AdjustEntryRequest{
				ReasonCode:  "AMOUNT_CORRECTION",
				Notes:       "First adjustment",
				Amount:      decimal.NewFromFloat(10.00),
				Currency:    "USD",
				EffectiveAt: time.Now(),
			})
			require.NoError(t, err)

			// Second adjustment on resolved exception should fail
			_, err = apiClient.Exception.AdjustEntry(ctx, exceptionID, client.AdjustEntryRequest{
				ReasonCode:  "OTHER",
				Notes:       "Second adjustment attempt",
				Amount:      decimal.NewFromFloat(20.00),
				Currency:    "USD",
				EffectiveAt: time.Now(),
			})
			require.Error(t, err, "adjusting already resolved exception should fail")

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				require.Equal(
					t,
					422,
					apiErr.StatusCode,
					"should return 422 for invalid state transition",
				)
			}

			tc.Logf("✓ Already resolved exception correctly rejects second adjustment")
		},
	)
}

// =============================================================================
// Adjust Entry Audit Trail Tests
// =============================================================================

// TestAdjustEntry_CreatesAuditTrail tests that adjustments create audit history.
func TestAdjustEntry_CreatesAuditTrail(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("adjust-audit").MustCreate(ctx)
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

			// Create unmatched transaction (only on ledger, no bank transactions to match)
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("ADJ-AUDIT-001", "100.00", "USD", "2026-01-15", "audit trail test").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			// Resolve via adjustment
			_, err = apiClient.Exception.AdjustEntry(ctx, exceptionID, client.AdjustEntryRequest{
				ReasonCode:  "AMOUNT_CORRECTION",
				Notes:       "Adjustment for audit trail verification",
				Amount:      decimal.NewFromFloat(25.00),
				Currency:    "USD",
				EffectiveAt: time.Now(),
			})
			require.NoError(t, err)

			// Get history
			history, err := e2e.WaitForExceptionHistory(ctx, tc, apiClient, exceptionID, 1)
			require.NoError(t, err)
			require.NotEmpty(t, history.Items, "should have history entries")

			tc.Logf("Found %d history entries for exception", len(history.Items))

			// Verify history contains adjustment action
			foundAdjustment := false
			for _, entry := range history.Items {
				tc.Logf("History entry: action=%s", entry.Action)
				if entry.Action == "ADJUST_ENTRY" || entry.Action == "RESOLVED" {
					foundAdjustment = true
				}
			}

			require.True(t, foundAdjustment, "should have adjustment action in history")

			tc.Logf("✓ Adjust entry creates audit trail")
		},
	)
}
