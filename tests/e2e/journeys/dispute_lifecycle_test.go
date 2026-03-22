//go:build e2e

package journeys

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// =============================================================================
// Dispute Lifecycle Tests
// =============================================================================

// TestDispute_FullLifecycle tests opening, adding evidence, and closing a dispute.
func TestDispute_FullLifecycle(t *testing.T) {
	t.Skip("dispute endpoint returns 500 - pending server-side fix (see #27)")
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("dispute-lifecycle").
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
				AddRow("DISP-LC-001", "100.00", "USD", "2026-01-15", "ledger dispute lifecycle").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BANK-LC-XXX", "100.00", "USD", "2026-01-15", "bank dispute lifecycle").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items, "should have at least one exception")

			exceptionID := exceptions.Items[0].ID

			// Step 1: Open dispute
			dispute, err := apiClient.Exception.OpenDispute(
				ctx,
				exceptionID,
				client.OpenDisputeRequest{
					Category:    "AMOUNT_MISMATCH",
					Description: "Amount does not match bank statement",
				},
			)
			require.NoError(t, err)
			require.Equal(t, "OPEN", dispute.State)
			require.Equal(t, "AMOUNT_MISMATCH", dispute.Category)
			require.Equal(t, "Amount does not match bank statement", dispute.Description)
			require.Equal(t, exceptionID, dispute.ExceptionID)

			tc.Logf("✓ Dispute opened with ID: %s", dispute.ID)

			// Step 2: Submit evidence
			evidenceResp, err := apiClient.Exception.SubmitEvidence(
				ctx,
				dispute.ID,
				client.SubmitEvidenceRequest{
					Comment: "Bank statement shows correct amount",
					FileURL: "https://evidence.example.com/stmt-001.pdf",
				},
			)
			require.NoError(t, err)
			require.NotEmpty(t, evidenceResp.Evidence)

			tc.Logf("✓ Evidence submitted: %d items", len(evidenceResp.Evidence))

			// Step 3: Close dispute as won
			won := true
			closed, err := apiClient.Exception.CloseDispute(
				ctx,
				dispute.ID,
				client.CloseDisputeRequest{
					Resolution: "Bank confirmed the correct amount after review",
					Won:        &won,
				},
			)
			require.NoError(t, err)
			require.Equal(t, "WON", closed.State)

			tc.Logf("✓ Dispute closed as WON")
		},
	)
}

// TestDispute_CloseAsLost tests closing a dispute as lost.
func TestDispute_CloseAsLost(t *testing.T) {
	t.Skip("dispute endpoint returns 500 - pending server-side fix (see #27)")
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("dispute-lost").
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
				AddRow("DISP-LOST-001", "250.00", "USD", "2026-01-15", "ledger dispute lost").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BANK-LOST-XXX", "250.00", "USD", "2026-01-15", "bank dispute lost").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			dispute, err := apiClient.Exception.OpenDispute(
				ctx,
				exceptionID,
				client.OpenDisputeRequest{
					Category:    "UNRECOGNIZED_CHARGE",
					Description: "Charge not recognized in ledger",
				},
			)
			require.NoError(t, err)
			require.Equal(t, "OPEN", dispute.State)

			lost := false
			closed, err := apiClient.Exception.CloseDispute(
				ctx,
				dispute.ID,
				client.CloseDisputeRequest{
					Resolution: "Investigation confirmed the charge is valid",
					Won:        &lost,
				},
			)
			require.NoError(t, err)
			require.Equal(t, "LOST", closed.State)

			tc.Logf("✓ Dispute closed as LOST")
		},
	)
}

// TestDispute_MultipleEvidence tests submitting multiple evidence items.
func TestDispute_MultipleEvidence(t *testing.T) {
	t.Skip("dispute endpoint returns 500 - pending server-side fix (see #27)")
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("dispute-multi-evidence").
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
				AddRow("DISP-EVID-001", "300.00", "USD", "2026-01-15", "ledger multi evidence").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BANK-EVID-XXX", "300.00", "USD", "2026-01-15", "bank multi evidence").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, exceptions.Items)

			exceptionID := exceptions.Items[0].ID

			dispute, err := apiClient.Exception.OpenDispute(
				ctx,
				exceptionID,
				client.OpenDisputeRequest{
					Category:    "AMOUNT_MISMATCH",
					Description: "Multiple evidence items test",
				},
			)
			require.NoError(t, err)

			evidenceItems := []client.SubmitEvidenceRequest{
				{
					Comment: "Bank statement from January 2026",
					FileURL: "https://evidence.example.com/stmt-jan.pdf",
				},
				{
					Comment: "Internal ledger reconciliation report",
					FileURL: "https://evidence.example.com/ledger-report.pdf",
				},
				{
					Comment: "Email confirmation from counterparty",
					FileURL: "https://evidence.example.com/email-confirm.eml",
				},
			}

			for i, item := range evidenceItems {
				result, err := apiClient.Exception.SubmitEvidence(ctx, dispute.ID, item)
				require.NoError(t, err)
				require.GreaterOrEqual(
					t,
					len(result.Evidence),
					i+1,
					"should have at least %d evidence items after submission %d",
					i+1,
					i+1,
				)
			}

			tc.Logf("✓ Submitted 3 evidence items successfully")
		},
	)
}

// =============================================================================
// Dispute Validation Tests
// =============================================================================

// TestDispute_InvalidCategory tests rejection of empty category.
func TestDispute_InvalidCategory(t *testing.T) {
	t.Skip("dispute endpoint returns 500 - pending server-side fix (see #27)")
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("dispute-invalid-cat").
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
				AddRow("DISP-INVAL-001", "100.00", "USD", "2026-01-15", "invalid category test").
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

			_, err = apiClient.Exception.OpenDispute(
				ctx,
				exceptionID,
				client.OpenDisputeRequest{
					Category:    "",
					Description: "This should fail due to empty category",
				},
			)
			require.Error(t, err, "opening dispute with empty category should fail")

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			require.True(t, apiErr.IsBadRequest(), "expected 400, got %d", apiErr.StatusCode)

			tc.Logf("✓ Empty category correctly rejected")
		},
	)
}

// =============================================================================
// Dispute Edge Case Tests
// =============================================================================

// TestDispute_CloseNonExistentDispute tests closing a non-existent dispute.
func TestDispute_CloseNonExistentDispute(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			won := true
			_, err := apiClient.Exception.CloseDispute(
				ctx,
				"00000000-0000-0000-0000-000000000000",
				client.CloseDisputeRequest{
					Resolution: "Attempting to close non-existent dispute",
					Won:        &won,
				},
			)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			// TODO: 403 was previously accepted because auth may reject before route matching.
			require.True(t, apiErr.IsNotFound(), "should return 404, got %d", apiErr.StatusCode)

			tc.Logf("✓ Non-existent dispute close returns error")
		},
	)
}

// TestDispute_SubmitEvidenceToNonExistentDispute tests submitting evidence to a non-existent dispute.
func TestDispute_SubmitEvidenceToNonExistentDispute(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Exception.SubmitEvidence(
				ctx,
				"00000000-0000-0000-0000-000000000000",
				client.SubmitEvidenceRequest{
					Comment: "Evidence for non-existent dispute",
					FileURL: "https://evidence.example.com/orphan.pdf",
				},
			)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			require.True(t, apiErr.IsNotFound() || apiErr.StatusCode == 403, "should return 404 or 403, got %d", apiErr.StatusCode)

			tc.Logf("✓ Non-existent dispute evidence submission returns error")
		},
	)
}

// TestDispute_OpenOnNonExistentException tests opening a dispute on a non-existent exception.
func TestDispute_OpenOnNonExistentException(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		1*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			_, err := apiClient.Exception.OpenDispute(
				ctx,
				"00000000-0000-0000-0000-000000000000",
				client.OpenDisputeRequest{
					Category:    "AMOUNT_MISMATCH",
					Description: "Dispute on non-existent exception",
				},
			)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			// TODO: 403 was previously accepted because auth may reject before route matching.
			require.True(t, apiErr.IsNotFound() || apiErr.StatusCode == 403, "should return 404 or 403, got %d", apiErr.StatusCode)

			tc.Logf("✓ Non-existent exception dispute open returns error")
		},
	)
}

// =============================================================================
// Dispute Category Tests
// =============================================================================

// TestDispute_Categories tests all valid dispute categories.
func TestDispute_Categories(t *testing.T) {
	t.Skip("dispute endpoint returns 500 - pending server-side fix (see #27)")
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("dispute-categories").
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
				AddRow("DISP-CAT-001", "100.00", "USD", "2026-01-15", "cat test 1").
				AddRow("DISP-CAT-002", "200.00", "USD", "2026-01-16", "cat test 2").
				AddRow("DISP-CAT-003", "300.00", "USD", "2026-01-17", "cat test 3").
				AddRow("DISP-CAT-004", "400.00", "USD", "2026-01-18", "cat test 4").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BANK-CAT-XXX", "100.00", "USD", "2026-01-15", "bank cat 1").
				AddRow("BANK-CAT-YYY", "200.00", "USD", "2026-01-16", "bank cat 2").
				AddRow("BANK-CAT-ZZZ", "300.00", "USD", "2026-01-17", "bank cat 3").
				AddRow("BANK-CAT-WWW", "400.00", "USD", "2026-01-18", "bank cat 4").
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

			exceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(
				t,
				len(exceptions.Items),
				4,
				"should have at least 4 exceptions for category tests",
			)

			categories := []string{
				"BANK_FEE_ERROR",
				"UNRECOGNIZED_CHARGE",
				"DUPLICATE_TRANSACTION",
				"OTHER",
			}

			for i, category := range categories {
				t.Run(category, func(t *testing.T) {
					exceptionID := exceptions.Items[i].ID

					dispute, err := apiClient.Exception.OpenDispute(
						ctx,
						exceptionID,
						client.OpenDisputeRequest{
							Category:    category,
							Description: "Testing category: " + category,
						},
					)
					require.NoError(t, err)
					require.Equal(t, "OPEN", dispute.State)
					require.Equal(t, category, dispute.Category)
					require.Equal(t, exceptionID, dispute.ExceptionID)

					tc.Logf("✓ Category %s accepted", category)
				})
			}
		},
	)
}
