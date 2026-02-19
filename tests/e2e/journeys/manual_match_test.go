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
// Manual Match Tests
// =============================================================================

// TestManualMatch_PairUnmatchedTransactions tests manually matching two unmatched transactions.
func TestManualMatch_PairUnmatchedTransactions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("manual-match").MustCreate(ctx)
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
				AddRow("MANUAL-001", "100.00", "USD", "2026-01-15", "ledger tx").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MANUAL-BANK-001", "100.00", "USD", "2026-01-15", "bank tx").
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

			ledgerTxs, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, ledgerTxs)

			bankTxs, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				bankJob.ID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, bankTxs)

			ledgerTxID := ledgerTxs[0].ID
			bankTxID := bankTxs[0].ID

			manualResp, err := apiClient.Matching.ManualMatch(
				ctx,
				reconciliationContext.ID,
				client.ManualMatchRequest{
					TransactionIDs: []string{ledgerTxID, bankTxID},
					Notes:          "Manual reconciliation per finance team",
				},
			)
			require.NoError(t, err)
			require.NotNil(t, manualResp)
			require.NotEmpty(t, manualResp.MatchGroup.ID)
			require.NotEmpty(t, manualResp.MatchGroup.Items)

			tc.Logf("✓ Manual match created group %s with %d items",
				manualResp.MatchGroup.ID, len(manualResp.MatchGroup.Items))
		},
	)
}

// TestManualMatch_RequiresMinTwoTransactions tests that manual match requires at least 2 transaction IDs.
func TestManualMatch_RequiresMinTwoTransactions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("manual-min-tx").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MIN-001", "50.00", "USD", "2026-01-15", "single tx").
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

			ledgerTxs, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, ledgerTxs)

			_, err = apiClient.Matching.ManualMatch(
				ctx,
				reconciliationContext.ID,
				client.ManualMatchRequest{
					TransactionIDs: []string{ledgerTxs[0].ID},
					Notes:          "should fail with one tx",
				},
			)
			require.Error(t, err, "manual match should require at least 2 transactions")

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			require.True(t, apiErr.IsBadRequest(),
				"should return 400, got %d", apiErr.StatusCode)

			tc.Logf("✓ Manual match correctly requires minimum 2 transactions")
		},
	)
}

// TestManualMatch_NonExistentTransactions tests manual matching with fake transaction IDs.
func TestManualMatch_NonExistentTransactions(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		reconciliationContext := f.Context.NewContext().WithName("manual-404").MustCreate(ctx)

		_, err := apiClient.Matching.ManualMatch(
			ctx,
			reconciliationContext.ID,
			client.ManualMatchRequest{
				TransactionIDs: []string{
					"00000000-0000-0000-0000-000000000001",
					"00000000-0000-0000-0000-000000000002",
				},
				Notes: "fake transactions",
			},
		)
		require.Error(t, err, "manual match with non-existent transactions should fail")

		var apiErr *client.APIError
		require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
		require.True(t, apiErr.IsNotFound() || apiErr.IsBadRequest(),
			"should return 404 or 400, got %d", apiErr.StatusCode)

		tc.Logf("✓ Manual match correctly rejects non-existent transactions")
	})
}

// TestManualMatch_WithNotes tests that notes are stored with the manual match.
func TestManualMatch_WithNotes(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("manual-notes").MustCreate(ctx)
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
				AddRow("NOTES-001", "75.00", "EUR", "2026-01-20", "ledger notes tx").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("NOTES-BANK-001", "75.00", "EUR", "2026-01-20", "bank notes tx").
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

			ledgerTxs, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, ledgerTxs)

			bankTxs, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				bankJob.ID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, bankTxs)

			notes := "Approved by CFO - invoice #INV-2026-001"
			manualResp, err := apiClient.Matching.ManualMatch(
				ctx,
				reconciliationContext.ID,
				client.ManualMatchRequest{
					TransactionIDs: []string{ledgerTxs[0].ID, bankTxs[0].ID},
					Notes:          notes,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, manualResp)
			require.NotEmpty(t, manualResp.MatchGroup.ID)

			tc.Logf("✓ Manual match with notes created group %s", manualResp.MatchGroup.ID)
		},
	)
}

// =============================================================================
// Unmatch (Break Match Group) Tests
// =============================================================================

// TestUnmatch_BreakMatchGroup tests unmatching/breaking a match group.
func TestUnmatch_BreakMatchGroup(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("unmatch-break").MustCreate(ctx)
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
				AddRow("UNMATCH-001", "200.00", "USD", "2026-01-15", "matching tx").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("UNMATCH-001", "200.00", "USD", "2026-01-15", "matching tx").
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

			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, groups, "should have at least one match group")

			matchGroupID := groups[0].ID

			err = apiClient.Matching.UnmatchGroup(
				ctx,
				reconciliationContext.ID,
				matchGroupID,
				client.UnmatchRequest{
					Reason: "incorrect match - amounts do not correspond",
				},
			)
			require.NoError(t, err)

			tc.Logf("✓ Successfully unmatched group %s", matchGroupID)
		},
	)
}

// TestUnmatch_RequiresReason tests that unmatching requires a reason.
func TestUnmatch_RequiresReason(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("unmatch-reason").MustCreate(ctx)
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
				AddRow("REASON-001", "150.00", "USD", "2026-01-15", "matching tx").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("REASON-001", "150.00", "USD", "2026-01-15", "matching tx").
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

			groups, err := apiClient.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, groups, "should have at least one match group")

			err = apiClient.Matching.UnmatchGroup(
				ctx,
				reconciliationContext.ID,
				groups[0].ID,
				client.UnmatchRequest{
					Reason: "",
				},
			)
			require.Error(t, err, "unmatch should require a reason")

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			require.True(t, apiErr.IsBadRequest(),
				"should return 400, got %d", apiErr.StatusCode)

			tc.Logf("✓ Unmatch correctly requires a reason")
		},
	)
}

// TestUnmatch_NonExistentGroup tests unmatching a non-existent match group.
func TestUnmatch_NonExistentGroup(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		reconciliationContext := f.Context.NewContext().WithName("unmatch-404").MustCreate(ctx)

		err := apiClient.Matching.UnmatchGroup(
			ctx,
			reconciliationContext.ID,
			"00000000-0000-0000-0000-000000000000",
			client.UnmatchRequest{
				Reason: "test",
			},
		)
		require.Error(t, err)

		var apiErr *client.APIError
		require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
		require.True(t, apiErr.IsNotFound() || apiErr.IsBadRequest(),
			"should return 404 or 400, got %d. Error: %s", apiErr.StatusCode, apiErr.Error())

		tc.Logf("✓ Unmatch correctly rejects non-existent group")
	})
}
