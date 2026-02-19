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

// TestIgnoreTransaction_MarkAsPending tests ignoring a pending transaction.
func TestIgnoreTransaction_MarkAsPending(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("ignore-pending").
				MustCreate(ctx)

			source := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, source.ID).
				WithStandardMapping().
				MustCreate(ctx)

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("IGN-PEND-001", "100.00", "USD", "2026-01-15", "pending ignore test").
				Build()

			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				source.ID,
				"ledger.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID),
			)

			txs, err := apiClient.Ingestion.ListTransactionsByJob(ctx, reconciliationContext.ID, job.ID)
			require.NoError(t, err)
			require.NotEmpty(t, txs)

			txID := txs[0].ID

			resp, err := apiClient.Ingestion.IgnoreTransaction(ctx, reconciliationContext.ID, txID, client.IgnoreTransactionRequest{
				Reason: "Duplicate entry - already processed manually",
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, txID, resp.ID)

			tc.Logf("✓ Transaction ignored successfully")
		},
	)
}

// TestIgnoreTransaction_ExcludedFromMatching tests that ignored transactions are excluded from match runs.
func TestIgnoreTransaction_ExcludedFromMatching(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("ignore-exclude").
				OneToOne().
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
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("IGN-001", "100.00", "USD", "2026-01-15", "ledger tx 1").
				AddRow("IGN-002", "200.00", "USD", "2026-01-16", "ledger tx 2").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("IGN-001", "100.00", "USD", "2026-01-15", "bank tx 1").
				AddRow("IGN-002", "200.00", "USD", "2026-01-16", "bank tx 2").
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
			require.Len(t, ledgerTxs, 2)

			_, err = apiClient.Ingestion.IgnoreTransaction(
				ctx,
				reconciliationContext.ID,
				ledgerTxs[0].ID,
				client.IgnoreTransactionRequest{
					Reason: "Ignoring first transaction for matching exclusion test",
				},
			)
			require.NoError(t, err)
			tc.Logf("Ignored ledger transaction: %s", ledgerTxs[0].ID)

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
			require.Equal(t, 1, len(groups), "only IGN-002 should match; ignored IGN-001 should be excluded")

			tc.Logf("✓ Ignored transaction excluded from matching")
		},
	)
}

// TestIgnoreTransaction_RequiresReason tests that ignoring requires a reason.
func TestIgnoreTransaction_RequiresReason(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("ignore-no-reason").
				MustCreate(ctx)

			source := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, source.ID).
				WithStandardMapping().
				MustCreate(ctx)

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("IGN-REASON-001", "50.00", "USD", "2026-01-15", "reason test").
				Build()

			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				source.ID,
				"ledger.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID),
			)

			txs, err := apiClient.Ingestion.ListTransactionsByJob(ctx, reconciliationContext.ID, job.ID)
			require.NoError(t, err)
			require.NotEmpty(t, txs)

			_, err = apiClient.Ingestion.IgnoreTransaction(
				ctx,
				reconciliationContext.ID,
				txs[0].ID,
				client.IgnoreTransactionRequest{
					Reason: "",
				},
			)
			require.Error(t, err, "ignoring without reason should fail")

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			require.True(t, apiErr.IsBadRequest(),
				"should return 400, got %d", apiErr.StatusCode)

			tc.Logf("✓ Empty reason correctly rejected")
		},
	)
}

// TestIgnoreTransaction_NonExistentTransaction tests ignoring a non-existent transaction.
func TestIgnoreTransaction_NonExistentTransaction(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("ignore-not-found").
				MustCreate(ctx)

			_, err := apiClient.Ingestion.IgnoreTransaction(
				ctx,
				reconciliationContext.ID,
				"00000000-0000-0000-0000-000000000000",
				client.IgnoreTransactionRequest{
					Reason: "Testing non-existent transaction",
				},
			)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			require.True(t, apiErr.IsNotFound(),
				"should return 404, got %d", apiErr.StatusCode)

			tc.Logf("✓ Non-existent transaction correctly returns error")
		},
	)
}

// TestIgnoreTransaction_AlreadyMatchedTransaction tests ignoring an already matched transaction.
func TestIgnoreTransaction_AlreadyMatchedTransaction(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("ignore-matched").
				OneToOne().
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
				AddRow("IGN-MATCH-001", "300.00", "USD", "2026-01-15", "matched tx").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("IGN-MATCH-001", "300.00", "USD", "2026-01-15", "matched tx").
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

			ledgerTxs, err := apiClient.Ingestion.ListTransactionsByJob(
				ctx,
				reconciliationContext.ID,
				ledgerJob.ID,
			)
			require.NoError(t, err)
			require.NotEmpty(t, ledgerTxs)

			_, err = apiClient.Ingestion.IgnoreTransaction(
				ctx,
				reconciliationContext.ID,
				ledgerTxs[0].ID,
				client.IgnoreTransactionRequest{
					Reason: "Attempting to ignore already matched transaction",
				},
			)
			require.Error(t, err, "ignoring already matched transaction should fail")

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			require.True(t, apiErr.IsConflict(),
				"should return 409 Conflict, got %d", apiErr.StatusCode)

			tc.Logf("✓ Already matched transaction correctly rejected")
		},
	)
}

// TestIgnoreTransaction_AlreadyIgnored tests ignoring a transaction that was already ignored.
func TestIgnoreTransaction_AlreadyIgnored(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("ignore-duplicate").
				MustCreate(ctx)

			source := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, source.ID).
				WithStandardMapping().
				MustCreate(ctx)

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("IGN-DUP-001", "75.00", "USD", "2026-01-15", "duplicate ignore test").
				Build()

			job, err := apiClient.Ingestion.UploadCSV(
				ctx,
				reconciliationContext.ID,
				source.ID,
				"ledger.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, reconciliationContext.ID, job.ID),
			)

			txs, err := apiClient.Ingestion.ListTransactionsByJob(ctx, reconciliationContext.ID, job.ID)
			require.NoError(t, err)
			require.NotEmpty(t, txs)

			txID := txs[0].ID

			_, err = apiClient.Ingestion.IgnoreTransaction(ctx, reconciliationContext.ID, txID, client.IgnoreTransactionRequest{
				Reason: "First ignore",
			})
			require.NoError(t, err)

			_, err = apiClient.Ingestion.IgnoreTransaction(ctx, reconciliationContext.ID, txID, client.IgnoreTransactionRequest{
				Reason: "Second ignore attempt",
			})
			require.Error(t, err, "ignoring already ignored transaction should fail")

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr), "expected *client.APIError, got %T", err)
			require.True(t, apiErr.IsConflict(),
				"should return 409 Conflict, got %d", apiErr.StatusCode)

			tc.Logf("✓ Already ignored transaction correctly rejected")
		},
	)
}
