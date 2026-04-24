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

// TestDispatch_ToManualSystem tests dispatching an exception to MANUAL system.
func TestDispatch_ToManualSystem(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			// Setup: create unmatched transactions to generate exceptions
			reconciliationContext := f.Context.NewContext().
				WithName("dispatch-manual").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").AsLedger().MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").AsBank().MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().WithExactConfig(true, true).MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DISP-001", "100.00", "USD", "2026-01-15", "dispatch test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DISP-BANK-XXX", "999.00", "USD", "2026-01-15", "no match").
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
			require.NotEmpty(t, exceptions.Items, "should have open exceptions")

			exceptionID := exceptions.Items[0].ID

			dispatchResp, err := apiClient.Exception.DispatchToExternal(
				ctx,
				exceptionID,
				client.DispatchRequest{
					TargetSystem: "MANUAL",
					Queue:        "RECON-TEAM",
				},
			)
			require.NoError(t, err)
			require.NotNil(t, dispatchResp)
			require.Equal(t, exceptionID, dispatchResp.ExceptionID)
			require.Equal(t, "MANUAL", dispatchResp.Target)
			require.True(t, dispatchResp.Acknowledged)
			tc.Logf("✓ Exception dispatched to MANUAL system")
		},
	)
}

// TestDispatch_ManualMultipleExceptions tests dispatching multiple exceptions to MANUAL system.
func TestDispatch_ManualMultipleExceptions(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("dispatch-targets").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").AsLedger().MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").AsBank().MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().WithExactConfig(true, true).MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DISP-TGT-001", "100.00", "USD", "2026-01-15", "target test 1").
				AddRow("DISP-TGT-002", "200.00", "USD", "2026-01-16", "target test 2").
				AddRow("DISP-TGT-003", "300.00", "USD", "2026-01-17", "target test 3").
				AddRow("DISP-TGT-004", "400.00", "USD", "2026-01-18", "target test 4").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("BANK-NO-MATCH-001", "999.00", "USD", "2026-01-15", "no match").
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
			require.GreaterOrEqual(t, len(exceptions.Items), 4, "need at least 4 exceptions")

			// Index access is safe: ordering is deterministic within a single
			// test run since all exceptions were just created.
			for i := range 4 {
				tc.Logf("Testing dispatch of exception %d to MANUAL", i)
				resp, err := apiClient.Exception.DispatchToExternal(
					ctx,
					exceptions.Items[i].ID,
					client.DispatchRequest{
						TargetSystem: "MANUAL",
					},
				)
				require.NoError(t, err, "dispatch of exception %d should succeed", i)
				require.Equal(t, "MANUAL", resp.Target)
				tc.Logf("✓ Dispatched exception %d to MANUAL", i)
			}
		},
	)
}

// TestDispatch_NonExistentException tests dispatching a non-existent exception.
func TestDispatch_NonExistentException(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		_, err := apiClient.Exception.DispatchToExternal(
			ctx,
			"00000000-0000-0000-0000-000000000000",
			client.DispatchRequest{
				TargetSystem: "MANUAL",
			},
		)
		require.Error(t, err)
		var apiErr *client.APIError
		require.ErrorAs(t, err, &apiErr)
		tc.Logf(
			"✓ Non-existent exception dispatch correctly rejected with status %d",
			apiErr.StatusCode,
		)
	})
}

// TestDispatch_WithQueue tests dispatching with a queue assignment.
func TestDispatch_WithQueue(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("dispatch-queue").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").AsLedger().MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").AsBank().MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().WithExactConfig(true, true).MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DISP-Q-001", "100.00", "USD", "2026-01-15", "queue test").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("DISP-Q-NOMATCH", "999.00", "USD", "2026-01-15", "no match").
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

			resp, err := apiClient.Exception.DispatchToExternal(
				ctx,
				exceptions.Items[0].ID,
				client.DispatchRequest{
					TargetSystem: "MANUAL",
					Queue:        "FINANCE-RECON-TEAM",
				},
			)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, "MANUAL", resp.Target)
			tc.Logf("✓ Exception dispatched with queue assignment")
		},
	)
}
