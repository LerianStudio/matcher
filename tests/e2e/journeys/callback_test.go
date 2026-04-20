//go:build e2e

package journeys

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// =============================================================================
// Callback: Resolved Status Tests
// =============================================================================

// TestCallback_ProcessResolved tests that a callback with RESOLVED status
// transitions a dispatched exception to resolved state.
func TestCallback_ProcessResolved(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			existingOpenIDs, err := listOpenExceptionIDs(ctx, apiClient)
			require.NoError(t, err)

			// Setup: create context, sources, field maps, and rules
			reconciliationContext := f.Context.NewContext().
				WithName("callback-resolved").
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

			// Ingest deliberately non-matching data to generate exceptions
			resolvedTxnID := tc.NamePrefix() + "-CB-RES-001"
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow(resolvedTxnID, "100.00", "USD", "2026-01-15", "callback resolved test").
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

			exceptionID := findNewOpenExceptionID(t, ctx, tc, apiClient, existingOpenIDs)

			// Dispatch the exception to an external system (prerequisite for callback)
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
			require.True(t, dispatchResp.Acknowledged)
			tc.Logf("Exception %s dispatched to MANUAL", exceptionID)

			// Process callback: external system reports RESOLVED
			callbackResp, err := apiClient.Exception.ProcessCallback(
				ctx,
				exceptionID,
				client.ProcessCallbackRequest{
					CallbackType:    "STATUS_UPDATE",
					ExternalSystem:  "MANUAL",
					ExternalIssueID: "MANUAL-12345",
					Status:          "RESOLVED",
					ResolutionNotes: "Fixed externally by operations team",
				},
			)
			require.NoError(t, err)
			require.NotNil(t, callbackResp)
			require.Equal(t, "accepted", callbackResp.Status)

			// Verify exception is now resolved
			updated, err := apiClient.Exception.GetException(ctx, exceptionID)
			require.NoError(t, err)
			require.Equal(t, "RESOLVED", updated.Status)
			tc.Logf("Exception %s resolved via callback", exceptionID)
		},
	)
}

// =============================================================================
// Callback: Assigned Status Tests
// =============================================================================

// TestCallback_ProcessAssigned tests that a callback with ASSIGNED status
// transitions an open exception to assigned state with the given assignee.
func TestCallback_ProcessAssigned(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			existingOpenIDs, err := listOpenExceptionIDs(ctx, apiClient)
			require.NoError(t, err)

			// Setup: create context, sources, field maps, and rules
			reconciliationContext := f.Context.NewContext().
				WithName("callback-assigned").
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

			// Ingest non-matching data
			assignedTxnID := tc.NamePrefix() + "-CB-ASN-001"
			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow(assignedTxnID, "200.00", "USD", "2026-01-16", "callback assign test").
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

			exceptionID := findNewOpenExceptionID(t, ctx, tc, apiClient, existingOpenIDs)

			// Dispatch the exception first
			_, err = apiClient.Exception.DispatchToExternal(
				ctx,
				exceptionID,
				client.DispatchRequest{
					TargetSystem: "MANUAL",
				},
			)
			require.NoError(t, err)
			tc.Logf("Exception %s dispatched to MANUAL", exceptionID)

			// Process callback: external system reports ASSIGNED
			callbackResp, err := apiClient.Exception.ProcessCallback(
				ctx,
				exceptionID,
				client.ProcessCallbackRequest{
					CallbackType:    "STATUS_UPDATE",
					ExternalSystem:  "MANUAL",
					ExternalIssueID: "MANUAL-98765",
					Status:          "ASSIGNED",
					Assignee:        "agent@test.com",
				},
			)
			require.NoError(t, err)
			require.NotNil(t, callbackResp)
			require.Equal(t, "accepted", callbackResp.Status)

			// Verify exception is now assigned
			updated, err := apiClient.Exception.GetException(ctx, exceptionID)
			require.NoError(t, err)
			require.Equal(t, "ASSIGNED", updated.Status)
			require.NotNil(t, updated.AssignedTo)
			require.Equal(t, "agent@test.com", *updated.AssignedTo)
			tc.Logf("Exception %s assigned via callback to %s", exceptionID, *updated.AssignedTo)
		},
	)
}

// =============================================================================
// Callback: Error Cases
// =============================================================================

// TestCallback_NonExistentException tests that processing a callback for a
// non-existent exception returns an error.
func TestCallback_NonExistentException(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		nonExistentID := uuid.New().String()

		_, err := apiClient.Exception.ProcessCallback(
			ctx,
			nonExistentID,
			client.ProcessCallbackRequest{
				CallbackType:    "STATUS_UPDATE",
				ExternalSystem:  "JIRA",
				ExternalIssueID: "JIRA-00000",
				Status:          "RESOLVED",
				ResolutionNotes: "Should fail",
			},
		)
		require.Error(t, err)

		var apiErr *client.APIError
		require.ErrorAs(t, err, &apiErr)
		require.True(t, apiErr.IsNotFound(), "non-existent exception should return 404, got %d", apiErr.StatusCode)
		tc.Logf(
			"Non-existent exception callback correctly rejected with status %d",
			apiErr.StatusCode,
		)
	})
}

func listOpenExceptionIDs(
	ctx context.Context,
	apiClient *e2e.Client,
) (map[string]struct{}, error) {
	exceptions, err := apiClient.Exception.ListExceptions(ctx, client.ExceptionListFilter{
		Status:    "OPEN",
		Limit:     1000,
		SortBy:    "created_at",
		SortOrder: "desc",
	})
	if err != nil {
		return nil, err
	}

	ids := make(map[string]struct{}, len(exceptions.Items))
	for _, exception := range exceptions.Items {
		ids[exception.ID] = struct{}{}
	}

	return ids, nil
}

func findNewOpenExceptionID(
	t *testing.T,
	ctx context.Context,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	existing map[string]struct{},
) string {
	t.Helper()

	result, err := e2e.EventuallyWithResult(ctx, e2e.DefaultPollOptions(tc.Config()), func() (*string, error) {
		exceptions, err := apiClient.Exception.ListExceptions(ctx, client.ExceptionListFilter{
			Status:    "OPEN",
			Limit:     1000,
			SortBy:    "created_at",
			SortOrder: "desc",
		})
		if err != nil {
			return nil, err
		}

		for _, exception := range exceptions.Items {
			if _, alreadyPresent := existing[exception.ID]; !alreadyPresent {
				id := exception.ID
				return &id, nil
			}
		}

		return nil, nil
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	return *result
}

// TestCallback_InvalidPayload_MissingExternalSystem tests that a callback
// with a missing externalSystem field is rejected with 400.
func TestCallback_InvalidPayload_MissingExternalSystem(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		_, err := apiClient.Exception.ProcessCallback(
			ctx,
			"00000000-0000-0000-0000-000000000001",
			client.ProcessCallbackRequest{
				CallbackType:    "STATUS_UPDATE",
				ExternalSystem:  "", // required field missing
				ExternalIssueID: "JIRA-00001",
				Status:          "RESOLVED",
			},
		)
		require.Error(t, err)

		var apiErr *client.APIError
		require.ErrorAs(t, err, &apiErr, "error should be APIError for validation failures")
		require.True(t, apiErr.IsBadRequest(),
			"missing externalSystem should return 400, got %d", apiErr.StatusCode)

		tc.Logf("Missing externalSystem correctly rejected")
	})
}

// TestCallback_InvalidPayload_MissingStatus tests that a callback with
// a missing status field is rejected.
func TestCallback_InvalidPayload_MissingStatus(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		_, err := apiClient.Exception.ProcessCallback(
			ctx,
			"00000000-0000-0000-0000-000000000002",
			client.ProcessCallbackRequest{
				CallbackType:    "STATUS_UPDATE",
				ExternalSystem:  "JIRA",
				ExternalIssueID: "JIRA-00002",
				Status:          "", // required field missing
			},
		)
		require.Error(t, err)

		var apiErr *client.APIError
		require.ErrorAs(t, err, &apiErr, "error should be APIError for validation failures")
		require.True(t, apiErr.IsBadRequest(),
			"missing status should return 400, got %d", apiErr.StatusCode)

		tc.Logf("Missing status correctly rejected")
	})
}

// TestCallback_InvalidPayload_MissingExternalIssueID tests that a callback
// with a missing externalIssueId field is rejected.
func TestCallback_InvalidPayload_MissingExternalIssueID(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()

		_, err := apiClient.Exception.ProcessCallback(
			ctx,
			"00000000-0000-0000-0000-000000000003",
			client.ProcessCallbackRequest{
				CallbackType:    "STATUS_UPDATE",
				ExternalSystem:  "JIRA",
				ExternalIssueID: "", // required field missing
				Status:          "RESOLVED",
			},
		)
		require.Error(t, err)

		var apiErr *client.APIError
		require.ErrorAs(t, err, &apiErr, "error should be APIError for validation failures")
		require.True(t, apiErr.IsBadRequest(),
			"missing externalIssueId should return 400, got %d", apiErr.StatusCode)

		tc.Logf("Missing externalIssueId correctly rejected")
	})
}
