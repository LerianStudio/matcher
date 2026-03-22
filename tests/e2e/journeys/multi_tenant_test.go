//go:build e2e

package journeys

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// skipIfAuthDisabled skips the test when AUTH_ENABLED is not "true".
// Multi-tenant isolation tests require authentication to be enabled because
// the tenant extractor middleware ignores X-Tenant-ID headers when auth is
// disabled, defaulting all requests to the same tenant.
func skipIfAuthDisabled(t *testing.T) {
	t.Helper()

	if os.Getenv("AUTH_ENABLED") != "true" {
		t.Skip("Multi-tenant isolation tests require AUTH_ENABLED=true")
	}
}

// TestMultiTenant_Isolation verifies that tenant A cannot see tenant B's data.
func TestMultiTenant_Isolation(t *testing.T) {
	skipIfAuthDisabled(t)
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			// Generate two unique tenant IDs
			tenantA := uuid.New().String()
			tenantB := uuid.New().String()

			tc.Logf(
				"Testing isolation between Tenant A (%s) and Tenant B (%s)",
				tenantA[:8],
				tenantB[:8],
			)

			// Create data as Tenant A
			apiClient.SetTenantID(tenantA)
			f := factories.New(tc, apiClient)

			contextA := f.Context.NewContext().WithName("tenant-a-ctx").MustCreate(ctx)
			tc.Logf("Tenant A created context: %s", contextA.ID)

			sourceA := f.Source.NewSource(contextA.ID).
				WithName("tenant-a-src").
				AsLedger().
				MustCreate(ctx)
			tc.Logf("Tenant A created source: %s", sourceA.ID)

			// Verify Tenant A can see their own data
			contexts, err := apiClient.Configuration.ListContexts(ctx)
			require.NoError(t, err)
			foundContextA := false
			for _, c := range contexts {
				if c.ID == contextA.ID {
					foundContextA = true
					break
				}
			}
			require.True(t, foundContextA, "Tenant A should see their own context")

			// Switch to Tenant B
			apiClient.SetTenantID(tenantB)

			// Tenant B should NOT see Tenant A's context
			contextsB, err := apiClient.Configuration.ListContexts(ctx)
			require.NoError(t, err)
			for _, c := range contextsB {
				require.NotEqual(t, contextA.ID, c.ID, "Tenant B should not see Tenant A's context")
			}
			tc.Logf("Tenant B correctly cannot see Tenant A's contexts")

			// Tenant B trying to access Tenant A's context directly should fail
			_, err = apiClient.Configuration.GetContext(ctx, contextA.ID)
			require.Error(t, err, "Tenant B should not be able to access Tenant A's context")

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsNotFound() || apiErr.StatusCode == 403,
					"Should return 404 or 403, got %d", apiErr.StatusCode)
			}
			tc.Logf("Tenant B correctly denied access to Tenant A's context")

			// Create Tenant B's own data
			fB := factories.New(tc, apiClient)
			contextB := fB.Context.NewContext().WithName("tenant-b-ctx").MustCreate(ctx)
			tc.Logf("Tenant B created context: %s", contextB.ID)

			// Verify Tenant B sees only their own context
			contextsB2, err := apiClient.Configuration.ListContexts(ctx)
			require.NoError(t, err)
			foundContextB := false
			for _, c := range contextsB2 {
				require.NotEqual(
					t,
					contextA.ID,
					c.ID,
					"Tenant B still should not see Tenant A's context",
				)
				if c.ID == contextB.ID {
					foundContextB = true
				}
			}
			require.True(t, foundContextB, "Tenant B should see their own context")

			// Switch back to Tenant A and verify isolation is bidirectional
			apiClient.SetTenantID(tenantA)
			contextsA2, err := apiClient.Configuration.ListContexts(ctx)
			require.NoError(t, err)
			for _, c := range contextsA2 {
				require.NotEqual(t, contextB.ID, c.ID, "Tenant A should not see Tenant B's context")
			}
			tc.Logf("Tenant A correctly cannot see Tenant B's contexts")

			// Reset to default tenant for cleanup
			apiClient.SetTenantID(tc.Config().DefaultTenantID)

			tc.Logf("✓ Multi-tenant isolation verified successfully")
		},
	)
}

// TestMultiTenant_SourceIsolation verifies source-level tenant isolation.
func TestMultiTenant_SourceIsolation(t *testing.T) {
	skipIfAuthDisabled(t)
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			tenantA := uuid.New().String()
			tenantB := uuid.New().String()

			// Create context and source as Tenant A
			apiClient.SetTenantID(tenantA)
			f := factories.New(tc, apiClient)

			contextA := f.Context.NewContext().WithName("src-isolation").MustCreate(ctx)
			sourceA := f.Source.NewSource(contextA.ID).
				WithName("isolated-src").
				AsLedger().
				MustCreate(ctx)

			// Switch to Tenant B
			apiClient.SetTenantID(tenantB)

			// Tenant B trying to list sources in Tenant A's context should fail
			_, err := apiClient.Configuration.ListSources(ctx, contextA.ID)
			require.Error(t, err, "Tenant B should not be able to list Tenant A's sources")

			// Tenant B trying to access Tenant A's source directly should fail
			_, err = apiClient.Configuration.GetSource(ctx, contextA.ID, sourceA.ID)
			require.Error(t, err, "Tenant B should not be able to access Tenant A's source")

			// Reset to default tenant
			apiClient.SetTenantID(tc.Config().DefaultTenantID)

			tc.Logf("✓ Source-level tenant isolation verified")
		},
	)
}

// TestMultiTenant_RuleIsolation verifies rule-level tenant isolation.
func TestMultiTenant_RuleIsolation(t *testing.T) {
	skipIfAuthDisabled(t)
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			tenantA := uuid.New().String()
			tenantB := uuid.New().String()

			// Create context and rule as Tenant A
			apiClient.SetTenantID(tenantA)
			f := factories.New(tc, apiClient)

			contextA := f.Context.NewContext().WithName("rule-isolation").MustCreate(ctx)
			ruleA := f.Rule.NewRule(contextA.ID).Exact().WithExactConfig(true, true).MustCreate(ctx)

			// Switch to Tenant B
			apiClient.SetTenantID(tenantB)

			// Tenant B trying to list rules in Tenant A's context should fail
			_, err := apiClient.Configuration.ListMatchRules(ctx, contextA.ID)
			require.Error(t, err, "Tenant B should not be able to list Tenant A's rules")

			// Tenant B trying to access Tenant A's rule directly should fail
			_, err = apiClient.Configuration.GetMatchRule(ctx, contextA.ID, ruleA.ID)
			require.Error(t, err, "Tenant B should not be able to access Tenant A's rule")

			// Reset to default tenant
			apiClient.SetTenantID(tc.Config().DefaultTenantID)

			tc.Logf("✓ Rule-level tenant isolation verified")
		},
	)
}

// TestMultiTenant_IngestionIsolation verifies that ingestion jobs are tenant-isolated.
func TestMultiTenant_IngestionIsolation(t *testing.T) {
	skipIfAuthDisabled(t)
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			tenantA := uuid.New().String()
			tenantB := uuid.New().String()

			// Create full setup and run ingestion as Tenant A
			apiClient.SetTenantID(tenantA)
			f := factories.New(tc, apiClient)

			contextA := f.Context.NewContext().WithName("ingest-isolation").MustCreate(ctx)
			sourceA := f.Source.NewSource(contextA.ID).WithName("src").AsLedger().MustCreate(ctx)
			f.Source.NewFieldMap(contextA.ID, sourceA.ID).WithStandardMapping().MustCreate(ctx)

			csv := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("TX-001", "100.00", "USD", "2026-01-15", "tenant a tx").
				Build()

			jobA, err := apiClient.Ingestion.UploadCSV(
				ctx,
				contextA.ID,
				sourceA.ID,
				"data.csv",
				csv,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, contextA.ID, jobA.ID))

			// Switch to Tenant B
			apiClient.SetTenantID(tenantB)

			// Tenant B trying to list jobs in Tenant A's context should fail
			_, err = apiClient.Ingestion.ListJobsByContext(ctx, contextA.ID)
			require.Error(t, err, "Tenant B should not be able to list Tenant A's jobs")

			// Tenant B trying to access Tenant A's job directly should fail
			_, err = apiClient.Ingestion.GetJob(ctx, contextA.ID, jobA.ID)
			require.Error(t, err, "Tenant B should not be able to access Tenant A's job")

			// Reset to default tenant
			apiClient.SetTenantID(tc.Config().DefaultTenantID)

			tc.Logf("✓ Ingestion job tenant isolation verified")
		},
	)
}

// TestMultiTenant_MatchRunIsolation verifies that match runs are tenant-isolated.
func TestMultiTenant_MatchRunIsolation(t *testing.T) {
	skipIfAuthDisabled(t)
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			tenantA := uuid.New().String()
			tenantB := uuid.New().String()

			// Create full reconciliation setup as Tenant A
			apiClient.SetTenantID(tenantA)
			f := factories.New(tc, apiClient)

			contextA := f.Context.NewContext().WithName("match-isolation").MustCreate(ctx)
			ledgerA := f.Source.NewSource(contextA.ID).WithName("ledger").AsLedger().MustCreate(ctx)
			bankA := f.Source.NewSource(contextA.ID).WithName("bank").AsBank().MustCreate(ctx)

			f.Source.NewFieldMap(contextA.ID, ledgerA.ID).WithStandardMapping().MustCreate(ctx)
			f.Source.NewFieldMap(contextA.ID, bankA.ID).WithStandardMapping().MustCreate(ctx)
			f.Rule.NewRule(contextA.ID).Exact().WithExactConfig(true, true).MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MATCH-001", "100.00", "USD", "2026-01-15", "ledger").
				Build()
			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("MATCH-001", "100.00", "USD", "2026-01-15", "bank").
				Build()

			ledgerJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				contextA.ID,
				ledgerA.ID,
				"ledger.csv",
				ledgerCSV,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForJobComplete(ctx, tc, apiClient, contextA.ID, ledgerJob.ID),
			)

			bankJob, err := apiClient.Ingestion.UploadCSV(
				ctx,
				contextA.ID,
				bankA.ID,
				"bank.csv",
				bankCSV,
			)
			require.NoError(t, err)
			require.NoError(t, e2e.WaitForJobComplete(ctx, tc, apiClient, contextA.ID, bankJob.ID))

			matchResp, err := apiClient.Matching.RunMatchCommit(ctx, contextA.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(ctx, tc, apiClient, contextA.ID, matchResp.RunID),
			)

			// Verify Tenant A can see their match run
			runA, err := apiClient.Matching.GetMatchRun(ctx, contextA.ID, matchResp.RunID)
			require.NoError(t, err)
			require.Equal(t, "completed", runA.Status)

			// Switch to Tenant B
			apiClient.SetTenantID(tenantB)

			// Tenant B trying to access Tenant A's match run should fail
			_, err = apiClient.Matching.GetMatchRun(ctx, contextA.ID, matchResp.RunID)
			require.Error(t, err, "Tenant B should not be able to access Tenant A's match run")

			// Tenant B trying to get match results should fail
			_, err = apiClient.Matching.GetMatchRunResults(ctx, contextA.ID, matchResp.RunID)
			require.Error(t, err, "Tenant B should not be able to access Tenant A's match results")

			// Reset to default tenant
			apiClient.SetTenantID(tc.Config().DefaultTenantID)

			tc.Logf("✓ Match run tenant isolation verified")
		},
	)
}

// TestMultiTenant_FeeRuleIsolation verifies that fee rules are tenant-isolated.
// Fee rules support direct-access endpoints (GET/PATCH/DELETE /fee-rules/{id})
// that don't carry a context ID in the path. Cross-tenant protection relies on
// the handler's contextVerifier confirming the fee rule's parent context belongs
// to the requesting tenant, plus schema-per-tenant isolation at the DB layer.
func TestMultiTenant_FeeRuleIsolation(t *testing.T) {
	skipIfAuthDisabled(t)
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()

			tenantA := uuid.New().String()
			tenantB := uuid.New().String()

			// Create context, fee schedule, and fee rule as Tenant A
			apiClient.SetTenantID(tenantA)
			f := factories.New(tc, apiClient)

			contextA := f.Context.NewContext().WithName("fee-rule-isolation").MustCreate(ctx)
			scheduleA := f.FeeSchedule.NewFeeSchedule().
				WithName("isolation-schedule").
				WithCurrency("USD").
				Parallel().
				WithFlatFee("flat", 1, "1.00").
				MustCreate(ctx)
			feeRuleA := f.FeeRule.NewFeeRule(contextA.ID).
				WithName("isolated-fee-rule").
				Left().
				WithFeeScheduleID(scheduleA.ID).
				WithPriority(1).
				WithEqualsPredicate("brand", "visa").
				MustCreate(ctx)

			// Verify Tenant A can access their own fee rule via direct endpoint
			fetched, err := apiClient.Configuration.GetFeeRule(ctx, feeRuleA.ID)
			require.NoError(t, err)
			require.Equal(t, feeRuleA.ID, fetched.ID)
			tc.Logf("Tenant A can access their own fee rule: %s", feeRuleA.ID)

			// Switch to Tenant B
			apiClient.SetTenantID(tenantB)

			// Tenant B trying to GET Tenant A's fee rule directly should fail
			_, err = apiClient.Configuration.GetFeeRule(ctx, feeRuleA.ID)
			require.Error(t, err, "Tenant B should not be able to GET Tenant A's fee rule")

			var apiErr *client.APIError
			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsNotFound() || apiErr.StatusCode == 403,
					"Should return 404 or 403, got %d", apiErr.StatusCode)
			}
			tc.Logf("Tenant B correctly denied GET access to Tenant A's fee rule")

			// Tenant B trying to UPDATE Tenant A's fee rule should fail
			newName := tc.UniqueName("hijacked")
			_, err = apiClient.Configuration.UpdateFeeRule(ctx, feeRuleA.ID, client.UpdateFeeRuleRequest{
				Name: &newName,
			})
			require.Error(t, err, "Tenant B should not be able to UPDATE Tenant A's fee rule")

			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsNotFound() || apiErr.StatusCode == 403,
					"Update should return 404 or 403, got %d", apiErr.StatusCode)
			}
			tc.Logf("Tenant B correctly denied UPDATE access to Tenant A's fee rule")

			// Tenant B trying to DELETE Tenant A's fee rule should fail
			err = apiClient.Configuration.DeleteFeeRule(ctx, feeRuleA.ID)
			require.Error(t, err, "Tenant B should not be able to DELETE Tenant A's fee rule")

			if errors.As(err, &apiErr) {
				require.True(t, apiErr.IsNotFound() || apiErr.StatusCode == 403,
					"Delete should return 404 or 403, got %d", apiErr.StatusCode)
			}
			tc.Logf("Tenant B correctly denied DELETE access to Tenant A's fee rule")

			// Tenant B trying to LIST fee rules in Tenant A's context should fail
			_, err = apiClient.Configuration.ListFeeRules(ctx, contextA.ID)
			require.Error(t, err, "Tenant B should not be able to list Tenant A's fee rules")
			tc.Logf("Tenant B correctly denied LIST access to Tenant A's fee rules")

			// Switch back to Tenant A and verify the fee rule is intact
			apiClient.SetTenantID(tenantA)
			intact, err := apiClient.Configuration.GetFeeRule(ctx, feeRuleA.ID)
			require.NoError(t, err)
			require.Equal(t, "isolated-fee-rule", intact.Name,
				"fee rule must be unmodified after cross-tenant access attempts")

			// Reset to default tenant
			apiClient.SetTenantID(tc.Config().DefaultTenantID)

			tc.Logf("✓ Fee rule tenant isolation verified (GET/UPDATE/DELETE/LIST)")
		},
	)
}
