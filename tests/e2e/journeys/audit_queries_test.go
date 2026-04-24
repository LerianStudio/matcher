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

// TestAuditQueries_ContextCRUDAuditTrail tests audit logs for context operations.
func TestAuditQueries_ContextCRUDAuditTrail(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("audit-ctx").
				WithDescription("initial description").
				MustCreate(ctx)

			name := tc.UniqueName("updated-ctx")
			desc := "updated description"
			_, err := apiClient.Configuration.UpdateContext(
				ctx,
				reconciliationContext.ID,
				client.UpdateContextRequest{
					Name:        &name,
					Description: &desc,
				},
			)
			require.NoError(t, err)

			logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "context", reconciliationContext.ID, 1)
			require.NoError(t, err)

			actions := make(map[string]bool)
			for _, log := range logs {
				actions[log.Action] = true
				require.Equal(t, "context", log.EntityType)
				require.Equal(t, reconciliationContext.ID, log.EntityID)
			}

			tc.Logf("✓ Context audit trail: %d logs, actions: %v", len(logs), actions)
		},
	)
}

// TestAuditQueries_SourceAuditTrail tests audit logs for source operations.
func TestAuditQueries_SourceAuditTrail(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("audit-src").MustCreate(ctx)
			source := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)

			name := tc.UniqueName("updated-src")
			_, err := apiClient.Configuration.UpdateSource(
				ctx,
				reconciliationContext.ID,
				source.ID,
				client.UpdateSourceRequest{
					Name: &name,
				},
			)
			require.NoError(t, err)

			logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "source", source.ID, 1)
			require.NoError(t, err)

			for _, log := range logs {
				require.Equal(t, "source", log.EntityType)
				require.Equal(t, source.ID, log.EntityID)
			}

			tc.Logf("✓ Source audit trail: %d logs", len(logs))
		},
	)
}

// TestAuditQueries_RuleAuditTrail tests audit logs for rule operations.
func TestAuditQueries_RuleAuditTrail(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().WithName("audit-rule").MustCreate(ctx)
			rule := f.Rule.NewRule(reconciliationContext.ID).
				WithPriority(1).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			priority := 2
			_, err := apiClient.Configuration.UpdateMatchRule(
				ctx,
				reconciliationContext.ID,
				rule.ID,
				client.UpdateMatchRuleRequest{
					Priority: &priority,
				},
			)
			require.NoError(t, err)

			logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "match_rule", rule.ID, 1)
			require.NoError(t, err)

			for _, log := range logs {
				require.Equal(t, "match_rule", log.EntityType)
				require.Equal(t, rule.ID, log.EntityID)
			}

			tc.Logf("✓ Rule audit trail: %d logs", len(logs))
		},
	)
}

// TestAuditQueries_FilterByAction tests filtering audit logs by action type.
func TestAuditQueries_FilterByAction(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			f.Context.NewContext().WithName("action-filter-1").MustCreate(ctx)
			f.Context.NewContext().WithName("action-filter-2").MustCreate(ctx)
			f.Context.NewContext().WithName("action-filter-3").MustCreate(ctx)

			createLogs, err := apiClient.Governance.ListAuditLogsByAction(ctx, "create")
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(createLogs), 3, "should have create action logs")

			for _, log := range createLogs {
				require.Equal(t, "create", log.Action)
			}

			tc.Logf("✓ Action filter: found %d 'create' logs", len(createLogs))
		},
	)
}

// TestAuditQueries_FilterByEntityType tests filtering audit logs by entity type.
func TestAuditQueries_FilterByEntityType(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("entity-filter").
				MustCreate(ctx)
			f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			f.Source.NewSource(reconciliationContext.ID).WithName("bank").AsBank().MustCreate(ctx)

			contextLogs, err := apiClient.Governance.ListAuditLogsByEntityType(ctx, "context")
			require.NoError(t, err)
			for _, log := range contextLogs {
				require.Equal(t, "context", log.EntityType)
			}

			sourceLogs, err := apiClient.Governance.ListAuditLogsByEntityType(ctx, "source")
			require.NoError(t, err)
			for _, log := range sourceLogs {
				require.Equal(t, "source", log.EntityType)
			}

			tc.Logf(
				"✓ Entity type filter: %d context logs, %d source logs",
				len(contextLogs),
				len(sourceLogs),
			)
		},
	)
}

// TestAuditQueries_AuditLogDetails tests audit log contains expected details.
func TestAuditQueries_AuditLogDetails(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		2*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, apiClient)

			reconciliationContext := f.Context.NewContext().
				WithName("audit-details").
				WithDescription("test description").
				MustCreate(ctx)

			logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "context", reconciliationContext.ID, 1)
			require.NoError(t, err)

			log := logs[0]
			require.NotEmpty(t, log.ID, "audit log should have ID")
			require.Equal(t, "context", log.EntityType)
			require.Equal(t, reconciliationContext.ID, log.EntityID)
			require.NotEmpty(t, log.Action, "audit log should have action")
			require.False(t, log.CreatedAt.IsZero(), "audit log should have timestamp")

			if log.ID != "" {
				fetchedLog, err := apiClient.Governance.GetAuditLog(ctx, log.ID)
				require.NoError(t, err)
				require.Equal(t, log.ID, fetchedLog.ID)
			}

			tc.Logf("✓ Audit log details verified: ID=%s, Action=%s", log.ID, log.Action)
		},
	)
}
