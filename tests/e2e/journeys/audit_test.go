//go:build e2e

package journeys

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestAudit_ContextCreationAuditLog tests that context creation generates audit log.
func TestAudit_ContextCreationAuditLog(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create a context
		reconciliationContext := f.Context.NewContext().
			WithName("audit-test").
			WithDescription("Testing audit trail").
			MustCreate(ctx)

		// Wait for audit logs (async outbox pattern)
		logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "context", reconciliationContext.ID, 1)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(logs), 1, "should have at least one audit log for creation")

		// Find the create action
		var foundCreate bool
		for _, log := range logs {
			if log.Action == "create" || log.Action == "CREATE" {
				foundCreate = true
				tc.Logf("Found create audit log: %s", log.ID)
				break
			}
		}
		require.True(t, foundCreate, "should have CREATE audit log")

		tc.Logf("✓ Context creation audit log verified")
	})
}

// TestAudit_ContextUpdateAuditLog tests that context updates generate audit logs.
func TestAudit_ContextUpdateAuditLog(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create a context
		reconciliationContext := f.Context.NewContext().WithName("audit-update").MustCreate(ctx)

		// Update the context
		newName := tc.UniqueName("audit-update-modified")
		_, err := apiClient.Configuration.UpdateContext(
			ctx,
			reconciliationContext.ID,
			client.UpdateContextRequest{
				Name: &newName,
			},
		)
		require.NoError(t, err)

		// Wait for audit logs (async outbox pattern)
		logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "context", reconciliationContext.ID, 2)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(logs), 2, "should have audit logs for create and update")

		// Find the update action
		var foundUpdate bool
		for _, log := range logs {
			if log.Action == "update" || log.Action == "UPDATE" {
				foundUpdate = true
				tc.Logf("Found update audit log: %s with changes: %v", log.ID, log.Changes)
				break
			}
		}
		require.True(t, foundUpdate, "should have UPDATE audit log")

		tc.Logf("✓ Context update audit log verified")
	})
}

// TestAudit_SourceLifecycleAuditLogs tests audit logs for source CRUD.
func TestAudit_SourceLifecycleAuditLogs(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		reconciliationContext := f.Context.NewContext().MustCreate(ctx)

		// Create source
		source := f.Source.NewSource(reconciliationContext.ID).
			WithName("audit-source").
			AsLedger().
			MustCreate(ctx)

		// Update source
		newName := tc.UniqueName("audit-source-updated")
		_, err := apiClient.Configuration.UpdateSource(
			ctx,
			reconciliationContext.ID,
			source.ID,
			client.UpdateSourceRequest{
				Name: &newName,
			},
		)
		require.NoError(t, err)

		// Wait for audit logs (async outbox pattern)
		logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "source", source.ID, 1)
		require.NoError(t, err)

		tc.Logf("Found %d audit logs for source", len(logs))
		for _, log := range logs {
			tc.Logf("  - %s: %s", log.Action, log.ID)
		}

		tc.Logf("✓ Source lifecycle audit logs verified")
	})
}

// TestAudit_MatchRuleAuditLogs tests audit logs for rule operations.
func TestAudit_MatchRuleAuditLogs(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		reconciliationContext := f.Context.NewContext().MustCreate(ctx)

		// Create rule
		rule := f.Rule.NewRule(reconciliationContext.ID).
			WithPriority(1).
			Exact().
			WithExactConfig(true, true).
			MustCreate(ctx)

		// Update rule priority
		newPriority := 5
		_, err := apiClient.Configuration.UpdateMatchRule(
			ctx,
			reconciliationContext.ID,
			rule.ID,
			client.UpdateMatchRuleRequest{
				Priority: &newPriority,
			},
		)
		require.NoError(t, err)

		// Wait for audit logs (async outbox pattern)
		logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "match_rule", rule.ID, 1)
		require.NoError(t, err)

		tc.Logf("Found %d audit logs for match rule", len(logs))

		tc.Logf("✓ Match rule audit logs verified")
	})
}

// TestAudit_AuditLogRetrieval tests retrieving a specific audit log by ID.
func TestAudit_AuditLogRetrieval(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create something that generates an audit log
		reconciliationContext := f.Context.NewContext().WithName("audit-retrieval").MustCreate(ctx)

		// Wait for audit logs (async outbox pattern)
		logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "context", reconciliationContext.ID, 1)
		require.NoError(t, err)
		require.NotEmpty(t, logs, "should have audit logs")

		// Retrieve specific audit log by ID
		firstLog := logs[0]
		fetched, err := apiClient.Governance.GetAuditLog(ctx, firstLog.ID)
		require.NoError(t, err)
		require.Equal(t, firstLog.ID, fetched.ID)
		require.Equal(t, firstLog.EntityType, fetched.EntityType)
		require.Equal(t, firstLog.EntityID, fetched.EntityID)

		tc.Logf("✓ Audit log retrieval verified: %s", fetched.ID)
	})
}
