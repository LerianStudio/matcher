//go:build e2e

package journeys

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestConfigCRUD_ContextLifecycle tests full CRUD operations on contexts.
func TestConfigCRUD_ContextLifecycle(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create
		created := f.Context.NewContext().
			WithName("crud-test").
			OneToOne().
			WithInterval("0 0 * * *").
			WithDescription("Test context for CRUD operations").
			MustCreate(ctx)

		require.NotEmpty(t, created.ID)
		require.Contains(t, created.Name, "crud-test")
		tc.Logf("Created context: %s", created.ID)

		// Read
		fetched, err := apiClient.Configuration.GetContext(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.Name, fetched.Name)

		// Update
		newName := tc.UniqueName("crud-test-updated")
		updated, err := apiClient.Configuration.UpdateContext(
			ctx,
			created.ID,
			client.UpdateContextRequest{
				Name: &newName,
			},
		)
		require.NoError(t, err)
		require.Equal(t, newName, updated.Name)

		// List - verify we can list contexts (pagination means our context may not be in first page)
		contexts, err := apiClient.Configuration.ListContexts(ctx)
		require.NoError(t, err)
		require.NotNil(t, contexts, "list should return a slice")
		tc.Logf("Listed %d contexts in first page", len(contexts))

		// Verify updated context is still fetchable by ID (read already tested above, but with new name)
		verifyUpdated, err := apiClient.Configuration.GetContext(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, newName, verifyUpdated.Name, "context should have updated name")

		// Delete is handled by cleanup
	})
}

// TestConfigCRUD_SourceLifecycle tests full CRUD operations on sources.
func TestConfigCRUD_SourceLifecycle(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create context first
		reconciliationContext := f.Context.NewContext().MustCreate(ctx)

		// Create source
		source := f.Source.NewSource(reconciliationContext.ID).
			WithName("ledger-source").
			AsLedger().
			WithConfig(map[string]any{"format": "csv"}).
			MustCreate(ctx)

		require.NotEmpty(t, source.ID)
		require.Contains(t, source.Name, "ledger-source")
		tc.Logf("Created source: %s", source.ID)

		// Read
		fetched, err := apiClient.Configuration.GetSource(ctx, reconciliationContext.ID, source.ID)
		require.NoError(t, err)
		require.Equal(t, source.ID, fetched.ID)

		// Update
		newName := tc.UniqueName("ledger-source-updated")
		updated, err := apiClient.Configuration.UpdateSource(
			ctx,
			reconciliationContext.ID,
			source.ID,
			client.UpdateSourceRequest{
				Name: &newName,
			},
		)
		require.NoError(t, err)
		require.Equal(t, newName, updated.Name)

		// List
		sources, err := apiClient.Configuration.ListSources(ctx, reconciliationContext.ID)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(sources), 1)
	})
}

// TestConfigCRUD_MatchRuleLifecycle tests full CRUD operations on match rules.
func TestConfigCRUD_MatchRuleLifecycle(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create context
		reconciliationContext := f.Context.NewContext().MustCreate(ctx)

		// Create exact rule
		exactRule := f.Rule.NewRule(reconciliationContext.ID).
			WithPriority(1).
			Exact().
			WithExactConfig(true, true).
			MustCreate(ctx)

		require.NotEmpty(t, exactRule.ID)
		require.Equal(t, 1, exactRule.Priority)
		require.Equal(t, "EXACT", exactRule.Type)

		// Create tolerance rule
		toleranceRule := f.Rule.NewRule(reconciliationContext.ID).
			WithPriority(2).
			Tolerance().
			WithToleranceConfig("1.00").
			MustCreate(ctx)

		require.NotEmpty(t, toleranceRule.ID)
		require.Equal(t, 2, toleranceRule.Priority)

		// Read
		fetched, err := apiClient.Configuration.GetMatchRule(
			ctx,
			reconciliationContext.ID,
			exactRule.ID,
		)
		require.NoError(t, err)
		require.Equal(t, exactRule.ID, fetched.ID)

		// Update priority
		newPriority := 3
		updated, err := apiClient.Configuration.UpdateMatchRule(
			ctx,
			reconciliationContext.ID,
			exactRule.ID,
			client.UpdateMatchRuleRequest{
				Priority: &newPriority,
			},
		)
		require.NoError(t, err)
		require.Equal(t, newPriority, updated.Priority)

		// List
		rules, err := apiClient.Configuration.ListMatchRules(ctx, reconciliationContext.ID)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(rules), 2)
	})
}

// TestConfigCRUD_FieldMapLifecycle tests full CRUD operations on field maps.
func TestConfigCRUD_FieldMapLifecycle(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create context and source
		reconciliationContext := f.Context.NewContext().MustCreate(ctx)
		source := f.Source.NewSource(reconciliationContext.ID).MustCreate(ctx)

		// Create field map
		fieldMap := f.Source.NewFieldMap(reconciliationContext.ID, source.ID).
			WithStandardMapping().
			MustCreate(ctx)

		require.NotEmpty(t, fieldMap.ID)
		tc.Logf("Created field map: %s", fieldMap.ID)

		// Read
		fetched, err := apiClient.Configuration.GetFieldMapBySource(
			ctx,
			reconciliationContext.ID,
			source.ID,
		)
		require.NoError(t, err)
		require.Equal(t, fieldMap.ID, fetched.ID)

		// Update
		updated, err := apiClient.Configuration.UpdateFieldMap(
			ctx,
			fieldMap.ID,
			client.UpdateFieldMapRequest{
				Mapping: map[string]any{
					"id":          "external_ref",
					"amount":      "total_amount",
					"currency":    "currency_code",
					"date":        "transaction_date",
					"description": "memo",
				},
			},
		)
		require.NoError(t, err)
		require.Equal(t, "external_ref", updated.Mapping["id"])
	})
}
