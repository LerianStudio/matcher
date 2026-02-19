//go:build integration

package configuration

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	fieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	sourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	configQuery "github.com/LerianStudio/matcher/internal/configuration/services/query"
	"github.com/LerianStudio/matcher/tests/integration"
)

// buildUseCases constructs the command and query use cases wired to real Postgres
// repositories through the test harness infrastructure provider.
func buildUseCases(t *testing.T, harness *integration.TestHarness) (*configCommand.UseCase, *configQuery.UseCase) {
	t.Helper()

	provider := harness.Provider()

	ctxRepo := contextRepo.NewRepository(provider)

	srcRepo, err := sourceRepo.NewRepository(provider)
	require.NoError(t, err)

	fmRepo := fieldMapRepo.NewRepository(provider)
	mrRepo := matchRuleRepo.NewRepository(provider)

	cmdUC, err := configCommand.NewUseCase(ctxRepo, srcRepo, fmRepo, mrRepo,
		configCommand.WithInfrastructureProvider(provider))
	require.NoError(t, err)

	queryUC, err := configQuery.NewUseCase(ctxRepo, srcRepo, fmRepo, mrRepo)
	require.NoError(t, err)

	return cmdUC, queryUC
}

// createTestContext is a helper that creates a reconciliation context via the command
// use case and returns it. The name is suffixed with a UUID fragment to guarantee
// uniqueness across concurrent test runs against the same shared database.
func createTestContext(
	t *testing.T,
	cmdUC *configCommand.UseCase,
	harness *integration.TestHarness,
	name string,
) *entities.ReconciliationContext {
	t.Helper()

	ctx := harness.Ctx()

	created, err := cmdUC.CreateContext(ctx, harness.Seed.TenantID, entities.CreateReconciliationContextInput{
		Name:     name + " " + uuid.New().String()[:8],
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	})
	require.NoError(t, err)
	require.NotNil(t, created)

	return created
}

// TestConfigServiceCRUD_ContextLifecycle exercises the full create-read-update-list-delete cycle for reconciliation contexts.
func TestConfigServiceCRUD_ContextLifecycle(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, queryUC := buildUseCases(t, h)
		ctx := h.Ctx()

		// CREATE.
		contextName := "CRUD Context " + uuid.New().String()[:8]
		created, err := cmdUC.CreateContext(ctx, h.Seed.TenantID, entities.CreateReconciliationContextInput{
			Name:     contextName,
			Type:     value_objects.ContextTypeOneToMany,
			Interval: "0 */6 * * *",
		})
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, contextName, created.Name)
		require.Equal(t, value_objects.ContextTypeOneToMany, created.Type)
		require.Equal(t, value_objects.ContextStatusDraft, created.Status)

		// GET.
		fetched, err := queryUC.GetContext(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, contextName, fetched.Name)

		// UPDATE name.
		updatedName := "Updated Context " + uuid.New().String()[:8]
		updated, err := cmdUC.UpdateContext(ctx, created.ID, entities.UpdateReconciliationContextInput{
			Name: &updatedName,
		})
		require.NoError(t, err)
		require.Equal(t, updatedName, updated.Name)

		// LIST — verify present.
		contexts, _, err := queryUC.ListContexts(ctx, "", 50, nil, nil)
		require.NoError(t, err)

		var found bool

		for _, c := range contexts {
			if c.ID == created.ID {
				found = true

				require.Equal(t, updatedName, c.Name)

				break
			}
		}

		require.True(t, found, "created context must appear in list")

		// DELETE.
		err = cmdUC.DeleteContext(ctx, created.ID)
		require.NoError(t, err)

		// GET after delete — verify error.
		_, err = queryUC.GetContext(ctx, created.ID)
		require.Error(t, err)
	})
}

// TestConfigServiceCRUD_SourceLifecycle exercises the full create-read-update-list-delete cycle for reconciliation sources.
func TestConfigServiceCRUD_SourceLifecycle(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, queryUC := buildUseCases(t, h)
		ctx := h.Ctx()

		// Pre-requisite: create a context (sources belong to a context).
		parent := createTestContext(t, cmdUC, h, "Source Parent")

		// CREATE.
		created, err := cmdUC.CreateSource(ctx, parent.ID, entities.CreateReconciliationSourceInput{
			Name:   "Test Ledger Source",
			Type:   value_objects.SourceTypeLedger,
			Config: map[string]any{"endpoint": "https://ledger.example.com"},
		})
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, "Test Ledger Source", created.Name)
		require.Equal(t, value_objects.SourceTypeLedger, created.Type)

		// GET.
		fetched, err := queryUC.GetSource(ctx, parent.ID, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)

		// UPDATE.
		newName := "Updated Ledger Source"
		updated, err := cmdUC.UpdateSource(ctx, parent.ID, created.ID, entities.UpdateReconciliationSourceInput{
			Name: &newName,
		})
		require.NoError(t, err)
		require.Equal(t, newName, updated.Name)

		// LIST by context.
		sources, _, err := queryUC.ListSources(ctx, parent.ID, "", 10, nil)
		require.NoError(t, err)
		require.NotEmpty(t, sources)

		var found bool

		for _, s := range sources {
			if s.ID == created.ID {
				found = true

				require.Equal(t, newName, s.Name)

				break
			}
		}

		require.True(t, found, "created source must appear in list")

		// DELETE.
		err = cmdUC.DeleteSource(ctx, parent.ID, created.ID)
		require.NoError(t, err)

		// GET after delete — verify error.
		_, err = queryUC.GetSource(ctx, parent.ID, created.ID)
		require.Error(t, err)
	})
}

// TestConfigServiceCRUD_FieldMapLifecycle exercises the full create-read-update-delete cycle for field maps.
func TestConfigServiceCRUD_FieldMapLifecycle(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, queryUC := buildUseCases(t, h)
		ctx := h.Ctx()

		// Pre-requisites: context + source.
		parent := createTestContext(t, cmdUC, h, "FieldMap Parent")

		source, err := cmdUC.CreateSource(ctx, parent.ID, entities.CreateReconciliationSourceInput{
			Name:   "FM Source",
			Type:   value_objects.SourceTypeBank,
			Config: map[string]any{},
		})
		require.NoError(t, err)

		// CREATE.
		created, err := cmdUC.CreateFieldMap(ctx, parent.ID, source.ID, entities.CreateFieldMapInput{
			Mapping: map[string]any{
				"amount":   "tx_amount",
				"currency": "tx_currency",
				"date":     "tx_date",
			},
		})
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, 1, created.Version, "initial version must be 1")

		// GET by source.
		fetched, err := queryUC.GetFieldMapBySource(ctx, source.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, "tx_amount", fetched.Mapping["amount"])

		// UPDATE mapping.
		updated, err := cmdUC.UpdateFieldMap(ctx, created.ID, entities.UpdateFieldMapInput{
			Mapping: map[string]any{
				"amount":    "transaction_amount",
				"currency":  "transaction_currency",
				"date":      "transaction_date",
				"reference": "transaction_ref",
			},
		})
		require.NoError(t, err)
		require.Equal(t, 2, updated.Version, "version must increment on update")
		require.Equal(t, "transaction_amount", updated.Mapping["amount"])

		// DELETE.
		err = cmdUC.DeleteFieldMap(ctx, created.ID)
		require.NoError(t, err)

		// GET after delete — verify error.
		_, err = queryUC.GetFieldMap(ctx, created.ID)
		require.Error(t, err)
	})
}

// TestConfigServiceCRUD_MatchRuleLifecycle exercises the full create-read-update-list-delete cycle for match rules.
func TestConfigServiceCRUD_MatchRuleLifecycle(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, queryUC := buildUseCases(t, h)
		ctx := h.Ctx()

		parent := createTestContext(t, cmdUC, h, "MatchRule Parent")

		// CREATE.
		created, err := cmdUC.CreateMatchRule(ctx, parent.ID, entities.CreateMatchRuleInput{
			Priority: 1,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchAmount": true, "matchCurrency": true},
		})
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, 1, created.Priority)
		require.Equal(t, value_objects.RuleTypeExact, created.Type)

		// GET.
		fetched, err := queryUC.GetMatchRule(ctx, parent.ID, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)

		// UPDATE config.
		newConfig := map[string]any{"matchAmount": true, "matchCurrency": true, "matchDate": true}
		updated, err := cmdUC.UpdateMatchRule(ctx, parent.ID, created.ID, entities.UpdateMatchRuleInput{
			Config: newConfig,
		})
		require.NoError(t, err)
		require.Equal(t, true, updated.Config["matchDate"])

		// LIST by context.
		rules, _, err := queryUC.ListMatchRules(ctx, parent.ID, "", 10, nil)
		require.NoError(t, err)
		require.NotEmpty(t, rules)

		var found bool

		for _, r := range rules {
			if r.ID == created.ID {
				found = true

				break
			}
		}

		require.True(t, found, "created rule must appear in list")

		// DELETE.
		err = cmdUC.DeleteMatchRule(ctx, parent.ID, created.ID)
		require.NoError(t, err)

		// GET after delete — verify error.
		_, err = queryUC.GetMatchRule(ctx, parent.ID, created.ID)
		require.Error(t, err)
	})
}

// TestConfigServiceCRUD_CreateContextValidation verifies that creating a context with an empty name returns an error.
func TestConfigServiceCRUD_CreateContextValidation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, _ := buildUseCases(t, h)
		ctx := h.Ctx()

		// Empty name should fail validation.
		_, err := cmdUC.CreateContext(ctx, h.Seed.TenantID, entities.CreateReconciliationContextInput{
			Name:     "",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		})
		require.Error(t, err, "creating a context with an empty name must fail")
	})
}

// TestConfigServiceCRUD_CreateSourceNonExistentContext verifies that creating a source for a non-existent context fails.
func TestConfigServiceCRUD_CreateSourceNonExistentContext(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, _ := buildUseCases(t, h)
		ctx := h.Ctx()

		// Using a random UUID that does not correspond to any context. The source
		// entity constructor requires a non-nil contextID but the repo insert will
		// fail on the FK constraint.
		bogusContextID := uuid.New()

		_, err := cmdUC.CreateSource(ctx, bogusContextID, entities.CreateReconciliationSourceInput{
			Name:   "Orphan Source",
			Type:   value_objects.SourceTypeLedger,
			Config: map[string]any{},
		})
		require.Error(t, err, "creating a source under a non-existent context must fail")
	})
}

// TestConfigServiceCRUD_UpdateNonExistentContext verifies that updating a non-existent context returns an error.
func TestConfigServiceCRUD_UpdateNonExistentContext(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, _ := buildUseCases(t, h)
		ctx := h.Ctx()

		newName := "Ghost"

		_, err := cmdUC.UpdateContext(ctx, uuid.New(), entities.UpdateReconciliationContextInput{
			Name: &newName,
		})
		require.Error(t, err, "updating a non-existent context must fail")
	})
}

// TestConfigServiceCRUD_DeleteNonExistentContext verifies that deleting a non-existent context returns an error.
func TestConfigServiceCRUD_DeleteNonExistentContext(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, _ := buildUseCases(t, h)
		ctx := h.Ctx()

		err := cmdUC.DeleteContext(ctx, uuid.New())
		require.Error(t, err, "deleting a non-existent context must fail")
	})
}

// TestConfigServiceCRUD_RuleReorderPriorities verifies that match rule priorities can be reordered.
func TestConfigServiceCRUD_RuleReorderPriorities(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, queryUC := buildUseCases(t, h)
		ctx := h.Ctx()

		parent := createTestContext(t, cmdUC, h, "Reorder Parent")

		// Create 3 rules with priorities 1, 2, 3.
		rule1, err := cmdUC.CreateMatchRule(ctx, parent.ID, entities.CreateMatchRuleInput{
			Priority: 1,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchAmount": true},
		})
		require.NoError(t, err)

		rule2, err := cmdUC.CreateMatchRule(ctx, parent.ID, entities.CreateMatchRuleInput{
			Priority: 2,
			Type:     value_objects.RuleTypeTolerance,
			Config:   map[string]any{"absTolerance": 0.01},
		})
		require.NoError(t, err)

		rule3, err := cmdUC.CreateMatchRule(ctx, parent.ID, entities.CreateMatchRuleInput{
			Priority: 3,
			Type:     value_objects.RuleTypeDateLag,
			Config:   map[string]any{"minDays": 0, "maxDays": 5},
		})
		require.NoError(t, err)

		// Reorder: 3 → 1st, 1 → 2nd, 2 → 3rd.
		err = cmdUC.ReorderMatchRulePriorities(ctx, parent.ID, []uuid.UUID{rule3.ID, rule1.ID, rule2.ID})
		require.NoError(t, err)

		// Verify new priorities.
		fetchedRule3, err := queryUC.GetMatchRule(ctx, parent.ID, rule3.ID)
		require.NoError(t, err)
		require.Equal(t, 1, fetchedRule3.Priority, "rule3 should now have priority 1")

		fetchedRule1, err := queryUC.GetMatchRule(ctx, parent.ID, rule1.ID)
		require.NoError(t, err)
		require.Equal(t, 2, fetchedRule1.Priority, "rule1 should now have priority 2")

		fetchedRule2, err := queryUC.GetMatchRule(ctx, parent.ID, rule2.ID)
		require.NoError(t, err)
		require.Equal(t, 3, fetchedRule2.Priority, "rule2 should now have priority 3")
	})
}

// TestConfigServiceCRUD_ListContextsPagination verifies that listing contexts respects cursor-based pagination.
func TestConfigServiceCRUD_ListContextsPagination(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		cmdUC, queryUC := buildUseCases(t, h)
		ctx := h.Ctx()

		// Create 5 contexts (the seed data already has 1, but we create 5 fresh ones
		// with known names to reason about deterministically).
		createdIDs := make(map[uuid.UUID]bool, 5)

		for i := range 5 {
			c := createTestContext(t, cmdUC, h, "Page "+string(rune('A'+i)))
			createdIDs[c.ID] = true
		}

		// Request with limit=2 to exercise pagination.
		page1, pagination, err := queryUC.ListContexts(ctx, "", 2, nil, nil)
		require.NoError(t, err)
		require.Len(t, page1, 2, "first page must return exactly the requested limit")

		// Use the cursor from the first page to get the second page.
		require.NotEmpty(t, pagination.Next, "pagination cursor must be set when more results exist")

		page2, _, err := queryUC.ListContexts(ctx, pagination.Next, 2, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, page2, "second page must not be empty")

		// Verify no overlap between page 1 and page 2.
		page1IDs := make(map[uuid.UUID]bool, len(page1))

		for _, c := range page1 {
			page1IDs[c.ID] = true
		}

		for _, c := range page2 {
			require.False(t, page1IDs[c.ID], "page 2 must not contain IDs from page 1")
		}
	})
}
