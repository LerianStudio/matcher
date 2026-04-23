//go:build integration

package configuration

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	fieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	matchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	sourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestContextRepository_UniqueNameConstraint(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity1, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Duplicate Name Context",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		_, err = repo.Create(ctx, entity1)
		require.NoError(t, err)

		entity2, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Duplicate Name Context",
				Type:     shared.ContextTypeOneToMany,
				Interval: "0 */6 * * *",
			},
		)
		require.NoError(t, err)

		_, err = repo.Create(ctx, entity2)
		require.Error(t, err)
	})
}

func TestSourceRepository_ForeignKeyConstraint(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		nonExistentContextID := uuid.New()

		entity, err := entities.NewReconciliationSource(
			ctx,
			nonExistentContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Orphan Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)

		_, err = repo.Create(ctx, entity)
		require.Error(t, err)
	})
}

func TestFieldMapRepository_ForeignKeyConstraint(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := fieldMapRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentSourceID := uuid.New()

		entity, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			nonExistentSourceID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{"amount": "amt"},
			},
		)
		require.NoError(t, err)

		_, err = repo.Create(ctx, entity)
		require.Error(t, err)
	})
}

func TestMatchRuleRepository_ForeignKeyConstraint(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		nonExistentContextID := uuid.New()

		entity, err := entities.NewMatchRule(
			ctx,
			nonExistentContextID,
			entities.CreateMatchRuleInput{
				Priority: 1,
				Type:     shared.RuleTypeExact,
				Config:   map[string]any{"matchCurrency": true},
			},
		)
		require.NoError(t, err)

		_, err = repo.Create(ctx, entity)
		require.Error(t, err)
	})
}

func TestContextRepository_DeleteWithSources_CascadeDeletesChildren(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxRepo := contextRepo.NewRepository(h.Provider())
		srcRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		contextEntity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Context With Sources",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)
		createdContext, err := ctxRepo.Create(ctx, contextEntity)
		require.NoError(t, err)

		sourceEntity, err := entities.NewReconciliationSource(
			ctx,
			createdContext.ID,
			entities.CreateReconciliationSourceInput{
				Name:   "Child Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		createdSource, err := srcRepo.Create(ctx, sourceEntity)
		require.NoError(t, err)

		err = ctxRepo.Delete(ctx, createdContext.ID)
		require.NoError(t, err, "CASCADE DELETE should succeed")

		_, err = srcRepo.FindByID(ctx, createdContext.ID, createdSource.ID)
		require.Error(t, err, "Child source should be deleted via CASCADE")
	})
}

func TestSourceRepository_DeleteWithFieldMaps_CascadeDeletesChildren(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		srcRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		fmRepo := fieldMapRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		sourceEntity, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Source With FieldMap",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		createdSource, err := srcRepo.Create(ctx, sourceEntity)
		require.NoError(t, err)

		fieldMapEntity, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			createdSource.ID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{"amount": "amt"},
			},
		)
		require.NoError(t, err)
		createdFieldMap, err := fmRepo.Create(ctx, fieldMapEntity)
		require.NoError(t, err)

		err = srcRepo.Delete(ctx, h.Seed.ContextID, createdSource.ID)
		require.NoError(t, err, "CASCADE DELETE should succeed")

		_, err = fmRepo.FindByID(ctx, createdFieldMap.ID)
		require.Error(t, err, "Child field map should be deleted via CASCADE")
	})
}

func TestMatchRuleRepository_PriorityUniqueness(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := matchRuleRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		rule1, err := entities.NewMatchRule(ctx, h.Seed.ContextID, entities.CreateMatchRuleInput{
			Priority: 100,
			Type:     shared.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		})
		require.NoError(t, err)
		_, err = repo.Create(ctx, rule1)
		require.NoError(t, err)

		rule2, err := entities.NewMatchRule(ctx, h.Seed.ContextID, entities.CreateMatchRuleInput{
			Priority: 100,
			Type:     shared.RuleTypeTolerance,
			Config:   map[string]any{"absTolerance": "0.01"},
		})
		require.NoError(t, err)
		_, err = repo.Create(ctx, rule2)
		require.Error(t, err)
	})
}

func TestContextRepository_UpdateNonExistent(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Non Existent",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		entity.ID = uuid.New()

		_, err = repo.Update(ctx, entity)
		require.Error(t, err)
	})
}

func TestContextRepository_DeleteNonExistent(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		err := repo.Delete(ctx, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows, "deleting non-existent should return sql.ErrNoRows")
	})
}

func TestSourceRepository_EmptyConfig(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		entity, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Empty Config Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.Empty(t, created.Config)
	})
}

func TestSourceRepository_ComplexConfig(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		complexConfig := map[string]any{
			"table":      "transactions",
			"columns":    []string{"id", "amount", "currency", "date"},
			"filter":     map[string]any{"status": "completed", "type": "credit"},
			"pagination": map[string]any{"limit": 1000, "offset": 0},
			"nested": map[string]any{
				"level1": map[string]any{
					"level2": "deep value",
				},
			},
		}

		entity, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Complex Config Source",
				Type:   value_objects.SourceTypeBank,
				Side:   sharedfee.MatchingSideRight,
				Config: complexConfig,
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEmpty(t, created.Config)
		require.Equal(t, "transactions", created.Config["table"])
	})
}

func TestFieldMapRepository_ComplexMapping(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := fieldMapRepo.NewRepository(h.Provider())
		srcRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		source, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Complex Mapping Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		createdSource, err := srcRepo.Create(ctx, source)
		require.NoError(t, err)

		complexMapping := map[string]any{
			"amount":       "transaction_amount",
			"currency":     "currency_code",
			"date":         "posting_date",
			"external_id":  "reference_id",
			"description":  "memo",
			"account_from": "debit_account",
			"account_to":   "credit_account",
			"metadata":     map[string]any{"field1": "value1", "field2": "value2"},
		}

		entity, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			createdSource.ID,
			shared.CreateFieldMapInput{
				Mapping: complexMapping,
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEmpty(t, created.Mapping)
		require.Equal(t, "transaction_amount", created.Mapping["amount"])
		require.Equal(t, "posting_date", created.Mapping["date"])
	})
}

func TestContextRepository_Pagination(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		for i := 0; i < 10; i++ {
			entity, err := entities.NewReconciliationContext(
				ctx,
				h.Seed.TenantID,
				entities.CreateReconciliationContextInput{
					Name:     "Pagination Context " + uuid.New().String()[:8],
					Type:     shared.ContextTypeOneToOne,
					Interval: "0 0 * * *",
				},
			)
			require.NoError(t, err)
			_, err = repo.Create(ctx, entity)
			require.NoError(t, err)
		}

		page1, pagination1, err := repo.FindAll(ctx, "", 5, nil, nil)
		require.NoError(t, err)
		require.Len(t, page1, 5)
		require.NotEmpty(t, pagination1.Next, "Should have next cursor for page 2")

		page2, _, err := repo.FindAll(ctx, pagination1.Next, 5, nil, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(page2), 5)

		for _, p1 := range page1 {
			for _, p2 := range page2 {
				require.NotEqual(t, p1.ID, p2.ID, "Pages should not overlap")
			}
		}
	})
}
