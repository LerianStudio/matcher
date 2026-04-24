//go:build integration

package configuration

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	contextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegration_Configuration_ContextRepository_CreateAndFindByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Test Context",
				Type:     shared.ContextTypeOneToMany,
				Interval: "0 */6 * * *",
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, "Test Context", created.Name)

		fetched, err := repo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.Name, fetched.Name)
		require.Equal(t, created.Type, fetched.Type)
	})
}

func TestIntegration_Configuration_ContextRepository_FindByName(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Unique Name Context",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)

		fetched, err := repo.FindByName(ctx, "Unique Name Context")
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.Equal(t, created.ID, fetched.ID)

		notFound, err := repo.FindByName(ctx, "Non Existent")
		require.NoError(t, err)
		require.Nil(t, notFound)
	})
}

func TestIntegration_Configuration_ContextRepository_FindAll(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		for i := 0; i < 3; i++ {
			entity, err := entities.NewReconciliationContext(
				ctx,
				h.Seed.TenantID,
				entities.CreateReconciliationContextInput{
					Name:     "List Context " + string(rune('A'+i)),
					Type:     shared.ContextTypeOneToOne,
					Interval: "0 0 * * *",
				},
			)
			require.NoError(t, err)
			_, err = repo.Create(ctx, entity)
			require.NoError(t, err)
		}

		contexts, _, err := repo.FindAll(ctx, "", 10, nil, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(contexts), 3)

		contexts, _, err = repo.FindAll(ctx, "", 2, nil, nil)
		require.NoError(t, err)
		require.Len(t, contexts, 2)
	})
}

func TestIntegration_Configuration_ContextRepository_Update(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Original Name",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)

		created.Name = "Updated Name"
		updated, err := repo.Update(ctx, created)
		require.NoError(t, err)
		require.Equal(t, "Updated Name", updated.Name)

		fetched, err := repo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, "Updated Name", fetched.Name)
	})
}

func TestIntegration_Configuration_ContextRepository_Delete(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "To Be Deleted",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)

		err = repo.Delete(ctx, created.ID)
		require.NoError(t, err)

		_, err = repo.FindByID(ctx, created.ID)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestIntegration_Configuration_ContextRepository_Count(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		initialCount, err := repo.Count(ctx)
		require.NoError(t, err)

		entity, err := entities.NewReconciliationContext(
			ctx,
			h.Seed.TenantID,
			entities.CreateReconciliationContextInput{
				Name:     "Count Test",
				Type:     shared.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, entity)
		require.NoError(t, err)

		newCount, err := repo.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, initialCount+1, newCount)
	})
}

func TestIntegration_Configuration_ContextRepository_FindByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := contextRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		_, err := repo.FindByID(ctx, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}
