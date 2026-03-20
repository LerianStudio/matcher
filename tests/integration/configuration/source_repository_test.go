//go:build integration

package configuration

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	sourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestSourceRepository_CreateAndFindByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		entity, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Bank Source",
				Type:   value_objects.SourceTypeBank,
				Side:   sharedfee.MatchingSideRight,
				Config: map[string]any{"format": "mt940"},
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, "Bank Source", created.Name)
		require.Equal(t, value_objects.SourceTypeBank, created.Type)

		fetched, err := repo.FindByID(ctx, h.Seed.ContextID, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.Name, fetched.Name)
	})
}

func TestSourceRepository_FindByContextID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		ledgerSource, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Ledger Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, ledgerSource)
		require.NoError(t, err)

		bankSource, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Bank Source",
				Type:   value_objects.SourceTypeBank,
				Side:   sharedfee.MatchingSideRight,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		_, err = repo.Create(ctx, bankSource)
		require.NoError(t, err)

		sources, _, err := repo.FindByContextID(ctx, h.Seed.ContextID, "", 100)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(sources), 2)
	})
}

func TestSourceRepository_FindByContextIDAndType(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		bankSource, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Bank Source For Type Filter",
				Type:   value_objects.SourceTypeBank,
				Side:   sharedfee.MatchingSideRight,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		created, err := repo.Create(ctx, bankSource)
		require.NoError(t, err)

		sources, _, err := repo.FindByContextIDAndType(
			ctx,
			h.Seed.ContextID,
			value_objects.SourceTypeBank,
			"",
			100,
		)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(sources), 1)

		var found bool
		for _, s := range sources {
			if s.ID == created.ID {
				found = true

				break
			}
		}

		require.True(t, found, "created source not found in type-filtered results")
	})
}

func TestSourceRepository_Update(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		entity, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Original Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)

		created.Name = "Updated Source"
		updated, err := repo.Update(ctx, created)
		require.NoError(t, err)
		require.Equal(t, "Updated Source", updated.Name)

		fetched, err := repo.FindByID(ctx, h.Seed.ContextID, created.ID)
		require.NoError(t, err)
		require.Equal(t, "Updated Source", fetched.Name)
	})
}

func TestSourceRepository_Delete(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		entity, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "To Be Deleted Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)

		err = repo.Delete(ctx, h.Seed.ContextID, created.ID)
		require.NoError(t, err)

		_, err = repo.FindByID(ctx, h.Seed.ContextID, created.ID)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestSourceRepository_FindByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		_, err = repo.FindByID(ctx, h.Seed.ContextID, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}
