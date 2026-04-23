//go:build integration

package configuration

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	fieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	sourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestFieldMapRepository_CreateAndFindByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := fieldMapRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{
					"amount":      "transaction_amount",
					"currency":    "currency_code",
					"date":        "transaction_date",
					"external_id": "reference_id",
				},
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, h.Seed.SourceID, created.SourceID)
		require.Equal(t, 1, created.Version)

		fetched, err := repo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.SourceID, fetched.SourceID)
	})
}

func TestFieldMapRepository_FindBySourceID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := fieldMapRepo.NewRepository(h.Provider())
		srcRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		source, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Field Map Test Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		createdSource, err := srcRepo.Create(ctx, source)
		require.NoError(t, err)

		fieldMap, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			createdSource.ID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{"amount": "amt"},
			},
		)
		require.NoError(t, err)
		created, err := repo.Create(ctx, fieldMap)
		require.NoError(t, err)

		fetched, err := repo.FindBySourceID(ctx, createdSource.ID)
		require.NoError(t, err)
		require.NotNil(t, fetched)
		require.Equal(t, created.ID, fetched.ID)
	})
}

func TestFieldMapRepository_Update(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := fieldMapRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		entity, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			h.Seed.SourceID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{"amount": "old_amount"},
			},
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, entity)
		require.NoError(t, err)
		require.Equal(t, 1, created.Version)

		created.Mapping = map[string]any{"amount": "new_amount", "currency": "curr"}
		created.Version = 2

		updated, err := repo.Update(ctx, created)
		require.NoError(t, err)
		require.Equal(t, 2, updated.Version)

		fetched, err := repo.FindByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, 2, fetched.Version)
	})
}

func TestFieldMapRepository_Delete(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := fieldMapRepo.NewRepository(h.Provider())
		srcRepo, err := sourceRepo.NewRepository(h.Provider())
		require.NoError(t, err)

		ctx := h.Ctx()

		source, err := entities.NewReconciliationSource(
			ctx,
			h.Seed.ContextID,
			entities.CreateReconciliationSourceInput{
				Name:   "Delete Field Map Source",
				Type:   value_objects.SourceTypeLedger,
				Side:   sharedfee.MatchingSideLeft,
				Config: map[string]any{},
			},
		)
		require.NoError(t, err)
		createdSource, err := srcRepo.Create(ctx, source)
		require.NoError(t, err)

		fieldMap, err := shared.NewFieldMap(
			ctx,
			h.Seed.ContextID,
			createdSource.ID,
			shared.CreateFieldMapInput{
				Mapping: map[string]any{"amount": "amt"},
			},
		)
		require.NoError(t, err)
		created, err := repo.Create(ctx, fieldMap)
		require.NoError(t, err)

		err = repo.Delete(ctx, created.ID)
		require.NoError(t, err)

		_, err = repo.FindByID(ctx, created.ID)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestFieldMapRepository_FindByID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := fieldMapRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		_, err := repo.FindByID(ctx, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestFieldMapRepository_FindBySourceID_NotFound(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := fieldMapRepo.NewRepository(h.Provider())
		ctx := h.Ctx()

		fetched, err := repo.FindBySourceID(ctx, uuid.New())
		require.ErrorIs(t, err, sql.ErrNoRows)
		require.Nil(t, fetched)
	})
}
