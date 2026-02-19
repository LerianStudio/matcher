//go:build integration

package matching

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	feeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

// newTestSchedule creates a fee schedule entity for integration tests.
func newTestSchedule(
	t *testing.T,
	h *integration.TestHarness,
	name, currency string,
	items []fee.FeeScheduleItemInput,
) *fee.FeeSchedule {
	t.Helper()

	ctx := e4t9Ctx(t, h)

	schedule, err := fee.NewFeeSchedule(ctx, fee.NewFeeScheduleInput{
		TenantID:         h.Seed.TenantID,
		Name:             name,
		Currency:         currency,
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items:            items,
	})
	require.NoError(t, err)

	return schedule
}

func TestFeeScheduleRepository_CreateAndGetByID(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		items := []fee.FeeScheduleItemInput{
			{
				Name:      "Flat Processing Fee",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromFloat(1.50)},
			},
			{
				Name:      "Interchange Fee",
				Priority:  2,
				Structure: fee.PercentageFee{Rate: decimal.NewFromFloat(0.015)},
			},
		}

		schedule := newTestSchedule(t, h, "Create-Get Test", "USD", items)
		created, err := repo.Create(ctx, schedule)
		require.NoError(t, err)
		require.NotEqual(t, uuid.Nil, created.ID)
		require.Equal(t, "Create-Get Test", created.Name)
		require.Equal(t, "USD", created.Currency)
		require.Equal(t, fee.ApplicationOrderParallel, created.ApplicationOrder)
		require.Equal(t, 2, created.RoundingScale)
		require.Equal(t, fee.RoundingModeHalfUp, created.RoundingMode)
		require.Len(t, created.Items, 2)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, created.ID, fetched.ID)
		require.Equal(t, created.Name, fetched.Name)
		require.Equal(t, created.Currency, fetched.Currency)
		require.Len(t, fetched.Items, 2)

		// Verify items ordered by priority
		require.Equal(t, "Flat Processing Fee", fetched.Items[0].Name)
		require.Equal(t, fee.FeeStructureFlat, fetched.Items[0].Structure.Type())
		flat, ok := fetched.Items[0].Structure.(fee.FlatFee)
		require.True(t, ok)
		require.True(t, decimal.NewFromFloat(1.50).Equal(flat.Amount))

		require.Equal(t, "Interchange Fee", fetched.Items[1].Name)
		require.Equal(t, fee.FeeStructurePercentage, fetched.Items[1].Structure.Type())
		pct, ok := fetched.Items[1].Structure.(fee.PercentageFee)
		require.True(t, ok)
		require.True(t, decimal.NewFromFloat(0.015).Equal(pct.Rate))
	})
}

func TestFeeScheduleRepository_CreateWithTieredFee(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		upTo100 := decimal.NewFromInt(100)
		upTo500 := decimal.NewFromInt(500)

		items := []fee.FeeScheduleItemInput{
			{
				Name:     "Tiered Rate",
				Priority: 1,
				Structure: fee.TieredFee{
					Tiers: []fee.Tier{
						{UpTo: &upTo100, Rate: decimal.NewFromFloat(0.03)},
						{UpTo: &upTo500, Rate: decimal.NewFromFloat(0.02)},
						{UpTo: nil, Rate: decimal.NewFromFloat(0.01)},
					},
				},
			},
		}

		schedule := newTestSchedule(t, h, "Tiered Test", "EUR", items)
		created, err := repo.Create(ctx, schedule)
		require.NoError(t, err)

		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Len(t, fetched.Items, 1)

		tiered, ok := fetched.Items[0].Structure.(fee.TieredFee)
		require.True(t, ok)
		require.Len(t, tiered.Tiers, 3)

		// Tier 0: 0-100 at 3%
		require.NotNil(t, tiered.Tiers[0].UpTo)
		require.True(t, upTo100.Equal(*tiered.Tiers[0].UpTo))
		require.True(t, decimal.NewFromFloat(0.03).Equal(tiered.Tiers[0].Rate))

		// Tier 1: 100-500 at 2%
		require.NotNil(t, tiered.Tiers[1].UpTo)
		require.True(t, upTo500.Equal(*tiered.Tiers[1].UpTo))
		require.True(t, decimal.NewFromFloat(0.02).Equal(tiered.Tiers[1].Rate))

		// Tier 2: 500+ at 1% (no upper bound)
		require.Nil(t, tiered.Tiers[2].UpTo)
		require.True(t, decimal.NewFromFloat(0.01).Equal(tiered.Tiers[2].Rate))
	})
}

func TestFeeScheduleRepository_Update(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		items := []fee.FeeScheduleItemInput{
			{
				Name:      "Old Fee",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromInt(1)},
			},
		}

		schedule := newTestSchedule(t, h, "Update Test", "USD", items)
		created, err := repo.Create(ctx, schedule)
		require.NoError(t, err)
		require.Len(t, created.Items, 1)

		// Mutate the entity for update: change name, currency, application order, and replace items
		created.Name = "Updated Schedule"
		created.Currency = "EUR"
		created.ApplicationOrder = fee.ApplicationOrderCascading
		created.Items = []fee.FeeScheduleItem{
			{
				ID:        uuid.New(),
				Name:      "New Pct Fee",
				Priority:  1,
				Structure: fee.PercentageFee{Rate: decimal.NewFromFloat(0.025)},
				CreatedAt: created.CreatedAt,
				UpdatedAt: created.UpdatedAt,
			},
			{
				ID:        uuid.New(),
				Name:      "New Flat Fee",
				Priority:  2,
				Structure: fee.FlatFee{Amount: decimal.NewFromFloat(3.00)},
				CreatedAt: created.CreatedAt,
				UpdatedAt: created.UpdatedAt,
			},
		}

		updated, err := repo.Update(ctx, created)
		require.NoError(t, err)
		require.Equal(t, "Updated Schedule", updated.Name)
		require.Equal(t, "EUR", updated.Currency)
		require.Len(t, updated.Items, 2)

		// Re-fetch to verify persistence
		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, "Updated Schedule", fetched.Name)
		require.Equal(t, "EUR", fetched.Currency)
		require.Equal(t, fee.ApplicationOrderCascading, fetched.ApplicationOrder)
		require.Len(t, fetched.Items, 2)
		require.Equal(t, "New Pct Fee", fetched.Items[0].Name)
		require.Equal(t, "New Flat Fee", fetched.Items[1].Name)
	})
}

func TestFeeScheduleRepository_Delete(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		items := []fee.FeeScheduleItemInput{
			{
				Name:      "To Delete",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromInt(5)},
			},
		}

		schedule := newTestSchedule(t, h, "Delete Test", "USD", items)
		created, err := repo.Create(ctx, schedule)
		require.NoError(t, err)

		err = repo.Delete(ctx, created.ID)
		require.NoError(t, err)

		_, err = repo.GetByID(ctx, created.ID)
		require.ErrorIs(t, err, fee.ErrFeeScheduleNotFound)
	})
}

func TestFeeScheduleRepository_DeleteNonExistent(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		err := repo.Delete(ctx, uuid.New())
		require.ErrorIs(t, err, fee.ErrFeeScheduleNotFound)
	})
}

func TestFeeScheduleRepository_List(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		names := []string{"Alpha Schedule", "Beta Schedule", "Gamma Schedule"}
		for _, name := range names {
			s := newTestSchedule(t, h, name, "USD", []fee.FeeScheduleItemInput{
				{
					Name:      "Fee",
					Priority:  1,
					Structure: fee.FlatFee{Amount: decimal.NewFromInt(1)},
				},
			})
			_, err := repo.Create(ctx, s)
			require.NoError(t, err)
		}

		listed, err := repo.List(ctx, 100)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listed), 3)

		// List is ordered by name; verify alphabetical ordering among our schedules
		var foundNames []string
		for _, s := range listed {
			for _, n := range names {
				if s.Name == n {
					foundNames = append(foundNames, s.Name)
				}
			}
		}

		require.Len(t, foundNames, 3)
		require.Equal(t, "Alpha Schedule", foundNames[0])
		require.Equal(t, "Beta Schedule", foundNames[1])
		require.Equal(t, "Gamma Schedule", foundNames[2])
	})
}

func TestFeeScheduleRepository_ListWithLimit(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		for i := 0; i < 3; i++ {
			s := newTestSchedule(t, h, "LimitTest"+string(rune('A'+i)), "USD", []fee.FeeScheduleItemInput{
				{
					Name:      "Fee",
					Priority:  1,
					Structure: fee.FlatFee{Amount: decimal.NewFromInt(1)},
				},
			})
			_, err := repo.Create(ctx, s)
			require.NoError(t, err)
		}

		listed, err := repo.List(ctx, 2)
		require.NoError(t, err)
		require.LessOrEqual(t, len(listed), 2)
	})
}

func TestFeeScheduleRepository_GetByIDs(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		var ids []uuid.UUID

		for i := 0; i < 3; i++ {
			s := newTestSchedule(t, h, "GetByIDs-"+string(rune('A'+i)), "USD", []fee.FeeScheduleItemInput{
				{
					Name:      "Fee",
					Priority:  1,
					Structure: fee.FlatFee{Amount: decimal.NewFromInt(int64(i + 1))},
				},
			})
			created, err := repo.Create(ctx, s)
			require.NoError(t, err)
			ids = append(ids, created.ID)
		}

		// Fetch only the first 2
		result, err := repo.GetByIDs(ctx, ids[:2])
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, result, ids[0])
		require.Contains(t, result, ids[1])
		require.NotContains(t, result, ids[2])

		// Verify items are loaded in the returned schedules
		for _, id := range ids[:2] {
			require.Len(t, result[id].Items, 1)
		}
	})
}

func TestFeeScheduleRepository_GetByIDsEmpty(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		result, err := repo.GetByIDs(ctx, []uuid.UUID{})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Empty(t, result)
	})
}

func TestFeeScheduleRepository_CascadeDeleteItems(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := feeScheduleRepo.NewRepository(h.Provider())
		ctx := e4t9Ctx(t, h)

		items := []fee.FeeScheduleItemInput{
			{
				Name:      "Cascade Fee A",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromInt(1)},
			},
			{
				Name:      "Cascade Fee B",
				Priority:  2,
				Structure: fee.PercentageFee{Rate: decimal.NewFromFloat(0.01)},
			},
		}

		schedule := newTestSchedule(t, h, "Cascade Delete Test", "USD", items)
		created, err := repo.Create(ctx, schedule)
		require.NoError(t, err)

		// Verify items exist in DB
		itemCount := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM fee_schedule_items WHERE fee_schedule_id = $1",
			created.ID.String(),
		)
		require.Equal(t, 2, itemCount)

		// Delete the parent schedule
		err = repo.Delete(ctx, created.ID)
		require.NoError(t, err)

		// Verify items were cascade deleted
		itemCountAfter := countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM fee_schedule_items WHERE fee_schedule_id = $1",
			created.ID.String(),
		)
		require.Equal(t, 0, itemCountAfter)
	})
}
