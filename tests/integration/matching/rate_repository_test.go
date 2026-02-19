//go:build integration

package matching

import (
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	rateRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/rate"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/tests/integration"
)

// insertRate is a test helper that inserts a rate row via raw SQL.
// It uses the actual migration column names: tenant_id, name, structure_type, structure.
func insertRate(
	t *testing.T,
	h *integration.TestHarness,
	id uuid.UUID,
	currency, structureType string,
	structureData any,
	now time.Time,
) {
	t.Helper()

	ctx := e4t9Ctx(t, h)

	structureJSON, err := json.Marshal(structureData)
	require.NoError(t, err, "marshal structure data")

	_, err = pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO rates (id, tenant_id, name, currency, structure_type, structure, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`,
			id.String(),
			h.Seed.TenantID.String(),
			"rate-"+id.String()[:8],
			currency,
			structureType,
			structureJSON,
			now,
			now,
		)
		return struct{}{}, execErr
	})
	require.NoError(t, err, "insert rate row")
}

func TestRateRepository_GetByID_NotFound(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := rateRepo.NewRepository(h.Provider())

		_, err := repo.GetByID(ctx, uuid.New())
		require.Error(t, err)
		require.True(t,
			errors.Is(err, rateRepo.ErrRateNotFound),
			"expected ErrRateNotFound, got: %v", err,
		)
	})
}

func TestRateRepository_GetByID_FlatRate(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := rateRepo.NewRepository(h.Provider())

		rateID := uuid.New()
		now := time.Now().UTC().Truncate(time.Microsecond)

		insertRate(t, h, rateID, "USD", "FLAT", map[string]any{
			"amount": "5.00",
		}, now)

		result, err := repo.GetByID(ctx, rateID)
		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, rateID, result.ID)
		require.Equal(t, "USD", result.Currency)
		require.Equal(t, fee.FeeStructureFlat, result.Structure.Type())

		flat, ok := result.Structure.(fee.FlatFee)
		require.True(t, ok, "expected FlatFee structure, got %T", result.Structure)
		require.True(t,
			decimal.NewFromFloat(5.00).Equal(flat.Amount),
			"expected flat amount 5.00, got %s", flat.Amount.String(),
		)

		require.WithinDuration(t, now, result.CreatedAt, time.Second)
		require.WithinDuration(t, now, result.UpdatedAt, time.Second)
	})
}

func TestRateRepository_GetByID_PercentageRate(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := rateRepo.NewRepository(h.Provider())

		rateID := uuid.New()
		now := time.Now().UTC().Truncate(time.Microsecond)

		insertRate(t, h, rateID, "EUR", "PERCENTAGE", map[string]any{
			"rate": "0.025",
		}, now)

		result, err := repo.GetByID(ctx, rateID)
		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, rateID, result.ID)
		require.Equal(t, "EUR", result.Currency)
		require.Equal(t, fee.FeeStructurePercentage, result.Structure.Type())

		pct, ok := result.Structure.(fee.PercentageFee)
		require.True(t, ok, "expected PercentageFee structure, got %T", result.Structure)
		require.True(t,
			decimal.NewFromFloat(0.025).Equal(pct.Rate),
			"expected percentage rate 0.025, got %s", pct.Rate.String(),
		)
	})
}

func TestRateRepository_GetByID_TieredRate(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := rateRepo.NewRepository(h.Provider())

		rateID := uuid.New()
		now := time.Now().UTC().Truncate(time.Microsecond)

		insertRate(t, h, rateID, "GBP", "TIERED", map[string]any{
			"tiers": []map[string]any{
				{"up_to": "1000", "rate": "0.03"},
				{"rate": "0.02"},
			},
		}, now)

		result, err := repo.GetByID(ctx, rateID)
		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, rateID, result.ID)
		require.Equal(t, "GBP", result.Currency)
		require.Equal(t, fee.FeeStructureTiered, result.Structure.Type())

		tiered, ok := result.Structure.(fee.TieredFee)
		require.True(t, ok, "expected TieredFee structure, got %T", result.Structure)
		require.Len(t, tiered.Tiers, 2)

		// Tier 0: up to 1000 at 3%
		require.NotNil(t, tiered.Tiers[0].UpTo)
		require.True(t,
			decimal.NewFromInt(1000).Equal(*tiered.Tiers[0].UpTo),
			"expected tier 0 up_to=1000, got %s", tiered.Tiers[0].UpTo.String(),
		)
		require.True(t,
			decimal.NewFromFloat(0.03).Equal(tiered.Tiers[0].Rate),
			"expected tier 0 rate=0.03, got %s", tiered.Tiers[0].Rate.String(),
		)

		// Tier 1: no upper bound at 2%
		require.Nil(t, tiered.Tiers[1].UpTo)
		require.True(t,
			decimal.NewFromFloat(0.02).Equal(tiered.Tiers[1].Rate),
			"expected tier 1 rate=0.02, got %s", tiered.Tiers[1].Rate.String(),
		)
	})
}

func TestRateRepository_GetByID_EmptyStructureData(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := e4t9Ctx(t, h)
		repo := rateRepo.NewRepository(h.Provider())

		rateID := uuid.New()
		now := time.Now().UTC().Truncate(time.Microsecond)

		// Insert a FLAT rate with empty JSON — the adapter will fail to parse
		// the "amount" key because it doesn't exist, so we expect a parse error.
		// This validates the adapter's error path for malformed structure data.
		insertRate(t, h, rateID, "BRL", "FLAT", map[string]any{}, now)

		_, err := repo.GetByID(ctx, rateID)
		require.Error(t, err, "expected error when structure data lacks required fields")
	})
}
