//go:build unit

package fee_schedule

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestToEntity_NilModel(t *testing.T) {
	t.Parallel()

	result, err := ToEntity(nil, nil)

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrFeeScheduleModelNeeded)
}

func TestToEntity_InvalidID(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               "not-a-uuid",
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	result, err := ToEntity(model, nil)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse id")
}

func TestToEntity_InvalidTenantID(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         "not-a-uuid",
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	result, err := ToEntity(model, nil)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tenant id")
}

func TestToEntity_ValidScheduleNoItems(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	tenantID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	model := &PostgreSQLModel{
		ID:               id.String(),
		TenantID:         tenantID.String(),
		Name:             "Test Schedule",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	result, err := ToEntity(model, nil)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, tenantID, result.TenantID)
	assert.Equal(t, "Test Schedule", result.Name)
	assert.Equal(t, "USD", result.Currency)
	assert.Equal(t, fee.ApplicationOrderParallel, result.ApplicationOrder)
	assert.Equal(t, 2, result.RoundingScale)
	assert.Equal(t, fee.RoundingModeHalfUp, result.RoundingMode)
	assert.Empty(t, result.Items)
	assert.True(t, result.CreatedAt.Equal(now))
	assert.True(t, result.UpdatedAt.Equal(now))
}

func TestToEntity_WithFlatFeeItem(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	tenantID := uuid.New()
	itemID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	model := &PostgreSQLModel{
		ID:               scheduleID.String(),
		TenantID:         tenantID.String(),
		Name:             "Flat Schedule",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            itemID.String(),
			FeeScheduleID: scheduleID.String(),
			Name:          "Fixed Fee",
			Priority:      0,
			StructureType: string(fee.FeeStructureFlat),
			StructureData: []byte(`{"amount":"10.50"}`),
			CreatedAt:     now,
			UpdatedAt:     now,
		},
	}

	result, err := ToEntity(model, items)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Items, 1)
	assert.Equal(t, itemID, result.Items[0].ID)
	assert.Equal(t, "Fixed Fee", result.Items[0].Name)
	assert.Equal(t, 0, result.Items[0].Priority)
	assert.Equal(t, fee.FeeStructureFlat, result.Items[0].Structure.Type())

	flatFee, ok := result.Items[0].Structure.(fee.FlatFee)
	require.True(t, ok)
	assert.True(t, flatFee.Amount.Equal(decimal.NewFromFloat(10.50)))
}

func TestToEntity_WithPercentageFeeItem(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	tenantID := uuid.New()
	itemID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	model := &PostgreSQLModel{
		ID:               scheduleID.String(),
		TenantID:         tenantID.String(),
		Name:             "Percentage Schedule",
		Currency:         "EUR",
		ApplicationOrder: "CASCADING",
		RoundingScale:    4,
		RoundingMode:     "BANKERS",
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            itemID.String(),
			FeeScheduleID: scheduleID.String(),
			Name:          "Service Fee",
			Priority:      1,
			StructureType: string(fee.FeeStructurePercentage),
			StructureData: []byte(`{"rate":"0.025"}`),
			CreatedAt:     now,
			UpdatedAt:     now,
		},
	}

	result, err := ToEntity(model, items)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Items, 1)

	pctFee, ok := result.Items[0].Structure.(fee.PercentageFee)
	require.True(t, ok)
	assert.True(t, pctFee.Rate.Equal(decimal.NewFromFloat(0.025)))
}

func TestToEntity_WithTieredFeeItem(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	tenantID := uuid.New()
	itemID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	model := &PostgreSQLModel{
		ID:               scheduleID.String(),
		TenantID:         tenantID.String(),
		Name:             "Tiered Schedule",
		Currency:         "BRL",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "FLOOR",
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            itemID.String(),
			FeeScheduleID: scheduleID.String(),
			Name:          "Volume Discount",
			Priority:      0,
			StructureType: string(fee.FeeStructureTiered),
			StructureData: []byte(`{"tiers":[{"up_to":"100","rate":"0.01"},{"up_to":"500","rate":"0.02"},{"rate":"0.03"}]}`),
			CreatedAt:     now,
			UpdatedAt:     now,
		},
	}

	result, err := ToEntity(model, items)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Items, 1)

	tieredFee, ok := result.Items[0].Structure.(fee.TieredFee)
	require.True(t, ok)
	require.Len(t, tieredFee.Tiers, 3)
	require.NotNil(t, tieredFee.Tiers[0].UpTo)
	assert.True(t, tieredFee.Tiers[0].UpTo.Equal(decimal.NewFromFloat(100)))
	assert.True(t, tieredFee.Tiers[0].Rate.Equal(decimal.NewFromFloat(0.01)))
	require.Nil(t, tieredFee.Tiers[2].UpTo)
	assert.True(t, tieredFee.Tiers[2].Rate.Equal(decimal.NewFromFloat(0.03)))
}

func TestToEntity_InvalidItemID(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            "not-a-uuid",
			FeeScheduleID: model.ID,
			Name:          "Bad Item",
			StructureType: string(fee.FeeStructureFlat),
			StructureData: []byte(`{"amount":"10"}`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse item[0]")
}

func TestToEntity_UnknownStructureType(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            uuid.NewString(),
			FeeScheduleID: model.ID,
			Name:          "Unknown",
			StructureType: "UNKNOWN",
			StructureData: []byte(`{}`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown fee structure type")
}

func TestFromEntity_NilSchedule(t *testing.T) {
	t.Parallel()

	model, items, err := FromEntity(nil)

	require.NoError(t, err)
	assert.Nil(t, model)
	assert.Nil(t, items)
}

func TestFromEntity_ValidScheduleWithItems(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	schedule := &fee.FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Test Schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:        uuid.New(),
				Name:      "Flat Fee",
				Priority:  0,
				Structure: fee.FlatFee{Amount: decimal.NewFromFloat(10.50)},
				CreatedAt: now,
				UpdatedAt: now,
			},
			{
				ID:        uuid.New(),
				Name:      "Pct Fee",
				Priority:  1,
				Structure: fee.PercentageFee{Rate: decimal.NewFromFloat(0.025)},
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, items, err := FromEntity(schedule)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, schedule.ID.String(), model.ID)
	assert.Equal(t, schedule.TenantID.String(), model.TenantID)
	assert.Equal(t, "Test Schedule", model.Name)
	assert.Equal(t, "USD", model.Currency)
	assert.Equal(t, "PARALLEL", model.ApplicationOrder)
	assert.Equal(t, 2, model.RoundingScale)
	assert.Equal(t, "HALF_UP", model.RoundingMode)

	require.Len(t, items, 2)
	assert.Equal(t, schedule.Items[0].ID.String(), items[0].ID)
	assert.Equal(t, schedule.ID.String(), items[0].FeeScheduleID)
	assert.Equal(t, "Flat Fee", items[0].Name)
	assert.Equal(t, 0, items[0].Priority)
	assert.Equal(t, string(fee.FeeStructureFlat), items[0].StructureType)
	assert.Contains(t, string(items[0].StructureData), "10.5")

	assert.Equal(t, "Pct Fee", items[1].Name)
	assert.Equal(t, 1, items[1].Priority)
	assert.Equal(t, string(fee.FeeStructurePercentage), items[1].StructureType)
	assert.Contains(t, string(items[1].StructureData), "0.025")
}

func TestFromEntity_TieredFeeItem(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	upTo := decimal.NewFromFloat(100)

	schedule := &fee.FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Tiered",
		Currency:         "EUR",
		ApplicationOrder: fee.ApplicationOrderCascading,
		RoundingScale:    4,
		RoundingMode:     fee.RoundingModeBankers,
		Items: []fee.FeeScheduleItem{
			{
				ID:       uuid.New(),
				Name:     "Tiered Fee",
				Priority: 0,
				Structure: fee.TieredFee{
					Tiers: []fee.Tier{
						{UpTo: &upTo, Rate: decimal.NewFromFloat(0.01)},
						{Rate: decimal.NewFromFloat(0.02)},
					},
				},
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, items, err := FromEntity(schedule)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.Len(t, items, 1)
	assert.Equal(t, string(fee.FeeStructureTiered), items[0].StructureType)
	assert.Contains(t, string(items[0].StructureData), "up_to")
	assert.Contains(t, string(items[0].StructureData), "0.01")
	assert.Contains(t, string(items[0].StructureData), "0.02")
}

func TestFromEntity_NilStructure(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	schedule := &fee.FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Nil Structure",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:        uuid.New(),
				Name:      "Nil",
				Priority:  0,
				Structure: nil,
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, items, err := FromEntity(schedule)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.Len(t, items, 1)
	assert.Equal(t, "", items[0].StructureType)
	assert.Equal(t, "{}", string(items[0].StructureData))
}

func TestRoundTrip_FlatFee(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	original := &fee.FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Round Trip Test",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:        uuid.New(),
				Name:      "Flat",
				Priority:  0,
				Structure: fee.FlatFee{Amount: decimal.NewFromFloat(25.00)},
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, items, convErr := FromEntity(original)
	require.NoError(t, convErr)

	result, err := ToEntity(model, items)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, original.ID, result.ID)
	assert.Equal(t, original.TenantID, result.TenantID)
	assert.Equal(t, original.Name, result.Name)
	assert.Equal(t, original.Currency, result.Currency)
	assert.Equal(t, original.ApplicationOrder, result.ApplicationOrder)
	assert.Equal(t, original.RoundingScale, result.RoundingScale)
	assert.Equal(t, original.RoundingMode, result.RoundingMode)
	require.Len(t, result.Items, 1)

	flatFee, ok := result.Items[0].Structure.(fee.FlatFee)
	require.True(t, ok)
	assert.True(t, flatFee.Amount.Equal(decimal.NewFromFloat(25.00)))
}

func TestParseStructure_InvalidFlatJSON(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            uuid.NewString(),
			FeeScheduleID: model.ID,
			Name:          "Bad JSON",
			StructureType: string(fee.FeeStructureFlat),
			StructureData: []byte(`not-json`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal flat fee")
}

func TestParseStructure_InvalidPercentageJSON(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            uuid.NewString(),
			FeeScheduleID: model.ID,
			Name:          "Bad",
			StructureType: string(fee.FeeStructurePercentage),
			StructureData: []byte(`{invalid}`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal percentage fee")
}

func TestParseStructure_InvalidTieredJSON(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            uuid.NewString(),
			FeeScheduleID: model.ID,
			Name:          "Bad",
			StructureType: string(fee.FeeStructureTiered),
			StructureData: []byte(`[broken`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal tiered fee")
}

func TestParseStructure_InvalidFlatAmount(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            uuid.NewString(),
			FeeScheduleID: model.ID,
			Name:          "Bad Amount",
			StructureType: string(fee.FeeStructureFlat),
			StructureData: []byte(`{"amount":"abc"}`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse flat fee amount")
}

func TestParseStructure_InvalidPercentageRate(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            uuid.NewString(),
			FeeScheduleID: model.ID,
			Name:          "Bad Rate",
			StructureType: string(fee.FeeStructurePercentage),
			StructureData: []byte(`{"rate":"xyz"}`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse percentage rate")
}

func TestParseStructure_InvalidTierRate(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            uuid.NewString(),
			FeeScheduleID: model.ID,
			Name:          "Bad Tier Rate",
			StructureType: string(fee.FeeStructureTiered),
			StructureData: []byte(`{"tiers":[{"rate":"invalid"}]}`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tier 0 rate")
}

func TestParseStructure_InvalidTierUpTo(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:               uuid.NewString(),
		TenantID:         uuid.NewString(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
	}

	items := []ItemPostgreSQLModel{
		{
			ID:            uuid.NewString(),
			FeeScheduleID: model.ID,
			Name:          "Bad Tier UpTo",
			StructureType: string(fee.FeeStructureTiered),
			StructureData: []byte(`{"tiers":[{"up_to":"bad","rate":"0.01"}]}`),
		},
	}

	result, err := ToEntity(model, items)

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tier 0 up_to")
}

func TestBuildINClause(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()

	placeholders, args := buildINClause([]uuid.UUID{id1, id2})

	assert.Equal(t, "$1, $2", placeholders)
	require.Len(t, args, 2)
	assert.Equal(t, id1.String(), args[0])
	assert.Equal(t, id2.String(), args[1])
}

func TestBuildINClause_Single(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()

	placeholders, args := buildINClause([]uuid.UUID{id1})

	assert.Equal(t, "$1", placeholders)
	require.Len(t, args, 1)
	assert.Equal(t, id1.String(), args[0])
}

func TestBuildINClause_Empty(t *testing.T) {
	t.Parallel()

	placeholders, args := buildINClause(nil)

	assert.Equal(t, "", placeholders)
	assert.Empty(t, args)
}
