//go:build unit

package rate

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestPostgreSQLModel_ToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *PostgreSQLModel

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.ErrorIs(t, err, ErrRateModelNeeded)
}

func TestPostgreSQLModel_ToEntity_InvalidID(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            "not-a-uuid",
		Currency:      "USD",
		StructureType: string(fee.FeeStructureFlat),
		StructureData: []byte(`{"amount":"10.00"}`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse id")
}

func TestPostgreSQLModel_ToEntity_FlatFee(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	model := &PostgreSQLModel{
		ID:            id.String(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureFlat),
		StructureData: []byte(`{"amount":"25.50"}`),
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	result, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, "USD", result.Currency)
	assert.Equal(t, fee.FeeStructureFlat, result.Structure.Type())
	assert.True(t, result.CreatedAt.Equal(now))
	assert.True(t, result.UpdatedAt.Equal(now))

	flatFee, ok := result.Structure.(fee.FlatFee)
	require.True(t, ok)
	assert.True(t, flatFee.Amount.Equal(decimal.NewFromFloat(25.50)))
}

func TestPostgreSQLModel_ToEntity_PercentageFee(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	model := &PostgreSQLModel{
		ID:            id.String(),
		Currency:      "EUR",
		StructureType: string(fee.FeeStructurePercentage),
		StructureData: []byte(`{"rate":"0.025"}`),
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	result, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, "EUR", result.Currency)
	assert.Equal(t, fee.FeeStructurePercentage, result.Structure.Type())

	percentageFee, ok := result.Structure.(fee.PercentageFee)
	require.True(t, ok)
	assert.True(t, percentageFee.Rate.Equal(decimal.NewFromFloat(0.025)))
}

func TestPostgreSQLModel_ToEntity_TieredFee_WithUpTo(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	model := &PostgreSQLModel{
		ID:            id.String(),
		Currency:      "BRL",
		StructureType: string(fee.FeeStructureTiered),
		StructureData: []byte(
			`{"tiers":[{"up_to":"100.00","rate":"0.01"},{"up_to":"500.00","rate":"0.02"},{"rate":"0.03"}]}`,
		),
		CreatedAt: now,
		UpdatedAt: now,
	}

	result, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, id, result.ID)
	assert.Equal(t, "BRL", result.Currency)
	assert.Equal(t, fee.FeeStructureTiered, result.Structure.Type())

	tieredFee, ok := result.Structure.(fee.TieredFee)
	require.True(t, ok)
	require.Len(t, tieredFee.Tiers, 3)

	require.NotNil(t, tieredFee.Tiers[0].UpTo)
	assert.True(t, tieredFee.Tiers[0].UpTo.Equal(decimal.NewFromFloat(100.00)))
	assert.True(t, tieredFee.Tiers[0].Rate.Equal(decimal.NewFromFloat(0.01)))

	require.NotNil(t, tieredFee.Tiers[1].UpTo)
	assert.True(t, tieredFee.Tiers[1].UpTo.Equal(decimal.NewFromFloat(500.00)))
	assert.True(t, tieredFee.Tiers[1].Rate.Equal(decimal.NewFromFloat(0.02)))

	require.Nil(t, tieredFee.Tiers[2].UpTo)
	assert.True(t, tieredFee.Tiers[2].Rate.Equal(decimal.NewFromFloat(0.03)))
}

func TestPostgreSQLModel_ToEntity_TieredFee_WithoutUpTo(t *testing.T) {
	t.Parallel()

	id := uuid.New()

	model := &PostgreSQLModel{
		ID:            id.String(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureTiered),
		StructureData: []byte(`{"tiers":[{"rate":"0.05"}]}`),
	}

	result, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, result)

	tieredFee, ok := result.Structure.(fee.TieredFee)
	require.True(t, ok)
	require.Len(t, tieredFee.Tiers, 1)
	require.Nil(t, tieredFee.Tiers[0].UpTo)
	assert.True(t, tieredFee.Tiers[0].Rate.Equal(decimal.NewFromFloat(0.05)))
}

func TestParseStructure_UnknownType(t *testing.T) {
	t.Parallel()

	id := uuid.New()

	model := &PostgreSQLModel{
		ID:            id.String(),
		Currency:      "USD",
		StructureType: "UNKNOWN_TYPE",
		StructureData: []byte(`{}`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse structure")
	assert.Contains(t, err.Error(), "unknown fee structure type")
	assert.Contains(t, err.Error(), "UNKNOWN_TYPE")
}

func TestParseFlatFee_InvalidJSON(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureFlat),
		StructureData: []byte(`not-json`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal flat fee")
}

func TestParseFlatFee_InvalidAmount(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureFlat),
		StructureData: []byte(`{"amount":"not-a-number"}`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse flat fee amount")
}

func TestParsePercentageFee_InvalidJSON(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructurePercentage),
		StructureData: []byte(`{invalid-json}`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal percentage fee")
}

func TestParsePercentageFee_InvalidRate(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructurePercentage),
		StructureData: []byte(`{"rate":"abc"}`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse percentage rate")
}

func TestParseTieredFee_InvalidJSON(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureTiered),
		StructureData: []byte(`[broken`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal tiered fee")
}

func TestParseTieredFee_InvalidRate(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureTiered),
		StructureData: []byte(`{"tiers":[{"rate":"invalid"}]}`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tier 0 rate")
}

func TestParseTieredFee_InvalidUpTo(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureTiered),
		StructureData: []byte(`{"tiers":[{"up_to":"not-number","rate":"0.01"}]}`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tier 0 up_to")
}

func TestParseTieredFee_SecondTierInvalidRate(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureTiered),
		StructureData: []byte(`{"tiers":[{"up_to":"100","rate":"0.01"},{"rate":"xyz"}]}`),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tier 1 rate")
}

func TestParseTieredFee_SecondTierInvalidUpTo(t *testing.T) {
	t.Parallel()

	model := &PostgreSQLModel{
		ID:            uuid.NewString(),
		Currency:      "USD",
		StructureType: string(fee.FeeStructureTiered),
		StructureData: []byte(
			`{"tiers":[{"up_to":"100","rate":"0.01"},{"up_to":"bad","rate":"0.02"}]}`,
		),
	}

	result, err := model.ToEntity()

	require.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tier 1 up_to")
}

func TestToEntity_TableDriven(t *testing.T) {
	t.Parallel()

	validID := uuid.New()

	tests := []struct {
		name        string
		model       *PostgreSQLModel
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil model returns error",
			model:       nil,
			wantErr:     true,
			errContains: "rate model is required",
		},
		{
			name: "invalid UUID returns error",
			model: &PostgreSQLModel{
				ID:            "bad-uuid",
				Currency:      "USD",
				StructureType: string(fee.FeeStructureFlat),
				StructureData: []byte(`{"amount":"10"}`),
			},
			wantErr:     true,
			errContains: "parse id",
		},
		{
			name: "unknown structure type returns error",
			model: &PostgreSQLModel{
				ID:            validID.String(),
				Currency:      "USD",
				StructureType: "INVALID",
				StructureData: []byte(`{}`),
			},
			wantErr:     true,
			errContains: "unknown fee structure type",
		},
		{
			name: "valid flat fee succeeds",
			model: &PostgreSQLModel{
				ID:            validID.String(),
				Currency:      "USD",
				StructureType: string(fee.FeeStructureFlat),
				StructureData: []byte(`{"amount":"99.99"}`),
			},
			wantErr: false,
		},
		{
			name: "valid percentage fee succeeds",
			model: &PostgreSQLModel{
				ID:            validID.String(),
				Currency:      "EUR",
				StructureType: string(fee.FeeStructurePercentage),
				StructureData: []byte(`{"rate":"0.05"}`),
			},
			wantErr: false,
		},
		{
			name: "valid tiered fee succeeds",
			model: &PostgreSQLModel{
				ID:            validID.String(),
				Currency:      "BRL",
				StructureType: string(fee.FeeStructureTiered),
				StructureData: []byte(`{"tiers":[{"up_to":"1000","rate":"0.01"},{"rate":"0.02"}]}`),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := tt.model.ToEntity()

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}
