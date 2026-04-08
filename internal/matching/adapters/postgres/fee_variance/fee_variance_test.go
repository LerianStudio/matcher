//go:build unit

package fee_variance

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

func TestNewPostgreSQLModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		entity    *matchingEntities.FeeVariance
		wantErr   error
		wantModel bool
	}{
		{
			name:      "nil entity returns error",
			entity:    nil,
			wantErr:   ErrFeeVarianceEntityNeeded,
			wantModel: false,
		},
		{
			name:      "valid entity returns model",
			entity:    createValidFeeVarianceEntity(),
			wantErr:   nil,
			wantModel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			model, err := NewPostgreSQLModel(tt.entity)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, model)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, model)

			assert.Equal(t, tt.entity.ID.String(), model.ID)
			assert.Equal(t, tt.entity.ContextID.String(), model.ContextID)
			assert.Equal(t, tt.entity.RunID.String(), model.RunID)
			assert.Equal(t, tt.entity.MatchGroupID.String(), model.MatchGroupID)
			assert.Equal(t, tt.entity.TransactionID.String(), model.TransactionID)
			assert.Equal(t, tt.entity.FeeScheduleID.String(), model.FeeScheduleID)
			assert.Equal(t, tt.entity.FeeScheduleNameSnapshot, model.FeeScheduleNameSnapshot)
			assert.Equal(t, tt.entity.Currency, model.Currency)
			assert.True(t, tt.entity.ExpectedFee.Equal(model.ExpectedFee))
			assert.True(t, tt.entity.ActualFee.Equal(model.ActualFee))
			assert.True(t, tt.entity.Delta.Equal(model.Delta))
			assert.True(t, tt.entity.ToleranceAbs.Equal(model.ToleranceAbs))
			assert.True(t, tt.entity.TolerancePct.Equal(model.TolerancePct))
			assert.Equal(t, tt.entity.VarianceType, model.VarianceType)
			assert.Equal(t, tt.entity.CreatedAt, model.CreatedAt)
			assert.Equal(t, tt.entity.UpdatedAt, model.UpdatedAt)
		})
	}
}

func TestToEntity(t *testing.T) {
	t.Parallel()

	validModel := createValidPostgreSQLModel()

	tests := []struct {
		name       string
		model      *PostgreSQLModel
		wantErr    bool
		errContain string
	}{
		{
			name:       "nil model returns error",
			model:      nil,
			wantErr:    true,
			errContain: "fee variance model is required",
		},
		{
			name:       "valid model returns entity",
			model:      validModel,
			wantErr:    false,
			errContain: "",
		},
		{
			name: "invalid ID returns error",
			model: func() *PostgreSQLModel {
				m := createValidPostgreSQLModel()
				m.ID = "invalid-uuid"

				return m
			}(),
			wantErr:    true,
			errContain: "parse id",
		},
		{
			name: "invalid context ID returns error",
			model: func() *PostgreSQLModel {
				m := createValidPostgreSQLModel()
				m.ContextID = "invalid-uuid"

				return m
			}(),
			wantErr:    true,
			errContain: "parse context id",
		},
		{
			name: "invalid run ID returns error",
			model: func() *PostgreSQLModel {
				m := createValidPostgreSQLModel()
				m.RunID = "invalid-uuid"

				return m
			}(),
			wantErr:    true,
			errContain: "parse run id",
		},
		{
			name: "invalid match group ID returns error",
			model: func() *PostgreSQLModel {
				m := createValidPostgreSQLModel()
				m.MatchGroupID = "invalid-uuid"

				return m
			}(),
			wantErr:    true,
			errContain: "parse match group id",
		},
		{
			name: "invalid transaction ID returns error",
			model: func() *PostgreSQLModel {
				m := createValidPostgreSQLModel()
				m.TransactionID = "invalid-uuid"

				return m
			}(),
			wantErr:    true,
			errContain: "parse transaction id",
		},
		{
			name: "invalid fee schedule ID returns error",
			model: func() *PostgreSQLModel {
				m := createValidPostgreSQLModel()
				m.FeeScheduleID = "invalid-uuid"

				return m
			}(),
			wantErr:    true,
			errContain: "parse fee schedule id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			entity, err := tt.model.ToEntity()

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContain)
				assert.Nil(t, entity)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, entity)

			assert.Equal(t, tt.model.ID, entity.ID.String())
			assert.Equal(t, tt.model.ContextID, entity.ContextID.String())
			assert.Equal(t, tt.model.RunID, entity.RunID.String())
			assert.Equal(t, tt.model.MatchGroupID, entity.MatchGroupID.String())
			assert.Equal(t, tt.model.TransactionID, entity.TransactionID.String())
			assert.Equal(t, tt.model.FeeScheduleID, entity.FeeScheduleID.String())
			assert.Equal(t, tt.model.FeeScheduleNameSnapshot, entity.FeeScheduleNameSnapshot)
			assert.Equal(t, tt.model.Currency, entity.Currency)
			assert.True(t, tt.model.ExpectedFee.Equal(entity.ExpectedFee))
			assert.True(t, tt.model.ActualFee.Equal(entity.ActualFee))
			assert.True(t, tt.model.Delta.Equal(entity.Delta))
			assert.True(t, tt.model.ToleranceAbs.Equal(entity.ToleranceAbs))
			assert.True(t, tt.model.TolerancePct.Equal(entity.TolerancePct))
			assert.Equal(t, tt.model.VarianceType, entity.VarianceType)
			assert.Equal(t, tt.model.CreatedAt, entity.CreatedAt)
			assert.Equal(t, tt.model.UpdatedAt, entity.UpdatedAt)
		})
	}
}

func TestPostgreSQLModel_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	entity, err := matchingEntities.NewFeeVariance(
		ctx,
		uuid.New(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
		"Visa Domestic",
		"USD",
		decimal.NewFromFloat(100.50),
		decimal.NewFromFloat(105.75),
		decimal.NewFromFloat(1.00),
		decimal.NewFromFloat(0.05),
		"over_tolerance",
	)
	require.NoError(t, err)

	model, err := NewPostgreSQLModel(entity)
	require.NoError(t, err)

	back, err := model.ToEntity()
	require.NoError(t, err)

	assert.Equal(t, entity.ID, back.ID)
	assert.Equal(t, entity.ContextID, back.ContextID)
	assert.Equal(t, entity.RunID, back.RunID)
	assert.Equal(t, entity.MatchGroupID, back.MatchGroupID)
	assert.Equal(t, entity.TransactionID, back.TransactionID)
	assert.Equal(t, entity.FeeScheduleID, back.FeeScheduleID)
	assert.Equal(t, entity.FeeScheduleNameSnapshot, back.FeeScheduleNameSnapshot)
	assert.Equal(t, entity.Currency, back.Currency)
	assert.True(t, entity.ExpectedFee.Equal(back.ExpectedFee))
	assert.True(t, entity.ActualFee.Equal(back.ActualFee))
	assert.True(t, entity.Delta.Equal(back.Delta))
	assert.True(t, entity.ToleranceAbs.Equal(back.ToleranceAbs))
	assert.True(t, entity.TolerancePct.Equal(back.TolerancePct))
	assert.Equal(t, entity.VarianceType, back.VarianceType)
}

func createValidFeeVarianceEntity() *matchingEntities.FeeVariance {
	now := time.Now().UTC()

	return &matchingEntities.FeeVariance{
		ID:                      uuid.New(),
		ContextID:               uuid.New(),
		RunID:                   uuid.New(),
		MatchGroupID:            uuid.New(),
		TransactionID:           uuid.New(),
		FeeScheduleID:           uuid.New(),
		FeeScheduleNameSnapshot: "Visa Domestic",
		Currency:                "USD",
		ExpectedFee:             decimal.NewFromFloat(100.50),
		ActualFee:               decimal.NewFromFloat(105.75),
		Delta:                   decimal.NewFromFloat(5.25),
		ToleranceAbs:            decimal.NewFromFloat(1.00),
		TolerancePct:            decimal.NewFromFloat(0.05),
		VarianceType:            "over_tolerance",
		CreatedAt:               now,
		UpdatedAt:               now,
	}
}

func createValidPostgreSQLModel() *PostgreSQLModel {
	now := time.Now().UTC()

	return &PostgreSQLModel{
		ID:                      uuid.New().String(),
		ContextID:               uuid.New().String(),
		RunID:                   uuid.New().String(),
		MatchGroupID:            uuid.New().String(),
		TransactionID:           uuid.New().String(),
		FeeScheduleID:           uuid.New().String(),
		FeeScheduleNameSnapshot: "Visa Domestic",
		Currency:                "USD",
		ExpectedFee:             decimal.NewFromFloat(100.50),
		ActualFee:               decimal.NewFromFloat(105.75),
		Delta:                   decimal.NewFromFloat(5.25),
		ToleranceAbs:            decimal.NewFromFloat(1.00),
		TolerancePct:            decimal.NewFromFloat(0.05),
		VarianceType:            "over_tolerance",
		CreatedAt:               now,
		UpdatedAt:               now,
	}
}
