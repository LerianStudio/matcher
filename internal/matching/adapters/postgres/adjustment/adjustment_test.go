// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package adjustment

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

	t.Run("converts entity with match group id", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		contextID := uuid.New()
		matchGroupID := uuid.New()

		entity, err := matchingEntities.NewAdjustment(
			ctx,
			contextID,
			&matchGroupID,
			nil,
			matchingEntities.AdjustmentTypeBankFee,
			matchingEntities.AdjustmentDirectionDebit,
			decimal.NewFromFloat(10.50),
			"USD",
			"Bank fee adjustment",
			"Processing fee",
			"user@example.com",
		)
		require.NoError(t, err)

		model, err := NewPostgreSQLModel(entity)
		require.NoError(t, err)
		require.NotNil(t, model)

		assert.Equal(t, entity.ID, model.ID)
		assert.Equal(t, entity.ContextID, model.ContextID)
		assert.True(t, model.MatchGroupID.Valid)
		assert.Equal(t, matchGroupID, model.MatchGroupID.UUID)
		assert.False(t, model.TransactionID.Valid)
		assert.Equal(t, "BANK_FEE", model.Type)
		assert.True(t, entity.Amount.Equal(model.Amount))
		assert.Equal(t, "USD", model.Currency)
		assert.Equal(t, "Bank fee adjustment", model.Description)
		assert.Equal(t, "Processing fee", model.Reason)
		assert.Equal(t, "user@example.com", model.CreatedBy)
	})

	t.Run("converts entity with transaction id", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		contextID := uuid.New()
		transactionID := uuid.New()

		entity, err := matchingEntities.NewAdjustment(
			ctx,
			contextID,
			nil,
			&transactionID,
			matchingEntities.AdjustmentTypeRounding,
			matchingEntities.AdjustmentDirectionDebit,
			decimal.NewFromFloat(0.01),
			"EUR",
			"Rounding adjustment",
			"Sub-cent rounding",
			"system",
		)
		require.NoError(t, err)

		model, err := NewPostgreSQLModel(entity)
		require.NoError(t, err)
		require.NotNil(t, model)

		assert.False(t, model.MatchGroupID.Valid)
		assert.True(t, model.TransactionID.Valid)
		assert.Equal(t, transactionID, model.TransactionID.UUID)
	})

	t.Run("converts entity with both ids", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		contextID := uuid.New()
		matchGroupID := uuid.New()
		transactionID := uuid.New()

		entity, err := matchingEntities.NewAdjustment(
			ctx,
			contextID,
			&matchGroupID,
			&transactionID,
			matchingEntities.AdjustmentTypeFXDifference,
			matchingEntities.AdjustmentDirectionDebit,
			decimal.NewFromFloat(5.25),
			"GBP",
			"FX adjustment",
			"Currency variance",
			"fx-service",
		)
		require.NoError(t, err)

		model, err := NewPostgreSQLModel(entity)
		require.NoError(t, err)
		require.NotNil(t, model)

		assert.True(t, model.MatchGroupID.Valid)
		assert.True(t, model.TransactionID.Valid)
	})

	t.Run("returns error for nil entity", func(t *testing.T) {
		t.Parallel()

		model, err := NewPostgreSQLModel(nil)
		require.Error(t, err)
		assert.Nil(t, model)
		require.ErrorIs(t, err, ErrAdjustmentEntityNeeded)
	})
}

func TestPostgreSQLModel_ToEntity(t *testing.T) {
	t.Parallel()

	t.Run("converts model with match group id", func(t *testing.T) {
		t.Parallel()

		id := uuid.New()
		contextID := uuid.New()
		matchGroupID := uuid.New()
		now := time.Now().UTC()

		model := &PostgreSQLModel{
			ID:           id,
			ContextID:    contextID,
			MatchGroupID: uuid.NullUUID{UUID: matchGroupID, Valid: true},
			Type:         "BANK_FEE",
			Direction:    "DEBIT",
			Amount:       decimal.NewFromFloat(10.50),
			Currency:     "USD",
			Description:  "Bank fee adjustment",
			Reason:       "Processing fee",
			CreatedBy:    "user@example.com",
			CreatedAt:    now,
			UpdatedAt:    now,
		}

		entity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, entity)

		assert.Equal(t, id, entity.ID)
		assert.Equal(t, contextID, entity.ContextID)
		require.NotNil(t, entity.MatchGroupID)
		assert.Equal(t, matchGroupID, *entity.MatchGroupID)
		assert.Nil(t, entity.TransactionID)
		assert.Equal(t, matchingEntities.AdjustmentTypeBankFee, entity.Type)
		assert.True(t, model.Amount.Equal(entity.Amount))
		assert.Equal(t, "USD", entity.Currency)
		assert.Equal(t, "Bank fee adjustment", entity.Description)
		assert.Equal(t, "Processing fee", entity.Reason)
		assert.Equal(t, "user@example.com", entity.CreatedBy)
		assert.Equal(t, now, entity.CreatedAt)
		assert.Equal(t, now, entity.UpdatedAt)
	})

	t.Run("converts model with transaction id", func(t *testing.T) {
		t.Parallel()

		id := uuid.New()
		contextID := uuid.New()
		transactionID := uuid.New()
		now := time.Now().UTC()

		model := &PostgreSQLModel{
			ID:            id,
			ContextID:     contextID,
			TransactionID: uuid.NullUUID{UUID: transactionID, Valid: true},
			Type:          "ROUNDING",
			Direction:     "DEBIT",
			Amount:        decimal.NewFromFloat(0.01),
			Currency:      "EUR",
			Description:   "Rounding adjustment",
			Reason:        "Sub-cent rounding",
			CreatedBy:     "system",
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		entity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, entity)

		assert.Nil(t, entity.MatchGroupID)
		require.NotNil(t, entity.TransactionID)
		assert.Equal(t, transactionID, *entity.TransactionID)
	})

	t.Run("converts model with both ids", func(t *testing.T) {
		t.Parallel()

		id := uuid.New()
		contextID := uuid.New()
		matchGroupID := uuid.New()
		transactionID := uuid.New()
		now := time.Now().UTC()

		model := &PostgreSQLModel{
			ID:            id,
			ContextID:     contextID,
			MatchGroupID:  uuid.NullUUID{UUID: matchGroupID, Valid: true},
			TransactionID: uuid.NullUUID{UUID: transactionID, Valid: true},
			Type:          "FX_DIFFERENCE",
			Direction:     "DEBIT",
			Amount:        decimal.NewFromFloat(5.25),
			Currency:      "GBP",
			Description:   "FX adjustment",
			Reason:        "Currency variance",
			CreatedBy:     "fx-service",
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		entity, err := model.ToEntity()
		require.NoError(t, err)
		require.NotNil(t, entity)

		require.NotNil(t, entity.MatchGroupID)
		assert.Equal(t, matchGroupID, *entity.MatchGroupID)
		require.NotNil(t, entity.TransactionID)
		assert.Equal(t, transactionID, *entity.TransactionID)
	})

	t.Run("returns error for nil model", func(t *testing.T) {
		t.Parallel()

		var model *PostgreSQLModel

		entity, err := model.ToEntity()
		require.Error(t, err)
		assert.Nil(t, entity)
		require.ErrorIs(t, err, ErrAdjustmentModelNeeded)
	})

}

func TestPostgreSQLModel_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with match group id", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		matchGroupID := uuid.New()

		original, err := matchingEntities.NewAdjustment(
			ctx,
			contextID,
			&matchGroupID,
			nil,
			matchingEntities.AdjustmentTypeBankFee,
			matchingEntities.AdjustmentDirectionDebit,
			decimal.NewFromFloat(10.50),
			"USD",
			"Bank fee adjustment",
			"Processing fee",
			"user@example.com",
		)
		require.NoError(t, err)

		model, err := NewPostgreSQLModel(original)
		require.NoError(t, err)

		restored, err := model.ToEntity()
		require.NoError(t, err)

		assert.Equal(t, original.ID, restored.ID)
		assert.Equal(t, original.ContextID, restored.ContextID)
		assert.Equal(t, original.MatchGroupID, restored.MatchGroupID)
		assert.Equal(t, original.TransactionID, restored.TransactionID)
		assert.Equal(t, original.Type, restored.Type)
		assert.True(t, original.Amount.Equal(restored.Amount))
		assert.Equal(t, original.Currency, restored.Currency)
		assert.Equal(t, original.Description, restored.Description)
		assert.Equal(t, original.Reason, restored.Reason)
		assert.Equal(t, original.CreatedBy, restored.CreatedBy)
		assert.True(t, original.CreatedAt.Equal(restored.CreatedAt))
		assert.True(t, original.UpdatedAt.Equal(restored.UpdatedAt))
	})

	t.Run("with transaction id", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		transactionID := uuid.New()

		original, err := matchingEntities.NewAdjustment(
			ctx,
			contextID,
			nil,
			&transactionID,
			matchingEntities.AdjustmentTypeRounding,
			matchingEntities.AdjustmentDirectionDebit,
			decimal.NewFromFloat(0.01),
			"EUR",
			"Rounding adjustment",
			"Sub-cent rounding",
			"system",
		)
		require.NoError(t, err)

		model, err := NewPostgreSQLModel(original)
		require.NoError(t, err)

		restored, err := model.ToEntity()
		require.NoError(t, err)

		assert.Equal(t, original.ID, restored.ID)
		assert.Equal(t, original.TransactionID, restored.TransactionID)
		assert.Nil(t, restored.MatchGroupID)
	})

	t.Run("all adjustment types", func(t *testing.T) {
		t.Parallel()

		types := []matchingEntities.AdjustmentType{
			matchingEntities.AdjustmentTypeBankFee,
			matchingEntities.AdjustmentTypeFXDifference,
			matchingEntities.AdjustmentTypeRounding,
			matchingEntities.AdjustmentTypeWriteOff,
			matchingEntities.AdjustmentTypeMiscellaneous,
		}

		for _, adjType := range types {
			t.Run(string(adjType), func(t *testing.T) {
				t.Parallel()

				contextID := uuid.New()
				matchGroupID := uuid.New()

				original, err := matchingEntities.NewAdjustment(
					ctx,
					contextID,
					&matchGroupID,
					nil,
					adjType,
					matchingEntities.AdjustmentDirectionDebit,
					decimal.NewFromFloat(1.0),
					"USD",
					"Test",
					"Reason",
					"user",
				)
				require.NoError(t, err)

				model, err := NewPostgreSQLModel(original)
				require.NoError(t, err)

				restored, err := model.ToEntity()
				require.NoError(t, err)

				assert.Equal(t, adjType, restored.Type)
			})
		}
	})
}

func TestPostgreSQLModel_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("small amount", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		contextID := uuid.New()
		matchGroupID := uuid.New()

		original, err := matchingEntities.NewAdjustment(
			ctx,
			contextID,
			&matchGroupID,
			nil,
			matchingEntities.AdjustmentTypeRounding,
			matchingEntities.AdjustmentDirectionDebit,
			decimal.NewFromFloat(0.01),
			"USD",
			"Small adjustment",
			"Sub-cent variance",
			"user",
		)
		require.NoError(t, err)

		model, err := NewPostgreSQLModel(original)
		require.NoError(t, err)

		restored, err := model.ToEntity()
		require.NoError(t, err)

		assert.True(t, original.Amount.Equal(restored.Amount))
	})

	t.Run("credit direction", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		contextID := uuid.New()
		matchGroupID := uuid.New()

		original, err := matchingEntities.NewAdjustment(
			ctx,
			contextID,
			&matchGroupID,
			nil,
			matchingEntities.AdjustmentTypeWriteOff,
			matchingEntities.AdjustmentDirectionCredit,
			decimal.NewFromFloat(100.50),
			"USD",
			"Credit adjustment",
			"Refund",
			"user",
		)
		require.NoError(t, err)

		model, err := NewPostgreSQLModel(original)
		require.NoError(t, err)

		restored, err := model.ToEntity()
		require.NoError(t, err)

		assert.Equal(t, matchingEntities.AdjustmentDirectionCredit, restored.Direction)
		assert.True(t, original.Amount.Equal(restored.Amount))
	})

	t.Run("large amount", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		contextID := uuid.New()
		matchGroupID := uuid.New()

		largeAmount := decimal.NewFromFloat(999999999999.999999)

		original, err := matchingEntities.NewAdjustment(
			ctx,
			contextID,
			&matchGroupID,
			nil,
			matchingEntities.AdjustmentTypeMiscellaneous,
			matchingEntities.AdjustmentDirectionDebit,
			largeAmount,
			"USD",
			"Large adjustment",
			"High value",
			"user",
		)
		require.NoError(t, err)

		model, err := NewPostgreSQLModel(original)
		require.NoError(t, err)

		restored, err := model.ToEntity()
		require.NoError(t, err)

		assert.True(t, original.Amount.Equal(restored.Amount))
	})
}
