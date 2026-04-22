//go:build unit

package match_item

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

func TestPostgreSQLModel_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()

		item, err := matchingEntities.NewMatchItem(
			ctx,
			uuid.New(),
			decimal.NewFromInt(10),
			"USD",
			decimal.NewFromInt(10),
		)
		require.NoError(t, err)

		item.AllowPartial = true

		model, err := NewPostgreSQLModel(item)
		require.NoError(t, err)

		again, err := model.ToEntity()
		require.NoError(t, err)

		require.Equal(t, item.ID, again.ID)
		require.Equal(t, item.MatchGroupID, again.MatchGroupID)
		require.Equal(t, item.TransactionID, again.TransactionID)
		require.True(t, again.AllowPartial)
		require.True(t, again.ExpectedAmount.Equal(item.ExpectedAmount))
	})
}

func TestNewPostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewPostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrMatchItemEntityNeeded)
}

func TestNewPostgreSQLModel_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()
	txID := uuid.New()

	item, err := matchingEntities.NewMatchItem(
		ctx,
		txID,
		decimal.NewFromFloat(150.50),
		"EUR",
		decimal.NewFromFloat(150.50),
	)
	require.NoError(t, err)

	item.CreatedAt = now
	item.UpdatedAt = now
	item.AllowPartial = true

	model, err := NewPostgreSQLModel(item)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, item.ID, model.ID)
	assert.Equal(t, item.MatchGroupID, model.MatchGroupID)
	assert.Equal(t, txID, model.TransactionID)
	assert.True(t, model.AllocatedAmount.Equal(decimal.NewFromFloat(150.50)))
	assert.Equal(t, "EUR", model.AllocatedCurrency)
	assert.True(t, model.ExpectedAmount.Equal(decimal.NewFromFloat(150.50)))
	assert.True(t, model.AllowPartial)
	assert.Equal(t, now, model.CreatedAt)
	assert.Equal(t, now, model.UpdatedAt)
}

func TestToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *PostgreSQLModel
	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrMatchItemModelNeeded)
}

func TestToEntity_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id := uuid.New()
	groupID := uuid.New()
	txID := uuid.New()

	model := &PostgreSQLModel{
		ID:                id,
		MatchGroupID:      groupID,
		TransactionID:     txID,
		AllocatedAmount:   decimal.NewFromFloat(200.00),
		AllocatedCurrency: "USD",
		ExpectedAmount:    decimal.NewFromFloat(200.00),
		AllowPartial:      false,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, id, entity.ID)
	assert.Equal(t, groupID, entity.MatchGroupID)
	assert.Equal(t, txID, entity.TransactionID)
	assert.True(t, entity.AllocatedAmount.Equal(decimal.NewFromFloat(200.00)))
	assert.Equal(t, "USD", entity.AllocatedCurrency)
	assert.True(t, entity.ExpectedAmount.Equal(decimal.NewFromFloat(200.00)))
	assert.False(t, entity.AllowPartial)
	assert.Equal(t, now, entity.CreatedAt)
	assert.Equal(t, now, entity.UpdatedAt)
}
