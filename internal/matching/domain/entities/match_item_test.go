// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

func TestMatchItemAllocation(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()

	item, err := entities.NewMatchItem(
		context.Background(),
		transactionID,
		decimal.NewFromFloat(10),
		"USD",
		decimal.NewFromFloat(10),
	)
	require.NoError(t, err)
	require.True(t, item.AllocatedAmount.Equal(decimal.NewFromFloat(10)))

	zeroItem, err := entities.NewMatchItem(
		context.Background(),
		transactionID,
		decimal.Zero,
		"USD",
		decimal.Zero,
	)
	require.NoError(t, err)
	require.True(t, zeroItem.AllocatedAmount.Equal(decimal.Zero))
	require.NoError(t, zeroItem.ApplyAllocation(context.Background(), decimal.Zero, true))
	require.NoError(t, zeroItem.ApplyAllocation(context.Background(), decimal.Zero, false))

	require.Error(t, item.ApplyAllocation(context.Background(), decimal.NewFromFloat(12), true))
	require.Error(t, item.ApplyAllocation(context.Background(), decimal.NewFromFloat(8), true))
	require.NoError(t, item.ApplyAllocation(context.Background(), decimal.NewFromFloat(10), true))
	require.True(t, item.AllocatedAmount.Equal(decimal.NewFromFloat(10)))

	require.Error(t, item.ApplyAllocation(context.Background(), decimal.NewFromFloat(8), false))
	require.NoError(t, item.ApplyAllocation(context.Background(), decimal.NewFromFloat(10), false))
	require.Error(t, item.ApplyAllocation(context.Background(), decimal.NewFromFloat(12), false))
}

func TestMatchItemValidation(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()

	_, err := entities.NewMatchItem(
		context.Background(),
		transactionID,
		decimal.NewFromFloat(-1),
		"USD",
		decimal.NewFromFloat(1),
	)
	require.Error(t, err)

	_, err = entities.NewMatchItem(
		context.Background(),
		transactionID,
		decimal.NewFromFloat(1),
		"",
		decimal.NewFromFloat(1),
	)
	require.Error(t, err)

	_, err = entities.NewMatchItem(
		context.Background(),
		transactionID,
		decimal.NewFromFloat(1),
		"USD",
		decimal.NewFromFloat(-1),
	)
	require.Error(t, err)

	_, err = entities.NewMatchItem(
		context.Background(),
		transactionID,
		decimal.NewFromFloat(12),
		"USD",
		decimal.NewFromFloat(10),
	)
	require.Error(t, err)

	partialItem, err := entities.NewMatchItemWithPolicy(
		context.Background(),
		transactionID,
		decimal.NewFromFloat(5),
		"USD",
		decimal.NewFromFloat(10),
		true,
	)
	require.NoError(t, err)
	require.True(t, partialItem.AllocatedAmount.Equal(decimal.NewFromFloat(5)))
	require.NoError(
		t,
		partialItem.ApplyAllocation(context.Background(), decimal.NewFromFloat(7), false),
	)
	require.Error(
		t,
		partialItem.ApplyAllocation(context.Background(), decimal.NewFromFloat(12), false),
	)

	_, err = entities.NewMatchItemWithPolicy(
		context.Background(),
		transactionID,
		decimal.NewFromFloat(12),
		"USD",
		decimal.NewFromFloat(10),
		true,
	)
	require.Error(t, err)

	_, err = entities.NewMatchItem(
		context.Background(),
		uuid.Nil,
		decimal.NewFromFloat(1),
		"USD",
		decimal.NewFromFloat(1),
	)
	require.Error(t, err)
}

func TestMatchItemApplyAllocationGuards(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()

	item, err := entities.NewMatchItem(
		context.Background(),
		transactionID,
		decimal.NewFromFloat(10),
		"USD",
		decimal.NewFromFloat(10),
	)
	require.NoError(t, err)

	require.Error(t, item.ApplyAllocation(context.Background(), decimal.NewFromFloat(-1), false))
}

func TestMatchItemApplyAllocationNil(t *testing.T) {
	t.Parallel()

	item := (*entities.MatchItem)(nil)

	require.Error(t, item.ApplyAllocation(context.Background(), decimal.NewFromFloat(1), false))
}
