//go:build unit

package fee

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validScheduleInput() NewFeeScheduleInput {
	return NewFeeScheduleInput{
		TenantID:         uuid.New(),
		Name:             "Standard Payment Fees",
		Currency:         "USD",
		ApplicationOrder: ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     RoundingModeHalfUp,
		Items: []FeeScheduleItemInput{
			{
				Name:      "Interchange",
				Priority:  1,
				Structure: PercentageFee{Rate: decimal.RequireFromString("0.015")},
			},
		},
	}
}

func TestNewFeeSchedule(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("creates schedule with single percentage item", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()

		schedule, err := NewFeeSchedule(ctx, input)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, schedule.ID)
		assert.Equal(t, input.TenantID, schedule.TenantID)
		assert.Equal(t, "Standard Payment Fees", schedule.Name)
		assert.Equal(t, "USD", schedule.Currency)
		assert.Equal(t, ApplicationOrderParallel, schedule.ApplicationOrder)
		assert.Equal(t, 2, schedule.RoundingScale)
		assert.Equal(t, RoundingModeHalfUp, schedule.RoundingMode)
		assert.Len(t, schedule.Items, 1)
		assert.False(t, schedule.CreatedAt.IsZero())
		assert.False(t, schedule.UpdatedAt.IsZero())
	})

	t.Run("creates schedule with multiple item types", func(t *testing.T) {
		t.Parallel()

		upTo1000 := decimal.NewFromInt(1000)
		input := NewFeeScheduleInput{
			TenantID:         uuid.New(),
			Name:             "Multi-Fee Schedule",
			Currency:         "eur",
			ApplicationOrder: ApplicationOrderCascading,
			RoundingScale:    4,
			RoundingMode:     RoundingModeBankers,
			Items: []FeeScheduleItemInput{
				{
					Name:      "Flat Processing",
					Priority:  1,
					Structure: FlatFee{Amount: decimal.RequireFromString("0.30")},
				},
				{
					Name:      "Interchange",
					Priority:  2,
					Structure: PercentageFee{Rate: decimal.RequireFromString("0.015")},
				},
				{
					Name:     "Tiered Scheme Fee",
					Priority: 3,
					Structure: TieredFee{Tiers: []Tier{
						{UpTo: &upTo1000, Rate: decimal.RequireFromString("0.001")},
						{UpTo: nil, Rate: decimal.RequireFromString("0.0005")},
					}},
				},
			},
		}

		schedule, err := NewFeeSchedule(ctx, input)
		require.NoError(t, err)
		assert.Equal(t, "EUR", schedule.Currency)
		assert.Len(t, schedule.Items, 3)

		for _, item := range schedule.Items {
			assert.NotEqual(t, uuid.Nil, item.ID)
			assert.False(t, item.CreatedAt.IsZero())
			assert.False(t, item.UpdatedAt.IsZero())
		}
	})

	t.Run("fails with empty name", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Name = ""

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrScheduleNameRequired)
	})

	t.Run("fails with name exceeding 100 characters", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Name = strings.Repeat("x", 101)

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrScheduleNameTooLong)
	})

	t.Run("succeeds with name exactly 100 characters", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Name = strings.Repeat("x", 100)

		schedule, err := NewFeeSchedule(ctx, input)
		require.NoError(t, err)
		assert.Len(t, schedule.Name, 100)
	})

	t.Run("fails with empty items", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Items = nil

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrScheduleItemsRequired)
	})

	t.Run("fails with duplicate priorities", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Items = []FeeScheduleItemInput{
			{
				Name:      "Fee A",
				Priority:  1,
				Structure: FlatFee{Amount: decimal.NewFromInt(1)},
			},
			{
				Name:      "Fee B",
				Priority:  1,
				Structure: FlatFee{Amount: decimal.NewFromInt(2)},
			},
		}

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDuplicateItemPriority)
	})

	t.Run("fails with invalid currency", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Currency = ""

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidCurrency)
	})

	t.Run("fails with nil structure in item", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Items = []FeeScheduleItemInput{
			{
				Name:      "Bad Item",
				Priority:  1,
				Structure: nil,
			},
		}

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNilFeeStructure)
	})

	t.Run("fails with invalid application order", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.ApplicationOrder = "INVALID"

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidApplicationOrder)
	})

	t.Run("fails with rounding scale below zero", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.RoundingScale = -1

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidRoundingScale)
	})

	t.Run("fails with rounding scale above 10", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.RoundingScale = 11

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidRoundingScale)
	})

	t.Run("succeeds with rounding scale at boundaries", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.RoundingScale = 0

		schedule, err := NewFeeSchedule(ctx, input)
		require.NoError(t, err)
		assert.Equal(t, 0, schedule.RoundingScale)

		input2 := validScheduleInput()
		input2.RoundingScale = 10

		schedule2, err := NewFeeSchedule(ctx, input2)
		require.NoError(t, err)
		assert.Equal(t, 10, schedule2.RoundingScale)
	})

	t.Run("fails with invalid rounding mode", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.RoundingMode = "INVALID"

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidRoundingMode)
	})

	t.Run("fails with empty item name", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Items = []FeeScheduleItemInput{
			{
				Name:      "",
				Priority:  1,
				Structure: FlatFee{Amount: decimal.NewFromInt(1)},
			},
		}

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrItemNameRequired)
	})

	t.Run("generates unique UUIDs for schedule and items", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Items = []FeeScheduleItemInput{
			{
				Name:      "Fee A",
				Priority:  1,
				Structure: FlatFee{Amount: decimal.NewFromInt(1)},
			},
			{
				Name:      "Fee B",
				Priority:  2,
				Structure: FlatFee{Amount: decimal.NewFromInt(2)},
			},
		}

		schedule, err := NewFeeSchedule(ctx, input)
		require.NoError(t, err)

		ids := map[uuid.UUID]struct{}{
			schedule.ID: {},
		}
		for _, item := range schedule.Items {
			assert.NotEqual(t, uuid.Nil, item.ID)
			_, exists := ids[item.ID]
			assert.False(t, exists, "duplicate UUID detected: %s", item.ID)
			ids[item.ID] = struct{}{}
		}
	})

	t.Run("normalizes currency to uppercase", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Currency = "  gbp  "

		schedule, err := NewFeeSchedule(ctx, input)
		require.NoError(t, err)
		assert.Equal(t, "GBP", schedule.Currency)
	})

	t.Run("fails with invalid fee structure in item", func(t *testing.T) {
		t.Parallel()

		input := validScheduleInput()
		input.Items = []FeeScheduleItemInput{
			{
				Name:      "Bad Percentage",
				Priority:  1,
				Structure: PercentageFee{Rate: decimal.RequireFromString("1.5")},
			},
		}

		_, err := NewFeeSchedule(ctx, input)
		require.Error(t, err)
	})
}

func TestApplicationOrder_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		order ApplicationOrder
		want  bool
	}{
		{name: "PARALLEL is valid", order: ApplicationOrderParallel, want: true},
		{name: "CASCADING is valid", order: ApplicationOrderCascading, want: true},
		{name: "empty is invalid", order: "", want: false},
		{name: "lowercase is invalid", order: "parallel", want: false},
		{name: "unknown is invalid", order: "SEQUENTIAL", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.order.IsValid())
		})
	}
}

func TestRoundingMode_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode RoundingMode
		want bool
	}{
		{name: "HALF_UP is valid", mode: RoundingModeHalfUp, want: true},
		{name: "BANKERS is valid", mode: RoundingModeBankers, want: true},
		{name: "FLOOR is valid", mode: RoundingModeFloor, want: true},
		{name: "CEIL is valid", mode: RoundingModeCeil, want: true},
		{name: "TRUNCATE is valid", mode: RoundingModeTruncate, want: true},
		{name: "empty is invalid", mode: "", want: false},
		{name: "lowercase is invalid", mode: "half_up", want: false},
		{name: "unknown is invalid", mode: "ROUND_UP", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.mode.IsValid())
		})
	}
}
