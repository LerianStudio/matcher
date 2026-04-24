// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestFeeScheduleToResponse_WithItems(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	schedule := &fee.FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Test",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:        uuid.New(),
				Name:      "flat-fee",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromFloat(5.00)},
				CreatedAt: now,
				UpdatedAt: now,
			},
			{
				ID:        uuid.New(),
				Name:      "percentage",
				Priority:  2,
				Structure: fee.PercentageFee{Rate: decimal.NewFromFloat(0.015)},
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	resp := FeeScheduleToResponse(schedule)

	assert.Equal(t, schedule.ID.String(), resp.ID)
	assert.Equal(t, "PARALLEL", resp.ApplicationOrder)
	assert.Len(t, resp.Items, 2)
	assert.Equal(t, "FLAT", resp.Items[0].StructureType)
	assert.Equal(t, "PERCENTAGE", resp.Items[1].StructureType)
}

func TestFeeScheduleToResponse_NilSchedule(t *testing.T) {
	t.Parallel()

	resp := FeeScheduleToResponse(nil)
	assert.Empty(t, resp.ID)
	assert.NotNil(t, resp.Items)
}

func TestFeeSchedulesToResponse_Empty(t *testing.T) {
	t.Parallel()

	resp := FeeSchedulesToResponse(nil)
	assert.NotNil(t, resp)
	assert.Empty(t, resp)
}

func TestFeeSchedulesToResponse_WithSchedules(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	schedules := []*fee.FeeSchedule{
		{
			ID:               uuid.New(),
			Name:             "Schedule 1",
			ApplicationOrder: fee.ApplicationOrderParallel,
			RoundingMode:     fee.RoundingModeHalfUp,
			Items:            []fee.FeeScheduleItem{},
			CreatedAt:        now,
			UpdatedAt:        now,
		},
		nil,
		{
			ID:               uuid.New(),
			Name:             "Schedule 2",
			ApplicationOrder: fee.ApplicationOrderCascading,
			RoundingMode:     fee.RoundingModeBankers,
			Items:            []fee.FeeScheduleItem{},
			CreatedAt:        now,
			UpdatedAt:        now,
		},
	}

	resp := FeeSchedulesToResponse(schedules)
	assert.Len(t, resp, 2)
}

func TestFeeStructureToMap_Tiered(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	upTo := decimal.NewFromInt(100)
	schedule := &fee.FeeSchedule{
		ID:               uuid.New(),
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:       uuid.New(),
				Name:     "tiered",
				Priority: 1,
				Structure: fee.TieredFee{
					Tiers: []fee.Tier{
						{UpTo: &upTo, Rate: decimal.NewFromFloat(0.01)},
						{Rate: decimal.NewFromFloat(0.005)},
					},
				},
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	resp := FeeScheduleToResponse(schedule)
	assert.Len(t, resp.Items, 1)
	assert.Equal(t, "TIERED", resp.Items[0].StructureType)

	assert.Contains(t, resp.Items[0].Structure, "tiers")
}

func TestFeeBreakdownToSimulateResponse_NilBreakdown(t *testing.T) {
	t.Parallel()

	resp := FeeBreakdownToSimulateResponse(decimal.NewFromFloat(100.00), "USD", nil)
	assert.Equal(t, "100", resp.GrossAmount)
	assert.Equal(t, "USD", resp.Currency)
	assert.NotNil(t, resp.Items)
	assert.Empty(t, resp.Items)
}

func TestFeeBreakdownToSimulateResponse_WithItems(t *testing.T) {
	t.Parallel()

	gross := decimal.NewFromFloat(100.00)
	breakdown := &fee.FeeBreakdown{
		TotalFee:  fee.Money{Amount: decimal.NewFromFloat(3.50), Currency: "USD"},
		NetAmount: fee.Money{Amount: decimal.NewFromFloat(96.50), Currency: "USD"},
		ItemFees: []fee.ItemFee{
			{
				ItemID:   uuid.New(),
				ItemName: "flat",
				Fee:      fee.Money{Amount: decimal.NewFromFloat(2.00), Currency: "USD"},
				BaseUsed: fee.Money{Amount: decimal.NewFromFloat(100.00), Currency: "USD"},
			},
			{
				ItemID:   uuid.New(),
				ItemName: "percentage",
				Fee:      fee.Money{Amount: decimal.NewFromFloat(1.50), Currency: "USD"},
				BaseUsed: fee.Money{Amount: decimal.NewFromFloat(100.00), Currency: "USD"},
			},
		},
	}

	resp := FeeBreakdownToSimulateResponse(gross, "USD", breakdown)
	assert.Equal(t, "3.5", resp.TotalFee)
	assert.Equal(t, "96.5", resp.NetAmount)
	assert.Len(t, resp.Items, 2)
}
