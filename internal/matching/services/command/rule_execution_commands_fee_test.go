//go:build unit

package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func newTestFeeSchedule(currency string, rate decimal.Decimal) *fee.FeeSchedule {
	return &fee.FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Test Schedule",
		Currency:         currency,
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:       uuid.New(),
				Name:     "Processing Fee",
				Priority: 1,
				Structure: fee.PercentageFee{
					Rate: rate,
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func TestMapTransactionsWithFees_NoSchedule(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceID,
		Amount:     decimal.RequireFromString("100.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-001",
	}

	nopLogger := &libLog.NopLogger{}

	// nil schedules map
	result := mapTransactionsWithFees(context.Background(), []*shared.Transaction{txn}, nil, fee.NormalizationModeNet, nopLogger)

	require.Len(t, result, 1)
	assert.True(t, txn.Amount.Equal(result[0].Amount), "amount should pass through unchanged")
	assert.True(t, txn.Amount.Equal(result[0].OriginalAmount), "original amount should equal amount")
	assert.Nil(t, result[0].FeeBreakdown, "no breakdown when no schedule")

	// empty schedules map
	result = mapTransactionsWithFees(context.Background(), []*shared.Transaction{txn}, map[uuid.UUID]*fee.FeeSchedule{}, fee.NormalizationModeNet, nopLogger)

	require.Len(t, result, 1)
	assert.True(t, txn.Amount.Equal(result[0].Amount), "amount should pass through unchanged with empty map")
	assert.True(t, txn.Amount.Equal(result[0].OriginalAmount), "original amount should equal amount with empty map")
	assert.Nil(t, result[0].FeeBreakdown, "no breakdown when empty schedule map")
}

func TestMapTransactionsWithFees_NetMode(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceID,
		Amount:     decimal.RequireFromString("1000.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-002",
	}

	// 1.5% fee
	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		sourceID: schedule,
	}

	result := mapTransactionsWithFees(context.Background(), []*shared.Transaction{txn}, schedules, fee.NormalizationModeNet, &libLog.NopLogger{})

	require.Len(t, result, 1)

	// Original amount preserved
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"original amount should be preserved")

	// Fee is 1.5% of 1000 = 15.00
	// Net = 1000 - 15 = 985.00
	assert.True(t, decimal.RequireFromString("985.00").Equal(result[0].Amount),
		"amount should be net (gross - fee): got %s", result[0].Amount)

	// FeeBreakdown populated
	require.NotNil(t, result[0].FeeBreakdown, "fee breakdown should be populated")
	assert.True(t, decimal.RequireFromString("15.00").Equal(result[0].FeeBreakdown.TotalFee.Amount),
		"total fee should be 15.00")
	assert.True(t, decimal.RequireFromString("985.00").Equal(result[0].FeeBreakdown.NetAmount.Amount),
		"net amount in breakdown should be 985.00")
}

func TestMapTransactionsWithFees_GrossMode(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceID,
		Amount:     decimal.RequireFromString("1000.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-003",
	}

	// 1.5% fee
	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		sourceID: schedule,
	}

	result := mapTransactionsWithFees(context.Background(), []*shared.Transaction{txn}, schedules, fee.NormalizationModeGross, &libLog.NopLogger{})

	require.Len(t, result, 1)

	// Original amount preserved
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"original amount should be preserved")

	// GROSS mode uses inverse calculation: gross = net / (1 - rate)
	// For 1.5% fee on net=1000: gross = 1000 / (1 - 0.015) = 1000 / 0.985 = 1015.228...
	// With rounding scale 2, HALF_UP: fee on $1015.23 = $1015.23 * 0.015 = $15.228... → $15.23
	// impliedNet = 1015.23 - 15.23 = 1000.00 ✓
	expectedGross := decimal.RequireFromString("1015.23")
	assert.True(t, result[0].Amount.Equal(expectedGross),
		"expected gross %s, got %s", expectedGross, result[0].Amount)

	require.NotNil(t, result[0].FeeBreakdown, "fee breakdown should be populated")
	assert.True(t, result[0].FeeBreakdown.TotalFee.Amount.Equal(decimal.RequireFromString("15.23")),
		"expected fee 15.23, got %s", result[0].FeeBreakdown.TotalFee.Amount)
}

func TestMapTransactionsWithFees_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceID,
		Amount:     decimal.RequireFromString("1000.00"),
		Currency:   "EUR",
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-004",
	}

	// Schedule is in USD but transaction is in EUR
	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		sourceID: schedule,
	}

	result := mapTransactionsWithFees(context.Background(), []*shared.Transaction{txn}, schedules, fee.NormalizationModeNet, &libLog.NopLogger{})

	require.Len(t, result, 1)

	// Amount should pass through unchanged due to currency mismatch
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].Amount),
		"amount should pass through unchanged on currency mismatch")
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"original amount should equal amount")
	assert.Nil(t, result[0].FeeBreakdown, "no breakdown when currency mismatch")
}

func TestMapTransactionsWithFees_NilTransaction(t *testing.T) {
	t.Parallel()

	txns := []*shared.Transaction{nil, nil}

	result := mapTransactionsWithFees(context.Background(), txns, nil, fee.NormalizationModeNet, &libLog.NopLogger{})

	assert.Empty(t, result, "nil transactions should be skipped")
}

func TestMapTransactionsWithFees_MultipleSourcesDifferentSchedules(t *testing.T) {
	t.Parallel()

	sourceA := uuid.New()
	sourceB := uuid.New()

	txnA := &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceA,
		Amount:     decimal.RequireFromString("1000.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-A",
	}
	txnB := &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceB,
		Amount:     decimal.RequireFromString("2000.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-B",
	}

	// Source A: 1.5% fee, Source B: 3% fee
	scheduleA := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))
	scheduleB := newTestFeeSchedule("USD", decimal.RequireFromString("0.03"))

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		sourceA: scheduleA,
		sourceB: scheduleB,
	}

	result := mapTransactionsWithFees(
		context.Background(),
		[]*shared.Transaction{txnA, txnB},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 2)

	// Source A: 1000 - (1000 * 0.015) = 1000 - 15 = 985
	assert.True(t, decimal.RequireFromString("985.00").Equal(result[0].Amount),
		"source A net amount should be 985.00, got %s", result[0].Amount)
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"source A original should be preserved")

	// Source B: 2000 - (2000 * 0.03) = 2000 - 60 = 1940
	assert.True(t, decimal.RequireFromString("1940.00").Equal(result[1].Amount),
		"source B net amount should be 1940.00, got %s", result[1].Amount)
	assert.True(t, decimal.RequireFromString("2000.00").Equal(result[1].OriginalAmount),
		"source B original should be preserved")
}

func TestMapTransactionsWithFees_NoneMode(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceID,
		Amount:     decimal.RequireFromString("1000.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-005",
	}

	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		sourceID: schedule,
	}

	// NormalizationModeNone should pass through unchanged even with schedules
	result := mapTransactionsWithFees(context.Background(), []*shared.Transaction{txn}, schedules, fee.NormalizationModeNone, &libLog.NopLogger{})

	require.Len(t, result, 1)
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].Amount),
		"amount should pass through unchanged in None mode")
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"original amount should equal amount")
	assert.Nil(t, result[0].FeeBreakdown, "no breakdown in None mode")
}
