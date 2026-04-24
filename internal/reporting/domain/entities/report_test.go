// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestBuildSummaryReport_EmptySlices(t *testing.T) {
	t.Parallel()

	summary := BuildSummaryReport([]*MatchedItem{}, []*UnmatchedItem{})

	assert.Equal(t, 0, summary.MatchedCount)
	assert.Equal(t, 0, summary.UnmatchedCount)
	assert.True(t, summary.MatchedAmount.Equal(decimal.Zero))
	assert.True(t, summary.UnmatchedAmount.Equal(decimal.Zero))
	assert.True(t, summary.TotalAmount.Equal(decimal.Zero))
}

func TestBuildSummaryReport_NilSlices(t *testing.T) {
	t.Parallel()

	summary := BuildSummaryReport(nil, nil)

	assert.Equal(t, 0, summary.MatchedCount)
	assert.Equal(t, 0, summary.UnmatchedCount)
	assert.True(t, summary.MatchedAmount.Equal(decimal.Zero))
	assert.True(t, summary.UnmatchedAmount.Equal(decimal.Zero))
	assert.True(t, summary.TotalAmount.Equal(decimal.Zero))
}

func TestBuildSummaryReport_NilItemsSkipped(t *testing.T) {
	t.Parallel()

	matched := []*MatchedItem{
		nil,
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
		nil,
	}

	unmatched := []*UnmatchedItem{
		nil,
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(50),
			Currency:      "USD",
			Status:        "PENDING",
			Date:          time.Now().UTC(),
		},
	}

	summary := BuildSummaryReport(matched, unmatched)

	assert.Equal(t, 1, summary.MatchedCount)
	assert.Equal(t, 1, summary.UnmatchedCount)
	assert.True(t, summary.MatchedAmount.Equal(decimal.NewFromInt(100)))
	assert.True(t, summary.UnmatchedAmount.Equal(decimal.NewFromInt(50)))
}

func TestBuildSummaryReport_MultipleItemsAggregate(t *testing.T) {
	t.Parallel()

	matched := []*MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(300),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
	}

	unmatched := []*UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(50),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC(),
		},
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(150),
			Currency:      "USD",
			Status:        "EXCEPTION",
			Date:          time.Now().UTC(),
		},
	}

	summary := BuildSummaryReport(matched, unmatched)

	assert.Equal(t, 3, summary.MatchedCount)
	assert.Equal(t, 2, summary.UnmatchedCount)
	assert.True(t, summary.MatchedAmount.Equal(decimal.NewFromInt(600)))
	assert.True(t, summary.UnmatchedAmount.Equal(decimal.NewFromInt(200)))
	assert.True(t, summary.TotalAmount.Equal(decimal.NewFromInt(800)))
}

func TestBuildSummaryReport_TotalEqualsMatchedPlusUnmatched(t *testing.T) {
	t.Parallel()

	matched := []*MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(1000),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
	}

	unmatched := []*UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(500),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC(),
		},
	}

	summary := BuildSummaryReport(matched, unmatched)

	expectedTotal := summary.MatchedAmount.Add(summary.UnmatchedAmount)
	assert.True(t, summary.TotalAmount.Equal(expectedTotal))
	assert.True(t, summary.TotalAmount.Equal(decimal.NewFromInt(1500)))
}

func TestBuildVarianceRow_PositiveVariance(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	feeScheduleID := uuid.New()
	expected := decimal.NewFromInt(100)
	actual := decimal.NewFromInt(110)
	variance := actual.Sub(expected)

	row := BuildVarianceRow(sourceID, "USD", feeScheduleID, "PERCENTAGE", expected, actual, variance)

	assert.Equal(t, sourceID, row.SourceID)
	assert.Equal(t, "USD", row.Currency)
	assert.Equal(t, feeScheduleID, row.FeeScheduleID)
	assert.Equal(t, "PERCENTAGE", row.FeeScheduleName)
	assert.True(t, row.TotalExpected.Equal(expected))
	assert.True(t, row.TotalActual.Equal(actual))
	assert.True(t, row.NetVariance.Equal(variance))
	assert.NotNil(t, row.VariancePct)
	assert.True(t, row.VariancePct.Equal(decimal.NewFromInt(10)))
}

func TestBuildVarianceRow_NegativeVariance(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	feeScheduleID := uuid.New()
	expected := decimal.NewFromInt(100)
	actual := decimal.NewFromInt(90)
	variance := actual.Sub(expected)

	row := BuildVarianceRow(sourceID, "EUR", feeScheduleID, "FLAT", expected, actual, variance)

	assert.True(t, row.NetVariance.Equal(decimal.NewFromInt(-10)))
	assert.NotNil(t, row.VariancePct)
	assert.True(t, row.VariancePct.Equal(decimal.NewFromInt(-10)))
}

func TestBuildVarianceRow_ZeroExpected_NilPercentage(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	feeScheduleID := uuid.New()
	expected := decimal.Zero
	actual := decimal.NewFromInt(5)
	variance := actual.Sub(expected)

	row := BuildVarianceRow(sourceID, "USD", feeScheduleID, "TIERED", expected, actual, variance)

	assert.True(t, row.TotalExpected.IsZero())
	assert.True(t, row.TotalActual.Equal(decimal.NewFromInt(5)))
	assert.True(t, row.NetVariance.Equal(decimal.NewFromInt(5)))
	assert.Nil(t, row.VariancePct)
}

func TestBuildVarianceRow_ZeroVariance(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	feeScheduleID := uuid.New()
	expected := decimal.NewFromInt(100)
	actual := decimal.NewFromInt(100)
	variance := decimal.Zero

	row := BuildVarianceRow(sourceID, "BRL", feeScheduleID, "PERCENTAGE", expected, actual, variance)

	assert.True(t, row.NetVariance.IsZero())
	assert.NotNil(t, row.VariancePct)
	assert.True(t, row.VariancePct.IsZero())
}

func TestBuildVarianceRow_DecimalPrecision(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	feeScheduleID := uuid.New()
	expected := decimal.NewFromFloat(100.00)
	actual := decimal.NewFromFloat(101.50)
	variance := actual.Sub(expected)

	row := BuildVarianceRow(sourceID, "USD", feeScheduleID, "FLAT", expected, actual, variance)

	assert.True(t, row.NetVariance.Equal(decimal.NewFromFloat(1.50)))
	assert.NotNil(t, row.VariancePct)

	expectedPct := decimal.NewFromFloat(1.50)
	assert.True(t, row.VariancePct.Equal(expectedPct))
}
