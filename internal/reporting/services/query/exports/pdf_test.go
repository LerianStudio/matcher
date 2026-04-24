// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package exports

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestBuildMatchedPDF(t *testing.T) {
	t.Parallel()

	items := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		},
	}

	output, err := BuildMatchedPDF(items)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildMatchedPDF_NilItems(t *testing.T) {
	t.Parallel()

	items := []*entities.MatchedItem{nil, nil}

	output, err := BuildMatchedPDF(items)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildMatchedPDF_Empty(t *testing.T) {
	t.Parallel()

	output, err := BuildMatchedPDF(nil)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
}

func TestBuildUnmatchedPDF(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	dueAt := time.Now().Add(24 * time.Hour)

	items := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "EUR",
			Status:        "PENDING",
			Date:          time.Now().UTC(),
			ExceptionID:   &exceptionID,
			DueAt:         &dueAt,
		},
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "EUR",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC(),
		},
	}

	output, err := BuildUnmatchedPDF(items)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildSummaryPDF(t *testing.T) {
	t.Parallel()

	summary := &entities.SummaryReport{
		MatchedCount:    10,
		UnmatchedCount:  5,
		TotalAmount:     decimal.NewFromFloat(1500.50),
		MatchedAmount:   decimal.NewFromFloat(1000.25),
		UnmatchedAmount: decimal.NewFromFloat(500.25),
	}

	output, err := BuildSummaryPDF(summary)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildSummaryPDF_NilSummary(t *testing.T) {
	t.Parallel()

	_, err := BuildSummaryPDF(nil)
	require.ErrorIs(t, err, ErrSummaryRequired)
}

func TestBuildVariancePDF(t *testing.T) {
	t.Parallel()

	variancePct := decimal.NewFromFloat(10.0)
	rows := []*entities.VarianceReportRow{
		{
			SourceID:        uuid.New(),
			Currency:        "USD",
			FeeScheduleName: "PERCENTAGE",
			TotalExpected:   decimal.NewFromInt(100),
			TotalActual:     decimal.NewFromInt(110),
			NetVariance:     decimal.NewFromInt(10),
			VariancePct:     &variancePct,
		},
		{
			SourceID:        uuid.New(),
			Currency:        "EUR",
			FeeScheduleName: "FLAT",
			TotalExpected:   decimal.NewFromInt(50),
			TotalActual:     decimal.NewFromInt(50),
			NetVariance:     decimal.Zero,
			VariancePct:     nil,
		},
	}

	output, err := buildVariancePDF(rows, false)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
	assert.Contains(t, string(output), "Fee Schedule")
	assert.Contains(t, string(output), "PERCENTAGE")
}

func TestBuildVariancePDF_NilRowsSkipped(t *testing.T) {
	t.Parallel()

	variancePct := decimal.NewFromFloat(5.0)
	rows := []*entities.VarianceReportRow{
		nil,
		{
			SourceID:        uuid.New(),
			Currency:        "EUR",
			FeeScheduleName: "TIERED",
			TotalExpected:   decimal.NewFromInt(200),
			TotalActual:     decimal.NewFromInt(210),
			NetVariance:     decimal.NewFromInt(10),
			VariancePct:     &variancePct,
		},
	}

	output, err := buildVariancePDF(rows, false)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	assert.Contains(t, string(output), "TIERED")
}

func TestBuildVariancePDF_Empty(t *testing.T) {
	t.Parallel()

	output, err := BuildVariancePDF([]*entities.VarianceReportRow{})
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildMatchedPDF_Sorting(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	items := []*entities.MatchedItem{
		{
			TransactionID: id2,
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "USD",
			Date:          time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			TransactionID: id1,
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	output, err := BuildMatchedPDF(items)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildUnmatchedPDF_Empty(t *testing.T) {
	t.Parallel()

	output, err := BuildUnmatchedPDF([]*entities.UnmatchedItem{})
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildUnmatchedPDF_NilItems(t *testing.T) {
	t.Parallel()

	items := []*entities.UnmatchedItem{nil, nil}

	output, err := BuildUnmatchedPDF(items)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
}

func TestBuildUnmatchedPDF_NilOptionalFields(t *testing.T) {
	t.Parallel()

	items := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC(),
			ExceptionID:   nil,
			DueAt:         nil,
		},
	}

	output, err := BuildUnmatchedPDF(items)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildVariancePDF_NilVariancePct(t *testing.T) {
	t.Parallel()

	rows := []*entities.VarianceReportRow{
		{
			SourceID:        uuid.New(),
			Currency:        "USD",
			FeeScheduleName: "FLAT",
			TotalExpected:   decimal.NewFromInt(100),
			TotalActual:     decimal.NewFromInt(100),
			NetVariance:     decimal.Zero,
			VariancePct:     nil,
		},
	}

	output, err := buildVariancePDF(rows, false)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
	assert.Contains(t, string(output), "Fee Schedule")
}

func TestBuildVariancePDF_Sorting(t *testing.T) {
	t.Parallel()

	sourceA := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceB := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	rows := []*entities.VarianceReportRow{
		{
			SourceID:        sourceB,
			Currency:        "USD",
			FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-000000000200"),
			FeeScheduleName: "ZETA",
			TotalExpected:   decimal.NewFromInt(100),
			TotalActual:     decimal.NewFromInt(110),
			NetVariance:     decimal.NewFromInt(10),
		},
		{
			SourceID:        sourceA,
			Currency:        "EUR",
			FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-000000000100"),
			FeeScheduleName: "ALPHA",
			TotalExpected:   decimal.NewFromInt(50),
			TotalActual:     decimal.NewFromInt(50),
			NetVariance:     decimal.Zero,
		},
	}

	output, err := buildVariancePDF(rows, false)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	content := string(output)
	assert.True(t, strings.Index(content, "ALPHA") < strings.Index(content, "ZETA"))
}

func TestBuildVariancePDF_SanitizesUnsafeFeeScheduleText(t *testing.T) {
	t.Parallel()

	rows := []*entities.VarianceReportRow{
		{
			SourceID:        uuid.New(),
			Currency:        "USD",
			FeeScheduleName: " Visa\n\x00Domestic ",
			TotalExpected:   decimal.NewFromInt(100),
			TotalActual:     decimal.NewFromInt(100),
			NetVariance:     decimal.Zero,
		},
	}

	output, err := buildVariancePDF(rows, false)
	require.NoError(t, err)
	assert.Contains(t, string(output), "VisaDomestic")
}

func TestBuildMatchedPDF_LargeDataset(t *testing.T) {
	t.Parallel()

	items := make([]*entities.MatchedItem, 100)
	for i := 0; i < 100; i++ {
		items[i] = &entities.MatchedItem{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(int64(i * 10)),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		}
	}

	output, err := BuildMatchedPDF(items)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildUnmatchedPDF_LargeDataset(t *testing.T) {
	t.Parallel()

	items := make([]*entities.UnmatchedItem, 100)
	for i := 0; i < 100; i++ {
		exceptionID := uuid.New()
		dueAt := time.Now().UTC().Add(24 * time.Hour)
		items[i] = &entities.UnmatchedItem{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(int64(i * 10)),
			Currency:      "USD",
			Status:        "PENDING",
			Date:          time.Now().UTC(),
			ExceptionID:   &exceptionID,
			DueAt:         &dueAt,
		}
	}

	output, err := BuildUnmatchedPDF(items)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}

func TestBuildVariancePDF_LargeDataset(t *testing.T) {
	t.Parallel()

	rows := make([]*entities.VarianceReportRow, 100)
	for i := 0; i < 100; i++ {
		pct := decimal.NewFromFloat(float64(i) * 0.1)
		rows[i] = &entities.VarianceReportRow{
			SourceID:        uuid.New(),
			Currency:        "USD",
			FeeScheduleName: "PERCENTAGE",
			TotalExpected:   decimal.NewFromInt(int64(i * 100)),
			TotalActual:     decimal.NewFromInt(int64(i*100 + i)),
			NetVariance:     decimal.NewFromInt(int64(i)),
			VariancePct:     &pct,
		}
	}

	output, err := BuildVariancePDF(rows)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	require.GreaterOrEqual(t, len(output), 4)
	assert.Equal(t, "%PDF", string(output[:4]))
}
