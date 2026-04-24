// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package report

import (
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestScanVarianceRow(t *testing.T) {
	t.Parallel()

	totalExpected := decimal.NewFromInt(100)
	netVariance := decimal.NewFromInt(20)
	feeScheduleID := uuid.New()
	row, err := scanVarianceRow(fakeVarianceScanner{
		values: []any{
			uuid.New(),
			"USD",
			feeScheduleID,
			"FLAT",
			totalExpected,
			decimal.NewFromInt(80),
			netVariance,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "USD", row.Currency)
	assert.Equal(t, feeScheduleID, row.FeeScheduleID)
	assert.Equal(t, "FLAT", row.FeeScheduleName)
	require.NotNil(t, row.VariancePct)
	assert.True(
		t,
		row.VariancePct.Equal(netVariance.Div(totalExpected).Mul(decimal.NewFromInt(100))),
	)
}

func TestScanVarianceRow_Error(t *testing.T) {
	t.Parallel()

	_, err := scanVarianceRow(fakeVarianceScanner{err: errTestScanFailed})
	require.ErrorIs(t, err, errTestScanFailed)
}

func TestScanVarianceRow_ZeroExpected(t *testing.T) {
	t.Parallel()

	feeScheduleID := uuid.New()
	row, err := scanVarianceRow(fakeVarianceScanner{
		values: []any{
			uuid.New(),
			"EUR",
			feeScheduleID,
			"PERCENTAGE",
			decimal.Zero,
			decimal.NewFromInt(50),
			decimal.NewFromInt(50),
		},
	})

	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "EUR", row.Currency)
	assert.Equal(t, feeScheduleID, row.FeeScheduleID)
	assert.Equal(t, "PERCENTAGE", row.FeeScheduleName)
	require.Nil(t, row.VariancePct)
}

func TestScanVarianceRow_NegativeVariance(t *testing.T) {
	t.Parallel()

	totalExpected := decimal.NewFromInt(200)
	netVariance := decimal.NewFromInt(-40)
	feeScheduleID := uuid.New()
	row, err := scanVarianceRow(fakeVarianceScanner{
		values: []any{
			uuid.New(),
			"GBP",
			feeScheduleID,
			"TIERED",
			totalExpected,
			decimal.NewFromInt(240),
			netVariance,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "GBP", row.Currency)
	require.NotNil(t, row.VariancePct)
	expectedPct := netVariance.Div(totalExpected).Mul(decimal.NewFromInt(100))
	assert.True(t, row.VariancePct.Equal(expectedPct))
}

func TestScanMatchedItem(t *testing.T) {
	t.Parallel()

	t.Run("successful scan", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()
		amount := decimal.NewFromFloat(123.45)
		currency := "USD"
		date := time.Now().UTC()

		scanner := &fakeMatchedScanner{
			values: []any{txID, groupID, sourceID, amount, currency, date},
		}

		item, err := scanMatchedItem(scanner)

		require.NoError(t, err)
		require.NotNil(t, item)
		assert.Equal(t, txID, item.TransactionID)
		assert.Equal(t, groupID, item.MatchGroupID)
		assert.Equal(t, sourceID, item.SourceID)
		assert.True(t, amount.Equal(item.Amount))
		assert.Equal(t, currency, item.Currency)
		assert.Equal(t, date, item.Date)
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()

		scanner := &fakeMatchedScanner{err: errTestScanFailed}

		item, err := scanMatchedItem(scanner)

		assert.Nil(t, item)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scanning matched item")
	})
}

func TestScanUnmatchedItem(t *testing.T) {
	t.Parallel()

	t.Run("successful scan with all fields", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		sourceID := uuid.New()
		amount := decimal.NewFromFloat(99.99)
		currency := "EUR"
		status := "PENDING"
		date := time.Now().UTC()
		exceptionID := uuid.New()
		dueAt := time.Now().UTC().Add(24 * time.Hour).UTC()

		scanner := &fakeUnmatchedScanner{
			values: []any{txID, sourceID, amount, currency, status, date, &exceptionID, &dueAt},
		}

		item, err := scanUnmatchedItem(scanner)

		require.NoError(t, err)
		require.NotNil(t, item)
		assert.Equal(t, txID, item.TransactionID)
		assert.Equal(t, sourceID, item.SourceID)
		assert.True(t, amount.Equal(item.Amount))
		assert.Equal(t, currency, item.Currency)
		assert.Equal(t, status, item.Status)
		assert.Equal(t, date, item.Date)
		require.NotNil(t, item.ExceptionID)
		assert.Equal(t, exceptionID, *item.ExceptionID)
		require.NotNil(t, item.DueAt)
	})

	t.Run("successful scan with nil optional fields", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		sourceID := uuid.New()
		amount := decimal.NewFromFloat(50.00)
		currency := "GBP"
		status := "UNMATCHED"
		date := time.Now().UTC()

		scanner := &fakeUnmatchedScanner{
			values: []any{txID, sourceID, amount, currency, status, date, nil, nil},
		}

		item, err := scanUnmatchedItem(scanner)

		require.NoError(t, err)
		require.NotNil(t, item)
		assert.Equal(t, txID, item.TransactionID)
		assert.Nil(t, item.ExceptionID)
		assert.Nil(t, item.DueAt)
	})

	t.Run("scan error", func(t *testing.T) {
		t.Parallel()

		scanner := &fakeUnmatchedScanner{err: errTestScanFailed}

		item, err := scanUnmatchedItem(scanner)

		assert.Nil(t, item)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scanning unmatched item")
	})
}

func TestApplyUnmatchedExportFilters(t *testing.T) {
	t.Parallel()

	t.Run("no filters applied", func(t *testing.T) {
		t.Parallel()

		filter := entities.ReportFilter{}
		query := squirrel.Select("*").From("transactions")

		result := applyUnmatchedExportFilters(query, filter)

		sql, _, err := result.ToSql()
		require.NoError(t, err)
		assert.NotContains(t, sql, "source_id")
		assert.NotContains(t, sql, "status")
	})

	t.Run("source ID filter applied", func(t *testing.T) {
		t.Parallel()

		sourceID := uuid.New()
		filter := entities.ReportFilter{SourceID: &sourceID}
		query := squirrel.Select("*").From("transactions").PlaceholderFormat(squirrel.Dollar)

		result := applyUnmatchedExportFilters(query, filter)

		sql, args, err := result.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "source_id")
		require.Len(t, args, 1)
	})

	t.Run("status filter applied", func(t *testing.T) {
		t.Parallel()

		status := "PENDING"
		filter := entities.ReportFilter{Status: &status}
		query := squirrel.Select("*").From("transactions").PlaceholderFormat(squirrel.Dollar)

		result := applyUnmatchedExportFilters(query, filter)

		sql, args, err := result.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "status")
		assert.Contains(t, args, status)
	})

	t.Run("both filters applied", func(t *testing.T) {
		t.Parallel()

		sourceID := uuid.New()
		status := "EXCEPTION"
		filter := entities.ReportFilter{SourceID: &sourceID, Status: &status}
		query := squirrel.Select("*").From("transactions").PlaceholderFormat(squirrel.Dollar)

		result := applyUnmatchedExportFilters(query, filter)

		sql, args, err := result.ToSql()
		require.NoError(t, err)
		assert.Contains(t, sql, "source_id")
		assert.Contains(t, sql, "status")
		require.Len(t, args, 2)
		assert.Contains(t, args, status)
	})
}
