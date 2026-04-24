// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package report

import (
	"context"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestListMatched_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query with results", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		contextID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: contextID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}).
				AddRow(txID, groupID, sourceID, "100.00", "USD", time.Now().UTC()))

		items, _, err := repo.ListMatched(ctx, filter)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, txID, items[0].TransactionID)
	})

	t.Run("empty results", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}))

		items, pagination, err := repo.ListMatched(ctx, filter)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, pagination.Next)
	})
}

func TestListUnmatched_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query with results", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		txID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}).
				AddRow(txID, sourceID, "50.00", "EUR", "PENDING", time.Now().UTC(), nil, nil))

		items, _, err := repo.ListUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, txID, items[0].TransactionID)
	})

	t.Run("with source ID and status filters", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		status := "PENDING"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}))

		items, pagination, err := repo.ListUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, pagination.Next)
	})
}

func TestGetSummary_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(10, "1000.00"))
		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(5, "500.00"))

		summary, err := repo.GetSummary(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.Equal(t, 10, summary.MatchedCount)
		assert.Equal(t, 5, summary.UnmatchedCount)
	})
}

func TestGetVarianceReport_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query with results", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		sourceID := uuid.New()
		feeScheduleID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance"}).
				AddRow(sourceID, "USD", feeScheduleID, "Visa Domestic", "100.00", "95.00", "-5.00"))

		items, _, err := repo.GetVarianceReport(ctx, filter)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, "USD", items[0].Currency)
		assert.Equal(t, feeScheduleID, items[0].FeeScheduleID)
		assert.Equal(t, "Visa Domestic", items[0].FeeScheduleName)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
			Limit:     10,
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance"}))

		items, pagination, err := repo.GetVarianceReport(ctx, filter)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, pagination.Next)
	})
}

func TestCountMatched_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful count", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(42))

		count, err := repo.CountMatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(42), count)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(15))

		count, err := repo.CountMatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(15), count)
	})
}

func TestCountUnmatched_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful count", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(28))

		count, err := repo.CountUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(28), count)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(12))

		count, err := repo.CountUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(12), count)
	})

	t.Run("with status filter", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		status := "EXCEPTION"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT")).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))

		count, err := repo.CountUnmatched(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, int64(7), count)
	})
}
