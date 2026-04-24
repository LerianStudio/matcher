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

func TestListMatchedForExport_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful export", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}).
				AddRow(txID, groupID, sourceID, "100.00", "USD", time.Now().UTC()))

		items, err := repo.ListMatchedForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, txID, items[0].TransactionID)
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

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}))

		items, err := repo.ListMatchedForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestListUnmatchedForExport_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful export", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		txID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}).
				AddRow(txID, sourceID, "75.00", "GBP", "PENDING", time.Now().UTC(), nil, nil))

		items, err := repo.ListUnmatchedForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Equal(t, txID, items[0].TransactionID)
	})
}

func TestListVarianceForExport_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful export", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		sourceID := uuid.New()
		feeScheduleID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance"}).
				AddRow(sourceID, "USD", feeScheduleID, "Visa Domestic", "200.00", "190.00", "-10.00"))

		items, err := repo.ListVarianceForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Len(t, items, 1)
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
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance"}))

		items, err := repo.ListVarianceForExport(ctx, filter, 100)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestListMatchedPage_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful page query", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}).
				AddRow(txID, groupID, sourceID, "100.00", "USD", time.Now().UTC()))

		items, nextKey, err := repo.ListMatchedPage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Empty(t, nextKey)
	})

	t.Run("with valid afterKey", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		afterKey := uuid.New().String()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}))

		items, nextKey, err := repo.ListMatchedPage(ctx, filter, afterKey, 10)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, nextKey)
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

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "mg_id", "source_id", "amount", "currency", "date"}))

		items, _, err := repo.ListMatchedPage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestListUnmatchedPage_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful page query", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		txID := uuid.New()
		sourceID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}).
				AddRow(txID, sourceID, "50.00", "EUR", "PENDING", time.Now().UTC(), nil, nil))

		items, nextKey, err := repo.ListUnmatchedPage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Empty(t, nextKey)
	})

	t.Run("with valid afterKey", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		afterKey := uuid.New().String()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}))

		items, nextKey, err := repo.ListUnmatchedPage(ctx, filter, afterKey, 10)

		require.NoError(t, err)
		assert.Empty(t, items)
		assert.Empty(t, nextKey)
	})

	t.Run("with source ID and status filters", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		sourceID := uuid.New()
		status := "EXCEPTION"
		filter := entities.ReportFilter{
			ContextID: uuid.New(),
			SourceID:  &sourceID,
			Status:    &status,
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}))

		items, _, err := repo.ListUnmatchedPage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}

func TestListVariancePage_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful page query", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupReportRepository(t)
		defer finish()

		ctx := context.Background()
		filter := entities.VarianceReportFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
			DateTo:    time.Now().UTC(),
		}

		sourceID := uuid.New()
		feeScheduleID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance"}).
				AddRow(sourceID, "USD", feeScheduleID, "Visa Domestic", "100.00", "95.00", "-5.00"))

		items, nextKey, err := repo.ListVariancePage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Len(t, items, 1)
		assert.Empty(t, nextKey)
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
		}

		mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
			WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance"}))

		items, _, err := repo.ListVariancePage(ctx, filter, "", 10)

		require.NoError(t, err)
		assert.Empty(t, items)
	})
}
