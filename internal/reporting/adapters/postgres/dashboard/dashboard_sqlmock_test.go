// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dashboard

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errTestTransactionsUnsupported = errors.New("transactions unsupported")
	errTestDatabaseConnectionLost  = errors.New("database connection lost")
)

type fakeDashboardInfraProvider struct{}

func (f *fakeDashboardInfraProvider) GetRedisConnection(
	_ context.Context,
) (*ports.RedisConnectionLease, error) {
	return nil, nil
}

func (f *fakeDashboardInfraProvider) BeginTx(_ context.Context) (*ports.TxLease, error) {
	return nil, errTestTransactionsUnsupported
}

func (f *fakeDashboardInfraProvider) GetReplicaDB(_ context.Context) (*ports.DBLease, error) {
	return nil, nil
}

func (f *fakeDashboardInfraProvider) GetPrimaryDB(_ context.Context) (*ports.DBLease, error) {
	return nil, nil
}

var _ ports.InfrastructureProvider = (*fakeDashboardInfraProvider)(nil)

func TestNewRepository(t *testing.T) {
	t.Parallel()

	provider := &fakeDashboardInfraProvider{}
	repo := NewRepository(provider)

	assert.NotNil(t, repo)
	assert.Equal(t, provider, repo.provider)
}

func TestRepository_ValidateFilter_NilProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		repo     *Repository
		filter   *entities.DashboardFilter
		expected error
	}{
		{
			name:     "nil repository",
			repo:     nil,
			filter:   &entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil provider",
			repo:     &Repository{provider: nil},
			filter:   &entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil context ID",
			repo:     &Repository{provider: &fakeDashboardInfraProvider{}},
			filter:   &entities.DashboardFilter{ContextID: uuid.Nil},
			expected: ErrContextIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.repo.validateFilter(tt.filter)
			require.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestRepository_ValidateFilter_Success(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeDashboardInfraProvider{})
	filter := &entities.DashboardFilter{ContextID: uuid.New()}

	err := repo.validateFilter(filter)
	assert.NoError(t, err)
}

func TestRepository_GetVolumeStats_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name     string
		repo     *Repository
		filter   entities.DashboardFilter
		expected error
	}{
		{
			name:     "nil provider returns error",
			repo:     &Repository{provider: nil},
			filter:   entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil context ID returns error",
			repo:     NewRepository(&fakeDashboardInfraProvider{}),
			filter:   entities.DashboardFilter{ContextID: uuid.Nil},
			expected: ErrContextIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := tt.repo.GetVolumeStats(ctx, tt.filter)
			assert.Nil(t, result)
			require.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestRepository_GetSLAStats_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name     string
		repo     *Repository
		filter   entities.DashboardFilter
		expected error
	}{
		{
			name:     "nil provider returns error",
			repo:     &Repository{provider: nil},
			filter:   entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil context ID returns error",
			repo:     NewRepository(&fakeDashboardInfraProvider{}),
			filter:   entities.DashboardFilter{ContextID: uuid.Nil},
			expected: ErrContextIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := tt.repo.GetSLAStats(ctx, tt.filter)
			assert.Nil(t, result)
			require.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeDashboardInfraProvider{})
	assert.NotNil(t, repo)
}

func TestRepository_GetSummaryMetrics_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name     string
		repo     *Repository
		filter   entities.DashboardFilter
		expected error
	}{
		{
			name:     "nil provider returns error",
			repo:     &Repository{provider: nil},
			filter:   entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil context ID returns error",
			repo:     NewRepository(&fakeDashboardInfraProvider{}),
			filter:   entities.DashboardFilter{ContextID: uuid.Nil},
			expected: ErrContextIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := tt.repo.GetSummaryMetrics(ctx, tt.filter)
			assert.Nil(t, result)
			require.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestRepository_GetTrendMetrics_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name     string
		repo     *Repository
		filter   entities.DashboardFilter
		expected error
	}{
		{
			name:     "nil provider returns error",
			repo:     &Repository{provider: nil},
			filter:   entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil context ID returns error",
			repo:     NewRepository(&fakeDashboardInfraProvider{}),
			filter:   entities.DashboardFilter{ContextID: uuid.Nil},
			expected: ErrContextIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := tt.repo.GetTrendMetrics(ctx, tt.filter)
			assert.Nil(t, result)
			require.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestRepository_GetBreakdownMetrics_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name     string
		repo     *Repository
		filter   entities.DashboardFilter
		expected error
	}{
		{
			name:     "nil provider returns error",
			repo:     &Repository{provider: nil},
			filter:   entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil context ID returns error",
			repo:     NewRepository(&fakeDashboardInfraProvider{}),
			filter:   entities.DashboardFilter{ContextID: uuid.Nil},
			expected: ErrContextIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := tt.repo.GetBreakdownMetrics(ctx, tt.filter)
			assert.Nil(t, result)
			require.ErrorIs(t, err, tt.expected)
		})
	}
}

// setupRepository creates a Repository with sqlmock for testing SQL queries.
func setupRepository(t *testing.T) (*Repository, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	t.Cleanup(func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	return repo, mock
}

// summaryMetricsQueryPattern matches the GetSummaryMetrics CTE query.
var summaryMetricsQueryPattern = regexp.MustCompile(`WITH base_txns AS`)

func TestGetSummaryMetrics_HappyPath_AllTransactionsMatched(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	rows := sqlmock.NewRows([]string{
		"total_txn", "matched_count", "pending_exc", "critical_amount", "oldest_age_hours",
	}).AddRow(100, 100, 0, decimal.NewFromInt(0), 0.0)

	mock.ExpectQuery(summaryMetricsQueryPattern.String()).
		WithArgs(
			contextID,
			dateFrom,
			dateTo,
			matchGroupStatusConfirmed,
			exceptionSeverityCritical,
			exceptionStatusResolved,
			nil,
		).
		WillReturnRows(rows)

	result, err := repo.GetSummaryMetrics(ctx, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 100, result.TotalTransactions)
	assert.Equal(t, 100, result.TotalMatches)
	// 100 / 100 * 100 = 100% (percentage scale).
	assert.InDelta(t, 100.0, result.MatchRate, 0.0001)
	assert.Equal(t, 0, result.PendingExceptions)
	assert.True(t, result.CriticalExposure.IsZero())
}

func TestGetSummaryMetrics_HappyPath_PartialMatches(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	rows := sqlmock.NewRows([]string{
		"total_txn", "matched_count", "pending_exc", "critical_amount", "oldest_age_hours",
	}).AddRow(1000, 850, 150, decimal.NewFromFloat(25000.50), 48.5)

	mock.ExpectQuery(summaryMetricsQueryPattern.String()).
		WithArgs(
			contextID,
			dateFrom,
			dateTo,
			matchGroupStatusConfirmed,
			exceptionSeverityCritical,
			exceptionStatusResolved,
			nil,
		).
		WillReturnRows(rows)

	result, err := repo.GetSummaryMetrics(ctx, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1000, result.TotalTransactions)
	assert.Equal(t, 850, result.TotalMatches)
	// 850 / 1000 * 100 = 85% (percentage scale).
	assert.InDelta(t, 85.0, result.MatchRate, 0.01)
	assert.Equal(t, 150, result.PendingExceptions)
	assert.True(t, result.CriticalExposure.Equal(decimal.NewFromFloat(25000.50)))
	assert.InDelta(t, 48.5, result.OldestExceptionAge, 0.1)
}

func TestGetSummaryMetrics_EdgeCase_NoTransactions(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	rows := sqlmock.NewRows([]string{
		"total_txn", "matched_count", "pending_exc", "critical_amount", "oldest_age_hours",
	}).AddRow(0, 0, 0, decimal.NewFromInt(0), 0.0)

	mock.ExpectQuery(summaryMetricsQueryPattern.String()).
		WithArgs(
			contextID,
			dateFrom,
			dateTo,
			matchGroupStatusConfirmed,
			exceptionSeverityCritical,
			exceptionStatusResolved,
			nil,
		).
		WillReturnRows(rows)

	result, err := repo.GetSummaryMetrics(ctx, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.TotalTransactions)
	assert.Equal(t, 0, result.TotalMatches)
	assert.InDelta(t, 0.0, result.MatchRate, 0.0001)
	assert.Equal(t, 0, result.PendingExceptions)
}

func TestGetSummaryMetrics_EdgeCase_MatchRateClampingOver100Percent(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	rows := sqlmock.NewRows([]string{
		"total_txn", "matched_count", "pending_exc", "critical_amount", "oldest_age_hours",
	}).AddRow(100, 150, 0, decimal.NewFromInt(0), 0.0)

	mock.ExpectQuery(summaryMetricsQueryPattern.String()).
		WithArgs(
			contextID,
			dateFrom,
			dateTo,
			matchGroupStatusConfirmed,
			exceptionSeverityCritical,
			exceptionStatusResolved,
			nil,
		).
		WillReturnRows(rows)

	result, err := repo.GetSummaryMetrics(ctx, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 100, result.TotalTransactions)
	assert.Equal(t, 150, result.TotalMatches)
	// Clamped to the percentage ceiling (100.0) when matched > total.
	assert.InDelta(t, 100.0, result.MatchRate, 0.0001)
}

func TestGetSummaryMetrics_WithSourceIDFilter(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		SourceID:  &sourceID,
	}

	rows := sqlmock.NewRows([]string{
		"total_txn", "matched_count", "pending_exc", "critical_amount", "oldest_age_hours",
	}).AddRow(50, 45, 5, decimal.NewFromFloat(1000.00), 12.0)

	mock.ExpectQuery(summaryMetricsQueryPattern.String()).
		WithArgs(
			contextID,
			dateFrom,
			dateTo,
			matchGroupStatusConfirmed,
			exceptionSeverityCritical,
			exceptionStatusResolved,
			&sourceID,
		).
		WillReturnRows(rows)

	result, err := repo.GetSummaryMetrics(ctx, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 50, result.TotalTransactions)
	assert.Equal(t, 45, result.TotalMatches)
	// 45 / 50 * 100 = 90% (percentage scale).
	assert.InDelta(t, 90.0, result.MatchRate, 0.01)
	assert.Equal(t, 5, result.PendingExceptions)
}

func TestGetSummaryMetrics_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	mock.ExpectQuery(summaryMetricsQueryPattern.String()).
		WithArgs(
			contextID,
			dateFrom,
			dateTo,
			matchGroupStatusConfirmed,
			exceptionSeverityCritical,
			exceptionStatusResolved,
			nil,
		).
		WillReturnError(errTestDatabaseConnectionLost)

	result, err := repo.GetSummaryMetrics(ctx, filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database connection lost")
}

// TestGetSummaryMetrics_MatchRateReturnsPercentageNotDecimal pins the
// percentage-scale convention (0-100) for SummaryMetrics.MatchRate. Earlier
// versions returned a 0.0-1.0 ratio; callers and the console now expect the
// same scale as MatchRateStats.
func TestGetSummaryMetrics_MatchRateReturnsPercentageNotDecimal(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	rows := sqlmock.NewRows([]string{
		"total_txn", "matched_count", "pending_exc", "critical_amount", "oldest_age_hours",
	}).AddRow(200, 170, 30, decimal.NewFromFloat(5000.00), 24.0)

	mock.ExpectQuery(summaryMetricsQueryPattern.String()).
		WithArgs(
			contextID,
			dateFrom,
			dateTo,
			matchGroupStatusConfirmed,
			exceptionSeverityCritical,
			exceptionStatusResolved,
			nil,
		).
		WillReturnRows(rows)

	result, err := repo.GetSummaryMetrics(ctx, filter)

	require.NoError(t, err)
	require.NotNil(t, result)
	// 170 / 200 * 100 = 85 (percentage scale).
	assert.InDelta(t, 85.0, result.MatchRate, 0.01)
	assert.LessOrEqual(t, result.MatchRate, 100.0)
	assert.GreaterOrEqual(t, result.MatchRate, 0.0)
}

func TestGetVolumeStats_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(100, "10000.00"))
		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(120, "12000.00"))

		result, err := repo.GetVolumeStats(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 120, result.TotalTransactions)
		assert.Equal(t, 100, result.MatchedTransactions)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		sourceID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			SourceID:  &sourceID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(50, "5000.00"))
		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(60, "6000.00"))

		result, err := repo.GetVolumeStats(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 60, result.TotalTransactions)
	})

	t.Run("negative unmatched count clamped to zero", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(100, "10000.00"))
		mock.ExpectQuery("SELECT COUNT").
			WillReturnRows(sqlmock.NewRows([]string{"cnt", "total"}).
				AddRow(80, "8000.00"))

		result, err := repo.GetVolumeStats(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.GreaterOrEqual(t, result.UnmatchedCount, 0)
	})
}

func TestGetSLAStats_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{
				"total_exceptions", "resolved_on_time", "resolved_late",
				"pending_within_sla", "pending_overdue", "avg_resolution_ms",
			}).AddRow(100, 80, 10, 5, 5, 3600000.0))

		result, err := repo.GetSLAStats(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 100, result.TotalExceptions)
		assert.Equal(t, 80, result.ResolvedOnTime)
		assert.Equal(t, 10, result.ResolvedLate)
	})

	t.Run("with source ID filter", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		sourceID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			SourceID:  &sourceID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{
				"total_exceptions", "resolved_on_time", "resolved_late",
				"pending_within_sla", "pending_overdue", "avg_resolution_ms",
			}).AddRow(50, 40, 5, 2, 3, 1800000.0))

		result, err := repo.GetSLAStats(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 50, result.TotalExceptions)
	})
}

func TestGetTrendMetrics_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query with results", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 7, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"date", "ingested", "matched", "exceptions"}).
				AddRow(dateFrom, 100, 90, 5).
				AddRow(dateFrom.Add(24*time.Hour), 120, 110, 3))

		result, err := repo.GetTrendMetrics(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.NotEmpty(t, result.Dates)
	})

	t.Run("empty results", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 7, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"date", "ingested", "matched", "exceptions"}))

		result, err := repo.GetTrendMetrics(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
	})
}

func TestGetBreakdownMetrics_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query with all breakdowns", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT e.severity").
			WillReturnRows(sqlmock.NewRows([]string{"severity", "cnt"}).
				AddRow("CRITICAL", 10).
				AddRow("HIGH", 25))

		mock.ExpectQuery("SELECT COALESCE").
			WillReturnRows(sqlmock.NewRows([]string{"reason", "cnt"}).
				AddRow("Duplicate entry", 15).
				AddRow("Missing data", 10))

		mock.ExpectQuery("SELECT mr.id").
			WillReturnRows(sqlmock.NewRows([]string{"id", "type", "cnt"}).
				AddRow(uuid.New(), "EXACT", 50).
				AddRow(uuid.New(), "FUZZY", 30))

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"bucket", "ord", "cnt"}).
				AddRow("<24h", 1, 20).
				AddRow("1-3d", 2, 15).
				AddRow(">3d", 3, 10))

		result, err := repo.GetBreakdownMetrics(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.NotEmpty(t, result.BySeverity)
		assert.NotEmpty(t, result.ByReason)
		assert.NotEmpty(t, result.ByRule)
		assert.NotEmpty(t, result.ByAge)
	})
}

func TestRepository_GetSourceBreakdown_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name     string
		repo     *Repository
		filter   entities.DashboardFilter
		expected error
	}{
		{
			name:     "nil provider returns error",
			repo:     &Repository{provider: nil},
			filter:   entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil context ID returns error",
			repo:     NewRepository(&fakeDashboardInfraProvider{}),
			filter:   entities.DashboardFilter{ContextID: uuid.Nil},
			expected: ErrContextIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := tt.repo.GetSourceBreakdown(ctx, tt.filter)
			assert.Nil(t, result)
			require.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestRepository_GetCashImpactSummary_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name     string
		repo     *Repository
		filter   entities.DashboardFilter
		expected error
	}{
		{
			name:     "nil provider returns error",
			repo:     &Repository{provider: nil},
			filter:   entities.DashboardFilter{ContextID: uuid.New()},
			expected: ErrRepositoryNotInitialized,
		},
		{
			name:     "nil context ID returns error",
			repo:     NewRepository(&fakeDashboardInfraProvider{}),
			filter:   entities.DashboardFilter{ContextID: uuid.Nil},
			expected: ErrContextIDRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := tt.repo.GetCashImpactSummary(ctx, tt.filter)
			assert.Nil(t, result)
			require.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestGetSourceBreakdown_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		sourceID := uuid.New()

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{
				"source_id", "source_name", "total_txns", "matched_txns", "total_amount", "matched_amount", "currency",
			}).AddRow(sourceID, "Bank A", 100, 80, "50000.00", "40000.00", "USD").
				AddRow(uuid.New(), "Bank B", 50, 30, "25000.00", "15000.00", "EUR"))

		result, err := repo.GetSourceBreakdown(ctx, filter)

		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "Bank A", result[0].SourceName)
		assert.Equal(t, int64(100), result[0].TotalTxns)
		assert.Equal(t, int64(80), result[0].MatchedTxns)
		// 80 / 100 * 100 = 80% (percentage scale).
		assert.InDelta(t, 80.0, result[0].MatchRate, 0.01)
		assert.Equal(t, int64(20), result[0].UnmatchedTxns)
		assert.True(t, result[0].UnmatchedAmount.Equal(decimal.NewFromFloat(10000.00)))
	})

	t.Run("empty results", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{
				"source_id", "source_name", "total_txns", "matched_txns", "total_amount", "matched_amount", "currency",
			}))

		result, err := repo.GetSourceBreakdown(ctx, filter)

		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("database error", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT").
			WillReturnError(errTestDatabaseConnectionLost)

		result, err := repo.GetSourceBreakdown(ctx, filter)

		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("match rate clamped at 100 percent", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		// More matched than total (anomaly)
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{
				"source_id", "source_name", "total_txns", "matched_txns", "total_amount", "matched_amount", "currency",
			}).AddRow(uuid.New(), "Anomaly Source", 50, 100, "10000.00", "12000.00", "USD"))

		result, err := repo.GetSourceBreakdown(ctx, filter)

		require.NoError(t, err)
		require.Len(t, result, 1)
		// Clamped to the percentage ceiling (100.0) when matched > total.
		assert.InDelta(t, 100.0, result[0].MatchRate, 0.001)
		assert.Equal(t, int64(0), result[0].UnmatchedTxns) // Clamped to 0
		assert.True(t, result[0].UnmatchedAmount.IsZero()) // Clamped to 0
	})
}

func TestGetCashImpactSummary_DatabaseQuery(t *testing.T) {
	t.Parallel()

	t.Run("successful query", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		// Currency query
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"currency", "amount", "txn_count"}).
				AddRow("USD", "15000.00", 10).
				AddRow("EUR", "5000.00", 5))

		// Age query
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"bucket", "ord", "amount", "txn_count"}).
				AddRow("0-24h", 1, "3000.00", 3).
				AddRow("1-3d", 2, "7000.00", 5).
				AddRow("3-7d", 3, "10000.00", 7))

		result, err := repo.GetCashImpactSummary(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, result.TotalUnmatchedAmount.Equal(decimal.NewFromFloat(20000.00)))
		require.Len(t, result.ByCurrency, 2)
		assert.Equal(t, "USD", result.ByCurrency[0].Currency)
		require.Len(t, result.ByAge, 3)
		assert.Equal(t, "0-24h", result.ByAge[0].Bucket)
	})

	t.Run("empty results", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		// Currency query - empty
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"currency", "amount", "txn_count"}))

		// Age query - empty
		mock.ExpectQuery("SELECT").
			WillReturnRows(sqlmock.NewRows([]string{"bucket", "ord", "amount", "txn_count"}))

		result, err := repo.GetCashImpactSummary(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, result.TotalUnmatchedAmount.IsZero())
		assert.Empty(t, result.ByCurrency)
		assert.Empty(t, result.ByAge)
	})

	t.Run("currency query error", func(t *testing.T) {
		t.Parallel()

		repo, mock := setupRepository(t)

		ctx := context.Background()
		contextID := uuid.New()
		dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  dateFrom,
			DateTo:    dateTo,
		}

		mock.ExpectQuery("SELECT").
			WillReturnError(errTestDatabaseConnectionLost)

		result, err := repo.GetCashImpactSummary(ctx, filter)

		require.Error(t, err)
		require.Nil(t, result)
	})
}

func TestGetVolumeStats_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	mock.ExpectQuery("SELECT COUNT").
		WillReturnError(errTestDatabaseConnectionLost)

	result, err := repo.GetVolumeStats(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "get volume stats")
}

func TestGetSLAStats_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	mock.ExpectQuery("SELECT").
		WillReturnError(errTestDatabaseConnectionLost)

	result, err := repo.GetSLAStats(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "get sla stats")
}

func TestGetTrendMetrics_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	mock.ExpectQuery("SELECT").
		WillReturnError(errTestDatabaseConnectionLost)

	result, err := repo.GetTrendMetrics(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "get trend metrics")
}

func TestGetBreakdownMetrics_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock := setupRepository(t)

	ctx := context.Background()
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	mock.ExpectQuery("SELECT e.severity").
		WillReturnError(errTestDatabaseConnectionLost)

	result, err := repo.GetBreakdownMetrics(ctx, filter)

	require.Error(t, err)
	assert.Nil(t, result)
}

func TestRepository_NilReceiverChecks(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()
	filter := entities.DashboardFilter{ContextID: uuid.New()}

	t.Run("GetVolumeStats nil receiver", func(t *testing.T) {
		t.Parallel()

		result, err := repo.GetVolumeStats(ctx, filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("GetSLAStats nil receiver", func(t *testing.T) {
		t.Parallel()

		result, err := repo.GetSLAStats(ctx, filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("GetSummaryMetrics nil receiver", func(t *testing.T) {
		t.Parallel()

		result, err := repo.GetSummaryMetrics(ctx, filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("GetTrendMetrics nil receiver", func(t *testing.T) {
		t.Parallel()

		result, err := repo.GetTrendMetrics(ctx, filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("GetBreakdownMetrics nil receiver", func(t *testing.T) {
		t.Parallel()

		result, err := repo.GetBreakdownMetrics(ctx, filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("GetSourceBreakdown nil receiver", func(t *testing.T) {
		t.Parallel()

		result, err := repo.GetSourceBreakdown(ctx, filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})

	t.Run("GetCashImpactSummary nil receiver", func(t *testing.T) {
		t.Parallel()

		result, err := repo.GetCashImpactSummary(ctx, filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, ErrRepositoryNotInitialized)
	})
}
