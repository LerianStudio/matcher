//go:build unit

package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
)

// --- GetSourceBreakdown tests ---

func TestDashboardUseCase_GetSourceBreakdown(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	contextID := uuid.New()
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("returns source breakdown successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedBreakdowns := []entities.SourceBreakdown{
			{
				SourceID:        uuid.New(),
				SourceName:      "Bank A",
				TotalTxns:       100,
				MatchedTxns:     80,
				UnmatchedTxns:   20,
				MatchRate:       80.0,
				TotalAmount:     decimal.NewFromInt(10000),
				UnmatchedAmount: decimal.NewFromInt(2000),
				Currency:        "USD",
			},
			{
				SourceID:        uuid.New(),
				SourceName:      "Bank B",
				TotalTxns:       50,
				MatchedTxns:     45,
				UnmatchedTxns:   5,
				MatchRate:       90.0,
				TotalAmount:     decimal.NewFromInt(5000),
				UnmatchedAmount: decimal.NewFromInt(500),
				Currency:        "EUR",
			},
		}

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetSourceBreakdown(gomock.Any(), filter).Return(expectedBreakdowns, nil)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetSourceBreakdown(ctx, filter)

		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, "Bank A", result[0].SourceName)
		assert.Equal(t, "Bank B", result[1].SourceName)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetSourceBreakdown(gomock.Any(), filter).Return(nil, errTestDBError)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetSourceBreakdown(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting source breakdown")
	})

	t.Run("returns empty slice when no data", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetSourceBreakdown(gomock.Any(), filter).Return([]entities.SourceBreakdown{}, nil)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetSourceBreakdown(ctx, filter)

		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

// --- GetCashImpactSummary tests ---

func TestDashboardUseCase_GetCashImpactSummary(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	contextID := uuid.New()
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("returns cash impact summary successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedSummary := &entities.CashImpactSummary{
			TotalUnmatchedAmount: decimal.NewFromInt(5000),
			ByCurrency: []entities.CurrencyExposure{
				{Currency: "USD", Amount: decimal.NewFromInt(3000), TransactionCount: 10},
				{Currency: "EUR", Amount: decimal.NewFromInt(2000), TransactionCount: 5},
			},
			ByAge: []entities.AgeExposure{
				{Bucket: entities.CashImpactBucket0To24h, Amount: decimal.NewFromInt(2000), TransactionCount: 6},
				{Bucket: entities.CashImpactBucket1To3d, Amount: decimal.NewFromInt(3000), TransactionCount: 9},
			},
		}

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetCashImpactSummary(gomock.Any(), filter).Return(expectedSummary, nil)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetCashImpactSummary(ctx, filter)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, expectedSummary.TotalUnmatchedAmount.Equal(result.TotalUnmatchedAmount))
		assert.Len(t, result.ByCurrency, 2)
		assert.Len(t, result.ByAge, 2)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetCashImpactSummary(gomock.Any(), filter).Return(nil, errTestDBError)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetCashImpactSummary(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting cash impact summary")
	})
}

// --- Cache integration tests ---

var errTestCacheError = errors.New("cache error")

type mockDashboardCacheService struct {
	volumeStats    *entities.VolumeStats
	volumeStatsErr error
	slaStats       *entities.SLAStats
	slaStatsErr    error
	aggregates     *entities.DashboardAggregates
	aggregatesErr  error
	metricsResult  *entities.MatcherDashboardMetrics
	metricsErr     error
	setCalled      bool
}

func (m *mockDashboardCacheService) GetVolumeStats(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.VolumeStats, error) {
	return m.volumeStats, m.volumeStatsErr
}

func (m *mockDashboardCacheService) SetVolumeStats(
	_ context.Context,
	_ entities.DashboardFilter,
	_ *entities.VolumeStats,
) error {
	m.setCalled = true

	return nil
}

func (m *mockDashboardCacheService) GetSLAStats(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.SLAStats, error) {
	return m.slaStats, m.slaStatsErr
}

func (m *mockDashboardCacheService) SetSLAStats(
	_ context.Context,
	_ entities.DashboardFilter,
	_ *entities.SLAStats,
) error {
	m.setCalled = true

	return nil
}

func (m *mockDashboardCacheService) GetMatchRateStats(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.MatchRateStats, error) {
	return nil, errTestCacheError
}

func (m *mockDashboardCacheService) SetMatchRateStats(
	_ context.Context,
	_ entities.DashboardFilter,
	_ *entities.MatchRateStats,
) error {
	return nil
}

func (m *mockDashboardCacheService) GetDashboardAggregates(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.DashboardAggregates, error) {
	return m.aggregates, m.aggregatesErr
}

func (m *mockDashboardCacheService) SetDashboardAggregates(
	_ context.Context,
	_ entities.DashboardFilter,
	_ *entities.DashboardAggregates,
) error {
	m.setCalled = true

	return nil
}

func (m *mockDashboardCacheService) GetMatcherDashboardMetrics(
	_ context.Context,
	_ entities.DashboardFilter,
) (*entities.MatcherDashboardMetrics, error) {
	return m.metricsResult, m.metricsErr
}

func (m *mockDashboardCacheService) SetMatcherDashboardMetrics(
	_ context.Context,
	_ entities.DashboardFilter,
	_ *entities.MatcherDashboardMetrics,
) error {
	m.setCalled = true

	return nil
}

func (m *mockDashboardCacheService) InvalidateContext(_ context.Context, _ uuid.UUID) error {
	return nil
}

func TestDashboardUseCase_GetVolumeStats_CacheHit(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	cachedStats := &entities.VolumeStats{
		TotalTransactions:   999,
		MatchedTransactions: 888,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockDashboardRepository(ctrl)
	cache := &mockDashboardCacheService{volumeStats: cachedStats}

	uc, err := NewDashboardUseCase(repo, cache)
	require.NoError(t, err)

	result, err := uc.GetVolumeStats(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, cachedStats, result)
}

func TestDashboardUseCase_GetVolumeStats_CacheMiss(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	dbStats := &entities.VolumeStats{
		TotalTransactions:   200,
		MatchedTransactions: 150,
		TotalAmount:         decimal.NewFromInt(1000),
		MatchedAmount:       decimal.NewFromInt(800),
		UnmatchedAmount:     decimal.NewFromInt(200),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockDashboardRepository(ctrl)
	repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(dbStats, nil)

	cache := &mockDashboardCacheService{volumeStatsErr: errTestCacheError}

	uc, err := NewDashboardUseCase(repo, cache)
	require.NoError(t, err)

	result, err := uc.GetVolumeStats(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, dbStats, result)
	assert.True(t, cache.setCalled)
}

func TestDashboardUseCase_GetSLAStats_CacheHit(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	cachedStats := &entities.SLAStats{
		TotalExceptions: 42,
		ResolvedOnTime:  35,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockDashboardRepository(ctrl)
	cache := &mockDashboardCacheService{slaStats: cachedStats}

	uc, err := NewDashboardUseCase(repo, cache)
	require.NoError(t, err)

	result, err := uc.GetSLAStats(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, cachedStats, result)
}

func TestDashboardUseCase_GetSLAStats_CacheMiss(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	dbStats := &entities.SLAStats{
		TotalExceptions: 10,
		ResolvedOnTime:  8,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockDashboardRepository(ctrl)
	repo.EXPECT().GetSLAStats(gomock.Any(), filter).Return(dbStats, nil)

	cache := &mockDashboardCacheService{slaStatsErr: errTestCacheError}

	uc, err := NewDashboardUseCase(repo, cache)
	require.NoError(t, err)

	result, err := uc.GetSLAStats(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, dbStats, result)
	assert.True(t, cache.setCalled)
}

func TestDashboardUseCase_GetDashboardAggregates_CacheHit(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	cachedAgg := &entities.DashboardAggregates{
		Volume: &entities.VolumeStats{
			TotalTransactions: 500,
			TotalAmount:       decimal.NewFromInt(50000),
			MatchedAmount:     decimal.NewFromInt(40000),
			UnmatchedAmount:   decimal.NewFromInt(10000),
		},
		MatchRate: &entities.MatchRateStats{MatchRate: 80.0},
		SLA:       &entities.SLAStats{TotalExceptions: 5},
		UpdatedAt: time.Now().UTC(),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockDashboardRepository(ctrl)
	cache := &mockDashboardCacheService{aggregates: cachedAgg}

	uc, err := NewDashboardUseCase(repo, cache)
	require.NoError(t, err)

	result, err := uc.GetDashboardAggregates(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, cachedAgg, result)
}

func TestDashboardUseCase_GetDashboardAggregates_CacheMissAndSet(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	volumeStats := &entities.VolumeStats{
		TotalTransactions:   100,
		MatchedTransactions: 80,
		TotalAmount:         decimal.NewFromInt(10000),
		MatchedAmount:       decimal.NewFromInt(8000),
		UnmatchedAmount:     decimal.NewFromInt(2000),
	}
	slaStats := &entities.SLAStats{
		TotalExceptions: 10,
		ResolvedOnTime:  8,
	}

	repo := mocks.NewMockDashboardRepository(ctrl)
	repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(volumeStats, nil)
	repo.EXPECT().GetSLAStats(gomock.Any(), filter).Return(slaStats, nil)

	cache := &mockDashboardCacheService{
		aggregatesErr:  errTestCacheError,
		volumeStatsErr: errTestCacheError,
		slaStatsErr:    errTestCacheError,
	}

	uc, err := NewDashboardUseCase(repo, cache)
	require.NoError(t, err)

	result, err := uc.GetDashboardAggregates(ctx, filter)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, cache.setCalled)
}

func TestDashboardUseCase_GetMatcherDashboardMetrics_CacheHit(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	cachedMetrics := &entities.MatcherDashboardMetrics{
		Summary:    &entities.SummaryMetrics{TotalTransactions: 1000},
		Trends:     &entities.TrendMetrics{Dates: []string{"2024-01-01"}},
		Breakdowns: &entities.BreakdownMetrics{BySeverity: map[string]int{"HIGH": 5}},
		UpdatedAt:  time.Now().UTC(),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockDashboardRepository(ctrl)
	cache := &mockDashboardCacheService{metricsResult: cachedMetrics}

	uc, err := NewDashboardUseCase(repo, cache)
	require.NoError(t, err)

	result, err := uc.GetMatcherDashboardMetrics(ctx, filter)

	require.NoError(t, err)
	assert.Equal(t, cachedMetrics, result)
}

func TestDashboardUseCase_GetMatcherDashboardMetrics_CacheMissAndSet(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mocks.NewMockDashboardRepository(ctrl)
	repo.EXPECT().GetSummaryMetrics(gomock.Any(), filter).Return(&entities.SummaryMetrics{TotalTransactions: 100}, nil)
	repo.EXPECT().GetTrendMetrics(gomock.Any(), filter).Return(&entities.TrendMetrics{Dates: []string{}}, nil)
	repo.EXPECT().GetBreakdownMetrics(gomock.Any(), filter).Return(&entities.BreakdownMetrics{}, nil)

	cache := &mockDashboardCacheService{metricsErr: errTestCacheError}

	uc, err := NewDashboardUseCase(repo, cache)
	require.NoError(t, err)

	result, err := uc.GetMatcherDashboardMetrics(ctx, filter)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, cache.setCalled)
}

// --- UseCase streaming support detection ---

func TestNewUseCase_DetectsStreamingSupport(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Regular repo does not support streaming
	mockRepo := mocks.NewMockReportRepository(ctrl)

	uc, err := NewUseCase(mockRepo)
	require.NoError(t, err)
	assert.False(t, uc.SupportsStreaming())
}
