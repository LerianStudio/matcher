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
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
)

var (
	errTestDBError        = errors.New("test db error")
	errTestVolumeError    = errors.New("test volume error")
	errTestSLAError       = errors.New("test sla error")
	errTestSummaryError   = errors.New("test summary error")
	errTestTrendError     = errors.New("test trend error")
	errTestBreakdownError = errors.New("test breakdown error")
)

type tracerContextKey struct{}

func testContext() context.Context {
	tracer := noop.NewTracerProvider().Tracer("test")
	return context.WithValue(context.Background(), tracerContextKey{}, tracer)
}

func TestNewDashboardUseCase(t *testing.T) {
	t.Parallel()

	t.Run("returns error when repository is nil", func(t *testing.T) {
		t.Parallel()

		uc, err := NewDashboardUseCase(nil, nil)

		assert.Nil(t, uc)
		require.Error(t, err)
		assert.Equal(t, ErrNilDashboardRepository, err)
	})

	t.Run("creates use case successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo := mocks.NewMockDashboardRepository(ctrl)
		uc, err := NewDashboardUseCase(repo, nil)

		assert.NotNil(t, uc)
		require.NoError(t, err)
	})
}

func TestDashboardUseCase_GetVolumeStats(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	contextID := uuid.New()
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("returns volume stats successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedStats := &entities.VolumeStats{
			TotalTransactions:   100,
			MatchedTransactions: 80,
			UnmatchedCount:      20,
			TotalAmount:         decimal.NewFromInt(10000),
			MatchedAmount:       decimal.NewFromInt(8000),
			UnmatchedAmount:     decimal.NewFromInt(2000),
		}

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(expectedStats, nil)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetVolumeStats(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, expectedStats, result)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errTestDBError

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(nil, expectedErr)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetVolumeStats(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting volume stats")
	})
}

func TestDashboardUseCase_GetSLAStats(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	contextID := uuid.New()
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("returns SLA stats successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedStats := &entities.SLAStats{
			TotalExceptions:     50,
			ResolvedOnTime:      40,
			ResolvedLate:        5,
			PendingWithinSLA:    3,
			PendingOverdue:      2,
			SLAComplianceRate:   88.89,
			AverageResolutionMs: 3600000,
		}

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetSLAStats(gomock.Any(), filter).Return(expectedStats, nil)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetSLAStats(ctx, filter)

		require.NoError(t, err)
		assert.Equal(t, expectedStats, result)
	})

	t.Run("returns error when repository fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errTestDBError

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetSLAStats(gomock.Any(), filter).Return(nil, expectedErr)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetSLAStats(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting sla stats")
	})
}

func TestDashboardUseCase_GetMatchRateStats(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	contextID := uuid.New()
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("calculates match rate from volume stats", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		volumeStats := &entities.VolumeStats{
			TotalTransactions:   100,
			MatchedTransactions: 75,
			UnmatchedCount:      25,
			TotalAmount:         decimal.NewFromInt(1000),
			MatchedAmount:       decimal.NewFromInt(800),
			UnmatchedAmount:     decimal.NewFromInt(200),
		}

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(volumeStats, nil)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetMatchRateStats(ctx, filter)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.InDelta(t, 75.0, result.MatchRate, 0.01)
		assert.InDelta(t, 80.0, result.MatchRateAmount, 0.01)
		assert.Equal(t, 100, result.TotalCount)
		assert.Equal(t, 75, result.MatchedCount)
		assert.Equal(t, 25, result.UnmatchedCount)
	})

	t.Run("returns error when volume stats fail", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errTestDBError

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(nil, expectedErr)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetMatchRateStats(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting volume stats")
	})
}

func TestDashboardUseCase_GetDashboardAggregates(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	contextID := uuid.New()
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("returns all aggregates successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		volumeStats := &entities.VolumeStats{
			TotalTransactions:   100,
			MatchedTransactions: 80,
			UnmatchedCount:      20,
			TotalAmount:         decimal.NewFromInt(10000),
			MatchedAmount:       decimal.NewFromInt(8000),
			UnmatchedAmount:     decimal.NewFromInt(2000),
		}
		slaStats := &entities.SLAStats{
			TotalExceptions:   10,
			ResolvedOnTime:    8,
			ResolvedLate:      2,
			SLAComplianceRate: 80.0,
		}

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(volumeStats, nil)
		repo.EXPECT().GetSLAStats(gomock.Any(), filter).Return(slaStats, nil)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetDashboardAggregates(ctx, filter)

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.Volume)
		assert.NotNil(t, result.MatchRate)
		assert.NotNil(t, result.SLA)
		assert.False(t, result.UpdatedAt.IsZero())
	})

	t.Run("returns error when volume stats fail", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(nil, errTestVolumeError)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetDashboardAggregates(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
	})

	t.Run("returns error when SLA stats fail", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		volumeStats := &entities.VolumeStats{TotalTransactions: 100}

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(volumeStats, nil)
		repo.EXPECT().GetSLAStats(gomock.Any(), filter).Return(nil, errTestSLAError)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetDashboardAggregates(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
	})
}

func TestDashboardUseCase_GetMatcherDashboardMetrics(t *testing.T) {
	t.Parallel()

	ctx := testContext()
	contextID := uuid.New()
	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("returns complete metrics successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedSummary := &entities.SummaryMetrics{
			TotalTransactions: 1000,
			TotalMatches:      450,
			MatchRate:         90.0,
			PendingExceptions: 25,
		}
		expectedTrends := &entities.TrendMetrics{
			Dates:     []string{"2024-01-01"},
			Ingestion: []int{100},
		}
		expectedBreakdowns := &entities.BreakdownMetrics{
			BySeverity: map[string]int{"CRITICAL": 5},
		}

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetSummaryMetrics(gomock.Any(), filter).Return(expectedSummary, nil)
		repo.EXPECT().GetTrendMetrics(gomock.Any(), filter).Return(expectedTrends, nil)
		repo.EXPECT().GetBreakdownMetrics(gomock.Any(), filter).Return(expectedBreakdowns, nil)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetMatcherDashboardMetrics(ctx, filter)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, expectedSummary, result.Summary)
		assert.Equal(t, expectedTrends, result.Trends)
		assert.Equal(t, expectedBreakdowns, result.Breakdowns)
		assert.False(t, result.UpdatedAt.IsZero())
	})

	t.Run("returns error when summary fails", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().GetSummaryMetrics(gomock.Any(), filter).Return(nil, errTestSummaryError)
		repo.EXPECT().
			GetTrendMetrics(gomock.Any(), filter).
			Return(&entities.TrendMetrics{}, nil).
			AnyTimes()
		repo.EXPECT().
			GetBreakdownMetrics(gomock.Any(), filter).
			Return(&entities.BreakdownMetrics{}, nil).
			AnyTimes()

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetMatcherDashboardMetrics(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting summary metrics")
	})

	t.Run("returns error when trends fail", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().
			GetSummaryMetrics(gomock.Any(), filter).
			Return(&entities.SummaryMetrics{TotalTransactions: 100}, nil).
			AnyTimes()
		repo.EXPECT().GetTrendMetrics(gomock.Any(), filter).Return(nil, errTestTrendError)
		repo.EXPECT().
			GetBreakdownMetrics(gomock.Any(), filter).
			Return(&entities.BreakdownMetrics{}, nil).
			AnyTimes()

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetMatcherDashboardMetrics(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting trend metrics")
	})

	t.Run("returns error when breakdowns fail", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo := mocks.NewMockDashboardRepository(ctrl)
		repo.EXPECT().
			GetSummaryMetrics(gomock.Any(), filter).
			Return(&entities.SummaryMetrics{TotalTransactions: 100}, nil).
			AnyTimes()
		repo.EXPECT().
			GetTrendMetrics(gomock.Any(), filter).
			Return(&entities.TrendMetrics{Dates: []string{"2024-01-01"}}, nil).
			AnyTimes()
		repo.EXPECT().GetBreakdownMetrics(gomock.Any(), filter).Return(nil, errTestBreakdownError)

		uc, err := NewDashboardUseCase(repo, nil)
		require.NoError(t, err)

		result, err := uc.GetMatcherDashboardMetrics(ctx, filter)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting breakdown metrics")
	})
}
