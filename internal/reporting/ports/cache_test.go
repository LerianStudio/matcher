// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	portsmocks "github.com/LerianStudio/matcher/internal/reporting/ports/mocks"
)

var (
	errTestCacheMiss    = errors.New("cache miss")
	errTestCacheFailure = errors.New("cache failure")
)

func TestDashboardCacheService_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	cacheType := reflect.TypeOf((*DashboardCacheService)(nil)).Elem()
	assert.NotNil(t, cacheType)
}

func TestDashboardCacheService_IsInterface(t *testing.T) {
	t.Parallel()

	cacheType := reflect.TypeOf((*DashboardCacheService)(nil)).Elem()
	assert.Equal(t, reflect.Interface, cacheType.Kind())
}

func TestDashboardCacheService_MethodCount(t *testing.T) {
	t.Parallel()

	cacheType := reflect.TypeOf((*DashboardCacheService)(nil)).Elem()

	const expectedMethodCount = 11

	actualCount := cacheType.NumMethod()

	assert.Equal(t, expectedMethodCount, actualCount,
		"DashboardCacheService should have exactly %d methods - found %d",
		expectedMethodCount, actualCount)
}

func TestDashboardCacheService_RequiredMethods(t *testing.T) {
	t.Parallel()

	cacheType := reflect.TypeOf((*DashboardCacheService)(nil)).Elem()

	requiredMethods := []string{
		"GetVolumeStats",
		"SetVolumeStats",
		"GetSLAStats",
		"SetSLAStats",
		"GetMatchRateStats",
		"SetMatchRateStats",
		"GetDashboardAggregates",
		"SetDashboardAggregates",
		"GetMatcherDashboardMetrics",
		"SetMatcherDashboardMetrics",
		"InvalidateContext",
	}

	for _, methodName := range requiredMethods {
		t.Run(methodName+"_exists", func(t *testing.T) {
			t.Parallel()

			_, exists := cacheType.MethodByName(methodName)
			assert.True(t, exists, "method %s must exist in DashboardCacheService", methodName)
		})
	}
}

func newDashboardCacheMock(t *testing.T) *portsmocks.MockDashboardCacheService {
	t.Helper()

	ctrl := gomock.NewController(t)

	return portsmocks.NewMockDashboardCacheService(ctrl)
}

func TestMockDashboardCacheService_ImplementsInterface(t *testing.T) {
	t.Parallel()

	mock := newDashboardCacheMock(t)

	var cache DashboardCacheService = mock
	assert.NotNil(t, cache)
}

func TestMockDashboardCacheService_GetVolumeStats(t *testing.T) {
	t.Parallel()

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		expected := &entities.VolumeStats{TotalTransactions: 100}

		mock.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(expected, nil)

		result, err := mock.GetVolumeStats(context.Background(), filter)

		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("cache miss", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().GetVolumeStats(gomock.Any(), filter).Return(nil, errTestCacheMiss)

		result, err := mock.GetVolumeStats(context.Background(), filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, errTestCacheMiss)
	})
}

func TestMockDashboardCacheService_SetVolumeStats(t *testing.T) {
	t.Parallel()

	t.Run("successful set", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		stats := &entities.VolumeStats{TotalTransactions: 50}

		mock.EXPECT().SetVolumeStats(gomock.Any(), filter, stats).Return(nil)

		err := mock.SetVolumeStats(context.Background(), filter, stats)

		require.NoError(t, err)
	})

	t.Run("set error", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().SetVolumeStats(gomock.Any(), filter, gomock.Any()).Return(errTestCacheFailure)

		err := mock.SetVolumeStats(context.Background(), filter, &entities.VolumeStats{})

		require.ErrorIs(t, err, errTestCacheFailure)
	})
}

func TestMockDashboardCacheService_GetSLAStats(t *testing.T) {
	t.Parallel()

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		expected := &entities.SLAStats{TotalExceptions: 42}

		mock.EXPECT().GetSLAStats(gomock.Any(), filter).Return(expected, nil)

		result, err := mock.GetSLAStats(context.Background(), filter)

		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("cache miss", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().GetSLAStats(gomock.Any(), filter).Return(nil, errTestCacheMiss)

		result, err := mock.GetSLAStats(context.Background(), filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, errTestCacheMiss)
	})
}

func TestMockDashboardCacheService_SetSLAStats(t *testing.T) {
	t.Parallel()

	t.Run("successful set", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		stats := &entities.SLAStats{TotalExceptions: 10}

		mock.EXPECT().SetSLAStats(gomock.Any(), filter, stats).Return(nil)

		err := mock.SetSLAStats(context.Background(), filter, stats)

		require.NoError(t, err)
	})

	t.Run("set error", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().SetSLAStats(gomock.Any(), filter, gomock.Any()).Return(errTestCacheFailure)

		err := mock.SetSLAStats(context.Background(), filter, &entities.SLAStats{})

		require.ErrorIs(t, err, errTestCacheFailure)
	})
}

func TestMockDashboardCacheService_GetMatchRateStats(t *testing.T) {
	t.Parallel()

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		expected := &entities.MatchRateStats{MatchRate: 85.5, TotalCount: 100, MatchedCount: 85}

		mock.EXPECT().GetMatchRateStats(gomock.Any(), filter).Return(expected, nil)

		result, err := mock.GetMatchRateStats(context.Background(), filter)

		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("cache miss", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().GetMatchRateStats(gomock.Any(), filter).Return(nil, errTestCacheMiss)

		result, err := mock.GetMatchRateStats(context.Background(), filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, errTestCacheMiss)
	})
}

func TestMockDashboardCacheService_SetMatchRateStats(t *testing.T) {
	t.Parallel()

	t.Run("successful set", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		stats := &entities.MatchRateStats{MatchRate: 90.0, TotalCount: 50}

		mock.EXPECT().SetMatchRateStats(gomock.Any(), filter, stats).Return(nil)

		err := mock.SetMatchRateStats(context.Background(), filter, stats)

		require.NoError(t, err)
	})

	t.Run("set error", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().SetMatchRateStats(gomock.Any(), filter, gomock.Any()).Return(errTestCacheFailure)

		err := mock.SetMatchRateStats(context.Background(), filter, &entities.MatchRateStats{})

		require.ErrorIs(t, err, errTestCacheFailure)
	})
}

func TestMockDashboardCacheService_GetDashboardAggregates(t *testing.T) {
	t.Parallel()

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		expected := &entities.DashboardAggregates{
			Volume: &entities.VolumeStats{TotalTransactions: 200},
		}

		mock.EXPECT().GetDashboardAggregates(gomock.Any(), filter).Return(expected, nil)

		result, err := mock.GetDashboardAggregates(context.Background(), filter)

		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("cache miss", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().GetDashboardAggregates(gomock.Any(), filter).Return(nil, errTestCacheMiss)

		result, err := mock.GetDashboardAggregates(context.Background(), filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, errTestCacheMiss)
	})
}

func TestMockDashboardCacheService_SetDashboardAggregates(t *testing.T) {
	t.Parallel()

	t.Run("successful set", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		aggregates := &entities.DashboardAggregates{
			Volume: &entities.VolumeStats{TotalTransactions: 75},
		}

		mock.EXPECT().SetDashboardAggregates(gomock.Any(), filter, aggregates).Return(nil)

		err := mock.SetDashboardAggregates(context.Background(), filter, aggregates)

		require.NoError(t, err)
	})

	t.Run("set error", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().SetDashboardAggregates(gomock.Any(), filter, gomock.Any()).Return(errTestCacheFailure)

		err := mock.SetDashboardAggregates(context.Background(), filter, &entities.DashboardAggregates{})

		require.ErrorIs(t, err, errTestCacheFailure)
	})
}

func TestMockDashboardCacheService_GetMatcherDashboardMetrics(t *testing.T) {
	t.Parallel()

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		expected := &entities.MatcherDashboardMetrics{
			Summary: &entities.SummaryMetrics{TotalTransactions: 500},
		}

		mock.EXPECT().GetMatcherDashboardMetrics(gomock.Any(), filter).Return(expected, nil)

		result, err := mock.GetMatcherDashboardMetrics(context.Background(), filter)

		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("cache miss", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().GetMatcherDashboardMetrics(gomock.Any(), filter).Return(nil, errTestCacheMiss)

		result, err := mock.GetMatcherDashboardMetrics(context.Background(), filter)

		assert.Nil(t, result)
		require.ErrorIs(t, err, errTestCacheMiss)
	})
}

func TestMockDashboardCacheService_SetMatcherDashboardMetrics(t *testing.T) {
	t.Parallel()

	t.Run("successful set", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}
		metrics := &entities.MatcherDashboardMetrics{
			Summary: &entities.SummaryMetrics{TotalMatches: 250},
		}

		mock.EXPECT().SetMatcherDashboardMetrics(gomock.Any(), filter, metrics).Return(nil)

		err := mock.SetMatcherDashboardMetrics(context.Background(), filter, metrics)

		require.NoError(t, err)
	})

	t.Run("set error", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		filter := entities.DashboardFilter{ContextID: uuid.New()}

		mock.EXPECT().SetMatcherDashboardMetrics(gomock.Any(), filter, gomock.Any()).Return(errTestCacheFailure)

		err := mock.SetMatcherDashboardMetrics(context.Background(), filter, &entities.MatcherDashboardMetrics{})

		require.ErrorIs(t, err, errTestCacheFailure)
	})
}

func TestMockDashboardCacheService_InvalidateContext(t *testing.T) {
	t.Parallel()

	t.Run("successful invalidation", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		contextID := uuid.New()

		mock.EXPECT().InvalidateContext(gomock.Any(), contextID).Return(nil)

		err := mock.InvalidateContext(context.Background(), contextID)

		require.NoError(t, err)
	})

	t.Run("invalidation error", func(t *testing.T) {
		t.Parallel()

		mock := newDashboardCacheMock(t)
		contextID := uuid.New()

		mock.EXPECT().InvalidateContext(gomock.Any(), contextID).Return(errTestCacheFailure)

		err := mock.InvalidateContext(context.Background(), contextID)

		require.ErrorIs(t, err, errTestCacheFailure)
	})
}
