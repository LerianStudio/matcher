//go:build unit

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v3/commons/tenant-manager/core"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestNewCacheService(t *testing.T) {
	t.Parallel()

	t.Run("creates service with default TTL", func(t *testing.T) {
		t.Parallel()

		svc := NewCacheService(nil, 0)

		assert.NotNil(t, svc)
		assert.Equal(t, 5*time.Minute, svc.defaultTTL)
	})

	t.Run("creates service with custom TTL", func(t *testing.T) {
		t.Parallel()

		customTTL := 10 * time.Minute
		svc := NewCacheService(nil, customTTL)

		assert.NotNil(t, svc)
		assert.Equal(t, customTTL, svc.defaultTTL)
	})
}

func TestCacheService_BuildKey(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)

	contextID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	sourceID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440000")
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	t.Run("builds key without source filter in single-tenant mode", func(t *testing.T) {
		t.Parallel()

		// core.GetTenantIDFromContext returns empty when no tenant is set,
		// so valkey.GetKeyFromContext returns the key without tenant prefix.
		key := svc.buildKey(context.Background(), contextID, volumeKeyType, dateFrom, dateTo, nil)

		expected := "matcher:dashboard:550e8400-e29b-41d4-a716-446655440000:volume:2024-01-01:2024-01-31:all"
		assert.Equal(t, expected, key)
	})

	t.Run("builds key with source filter in single-tenant mode", func(t *testing.T) {
		t.Parallel()

		// core.GetTenantIDFromContext returns empty when no tenant is set,
		// so valkey.GetKeyFromContext returns the key without tenant prefix.
		key := svc.buildKey(context.Background(), contextID, volumeKeyType, dateFrom, dateTo, &sourceID)

		expected := "matcher:dashboard:550e8400-e29b-41d4-a716-446655440000:volume:2024-01-01:2024-01-31:660e8400-e29b-41d4-a716-446655440000"
		assert.Equal(t, expected, key)
	})

	t.Run("builds key with tenant prefix in multi-tenant mode", func(t *testing.T) {
		t.Parallel()

		tenantID := "550e8400-e29b-41d4-a716-446655440099"
		ctx := core.SetTenantIDInContext(context.Background(), tenantID)
		key := svc.buildKey(ctx, contextID, volumeKeyType, dateFrom, dateTo, nil)

		expected := "tenant:" + tenantID + ":matcher:dashboard:550e8400-e29b-41d4-a716-446655440000:volume:2024-01-01:2024-01-31:all"
		assert.Equal(t, expected, key)
	})
}

func TestCacheService_GetVolumeStats_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetVolumeStats(context.Background(), filter)

	assert.Nil(t, result)
	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_SetVolumeStats_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.VolumeStats{
		TotalTransactions: 100,
	}

	err := svc.SetVolumeStats(context.Background(), filter, stats)

	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_GetSLAStats_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetSLAStats(context.Background(), filter)

	assert.Nil(t, result)
	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_SetSLAStats_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.SLAStats{
		TotalExceptions: 10,
	}

	err := svc.SetSLAStats(context.Background(), filter, stats)

	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_GetMatchRateStats_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetMatchRateStats(context.Background(), filter)

	assert.Nil(t, result)
	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_SetMatchRateStats_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.MatchRateStats{
		MatchRate:  85.0,
		TotalCount: 100,
	}

	err := svc.SetMatchRateStats(context.Background(), filter, stats)

	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_GetDashboardAggregates_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetDashboardAggregates(context.Background(), filter)

	assert.Nil(t, result)
	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_SetDashboardAggregates_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	aggregates := &entities.DashboardAggregates{
		Volume: &entities.VolumeStats{
			TotalTransactions: 100,
			TotalAmount:       decimal.NewFromInt(10000),
		},
	}

	err := svc.SetDashboardAggregates(context.Background(), filter, aggregates)

	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_InvalidateContext_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)

	err := svc.InvalidateContext(context.Background(), uuid.New())

	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_NilService(t *testing.T) {
	t.Parallel()

	var svc *CacheService

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
	}

	t.Run("GetVolumeStats on nil service", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetVolumeStats(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetVolumeStats on nil service", func(t *testing.T) {
		t.Parallel()

		err := svc.SetVolumeStats(context.Background(), filter, &entities.VolumeStats{})
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("GetSLAStats on nil service", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetSLAStats(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetSLAStats on nil service", func(t *testing.T) {
		t.Parallel()

		err := svc.SetSLAStats(context.Background(), filter, &entities.SLAStats{})
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("GetMatchRateStats on nil service", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetMatchRateStats(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetMatchRateStats on nil service", func(t *testing.T) {
		t.Parallel()

		err := svc.SetMatchRateStats(context.Background(), filter, &entities.MatchRateStats{})
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("GetDashboardAggregates on nil service", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetDashboardAggregates(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetDashboardAggregates on nil service", func(t *testing.T) {
		t.Parallel()

		err := svc.SetDashboardAggregates(
			context.Background(),
			filter,
			&entities.DashboardAggregates{},
		)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("InvalidateContext on nil service", func(t *testing.T) {
		t.Parallel()

		err := svc.InvalidateContext(context.Background(), uuid.New())
		assert.Equal(t, ErrRedisConnRequired, err)
	})
}

func setupRedisCacheService(t *testing.T) (*miniredis.Miniredis, *goredis.Client, *CacheService) {
	t.Helper()

	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	service := NewCacheService(provider, time.Minute)

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close redis client: %v", err)
		}

		server.Close()
	})

	return server, client, service
}

func TestCacheService_GetVolumeStats_CacheMiss(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetVolumeStats(context.Background(), filter)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_SetAndGetVolumeStats(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.VolumeStats{
		TotalTransactions:   12,
		MatchedTransactions: 8,
		UnmatchedCount:      4,
		TotalAmount:         decimal.NewFromInt(1200),
		MatchedAmount:       decimal.NewFromInt(800),
		UnmatchedAmount:     decimal.NewFromInt(400),
	}

	err := svc.SetVolumeStats(context.Background(), filter, stats)
	require.NoError(t, err)

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		volumeKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	raw, err := client.Get(context.Background(), key).Bytes()
	require.NoError(t, err)

	var cached entities.VolumeStats
	require.NoError(t, json.Unmarshal(raw, &cached))
	assert.Equal(t, stats.TotalTransactions, cached.TotalTransactions)

	result, err := svc.GetVolumeStats(context.Background(), filter)
	require.NoError(t, err)
	assert.Equal(t, stats.TotalTransactions, result.TotalTransactions)
	assert.True(t, stats.TotalAmount.Equal(result.TotalAmount))
}

func TestCacheService_GetVolumeStats_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)
	server.SetError("LOADING Redis is loading the dataset in memory")

	defer server.SetError("")

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetVolumeStats(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_GetSLAStats_CacheMiss(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetSLAStats(context.Background(), filter)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_GetSLAStats_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		slaKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	require.NoError(t, client.Set(context.Background(), key, "not-json", 0).Err())

	result, err := svc.GetSLAStats(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_GetSLAStats_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)
	server.SetError("LOADING Redis is loading the dataset in memory")

	defer server.SetError("")

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetSLAStats(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_GetMatchRateStats_CacheMiss(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetMatchRateStats(context.Background(), filter)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_SetAndGetMatchRateStats(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.MatchRateStats{
		MatchRate:       85.5,
		MatchRateAmount: 90.2,
		TotalCount:      200,
		MatchedCount:    171,
		UnmatchedCount:  29,
	}

	err := svc.SetMatchRateStats(context.Background(), filter, stats)
	require.NoError(t, err)

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		matchRateKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	raw, err := client.Get(context.Background(), key).Bytes()
	require.NoError(t, err)

	var cached entities.MatchRateStats
	require.NoError(t, json.Unmarshal(raw, &cached))
	assert.Equal(t, stats.TotalCount, cached.TotalCount)

	result, err := svc.GetMatchRateStats(context.Background(), filter)
	require.NoError(t, err)
	assert.Equal(t, stats.MatchRate, result.MatchRate)
	assert.Equal(t, stats.MatchRateAmount, result.MatchRateAmount)
	assert.Equal(t, stats.TotalCount, result.TotalCount)
	assert.Equal(t, stats.MatchedCount, result.MatchedCount)
	assert.Equal(t, stats.UnmatchedCount, result.UnmatchedCount)
}

func TestCacheService_GetMatchRateStats_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		matchRateKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	require.NoError(t, client.Set(context.Background(), key, "not-json", 0).Err())

	result, err := svc.GetMatchRateStats(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestCacheService_GetMatchRateStats_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)
	server.SetError("LOADING Redis is loading the dataset in memory")

	defer server.SetError("")

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetMatchRateStats(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_SetMatchRateStats_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.MatchRateStats{
		MatchRate:  75.0,
		TotalCount: 100,
	}

	server.SetError("ERR write failed")
	defer server.SetError("")

	err := svc.SetMatchRateStats(context.Background(), filter, stats)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestCacheService_GetDashboardAggregates_CacheMiss(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetDashboardAggregates(context.Background(), filter)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_GetDashboardAggregates_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		aggregatesKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	require.NoError(t, client.Set(context.Background(), key, "not-json", 0).Err())

	result, err := svc.GetDashboardAggregates(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_GetDashboardAggregates_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)
	server.SetError("LOADING Redis is loading the dataset in memory")

	defer server.SetError("")

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetDashboardAggregates(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_InvalidateContext_ScanError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)
	server.SetError("ERR scan failed")

	defer server.SetError("")

	err := svc.InvalidateContext(context.Background(), uuid.New())

	require.Error(t, err)
}

func TestCacheService_SetAndGetDashboardAggregates(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	aggregates := &entities.DashboardAggregates{
		Volume: &entities.VolumeStats{
			TotalTransactions: 5,
			TotalAmount:       decimal.NewFromInt(500),
		},
		MatchRate: &entities.MatchRateStats{
			MatchRate: 80,
		},
		SLA: &entities.SLAStats{
			TotalExceptions: 2,
		},
		UpdatedAt: time.Now().UTC(),
	}

	err := svc.SetDashboardAggregates(context.Background(), filter, aggregates)
	require.NoError(t, err)

	result, err := svc.GetDashboardAggregates(context.Background(), filter)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Volume)
	assert.Equal(t, aggregates.Volume.TotalTransactions, result.Volume.TotalTransactions)
}

func TestCacheService_TTLExpiration(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.VolumeStats{
		TotalTransactions: 100,
		TotalAmount:       decimal.NewFromInt(10000),
	}

	err := svc.SetVolumeStats(context.Background(), filter, stats)
	require.NoError(t, err)

	result, err := svc.GetVolumeStats(context.Background(), filter)
	require.NoError(t, err)
	require.NotNil(t, result)

	server.FastForward(2 * time.Minute)

	result, err = svc.GetVolumeStats(context.Background(), filter)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	const goroutines = 10

	const iterations = 50

	// Buffered channel to collect errors from concurrent goroutines.
	errs := make(chan error, goroutines*iterations*2)

	var wg sync.WaitGroup

	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				stats := &entities.VolumeStats{
					TotalTransactions: id*iterations + j,
					TotalAmount:       decimal.NewFromInt(int64(id * 1000)),
				}
				if err := svc.SetVolumeStats(context.Background(), filter, stats); err != nil {
					errs <- err
				}
			}
		}(i)

		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				_, err := svc.GetVolumeStats(context.Background(), filter)
				// ErrCacheMiss is expected before any Set completes.
				if err != nil && !errors.Is(err, ErrCacheMiss) {
					errs <- err
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	// Assert no unexpected errors occurred during concurrent access.
	var collectedErrs []error
	for err := range errs {
		collectedErrs = append(collectedErrs, err)
	}

	assert.Empty(t, collectedErrs, "unexpected errors during concurrent access: %v", collectedErrs)

	// Verify final state consistency: the last write must be fully readable
	// and its fields must be internally consistent.
	result, err := svc.GetVolumeStats(context.Background(), filter)
	require.NoError(t, err)
	require.NotNil(t, result)

	// TotalTransactions = id*iterations + j, so must be in [0, goroutines*iterations).
	assert.GreaterOrEqual(t, result.TotalTransactions, 0,
		"TotalTransactions should be non-negative")
	assert.Less(t, result.TotalTransactions, goroutines*iterations,
		"TotalTransactions should be less than goroutines*iterations")

	// TotalAmount = id * 1000 where id = TotalTransactions / iterations,
	// verifying the cached value was not torn across two concurrent writes.
	expectedID := result.TotalTransactions / iterations
	expectedAmount := decimal.NewFromInt(int64(expectedID * 1000))
	assert.True(t, result.TotalAmount.Equal(expectedAmount),
		"TotalAmount should be consistent with TotalTransactions: got %s, expected %s",
		result.TotalAmount.String(), expectedAmount.String())
}

func TestCacheService_ConcurrentInvalidation(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	contextID := uuid.New()

	const goroutines = 5

	var wg sync.WaitGroup

	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()

			filter := entities.DashboardFilter{
				ContextID: contextID,
				DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
				DateTo:    time.Now().UTC(),
			}
			stats := &entities.VolumeStats{TotalTransactions: 100}

			_ = svc.SetVolumeStats(context.Background(), filter, stats)
		}()

		go func() {
			defer wg.Done()

			_ = svc.InvalidateContext(context.Background(), contextID)
		}()
	}

	wg.Wait()
}

func TestCacheService_GetMatcherDashboardMetrics_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)

	assert.Nil(t, result)
	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_SetMatcherDashboardMetrics_NilProvider(t *testing.T) {
	t.Parallel()

	svc := NewCacheService(nil, time.Minute)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	metrics := &entities.MatcherDashboardMetrics{
		Summary: &entities.SummaryMetrics{
			TotalTransactions: 100,
		},
	}

	err := svc.SetMatcherDashboardMetrics(context.Background(), filter, metrics)

	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_GetMatcherDashboardMetrics_NilService(t *testing.T) {
	t.Parallel()

	var svc *CacheService

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
	}

	result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)
	assert.Nil(t, result)
	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_SetMatcherDashboardMetrics_NilService(t *testing.T) {
	t.Parallel()

	var svc *CacheService

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
	}

	err := svc.SetMatcherDashboardMetrics(
		context.Background(),
		filter,
		&entities.MatcherDashboardMetrics{},
	)
	assert.Equal(t, ErrRedisConnRequired, err)
}

func TestCacheService_GetMatcherDashboardMetrics_CacheMiss(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_SetAndGetMatcherDashboardMetrics(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	metrics := &entities.MatcherDashboardMetrics{
		Summary: &entities.SummaryMetrics{
			TotalTransactions:  100,
			TotalMatches:       80,
			MatchRate:          80.0,
			PendingExceptions:  20,
			CriticalExposure:   decimal.NewFromInt(10000),
			OldestExceptionAge: 24.5,
		},
		Trends:     entities.NewEmptyTrendMetrics(),
		Breakdowns: entities.NewEmptyBreakdownMetrics(),
		UpdatedAt:  time.Now().UTC(),
	}

	err := svc.SetMatcherDashboardMetrics(context.Background(), filter, metrics)
	require.NoError(t, err)

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		metricsKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	raw, err := client.Get(context.Background(), key).Bytes()
	require.NoError(t, err)

	var cached entities.MatcherDashboardMetrics
	require.NoError(t, json.Unmarshal(raw, &cached))
	assert.Equal(t, metrics.Summary.TotalTransactions, cached.Summary.TotalTransactions)

	result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)
	require.NoError(t, err)
	require.NotNil(t, result.Summary)
	assert.Equal(t, metrics.Summary.TotalTransactions, result.Summary.TotalTransactions)
	assert.Equal(t, metrics.Summary.MatchRate, result.Summary.MatchRate)
	assert.True(t, metrics.Summary.CriticalExposure.Equal(result.Summary.CriticalExposure))
}

func TestCacheService_GetMatcherDashboardMetrics_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		metricsKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	require.NoError(t, client.Set(context.Background(), key, "not-json", 0).Err())

	result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestCacheService_GetMatcherDashboardMetrics_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)
	server.SetError("LOADING Redis is loading the dataset in memory")

	defer server.SetError("")

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
}

func TestCacheService_SetMatcherDashboardMetrics_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	metrics := &entities.MatcherDashboardMetrics{
		Summary: &entities.SummaryMetrics{
			TotalTransactions: 100,
		},
	}

	server.SetError("ERR write failed")
	defer server.SetError("")

	err := svc.SetMatcherDashboardMetrics(context.Background(), filter, metrics)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestCacheService_SetAndGetSLAStats(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.SLAStats{
		TotalExceptions:     50,
		ResolvedOnTime:      40,
		ResolvedLate:        5,
		PendingWithinSLA:    3,
		PendingOverdue:      2,
		SLAComplianceRate:   90.0,
		AverageResolutionMs: 3600000,
	}

	err := svc.SetSLAStats(context.Background(), filter, stats)
	require.NoError(t, err)

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		slaKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	raw, err := client.Get(context.Background(), key).Bytes()
	require.NoError(t, err)

	var cached entities.SLAStats
	require.NoError(t, json.Unmarshal(raw, &cached))
	assert.Equal(t, stats.TotalExceptions, cached.TotalExceptions)

	result, err := svc.GetSLAStats(context.Background(), filter)
	require.NoError(t, err)
	assert.Equal(t, stats.TotalExceptions, result.TotalExceptions)
	assert.Equal(t, stats.SLAComplianceRate, result.SLAComplianceRate)
}

func TestCacheService_SetSLAStats_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.SLAStats{
		TotalExceptions: 10,
	}

	server.SetError("ERR write failed")
	defer server.SetError("")

	err := svc.SetSLAStats(context.Background(), filter, stats)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestCacheService_SetVolumeStats_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	stats := &entities.VolumeStats{
		TotalTransactions: 100,
	}

	server.SetError("ERR write failed")
	defer server.SetError("")

	err := svc.SetVolumeStats(context.Background(), filter, stats)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestCacheService_SetDashboardAggregates_RedisError(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	aggregates := &entities.DashboardAggregates{
		Volume: &entities.VolumeStats{
			TotalTransactions: 100,
		},
	}

	server.SetError("ERR write failed")
	defer server.SetError("")

	err := svc.SetDashboardAggregates(context.Background(), filter, aggregates)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed")
}

func TestCacheService_GetVolumeStats_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	key := svc.buildKey(
		context.Background(),
		filter.ContextID,
		volumeKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)
	require.NoError(t, client.Set(context.Background(), key, "invalid-json", 0).Err())

	result, err := svc.GetVolumeStats(context.Background(), filter)

	assert.Nil(t, result)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrCacheMiss)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestCacheService_NilConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisConn: nil}
	svc := NewCacheService(provider, time.Minute)

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("GetVolumeStats with nil connection", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetVolumeStats(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetVolumeStats with nil connection", func(t *testing.T) {
		t.Parallel()

		err := svc.SetVolumeStats(context.Background(), filter, &entities.VolumeStats{})
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("GetSLAStats with nil connection", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetSLAStats(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetSLAStats with nil connection", func(t *testing.T) {
		t.Parallel()

		err := svc.SetSLAStats(context.Background(), filter, &entities.SLAStats{})
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("GetMatchRateStats with nil connection", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetMatchRateStats(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetMatchRateStats with nil connection", func(t *testing.T) {
		t.Parallel()

		err := svc.SetMatchRateStats(context.Background(), filter, &entities.MatchRateStats{})
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("GetDashboardAggregates with nil connection", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetDashboardAggregates(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetDashboardAggregates with nil connection", func(t *testing.T) {
		t.Parallel()

		err := svc.SetDashboardAggregates(
			context.Background(),
			filter,
			&entities.DashboardAggregates{},
		)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("GetMatcherDashboardMetrics with nil connection", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)
		assert.Nil(t, result)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("SetMatcherDashboardMetrics with nil connection", func(t *testing.T) {
		t.Parallel()

		err := svc.SetMatcherDashboardMetrics(
			context.Background(),
			filter,
			&entities.MatcherDashboardMetrics{},
		)
		assert.Equal(t, ErrRedisConnRequired, err)
	})

	t.Run("InvalidateContext with nil connection", func(t *testing.T) {
		t.Parallel()

		err := svc.InvalidateContext(context.Background(), uuid.New())
		assert.Equal(t, ErrRedisConnRequired, err)
	})
}

func TestCacheService_GetClientError(t *testing.T) {
	t.Parallel()

	// NewRedisClientWithMock(nil) returns a *libRedis.Client that is non-nil
	// but has no underlying redis client. GetClient(ctx) will return an error
	// because connected=false and reconnection has no config to work with.
	conn := testutil.NewRedisClientWithMock(nil)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	svc := NewCacheService(provider, time.Minute)

	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	t.Run("GetVolumeStats returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetVolumeStats(context.Background(), filter)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
		assert.NotErrorIs(t, err, ErrCacheMiss)
	})

	t.Run("SetVolumeStats returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		err := svc.SetVolumeStats(context.Background(), filter, &entities.VolumeStats{})
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
	})

	t.Run("GetSLAStats returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetSLAStats(context.Background(), filter)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
		assert.NotErrorIs(t, err, ErrCacheMiss)
	})

	t.Run("SetSLAStats returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		err := svc.SetSLAStats(context.Background(), filter, &entities.SLAStats{})
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
	})

	t.Run("GetMatchRateStats returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetMatchRateStats(context.Background(), filter)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
		assert.NotErrorIs(t, err, ErrCacheMiss)
	})

	t.Run("SetMatchRateStats returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		err := svc.SetMatchRateStats(context.Background(), filter, &entities.MatchRateStats{})
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
	})

	t.Run("GetDashboardAggregates returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetDashboardAggregates(context.Background(), filter)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
		assert.NotErrorIs(t, err, ErrCacheMiss)
	})

	t.Run("SetDashboardAggregates returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		err := svc.SetDashboardAggregates(
			context.Background(),
			filter,
			&entities.DashboardAggregates{},
		)
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
	})

	t.Run("GetMatcherDashboardMetrics returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
		assert.NotErrorIs(t, err, ErrCacheMiss)
	})

	t.Run("SetMatcherDashboardMetrics returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		err := svc.SetMatcherDashboardMetrics(
			context.Background(),
			filter,
			&entities.MatcherDashboardMetrics{},
		)
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
	})

	t.Run("InvalidateContext returns error on GetClient failure", func(t *testing.T) {
		t.Parallel()

		err := svc.InvalidateContext(context.Background(), uuid.New())
		require.Error(t, err)
		assert.NotEqual(t, ErrRedisConnRequired, err)
	})
}

func TestCacheService_MatcherDashboardMetrics_WithSourceFilter(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)
	sourceID := uuid.New()
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		SourceID:  &sourceID,
	}
	metrics := &entities.MatcherDashboardMetrics{
		Summary: &entities.SummaryMetrics{
			TotalTransactions: 50,
			MatchRate:         75.0,
		},
	}

	err := svc.SetMatcherDashboardMetrics(context.Background(), filter, metrics)
	require.NoError(t, err)

	result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)
	require.NoError(t, err)
	require.NotNil(t, result.Summary)
	assert.Equal(t, metrics.Summary.TotalTransactions, result.Summary.TotalTransactions)
	assert.Equal(t, metrics.Summary.MatchRate, result.Summary.MatchRate)
}

func TestCacheService_InvalidateContext_WithMultipleBatches(t *testing.T) {
	t.Parallel()

	_, client, svc := setupRedisCacheService(t)
	contextID := uuid.New()

	for i := 0; i < 10; i++ {
		filter := entities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  time.Now().UTC().Add(-time.Duration(i+1) * 24 * time.Hour),
			DateTo:    time.Now().UTC().Add(-time.Duration(i) * 24 * time.Hour),
		}

		_ = svc.SetVolumeStats(
			context.Background(),
			filter,
			&entities.VolumeStats{TotalTransactions: i},
		)
		_ = svc.SetSLAStats(context.Background(), filter, &entities.SLAStats{TotalExceptions: i})
		_ = svc.SetDashboardAggregates(
			context.Background(),
			filter,
			&entities.DashboardAggregates{},
		)
		_ = svc.SetMatcherDashboardMetrics(
			context.Background(),
			filter,
			&entities.MatcherDashboardMetrics{
				Summary: &entities.SummaryMetrics{TotalTransactions: i},
			},
		)
	}

	// In single-tenant mode (background context), keys have no tenant prefix
	// since core.GetTenantIDFromContext returns empty.
	pattern := "matcher:dashboard:" + contextID.String() + ":*"
	keys, err := client.Keys(context.Background(), pattern).Result()
	require.NoError(t, err)
	assert.Greater(t, len(keys), 0)

	err = svc.InvalidateContext(context.Background(), contextID)
	require.NoError(t, err)

	keys, err = client.Keys(context.Background(), pattern).Result()
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestCacheService_InvalidateContext_DeleteError(t *testing.T) {
	t.Parallel()

	server, client, svc := setupRedisCacheService(t)
	contextID := uuid.New()

	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	_ = svc.SetVolumeStats(
		context.Background(),
		filter,
		&entities.VolumeStats{TotalTransactions: 100},
	)

	// In single-tenant mode (background context), keys have no tenant prefix
	// since core.GetTenantIDFromContext returns empty.
	pattern := "matcher:dashboard:" + contextID.String() + ":*"
	keys, err := client.Keys(context.Background(), pattern).Result()
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	server.SetError("READONLY You can't write against a read only replica")
	defer server.SetError("")

	err = svc.InvalidateContext(context.Background(), contextID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "READONLY")
}

func TestCacheService_MatcherDashboardMetrics_TTL(t *testing.T) {
	t.Parallel()

	server, _, svc := setupRedisCacheService(t)
	filter := entities.DashboardFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}
	metrics := &entities.MatcherDashboardMetrics{
		Summary: &entities.SummaryMetrics{
			TotalTransactions: 100,
		},
	}

	err := svc.SetMatcherDashboardMetrics(context.Background(), filter, metrics)
	require.NoError(t, err)

	result, err := svc.GetMatcherDashboardMetrics(context.Background(), filter)
	require.NoError(t, err)
	require.NotNil(t, result)

	server.FastForward(2 * time.Minute)

	result, err = svc.GetMatcherDashboardMetrics(context.Background(), filter)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrCacheMiss)
}

// =============================================================================
// Tenant Isolation Tests
// =============================================================================

func TestCacheService_BuildKey_WithTenantContext(t *testing.T) {
	t.Parallel()

	svc := &CacheService{}
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	// Create context WITH tenant ID using canonical lib-commons v3 context setter
	tenantID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := core.SetTenantIDInContext(context.Background(), tenantID)

	key := svc.buildKey(ctx, contextID, "summary", dateFrom, dateTo, nil)

	require.Contains(t, key, "tenant:"+tenantID+":")
	require.Contains(t, key, contextID.String())
}

func TestCacheService_BuildKey_WithoutTenantContext(t *testing.T) {
	t.Parallel()

	svc := &CacheService{}
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	ctx := context.Background()

	key := svc.buildKey(ctx, contextID, "summary", dateFrom, dateTo, nil)

	// core.GetTenantIDFromContext returns empty when no tenant is set,
	// so valkey.GetKeyFromContext returns the key without tenant prefix.
	require.Contains(t, key, contextID.String())
	require.Contains(t, key, "matcher:dashboard")
	require.NotContains(t, key, "tenant:")
}

func TestCacheService_BuildKey_DifferentTenants_ProduceDifferentKeys(t *testing.T) {
	t.Parallel()

	svc := &CacheService{}
	contextID := uuid.New()
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	ctx1 := core.SetTenantIDInContext(context.Background(), "tenant-aaa")
	ctx2 := core.SetTenantIDInContext(context.Background(), "tenant-bbb")

	key1 := svc.buildKey(ctx1, contextID, "summary", dateFrom, dateTo, nil)
	key2 := svc.buildKey(ctx2, contextID, "summary", dateFrom, dateTo, nil)

	require.NotEqual(t, key1, key2)
	require.Contains(t, key1, "tenant:tenant-aaa:")
	require.Contains(t, key2, "tenant:tenant-bbb:")
}

// TestCacheService_BuildKey_EmptyTenantID_NoPrefix verifies that buildKey returns
// an unprefixed key when the tenant ID is explicitly set to an empty string.
// This confirms single-tenant fallback behavior for empty JWT tenant claims.
func TestCacheService_BuildKey_EmptyTenantID_NoPrefix(t *testing.T) {
	t.Parallel()

	svc := &CacheService{}
	contextID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	dateFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	// Set tenant ID to empty string explicitly (simulates JWT with empty/missing tenant claim)
	ctx := core.SetTenantIDInContext(context.Background(), "")

	key := svc.buildKey(ctx, contextID, volumeKeyType, dateFrom, dateTo, nil)

	// Key should NOT have tenant prefix when tenant ID is empty
	expected := "matcher:dashboard:" + contextID.String() + ":volume:2024-01-01:2024-01-31:all"
	assert.Equal(t, expected, key)
	assert.NotContains(t, key, "tenant:")
}

// TestCacheService_TenantIsolation_SetAndGet verifies end-to-end tenant isolation:
// data cached by tenant-A is invisible to tenant-B, and vice versa.
// Both tenants use the same contextID and filter, proving key-level isolation.
func TestCacheService_TenantIsolation_SetAndGet(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour)
	dateTo := time.Now().UTC()

	tenantA := "550e8400-e29b-41d4-a716-446655440001"
	tenantB := "550e8400-e29b-41d4-a716-446655440002"

	ctxA := core.SetTenantIDInContext(context.Background(), tenantA)
	ctxB := core.SetTenantIDInContext(context.Background(), tenantB)

	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	// Cache volume stats for tenant-A
	statsA := &entities.VolumeStats{
		TotalTransactions:   100,
		MatchedTransactions: 80,
		TotalAmount:         decimal.NewFromInt(10000),
	}
	err := svc.SetVolumeStats(ctxA, filter, statsA)
	require.NoError(t, err)

	// Verify tenant-A can read its own cached data
	resultA, err := svc.GetVolumeStats(ctxA, filter)
	require.NoError(t, err)
	require.NotNil(t, resultA)
	assert.Equal(t, 100, resultA.TotalTransactions)

	// Verify tenant-B gets cache miss for the same filter (isolation)
	resultB, err := svc.GetVolumeStats(ctxB, filter)
	assert.Nil(t, resultB, "ISOLATION VIOLATION: tenant-B can see tenant-A's cached data")
	require.ErrorIs(t, err, ErrCacheMiss, "tenant-B should get cache miss, not tenant-A's data")

	// Cache different data for tenant-B
	statsB := &entities.VolumeStats{
		TotalTransactions:   200,
		MatchedTransactions: 150,
		TotalAmount:         decimal.NewFromInt(20000),
	}
	err = svc.SetVolumeStats(ctxB, filter, statsB)
	require.NoError(t, err)

	// Verify tenant-B reads its own data
	resultB, err = svc.GetVolumeStats(ctxB, filter)
	require.NoError(t, err)
	require.NotNil(t, resultB)
	assert.Equal(t, 200, resultB.TotalTransactions)

	// Verify tenant-A's data is still intact (not overwritten by tenant-B)
	resultA, err = svc.GetVolumeStats(ctxA, filter)
	require.NoError(t, err)
	require.NotNil(t, resultA)
	assert.Equal(t, 100, resultA.TotalTransactions,
		"tenant-A data should not be affected by tenant-B's cache write")
}

// TestCacheService_InvalidateContext_TenantIsolation verifies that invalidating
// cache for one tenant does not affect another tenant's cached data.
func TestCacheService_InvalidateContext_TenantIsolation(t *testing.T) {
	t.Parallel()

	_, _, svc := setupRedisCacheService(t)

	contextID := uuid.New()
	dateFrom := time.Now().UTC().Add(-24 * time.Hour)
	dateTo := time.Now().UTC()

	tenantA := "550e8400-e29b-41d4-a716-446655440001"
	tenantB := "550e8400-e29b-41d4-a716-446655440002"

	ctxA := core.SetTenantIDInContext(context.Background(), tenantA)
	ctxB := core.SetTenantIDInContext(context.Background(), tenantB)

	filter := entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
	}

	// Cache data for both tenants
	statsA := &entities.VolumeStats{TotalTransactions: 100, TotalAmount: decimal.NewFromInt(10000)}
	statsB := &entities.VolumeStats{TotalTransactions: 200, TotalAmount: decimal.NewFromInt(20000)}

	require.NoError(t, svc.SetVolumeStats(ctxA, filter, statsA))
	require.NoError(t, svc.SetVolumeStats(ctxB, filter, statsB))

	// Verify both tenants have cached data
	resultA, err := svc.GetVolumeStats(ctxA, filter)
	require.NoError(t, err)
	require.NotNil(t, resultA)

	resultB, err := svc.GetVolumeStats(ctxB, filter)
	require.NoError(t, err)
	require.NotNil(t, resultB)

	// Invalidate cache for tenant-A only
	err = svc.InvalidateContext(ctxA, contextID)
	require.NoError(t, err)

	// Tenant-A's cache should be invalidated
	resultA, err = svc.GetVolumeStats(ctxA, filter)
	assert.Nil(t, resultA, "tenant-A cache should be invalidated")
	require.ErrorIs(t, err, ErrCacheMiss, "tenant-A should get cache miss after invalidation")

	// Tenant-B's cache should still be intact (isolation)
	resultB, err = svc.GetVolumeStats(ctxB, filter)
	require.NoError(t, err, "tenant-B cache should survive tenant-A invalidation")
	require.NotNil(t, resultB, "ISOLATION VIOLATION: tenant-A invalidation affected tenant-B")
	assert.Equal(t, 200, resultB.TotalTransactions,
		"tenant-B data should be unchanged after tenant-A invalidation")
}
