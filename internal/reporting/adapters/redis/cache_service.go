// Package redis provides Redis-based implementations for reporting services.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time check that CacheService implements DashboardCacheService.
var _ ports.DashboardCacheService = (*CacheService)(nil)

var (
	// ErrRedisConnRequired indicates redis connection is required.
	ErrRedisConnRequired = errors.New("redis connection is required")
	// ErrCacheMiss indicates the key was not found in cache.
	ErrCacheMiss = errors.New("cache miss")
)

const (
	dashboardCachePrefix = "matcher:dashboard"
	volumeKeyType        = "volume"
	slaKeyType           = "sla"
	matchRateKeyType     = "matchrate"
	aggregatesKeyType    = "aggregates"
	metricsKeyType       = "metrics"
	defaultCacheTTL      = 5 * time.Minute
	metricsCacheTTL      = 1 * time.Minute
)

// CacheService provides caching for dashboard data.
type CacheService struct {
	provider   sharedPorts.InfrastructureProvider
	defaultTTL time.Duration
}

// NewCacheService creates a new cache service.
func NewCacheService(provider sharedPorts.InfrastructureProvider, ttl time.Duration) *CacheService {
	if ttl == 0 {
		ttl = defaultCacheTTL
	}

	return &CacheService{
		provider:   provider,
		defaultTTL: ttl,
	}
}

func (svc *CacheService) buildKey(
	contextID uuid.UUID,
	keyType string,
	dateFrom, dateTo time.Time,
	sourceID *uuid.UUID,
) string {
	sourceKey := "all"
	if sourceID != nil {
		sourceKey = sourceID.String()
	}

	return fmt.Sprintf("%s:%s:%s:%s:%s:%s",
		dashboardCachePrefix,
		contextID.String(),
		keyType,
		dateFrom.Format(time.DateOnly),
		dateTo.Format(time.DateOnly),
		sourceKey,
	)
}

// GetVolumeStats retrieves cached volume stats.
func (svc *CacheService) GetVolumeStats(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.VolumeStats, error) {
	if svc == nil || svc.provider == nil {
		return nil, ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return nil, ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("get redis client for volume stats get: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		volumeKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, ErrCacheMiss
		}

		return nil, fmt.Errorf("failed to get cached data: %w", err)
	}

	var stats entities.VolumeStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	return &stats, nil
}

// SetVolumeStats caches volume stats.
func (svc *CacheService) SetVolumeStats(
	ctx context.Context,
	filter entities.DashboardFilter,
	stats *entities.VolumeStats,
) error {
	if svc == nil || svc.provider == nil {
		return ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for volume stats set: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		volumeKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("failed to marshal stats: %w", err)
	}

	if err := rdb.Set(ctx, key, data, svc.defaultTTL).Err(); err != nil {
		return fmt.Errorf("failed to set cache: %w", err)
	}

	return nil
}

// GetSLAStats retrieves cached SLA stats.
func (svc *CacheService) GetSLAStats(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.SLAStats, error) {
	if svc == nil || svc.provider == nil {
		return nil, ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return nil, ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("get redis client for sla stats get: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		slaKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, ErrCacheMiss
		}

		return nil, fmt.Errorf("failed to get cached data: %w", err)
	}

	var stats entities.SLAStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	return &stats, nil
}

// SetSLAStats caches SLA stats.
func (svc *CacheService) SetSLAStats(
	ctx context.Context,
	filter entities.DashboardFilter,
	stats *entities.SLAStats,
) error {
	if svc == nil || svc.provider == nil {
		return ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for sla stats set: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		slaKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("failed to marshal stats: %w", err)
	}

	if err := rdb.Set(ctx, key, data, svc.defaultTTL).Err(); err != nil {
		return fmt.Errorf("failed to set cache: %w", err)
	}

	return nil
}

// GetMatchRateStats retrieves cached match rate stats.
func (svc *CacheService) GetMatchRateStats(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.MatchRateStats, error) {
	if svc == nil || svc.provider == nil {
		return nil, ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return nil, ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("get redis client for match rate stats get: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		matchRateKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, ErrCacheMiss
		}

		return nil, fmt.Errorf("failed to get cached data: %w", err)
	}

	var stats entities.MatchRateStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	return &stats, nil
}

// SetMatchRateStats caches match rate stats.
func (svc *CacheService) SetMatchRateStats(
	ctx context.Context,
	filter entities.DashboardFilter,
	stats *entities.MatchRateStats,
) error {
	if svc == nil || svc.provider == nil {
		return ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for match rate stats set: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		matchRateKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("failed to marshal stats: %w", err)
	}

	if err := rdb.Set(ctx, key, data, svc.defaultTTL).Err(); err != nil {
		return fmt.Errorf("failed to set cache: %w", err)
	}

	return nil
}

// GetDashboardAggregates retrieves cached dashboard aggregates.
func (svc *CacheService) GetDashboardAggregates(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.DashboardAggregates, error) {
	if svc == nil || svc.provider == nil {
		return nil, ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return nil, ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("get redis client for dashboard aggregates get: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		aggregatesKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, ErrCacheMiss
		}

		return nil, fmt.Errorf("failed to get cached data: %w", err)
	}

	var aggregates entities.DashboardAggregates
	if err := json.Unmarshal(data, &aggregates); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	return &aggregates, nil
}

// SetDashboardAggregates caches dashboard aggregates.
func (svc *CacheService) SetDashboardAggregates(
	ctx context.Context,
	filter entities.DashboardFilter,
	aggregates *entities.DashboardAggregates,
) error {
	if svc == nil || svc.provider == nil {
		return ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for dashboard aggregates set: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		aggregatesKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := json.Marshal(aggregates)
	if err != nil {
		return fmt.Errorf("failed to marshal aggregates: %w", err)
	}

	if err := rdb.Set(ctx, key, data, svc.defaultTTL).Err(); err != nil {
		return fmt.Errorf("failed to set cache: %w", err)
	}

	return nil
}

// GetMatcherDashboardMetrics retrieves cached matcher dashboard metrics.
func (svc *CacheService) GetMatcherDashboardMetrics(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.MatcherDashboardMetrics, error) {
	if svc == nil || svc.provider == nil {
		return nil, ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return nil, ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("get redis client for dashboard metrics get: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		metricsKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, ErrCacheMiss
		}

		return nil, fmt.Errorf("failed to get cached data: %w", err)
	}

	var metrics entities.MatcherDashboardMetrics
	if err := json.Unmarshal(data, &metrics); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	return &metrics, nil
}

// SetMatcherDashboardMetrics caches matcher dashboard metrics with shorter TTL for real-time data.
func (svc *CacheService) SetMatcherDashboardMetrics(
	ctx context.Context,
	filter entities.DashboardFilter,
	metrics *entities.MatcherDashboardMetrics,
) error {
	if svc == nil || svc.provider == nil {
		return ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for dashboard metrics set: %w", err)
	}

	key := svc.buildKey(
		filter.ContextID,
		metricsKeyType,
		filter.DateFrom,
		filter.DateTo,
		filter.SourceID,
	)

	data, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}

	if err := rdb.Set(ctx, key, data, metricsCacheTTL).Err(); err != nil {
		return fmt.Errorf("failed to set cache: %w", err)
	}

	return nil
}

// InvalidateContext invalidates all cached data for a context.
func (svc *CacheService) InvalidateContext(ctx context.Context, contextID uuid.UUID) error {
	if svc == nil || svc.provider == nil {
		return ErrRedisConnRequired
	}

	conn, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}

	if conn == nil {
		return ErrRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for cache invalidation: %w", err)
	}

	pattern := fmt.Sprintf("%s:%s:*", dashboardCachePrefix, contextID.String())

	const batchSize = 500

	var batch []string

	iter := rdb.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		batch = append(batch, iter.Val())

		if len(batch) >= batchSize {
			if err := rdb.Del(ctx, batch...).Err(); err != nil {
				return fmt.Errorf("failed to delete keys batch: %w", err)
			}

			batch = batch[:0]
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan keys: %w", err)
	}

	// Delete any remaining keys
	if len(batch) > 0 {
		if err := rdb.Del(ctx, batch...).Err(); err != nil {
			return fmt.Errorf("failed to delete keys: %w", err)
		}
	}

	return nil
}
