//go:build integration

package reporting

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	reportingRedis "github.com/LerianStudio/matcher/internal/reporting/adapters/redis"
	reportingEntities "github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/tests/integration"
)

// mustCacheRedisConn creates a raw redis.Client backed by the testcontainer Redis instance.
func mustCacheRedisConn(t *testing.T, redisAddr string) *redis.Client {
	t.Helper()

	parsed, err := url.Parse(strings.TrimSpace(redisAddr))
	require.NoError(t, err)
	require.NotEmpty(t, parsed.Host)

	client := redis.NewClient(&redis.Options{Addr: parsed.Host})

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("redis cleanup: %v (expected in test teardown)", err)
		}
	})

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, client.Ping(pingCtx).Err())

	return client
}

// newCacheService creates a DashboardCacheService backed by the testcontainer Redis instance
// with the given TTL. If ttl is 0, the default (5 minutes) is used.
func newCacheService(
	t *testing.T,
	harness *integration.TestHarness,
	ttl time.Duration,
) *reportingRedis.CacheService {
	t.Helper()

	rawClient := mustCacheRedisConn(t, harness.RedisAddr)
	libClient := infraTestutil.NewRedisClientWithMock(rawClient)
	provider := tenantAdapters.NewSingleTenantInfrastructureProvider(harness.Connection, libClient)

	return reportingRedis.NewCacheService(provider, ttl)
}

// sampleFilter returns a DashboardFilter with deterministic values for cache key generation.
func sampleFilter() reportingEntities.DashboardFilter {
	return reportingEntities.DashboardFilter{
		ContextID: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
		DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		SourceID:  nil,
	}
}

// sampleVolumeStats returns a VolumeStats with known values for comparison.
func sampleVolumeStats() *reportingEntities.VolumeStats {
	return &reportingEntities.VolumeStats{
		TotalTransactions:   100,
		MatchedTransactions: 80,
		UnmatchedCount:      20,
		TotalAmount:         decimal.NewFromFloat(50000.00),
		MatchedAmount:       decimal.NewFromFloat(40000.00),
		UnmatchedAmount:     decimal.NewFromFloat(10000.00),
		PeriodStart:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:           time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
	}
}

// TestDashboardCache_SetAndGet verifies that a cached entry can be stored and retrieved
// with matching values through the CacheService.
func TestDashboardCache_SetAndGet(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		cacheSvc := newCacheService(t, h, 0)
		ctx := testCtx(t, h)
		filter := sampleFilter()
		expected := sampleVolumeStats()

		// Set the volume stats in cache.
		err := cacheSvc.SetVolumeStats(ctx, filter, expected)
		require.NoError(t, err)

		// Get should return the same data.
		got, err := cacheSvc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Equal(t, expected.TotalTransactions, got.TotalTransactions)
		require.Equal(t, expected.MatchedTransactions, got.MatchedTransactions)
		require.Equal(t, expected.UnmatchedCount, got.UnmatchedCount)
		require.True(t, expected.TotalAmount.Equal(got.TotalAmount),
			"total amount mismatch: want %s got %s", expected.TotalAmount, got.TotalAmount)
		require.True(t, expected.MatchedAmount.Equal(got.MatchedAmount),
			"matched amount mismatch: want %s got %s", expected.MatchedAmount, got.MatchedAmount)
		require.True(t, expected.UnmatchedAmount.Equal(got.UnmatchedAmount),
			"unmatched amount mismatch: want %s got %s", expected.UnmatchedAmount, got.UnmatchedAmount)
	})
}

// TestDashboardCache_CacheMiss verifies that requesting a non-existent key
// returns an ErrCacheMiss sentinel error.
func TestDashboardCache_CacheMiss(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		cacheSvc := newCacheService(t, h, 0)
		ctx := testCtx(t, h)

		// Build a filter with a random context ID to guarantee no prior cache entries.
		filter := reportingEntities.DashboardFilter{
			ContextID: uuid.New(),
			DateFrom:  time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 6, 30, 23, 59, 59, 0, time.UTC),
		}

		got, err := cacheSvc.GetVolumeStats(ctx, filter)
		require.Error(t, err)
		require.Nil(t, got)
		require.ErrorIs(t, err, reportingRedis.ErrCacheMiss)

		// Also verify miss on other stat types.
		slaStats, err := cacheSvc.GetSLAStats(ctx, filter)
		require.Error(t, err)
		require.Nil(t, slaStats)
		require.ErrorIs(t, err, reportingRedis.ErrCacheMiss)

		matchRate, err := cacheSvc.GetMatchRateStats(ctx, filter)
		require.Error(t, err)
		require.Nil(t, matchRate)
		require.ErrorIs(t, err, reportingRedis.ErrCacheMiss)
	})
}

// TestDashboardCache_Invalidation verifies that after InvalidateContext, previously
// cached entries for that context are no longer retrievable.
func TestDashboardCache_Invalidation(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		cacheSvc := newCacheService(t, h, 0)
		ctx := testCtx(t, h)

		contextID := uuid.New()
		filter := reportingEntities.DashboardFilter{
			ContextID: contextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		}

		// Populate multiple stat types for the same context.
		volumeStats := sampleVolumeStats()
		err := cacheSvc.SetVolumeStats(ctx, filter, volumeStats)
		require.NoError(t, err)

		slaStats := &reportingEntities.SLAStats{
			TotalExceptions:   10,
			ResolvedOnTime:    7,
			SLAComplianceRate: 70.0,
		}
		err = cacheSvc.SetSLAStats(ctx, filter, slaStats)
		require.NoError(t, err)

		matchRateStats := &reportingEntities.MatchRateStats{
			MatchRate:    80.0,
			TotalCount:   100,
			MatchedCount: 80,
		}
		err = cacheSvc.SetMatchRateStats(ctx, filter, matchRateStats)
		require.NoError(t, err)

		// Sanity check: all entries are retrievable before invalidation.
		got, err := cacheSvc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, got)

		// Invalidate the entire context.
		err = cacheSvc.InvalidateContext(ctx, contextID)
		require.NoError(t, err)

		// All stat types for this context should now be cache misses.
		got, err = cacheSvc.GetVolumeStats(ctx, filter)
		require.Error(t, err)
		require.Nil(t, got)
		require.ErrorIs(t, err, reportingRedis.ErrCacheMiss)

		gotSLA, err := cacheSvc.GetSLAStats(ctx, filter)
		require.Error(t, err)
		require.Nil(t, gotSLA)
		require.ErrorIs(t, err, reportingRedis.ErrCacheMiss)

		gotMatchRate, err := cacheSvc.GetMatchRateStats(ctx, filter)
		require.Error(t, err)
		require.Nil(t, gotMatchRate)
		require.ErrorIs(t, err, reportingRedis.ErrCacheMiss)
	})
}

// TestDashboardCache_TTLExpiry verifies that cached entries expire after the configured TTL.
// Uses a very short TTL to validate Redis key expiration.
func TestDashboardCache_TTLExpiry(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper // test body, not helper
		shortTTL := 100 * time.Millisecond
		cacheSvc := newCacheService(t, h, shortTTL)
		ctx := testCtx(t, h)
		filter := sampleFilter()
		expected := sampleVolumeStats()

		// Set the entry with the short TTL.
		err := cacheSvc.SetVolumeStats(ctx, filter, expected)
		require.NoError(t, err)

		// Immediately retrievable.
		got, err := cacheSvc.GetVolumeStats(ctx, filter)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, expected.TotalTransactions, got.TotalTransactions)

		// Wait for the TTL to expire.
		time.Sleep(200 * time.Millisecond)

		// Now it should be a cache miss.
		got, err = cacheSvc.GetVolumeStats(ctx, filter)
		require.Error(t, err)
		require.Nil(t, got)
		require.ErrorIs(t, err, reportingRedis.ErrCacheMiss)
	})
}
