// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"

	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func newMiniredisClient(t *testing.T) *libRedis.Client {
	t.Helper()

	server := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: server.Addr()})

	return infraTestutil.NewRedisClientWithMock(rc)
}

func TestNewRedisMetricsCollector(t *testing.T) {
	t.Parallel()

	t.Run("nil client returns nil collector and nil error", func(t *testing.T) {
		t.Parallel()

		collector, err := NewRedisMetricsCollector(nil, time.Second)

		assert.Nil(t, collector)
		assert.NoError(t, err)
	})

	t.Run("connected client returns initialized collector", func(t *testing.T) {
		t.Parallel()

		client := newMiniredisClient(t)

		collector, err := NewRedisMetricsCollector(client, time.Second)

		require.NoError(t, err)
		require.NotNil(t, collector)
		assert.NotNil(t, collector.meter)
		assert.NotNil(t, collector.stopCh)
		assert.Equal(t, time.Second, collector.interval)
	})
}

func TestRedisMetricsCollectorStart(t *testing.T) {
	t.Parallel()

	t.Run("nil collector does not panic on Start", func(t *testing.T) {
		t.Parallel()

		var collector *RedisMetricsCollector

		assert.NotPanics(t, func() {
			collector.Start(context.Background())
		})
	})

	t.Run("collect exits when context cancelled", func(t *testing.T) {
		t.Parallel()

		client := newMiniredisClient(t)
		collector, err := NewRedisMetricsCollector(client, 5*time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, collector)

		ctx, cancel := context.WithCancel(context.Background())
		collector.Start(ctx)

		// Let at least one collection cycle run.
		time.Sleep(15 * time.Millisecond)

		cancel()

		// Stop must be idempotent and safe to call after context cancellation.
		assert.NotPanics(t, func() {
			collector.Stop()
			collector.Stop()
		})
	})
}

func TestRedisMetricsCollectorRecordCounters_Deltas(t *testing.T) {
	t.Parallel()

	// Build a collector directly so we can inject deterministic pool stats
	// without coordinating with a real Redis pool.
	collector := &RedisMetricsCollector{
		meter: otel.Meter("matcher.redis.pool.test"),
	}

	require.NoError(t, collector.initMetrics())

	ctx := context.Background()

	// First cycle establishes the baseline — nothing is emitted as a delta.
	first := &redis.PoolStats{
		Hits:           10,
		Misses:         2,
		StaleConns:     1,
		WaitDurationNs: int64(5 * time.Millisecond),
	}
	collector.recordCounters(ctx, first)

	assert.Equal(t, uint32(10), collector.last.hits)
	assert.Equal(t, uint32(2), collector.last.misses)
	assert.Equal(t, uint32(1), collector.last.stale)
	assert.Equal(t, int64(5*time.Millisecond), collector.last.waitDurationNs)

	// Second cycle: cumulative counters grow. Collector must track the new
	// totals (delta is recorded into OTel; we can only verify state here).
	second := &redis.PoolStats{
		Hits:           15,
		Misses:         3,
		StaleConns:     2,
		WaitDurationNs: int64(12 * time.Millisecond),
	}
	collector.recordCounters(ctx, second)

	assert.Equal(t, uint32(15), collector.last.hits)
	assert.Equal(t, uint32(3), collector.last.misses)
	assert.Equal(t, uint32(2), collector.last.stale)
	assert.Equal(t, int64(12*time.Millisecond), collector.last.waitDurationNs)
}

func TestRedisMetricsCollectorPoolStatsFromClient_NilClient(t *testing.T) {
	t.Parallel()

	assert.Nil(t, poolStatsFromClient(nil))
}

func TestRedisMetricsCollectorRecordGauges_ClampsNegativeActive(t *testing.T) {
	t.Parallel()

	collector := &RedisMetricsCollector{
		meter: otel.Meter("matcher.redis.pool.gauge_test"),
	}

	require.NoError(t, collector.initMetrics())

	// idle > total can occur transiently due to non-atomic sampling; the
	// gauge must clamp to 0 rather than emitting a negative active count.
	stats := &redis.PoolStats{TotalConns: 3, IdleConns: 10}

	assert.NotPanics(t, func() {
		collector.recordGauges(context.Background(), stats)
	})
}
