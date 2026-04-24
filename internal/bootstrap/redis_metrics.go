// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
)

// ErrNilRedisClientWithoutError is returned when the redis client resolver
// returns nil without an error. Kept separate from ErrNilResolverWithoutError
// so log-scraping dashboards can distinguish which pool failed.
var ErrNilRedisClientWithoutError = errors.New("redis client returned nil without error")

// RedisMetricsCollector collects and reports Redis connection pool metrics
// to OpenTelemetry. It tracks total and idle connections plus cumulative
// hits / misses / stale closures and wait time.
//
// The collector reuses DefaultMetricsCollectionInterval and
// DB_METRICS_INTERVAL_SEC: Redis and Postgres pool pressure are inspected on
// the same operational cadence, and splitting them into a separate cadence
// would only add an env-var knob no operator asks for.
type RedisMetricsCollector struct {
	redis    *libRedis.Client
	meter    metric.Meter
	interval time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once

	// Gauges — current pool state.
	totalConns metric.Int64Gauge
	idleConns  metric.Int64Gauge
	activeConn metric.Int64Gauge

	// Counters — emitted as deltas so Prometheus-style rate() calculations
	// stay accurate across collector restarts and reconnects.
	hits         metric.Int64Counter
	misses       metric.Int64Counter
	stale        metric.Int64Counter
	waitDuration metric.Float64Counter

	last   redisCumulativeStats
	lastMu sync.Mutex
}

// redisCumulativeStats mirrors the cumulative fields from redis.PoolStats so
// collect() can emit deltas rather than absolute totals. Kept package-private
// because nothing outside this file needs to inspect it.
type redisCumulativeStats struct {
	hits           uint32
	misses         uint32
	stale          uint32
	waitDurationNs int64
}

// NewRedisMetricsCollector creates a new collector bound to the given Redis
// client. Returns (nil, nil) when client is nil or not connected — matching
// the shape of NewDBMetricsCollector so the bootstrap wiring can treat both
// collectors identically.
func NewRedisMetricsCollector(
	client *libRedis.Client,
	interval time.Duration,
) (*RedisMetricsCollector, error) {
	if client == nil {
		return nil, nil
	}

	connected, connErr := client.IsConnected()
	if connErr != nil || !connected {
		return nil, nil //nolint:nilerr // nil collector is acceptable when not connected
	}

	collector := &RedisMetricsCollector{
		redis:    client,
		meter:    otel.Meter("matcher.redis.pool"),
		interval: interval,
		stopCh:   make(chan struct{}),
	}

	if err := collector.initMetrics(); err != nil {
		return nil, err
	}

	return collector, nil
}

func (collector *RedisMetricsCollector) initMetrics() error {
	var err error

	collector.totalConns, err = collector.meter.Int64Gauge("redis.pool.total_connections",
		metric.WithDescription("Total number of connections in the Redis pool"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create redis total_connections gauge: %w", err)
	}

	collector.idleConns, err = collector.meter.Int64Gauge("redis.pool.idle_connections",
		metric.WithDescription("Number of idle connections in the Redis pool"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create redis idle_connections gauge: %w", err)
	}

	collector.activeConn, err = collector.meter.Int64Gauge("redis.pool.active_connections",
		metric.WithDescription("Number of Redis connections currently in use (total - idle)"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create redis active_connections gauge: %w", err)
	}

	collector.hits, err = collector.meter.Int64Counter("redis.pool.hits_total",
		metric.WithDescription("Total number of times a free connection was found in the pool"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create redis hits counter: %w", err)
	}

	collector.misses, err = collector.meter.Int64Counter("redis.pool.misses_total",
		metric.WithDescription("Total number of times a free connection was NOT found in the pool"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create redis misses counter: %w", err)
	}

	collector.stale, err = collector.meter.Int64Counter("redis.pool.stale_closed_total",
		metric.WithDescription("Total number of stale connections removed from the Redis pool"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create redis stale counter: %w", err)
	}

	collector.waitDuration, err = collector.meter.Float64Counter(
		"redis.pool.wait_duration_seconds_total",
		metric.WithDescription("Total time blocked waiting for a Redis connection"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("create redis wait_duration counter: %w", err)
	}

	return nil
}

// Start begins collecting metrics at the configured interval.
func (collector *RedisMetricsCollector) Start(ctx context.Context) {
	if collector == nil {
		return
	}

	runtime.SafeGoWithContextAndComponent(
		ctx,
		nil,
		"redis-metrics-collector",
		"collect",
		runtime.KeepRunning,
		func(ctx context.Context) {
			ticker := time.NewTicker(collector.interval)
			defer ticker.Stop()

			collector.collect(ctx)

			for {
				select {
				case <-ctx.Done():
					return
				case <-collector.stopCh:
					return
				case <-ticker.C:
					collector.collect(ctx)
				}
			}
		},
	)
}

// Stop stops the metrics collection. It is safe to call multiple times.
func (collector *RedisMetricsCollector) Stop() {
	if collector == nil {
		return
	}

	collector.stopOnce.Do(func() {
		close(collector.stopCh)
	})
}

// collect pulls current pool stats and records them.
//
// Gauges are emitted as-is; counters are emitted as deltas against the last
// observed cumulative value so dashboards can use OTel counter semantics
// correctly even when the collector is restarted.
func (collector *RedisMetricsCollector) collect(ctx context.Context) {
	if collector == nil || collector.redis == nil {
		return
	}

	client, err := collector.redis.GetClient(ctx)
	if err != nil || client == nil {
		// Soft failure: skip this cycle. The next tick will retry. Logging
		// here would spam during a known outage, and health-check metrics
		// already report the connection state.
		return
	}

	stats := poolStatsFromClient(client)
	if stats == nil {
		return
	}

	collector.recordGauges(ctx, stats)
	collector.recordCounters(ctx, stats)
}

// poolStatsFromClient safely calls PoolStats on a redis.UniversalClient,
// guarding against nil returns. go-redis clients may return nil when the
// underlying pool has not been initialized, e.g. when a failover is in flight.
func poolStatsFromClient(client redis.UniversalClient) *redis.PoolStats {
	if client == nil {
		return nil
	}

	return client.PoolStats()
}

func (collector *RedisMetricsCollector) recordGauges(ctx context.Context, stats *redis.PoolStats) {
	total := int64(stats.TotalConns)
	idle := int64(stats.IdleConns)

	collector.totalConns.Record(ctx, total)
	collector.idleConns.Record(ctx, idle)

	// active = total - idle. Clamp at zero because stats is sampled
	// non-atomically; transient skew shouldn't surface as negative gauges.
	active := total - idle
	if active < 0 {
		active = 0
	}

	collector.activeConn.Record(ctx, active)
}

func (collector *RedisMetricsCollector) recordCounters(ctx context.Context, stats *redis.PoolStats) {
	collector.lastMu.Lock()
	defer collector.lastMu.Unlock()

	if hitsDelta := stats.Hits - collector.last.hits; hitsDelta > 0 {
		collector.hits.Add(ctx, int64(hitsDelta))
	}

	if missesDelta := stats.Misses - collector.last.misses; missesDelta > 0 {
		collector.misses.Add(ctx, int64(missesDelta))
	}

	if staleDelta := stats.StaleConns - collector.last.stale; staleDelta > 0 {
		collector.stale.Add(ctx, int64(staleDelta))
	}

	if waitDelta := stats.WaitDurationNs - collector.last.waitDurationNs; waitDelta > 0 {
		collector.waitDuration.Add(ctx, time.Duration(waitDelta).Seconds())
	}

	collector.last = redisCumulativeStats{
		hits:           stats.Hits,
		misses:         stats.Misses,
		stale:          stats.StaleConns,
		waitDurationNs: stats.WaitDurationNs,
	}
}
