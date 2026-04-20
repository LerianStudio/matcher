// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/bxcodec/dbresolver/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
)

// ErrNilResolverWithoutError is returned when Resolver returns nil without an error.
var ErrNilResolverWithoutError = errors.New("database resolver returned nil without error")

// DefaultMetricsCollectionInterval is the default interval for collecting
// database connection pool metrics when no custom interval is specified.
// This value balances monitoring granularity with minimal overhead.
// The interval can be configured via DB_METRICS_INTERVAL_SEC environment variable.
const DefaultMetricsCollectionInterval = 15 * time.Second

// DBMetricsCollector collects and reports database connection pool metrics
// to OpenTelemetry. It tracks open, idle, and in-use connections, as well as
// wait counts, wait durations, and connection closures due to pool limits.
type DBMetricsCollector struct {
	db        dbresolver.DB
	currentDB func(context.Context) (dbresolver.DB, error)
	meter     metric.Meter
	interval  time.Duration
	stopCh    chan struct{}
	stopOnce  sync.Once

	// Gauges for connection pool stats
	openConns     metric.Int64Gauge
	idleConns     metric.Int64Gauge
	inUseConns    metric.Int64Gauge
	waitCount     metric.Int64Counter
	waitDuration  metric.Float64Counter
	maxIdleClosed metric.Int64Counter
	maxLifeClosed metric.Int64Counter

	// Previous values for delta calculation (cumulative counters)
	lastWaitCount     int64
	lastWaitDuration  float64
	lastMaxIdleClosed int64
	lastMaxLifeClosed int64
	lastStatsMu       sync.Mutex
	lastResolverID    uintptr
}

// NewDBMetricsCollector creates a new collector for the given PostgreSQL connection.
func NewDBMetricsCollector(
	postgres *libPostgres.Client,
	interval time.Duration,
) (*DBMetricsCollector, error) {
	if postgres == nil {
		return nil, nil
	}

	connected, connErr := postgres.IsConnected()
	if connErr != nil || !connected {
		return nil, nil //nolint:nilerr // nil collector is acceptable when not connected
	}

	ctx := context.Background()

	db, err := postgres.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("get database connection: %w", err)
	}

	if db == nil {
		return nil, fmt.Errorf("get database connection: %w", ErrNilResolverWithoutError)
	}

	meter := otel.Meter("matcher.db.pool")

	collector := &DBMetricsCollector{
		db:       db,
		meter:    meter,
		interval: interval,
		stopCh:   make(chan struct{}),
	}

	if err := collector.initMetrics(); err != nil {
		return nil, err
	}

	return collector, nil
}

// SetResolverGetter configures a resolver getter used to refresh the active
// database handle before each collection cycle.
func (collector *DBMetricsCollector) SetResolverGetter(getter func(context.Context) (dbresolver.DB, error)) {
	if collector == nil {
		return
	}

	collector.currentDB = getter
}

func (collector *DBMetricsCollector) initMetrics() error {
	var err error

	collector.openConns, err = collector.meter.Int64Gauge("db.pool.open_connections",
		metric.WithDescription("Number of open connections to the database"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create open_connections gauge: %w", err)
	}

	collector.idleConns, err = collector.meter.Int64Gauge("db.pool.idle_connections",
		metric.WithDescription("Number of idle connections in the pool"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create idle_connections gauge: %w", err)
	}

	collector.inUseConns, err = collector.meter.Int64Gauge("db.pool.in_use_connections",
		metric.WithDescription("Number of connections currently in use"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create in_use_connections gauge: %w", err)
	}

	collector.waitCount, err = collector.meter.Int64Counter("db.pool.wait_count_total",
		metric.WithDescription("Total number of connections waited for"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create wait_count counter: %w", err)
	}

	collector.waitDuration, err = collector.meter.Float64Counter(
		"db.pool.wait_duration_seconds_total",
		metric.WithDescription("Total time blocked waiting for a new connection"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("create wait_duration counter: %w", err)
	}

	collector.maxIdleClosed, err = collector.meter.Int64Counter("db.pool.max_idle_closed_total",
		metric.WithDescription("Total connections closed due to SetMaxIdleConns"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create max_idle_closed counter: %w", err)
	}

	collector.maxLifeClosed, err = collector.meter.Int64Counter("db.pool.max_lifetime_closed_total",
		metric.WithDescription("Total connections closed due to SetConnMaxLifetime"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("create max_lifetime_closed counter: %w", err)
	}

	return nil
}

// Start begins collecting metrics at the configured interval.
func (collector *DBMetricsCollector) Start(ctx context.Context) {
	if collector == nil {
		return
	}

	runtime.SafeGoWithContextAndComponent(
		ctx,
		nil,
		"db-metrics-collector",
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
func (collector *DBMetricsCollector) Stop() {
	if collector == nil {
		return
	}

	collector.stopOnce.Do(func() {
		close(collector.stopCh)
	})
}

// collect gathers current stats and records them.
// For cumulative counters (WaitCount, WaitDuration, MaxIdleClosed, MaxLifetimeClosed),
// we calculate deltas to avoid double-counting since sql.DBStats returns totals.
func (collector *DBMetricsCollector) collect(ctx context.Context) {
	if collector.currentDB != nil {
		db, err := collector.currentDB(ctx)
		if err == nil && db != nil {
			collector.lastStatsMu.Lock()

			currentID := resolverIdentity(db)
			if collector.lastResolverID != 0 && collector.lastResolverID != currentID {
				collector.lastWaitCount = 0
				collector.lastWaitDuration = 0
				collector.lastMaxIdleClosed = 0
				collector.lastMaxLifeClosed = 0
			}

			collector.lastResolverID = currentID
			collector.db = db
			collector.lastStatsMu.Unlock()
		}
	}

	if collector.db == nil {
		return
	}

	stats := collector.db.Stats()

	// Gauges: record current values directly
	collector.openConns.Record(ctx, int64(stats.OpenConnections))
	collector.idleConns.Record(ctx, int64(stats.Idle))
	collector.inUseConns.Record(ctx, int64(stats.InUse))

	// Counters: calculate deltas from cumulative values
	collector.lastStatsMu.Lock()
	defer collector.lastStatsMu.Unlock()

	waitCountDelta := stats.WaitCount - collector.lastWaitCount
	if waitCountDelta > 0 {
		collector.waitCount.Add(ctx, waitCountDelta)
	}

	collector.lastWaitCount = stats.WaitCount

	waitDurationSecs := stats.WaitDuration.Seconds()
	waitDurationDelta := waitDurationSecs - collector.lastWaitDuration

	if waitDurationDelta > 0 {
		collector.waitDuration.Add(ctx, waitDurationDelta)
	}

	collector.lastWaitDuration = waitDurationSecs

	maxIdleClosedDelta := stats.MaxIdleClosed - collector.lastMaxIdleClosed
	if maxIdleClosedDelta > 0 {
		collector.maxIdleClosed.Add(ctx, maxIdleClosedDelta)
	}

	collector.lastMaxIdleClosed = stats.MaxIdleClosed

	maxLifeClosedDelta := stats.MaxLifetimeClosed - collector.lastMaxLifeClosed
	if maxLifeClosedDelta > 0 {
		collector.maxLifeClosed.Add(ctx, maxLifeClosedDelta)
	}

	collector.lastMaxLifeClosed = stats.MaxLifetimeClosed
}

func resolverIdentity(db dbresolver.DB) uintptr {
	value := reflect.ValueOf(db)
	if value.Kind() == reflect.Pointer {
		return value.Pointer()
	}

	return 0
}
