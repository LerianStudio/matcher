// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/bxcodec/dbresolver/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
)

// Pool role labels emitted on every db.pool.* metric so dashboards can split
// primary-vs-replica saturation without sampling separate metric names.
const (
	dbPoolRolePrimary = "primary"
	dbPoolRoleReplica = "replica"
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
//
// Each metric is emitted with a role attribute (primary / replica) so a
// single connection-pool-saturation dashboard can split by role without
// needing separate metric names.
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

	// lastStatsByRole holds per-role cumulative counter state so delta
	// calculation doesn't mix primary and replica counters into a single
	// accumulator. Keyed by dbPoolRole* constants.
	lastStatsByRole map[string]*cumulativeStats
	lastStatsMu     sync.Mutex
	lastResolverID  uintptr
}

// cumulativeStats tracks the last observed value of each cumulative counter
// so collect() can emit deltas rather than totals.
type cumulativeStats struct {
	waitCount     int64
	waitDuration  float64
	maxIdleClosed int64
	maxLifeClosed int64
}

// newRoleStatsMap returns a ready-to-use per-role stats map pre-populated
// with zero entries for both primary and replica so callers can Lookup
// without nil-checking. Exposed within the package so tests that construct
// DBMetricsCollector by struct literal can initialize it the same way
// the production constructor does.
func newRoleStatsMap() map[string]*cumulativeStats {
	return map[string]*cumulativeStats{
		dbPoolRolePrimary: {},
		dbPoolRoleReplica: {},
	}
}

// NewDBMetricsCollector creates a new collector for the given PostgreSQL connection.
func NewDBMetricsCollector(
	postgres *libPostgres.Client,
	interval time.Duration,
) (*DBMetricsCollector, error) {
	if postgres == nil {
		return nil, nil
	}

	if interval <= 0 {
		interval = DefaultMetricsCollectionInterval
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
		db:              db,
		meter:           meter,
		interval:        interval,
		stopCh:          make(chan struct{}),
		lastStatsByRole: newRoleStatsMap(),
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
//
// Primary and replica pools are emitted independently, each tagged with a
// role attribute so saturation dashboards can surface which side of the
// replication topology is running hot.
func (collector *DBMetricsCollector) collect(ctx context.Context) {
	if collector.currentDB != nil {
		db, err := collector.currentDB(ctx)
		if err == nil && db != nil {
			collector.lastStatsMu.Lock()

			currentID := resolverIdentity(db)
			if collector.lastResolverID != 0 && collector.lastResolverID != currentID {
				// Resolver swapped (reconnect, failover). Reset both
				// role accumulators so the next cycle emits full values
				// rather than a negative delta against the old pool.
				for _, stats := range collector.lastStatsByRole {
					*stats = cumulativeStats{}
				}
			}

			collector.lastResolverID = currentID
			collector.db = db
			collector.lastStatsMu.Unlock()
		}
	}

	if collector.db == nil {
		return
	}

	// Primary: the dbresolver Stats() contract returns primary[0].Stats() —
	// faithfully reporting the primary's pool keeps prior dashboards whole
	// and lets mock-based tests (which only implement Stats) remain valid.
	collector.recordRoleStats(ctx, dbPoolRolePrimary, collector.db.Stats())

	// Replica: walk the individual replica *sql.DB handles and sum them
	// so a topology with multiple read replicas surfaces aggregate pool
	// pressure under a single role="replica" series.
	collector.recordRoleStats(ctx, dbPoolRoleReplica, aggregateStats(collector.db.ReplicaDBs()))
}

// aggregateStats folds sql.DBStats from every non-nil *sql.DB in dbs into a
// single cumulative snapshot. Returns a zero value when dbs is empty so
// recordRoleStats can emit nothing for a role that has no backing DBs.
func aggregateStats(dbs []*sql.DB) sql.DBStats {
	var aggregated sql.DBStats

	for _, db := range dbs {
		if db == nil {
			continue
		}

		stats := db.Stats()
		aggregated.OpenConnections += stats.OpenConnections
		aggregated.Idle += stats.Idle
		aggregated.InUse += stats.InUse
		aggregated.WaitCount += stats.WaitCount
		aggregated.WaitDuration += stats.WaitDuration
		aggregated.MaxIdleClosed += stats.MaxIdleClosed
		aggregated.MaxLifetimeClosed += stats.MaxLifetimeClosed
	}

	return aggregated
}

// recordRoleStats emits one round of metrics for the given role. Gauges are
// recorded directly; cumulative counters emit deltas against the per-role
// accumulator so primary and replica counter histories do not mix.
//
// A zero-valued stats snapshot is still recorded so dashboards can
// distinguish "no replicas configured, zero activity" from "replica failed
// to report" (the latter would stop emitting entirely).
func (collector *DBMetricsCollector) recordRoleStats(ctx context.Context, role string, aggregated sql.DBStats) {
	roleAttr := metric.WithAttributes(attribute.String("role", role))

	// Gauges: record current values directly.
	collector.openConns.Record(ctx, int64(aggregated.OpenConnections), roleAttr)
	collector.idleConns.Record(ctx, int64(aggregated.Idle), roleAttr)
	collector.inUseConns.Record(ctx, int64(aggregated.InUse), roleAttr)

	// Counters: calculate deltas from per-role cumulative values.
	collector.lastStatsMu.Lock()
	defer collector.lastStatsMu.Unlock()

	if collector.lastStatsByRole == nil {
		collector.lastStatsByRole = newRoleStatsMap()
	}

	last, ok := collector.lastStatsByRole[role]
	if !ok {
		last = &cumulativeStats{}
		collector.lastStatsByRole[role] = last
	}

	if waitCountDelta := aggregated.WaitCount - last.waitCount; waitCountDelta > 0 {
		collector.waitCount.Add(ctx, waitCountDelta, roleAttr)
	}

	last.waitCount = aggregated.WaitCount

	waitDurationSecs := aggregated.WaitDuration.Seconds()
	if waitDurationDelta := waitDurationSecs - last.waitDuration; waitDurationDelta > 0 {
		collector.waitDuration.Add(ctx, waitDurationDelta, roleAttr)
	}

	last.waitDuration = waitDurationSecs

	if maxIdleClosedDelta := aggregated.MaxIdleClosed - last.maxIdleClosed; maxIdleClosedDelta > 0 {
		collector.maxIdleClosed.Add(ctx, maxIdleClosedDelta, roleAttr)
	}

	last.maxIdleClosed = aggregated.MaxIdleClosed

	if maxLifeClosedDelta := aggregated.MaxLifetimeClosed - last.maxLifeClosed; maxLifeClosedDelta > 0 {
		collector.maxLifeClosed.Add(ctx, maxLifeClosedDelta, roleAttr)
	}

	last.maxLifeClosed = aggregated.MaxLifetimeClosed
}

func resolverIdentity(db dbresolver.DB) uintptr {
	value := reflect.ValueOf(db)
	if value.Kind() == reflect.Pointer {
		return value.Pointer()
	}

	return 0
}
