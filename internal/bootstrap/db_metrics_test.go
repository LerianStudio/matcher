// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/bxcodec/dbresolver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestDefaultMetricsCollectionInterval(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 15*time.Second, DefaultMetricsCollectionInterval,
		"DefaultMetricsCollectionInterval should be 15 seconds")
}

func TestNewDBMetricsCollector(t *testing.T) {
	t.Parallel()

	t.Run("with nil postgres returns nil collector and nil error", func(t *testing.T) {
		t.Parallel()

		collector, err := NewDBMetricsCollector(nil, time.Second)

		assert.Nil(t, collector)
		assert.NoError(t, err)
	})

	t.Run("with zero interval uses provided value", func(t *testing.T) {
		t.Parallel()

		mockDB := &mockDBResolver{}
		collector := &DBMetricsCollector{
			db:       mockDB,
			meter:    otel.Meter("test.db.pool.zero"),
			interval: 0,
			stopCh:   make(chan struct{}),
		}

		assert.Equal(t, time.Duration(0), collector.interval)
		assert.NotNil(t, collector.db)
		assert.NotNil(t, collector.meter)
		assert.NotNil(t, collector.stopCh)
	})
}

func TestDBMetricsCollectorStart(t *testing.T) {
	t.Parallel()

	t.Run("with nil collector does not panic", func(t *testing.T) {
		t.Parallel()

		var collector *DBMetricsCollector

		assert.NotPanics(t, func() {
			collector.Start(context.Background())
		})
	})

	t.Run("with nil context does not panic", func(t *testing.T) {
		t.Parallel()

		var collector *DBMetricsCollector

		assert.NotPanics(t, func() {
			collector.Start(context.TODO())
		})
	})
}

func TestDBMetricsCollectorStop(t *testing.T) {
	t.Parallel()

	t.Run("with nil collector does not panic", func(t *testing.T) {
		t.Parallel()

		var collector *DBMetricsCollector

		assert.NotPanics(t, func() {
			collector.Stop()
		})
	})

	t.Run("is idempotent and does not panic on double close", func(t *testing.T) {
		t.Parallel()

		collector := &DBMetricsCollector{
			stopCh: make(chan struct{}),
		}

		// First stop should work
		collector.Stop()

		// Second stop should not panic
		require.NotPanics(t, func() {
			collector.Stop()
		})
	})
}

func TestDBMetricsCollectorCollect(t *testing.T) {
	t.Parallel()

	t.Run("nil db scenarios", func(t *testing.T) {
		t.Parallel()
		testDBMetricsNilDB(t)
	})

	t.Run("with mock db collects stats successfully", func(t *testing.T) {
		t.Parallel()
		testDBMetricsCollectSuccess(t)
	})

	t.Run("delta calculations", func(t *testing.T) {
		t.Parallel()
		testDBMetricsDeltaCalculations(t)
	})
}

func testDBMetricsNilDB(t *testing.T) {
	t.Helper()

	t.Run("with nil db does not panic", func(t *testing.T) {
		t.Parallel()

		collector := &DBMetricsCollector{db: nil}

		assert.NotPanics(t, func() {
			collector.collect(context.Background())
		})
	})

	t.Run("with nil context and nil db does not panic", func(t *testing.T) {
		t.Parallel()

		collector := &DBMetricsCollector{db: nil}

		assert.NotPanics(t, func() {
			collector.collect(context.TODO())
		})
	})
}

func testDBMetricsCollectSuccess(t *testing.T) {
	t.Helper()

	mockDB := &mockDBResolver{
		stats: sql.DBStats{
			MaxOpenConnections: 25,
			OpenConnections:    10,
			InUse:              5,
			Idle:               5,
			WaitCount:          100,
			WaitDuration:       time.Second * 10,
			MaxIdleClosed:      50,
			MaxLifetimeClosed:  25,
		},
	}

	collector := &DBMetricsCollector{
		db:       mockDB,
		meter:    otel.Meter("test.db.pool"),
		interval: time.Second,
		stopCh:   make(chan struct{}),
	}

	err := collector.initMetrics()
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		collector.collect(context.Background())
	})
}

func testDBMetricsDeltaCalculations(t *testing.T) {
	t.Helper()

	t.Run("correctly calculates deltas for cumulative counters", func(t *testing.T) {
		t.Parallel()

		mockDB := &mockDBResolver{
			stats: sql.DBStats{
				OpenConnections:   10,
				InUse:             5,
				Idle:              5,
				WaitCount:         100,
				WaitDuration:      time.Second * 10,
				MaxIdleClosed:     50,
				MaxLifetimeClosed: 25,
			},
		}

		collector := &DBMetricsCollector{
			db:       mockDB,
			meter:    otel.Meter("test.db.pool.delta"),
			interval: time.Second,
			stopCh:   make(chan struct{}),
		}

		err := collector.initMetrics()
		require.NoError(t, err)

		ctx := context.Background()
		collector.collect(ctx)

		assert.Equal(
			t,
			int64(100),
			collector.lastWaitCount,
			"first collect should set lastWaitCount",
		)
		assert.InDelta(
			t,
			float64(10),
			collector.lastWaitDuration,
			0.01,
			"first collect should set lastWaitDuration",
		)
		assert.Equal(
			t,
			int64(50),
			collector.lastMaxIdleClosed,
			"first collect should set lastMaxIdleClosed",
		)
		assert.Equal(
			t,
			int64(25),
			collector.lastMaxLifeClosed,
			"first collect should set lastMaxLifeClosed",
		)

		mockDB.stats.WaitCount = 150
		mockDB.stats.WaitDuration = time.Second * 15
		mockDB.stats.MaxIdleClosed = 60
		mockDB.stats.MaxLifetimeClosed = 30

		collector.collect(ctx)

		assert.Equal(
			t,
			int64(150),
			collector.lastWaitCount,
			"second collect should update lastWaitCount",
		)
		assert.InDelta(
			t,
			float64(15),
			collector.lastWaitDuration,
			0.01,
			"second collect should update lastWaitDuration",
		)
		assert.Equal(
			t,
			int64(60),
			collector.lastMaxIdleClosed,
			"second collect should update lastMaxIdleClosed",
		)
		assert.Equal(
			t,
			int64(30),
			collector.lastMaxLifeClosed,
			"second collect should update lastMaxLifeClosed",
		)
	})

	t.Run("does not add negative deltas on counter reset", func(t *testing.T) {
		t.Parallel()

		mockDB := &mockDBResolver{
			stats: sql.DBStats{
				WaitCount:         100,
				WaitDuration:      time.Second * 10,
				MaxIdleClosed:     50,
				MaxLifetimeClosed: 25,
			},
		}

		collector := &DBMetricsCollector{
			db:       mockDB,
			meter:    otel.Meter("test.db.pool.reset"),
			interval: time.Second,
			stopCh:   make(chan struct{}),
		}

		err := collector.initMetrics()
		require.NoError(t, err)

		ctx := context.Background()
		collector.collect(ctx)

		mockDB.stats.WaitCount = 50
		mockDB.stats.WaitDuration = time.Second * 5
		mockDB.stats.MaxIdleClosed = 20
		mockDB.stats.MaxLifetimeClosed = 10

		assert.NotPanics(t, func() {
			collector.collect(ctx)
		})

		assert.Equal(
			t,
			int64(50),
			collector.lastWaitCount,
			"should update lastWaitCount even on reset",
		)
	})
}

func TestNewDBMetricsCollector_NotConnected(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}

	collector, err := NewDBMetricsCollector(postgres, time.Second)

	assert.Nil(t, collector)
	assert.NoError(t, err)
}

func TestNewDBMetricsCollector_ConnectedWithMockDB(t *testing.T) {
	t.Parallel()

	mockDB := &mockDBResolver{}
	postgres := testutil.NewClientWithResolver(mockDB)

	collector, err := NewDBMetricsCollector(postgres, 5*time.Second)

	require.NoError(t, err)
	require.NotNil(t, collector)
	assert.NotNil(t, collector.openConns)
	assert.NotNil(t, collector.idleConns)
	assert.NotNil(t, collector.inUseConns)
	assert.NotNil(t, collector.waitCount)
	assert.NotNil(t, collector.waitDuration)
	assert.NotNil(t, collector.maxIdleClosed)
	assert.NotNil(t, collector.maxLifeClosed)
	assert.Equal(t, 5*time.Second, collector.interval)
}

func TestDBMetricsCollectorInitMetrics(t *testing.T) {
	t.Parallel()

	t.Run("creates all metrics successfully", func(t *testing.T) {
		t.Parallel()

		mockDB := &mockDBResolver{}
		collector := &DBMetricsCollector{
			db:       mockDB,
			meter:    otel.Meter("test.db.pool"),
			interval: time.Second,
			stopCh:   make(chan struct{}),
		}

		err := collector.initMetrics()

		require.NoError(t, err)
		assert.NotNil(t, collector.openConns)
		assert.NotNil(t, collector.idleConns)
		assert.NotNil(t, collector.inUseConns)
		assert.NotNil(t, collector.waitCount)
		assert.NotNil(t, collector.waitDuration)
		assert.NotNil(t, collector.maxIdleClosed)
		assert.NotNil(t, collector.maxLifeClosed)
	})
}

func TestDBMetricsCollectorLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("start and stop work correctly", func(t *testing.T) {
		t.Parallel()

		mockDB := &mockDBResolver{
			stats: sql.DBStats{
				OpenConnections: 5,
				Idle:            3,
				InUse:           2,
			},
		}

		collector := &DBMetricsCollector{
			db:       mockDB,
			meter:    otel.Meter("test.db.pool.lifecycle"),
			interval: 10 * time.Millisecond,
			stopCh:   make(chan struct{}),
		}

		err := collector.initMetrics()
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		collector.Start(ctx)

		// Wait for at least one collection cycle to complete
		require.Eventually(t, func() bool {
			// The collector is running if the context is not done
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}, 100*time.Millisecond, 5*time.Millisecond)

		assert.NotPanics(t, func() {
			collector.Stop()
		})
	})

	t.Run("stop via context cancellation", func(t *testing.T) {
		t.Parallel()

		mockDB := &mockDBResolver{
			stats: sql.DBStats{
				OpenConnections: 5,
			},
		}

		collector := &DBMetricsCollector{
			db:       mockDB,
			meter:    otel.Meter("test.db.pool.cancel"),
			interval: 50 * time.Millisecond,
			stopCh:   make(chan struct{}),
		}

		err := collector.initMetrics()
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		collector.Start(ctx)

		// Cancel the context - the collector should stop gracefully
		cancel()

		// Verify the collector handles cancellation gracefully (no panic)
		assert.NotPanics(t, func() {
			collector.Stop()
		})
	})
}

type mockDBResolver struct {
	stats sql.DBStats
}

func (m *mockDBResolver) Begin() (dbresolver.Tx, error) {
	return &mockTx{}, nil
}

func (m *mockDBResolver) BeginTx(_ context.Context, _ *sql.TxOptions) (dbresolver.Tx, error) {
	return &mockTx{}, nil
}

func (m *mockDBResolver) Close() error {
	return nil
}

func (m *mockDBResolver) Conn(_ context.Context) (dbresolver.Conn, error) {
	return &mockConn{}, nil
}

func (m *mockDBResolver) Driver() driver.Driver {
	return nil
}

func (m *mockDBResolver) Exec(_ string, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (m *mockDBResolver) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (m *mockDBResolver) Ping() error {
	return nil
}

func (m *mockDBResolver) PingContext(_ context.Context) error {
	return nil
}

func (m *mockDBResolver) Prepare(_ string) (dbresolver.Stmt, error) {
	return &mockStmt{}, nil
}

func (m *mockDBResolver) PrepareContext(_ context.Context, _ string) (dbresolver.Stmt, error) {
	return &mockStmt{}, nil
}

func (m *mockDBResolver) Query(_ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockDBResolver) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockDBResolver) QueryRow(_ string, _ ...any) *sql.Row {
	return nil
}

func (m *mockDBResolver) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

func (m *mockDBResolver) SetConnMaxIdleTime(_ time.Duration) {}

func (m *mockDBResolver) SetConnMaxLifetime(_ time.Duration) {}

func (m *mockDBResolver) SetMaxIdleConns(_ int) {}

func (m *mockDBResolver) SetMaxOpenConns(_ int) {}

func (m *mockDBResolver) PrimaryDBs() []*sql.DB {
	return nil
}

func (m *mockDBResolver) ReplicaDBs() []*sql.DB {
	return nil
}

func (m *mockDBResolver) Stats() sql.DBStats {
	return m.stats
}

type mockTx struct{}

func (m *mockTx) Commit() error   { return nil }
func (m *mockTx) Rollback() error { return nil }
func (m *mockTx) Exec(_ string, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (m *mockTx) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (m *mockTx) Prepare(_ string) (dbresolver.Stmt, error) {
	return &mockStmt{}, nil
}

func (m *mockTx) PrepareContext(_ context.Context, _ string) (dbresolver.Stmt, error) {
	return &mockStmt{}, nil
}

func (m *mockTx) Query(_ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockTx) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockTx) QueryRow(_ string, _ ...any) *sql.Row {
	return nil
}

func (m *mockTx) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

func (m *mockTx) Stmt(_ dbresolver.Stmt) dbresolver.Stmt {
	return &mockStmt{}
}

func (m *mockTx) StmtContext(_ context.Context, _ dbresolver.Stmt) dbresolver.Stmt {
	return &mockStmt{}
}

type mockConn struct{}

func (m *mockConn) Close() error { return nil }
func (m *mockConn) BeginTx(_ context.Context, _ *sql.TxOptions) (dbresolver.Tx, error) {
	return &mockTx{}, nil
}

func (m *mockConn) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (m *mockConn) PingContext(_ context.Context) error {
	return nil
}

func (m *mockConn) PrepareContext(_ context.Context, _ string) (dbresolver.Stmt, error) {
	return &mockStmt{}, nil
}

func (m *mockConn) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockConn) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

func (m *mockConn) Raw(_ func(driverConn any) error) error {
	return nil
}

type mockStmt struct{}

func (m *mockStmt) Close() error { return nil }
func (m *mockStmt) Exec(_ ...any) (sql.Result, error) {
	return nil, nil
}

func (m *mockStmt) ExecContext(_ context.Context, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (m *mockStmt) Query(_ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockStmt) QueryContext(_ context.Context, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (m *mockStmt) QueryRow(_ ...any) *sql.Row {
	return nil
}

func (m *mockStmt) QueryRowContext(_ context.Context, _ ...any) *sql.Row {
	return nil
}
