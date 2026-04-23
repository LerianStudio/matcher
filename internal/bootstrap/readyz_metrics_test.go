// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The readyz metrics contract (dev-readyz skill, "Metrics (NON-NEGOTIABLE)"):
//
//   1. readyz_check_duration_ms — Float64Histogram keyed by {dep, status}
//      with explicit millisecond buckets.
//   2. readyz_check_status      — Int64Counter keyed by {dep, status}.
//   3. selfprobe_result         — gauge (recorded as 1/0) keyed by {dep}.
//
// These tests wire a manual reader into the OTel global provider, trigger
// the emit helpers, collect, and inspect the resource metrics.

// setupManualReader registers a ManualReader against the global meter
// provider. Call once per test (not per-package): every test writes its
// own data points, and Collect observes everything accumulated since the
// last Collect call. We share the same provider across tests — it's
// safe because we assert on the specific labels each test emits.
//
// Restores the previous global MeterProvider + resets the readyz metrics
// sync.Once via t.Cleanup so neighbouring tests (and the production defaults)
// are not polluted.
func setupManualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	prevMP := otel.GetMeterProvider()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	// After provider is set, re-init so the package-local meter picks up the
	// new provider. initReadyzMetrics is idempotent and safe to call here.
	resetReadyzMetricsForTest()
	initReadyzMetrics()

	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		resetReadyzMetricsForTest()
	})

	return reader
}

//nolint:paralleltest // mutates global OTel meter provider; MUST run serially
func TestEmitCheckDuration_RecordsHistogramDataPoint(t *testing.T) {
	reader := setupManualReader(t)

	ctx := context.Background()
	emitCheckDuration(ctx, "postgres", "up", 2*time.Millisecond)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	found := findMetric(rm, "readyz_check_duration_ms")
	require.NotNil(t, found, "readyz_check_duration_ms must be registered and collected")

	hist, ok := found.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "readyz_check_duration_ms must be a Float64 histogram; got %T", found.Data)
	require.NotEmpty(t, hist.DataPoints)

	dp := findHistogramPoint(hist.DataPoints, "postgres", "up")
	require.NotNil(t, dp, "histogram must carry a datapoint for {dep=postgres, status=up}")
	assert.GreaterOrEqual(t, dp.Count, uint64(1))
}

//nolint:paralleltest // mutates global OTel meter provider; MUST run serially
func TestEmitCheckStatus_IncrementsCounter(t *testing.T) {
	reader := setupManualReader(t)

	ctx := context.Background()
	emitCheckStatus(ctx, "rabbitmq", "down")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	found := findMetric(rm, "readyz_check_status")
	require.NotNil(t, found, "readyz_check_status must be registered and collected")

	sum, ok := found.Data.(metricdata.Sum[int64])
	require.True(t, ok, "readyz_check_status must be an int64 counter; got %T", found.Data)

	dp := findSumPoint(sum.DataPoints, "rabbitmq", "down")
	require.NotNil(t, dp, "counter must carry a datapoint for {dep=rabbitmq, status=down}")
	assert.GreaterOrEqual(t, dp.Value, int64(1))
}

//nolint:paralleltest // mutates global OTel meter provider; MUST run serially
func TestEmitSelfProbeResult_Up_SetsOne(t *testing.T) {
	reader := setupManualReader(t)

	ctx := context.Background()
	emitSelfProbeResult(ctx, "redis", true)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	found := findMetric(rm, "selfprobe_result")
	require.NotNil(t, found, "selfprobe_result must be registered and collected")

	// Implemented as Int64Gauge (or UpDownCounter used like a gauge). Both
	// expose a DataPoints []int64 via appropriate metricdata types.
	value := extractSelfProbeValue(t, found, "redis")
	assert.Equal(t, int64(1), value, "selfprobe gauge must report 1 for up")
}

//nolint:paralleltest // mutates global OTel meter provider; MUST run serially
func TestEmitSelfProbeResult_Down_SetsZero(t *testing.T) {
	reader := setupManualReader(t)

	ctx := context.Background()
	emitSelfProbeResult(ctx, "postgres", false)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	found := findMetric(rm, "selfprobe_result")
	require.NotNil(t, found)

	value := extractSelfProbeValue(t, found, "postgres")
	assert.Equal(t, int64(0), value, "selfprobe gauge must report 0 for down")
}

// TestEmitHelpers_NoopWhenInstrumentsNil exercises the nil-guard early-return
// branches in emitCheckDuration / emitCheckStatus / emitSelfProbeResult. These
// paths matter in production: when OTel instrument creation fails (e.g.,
// exporter misconfiguration), the globals stay nil and emit helpers must no-op
// without panicking. We simulate the failure by setting globals to nil after
// init, then verify each helper returns cleanly.
//
//nolint:paralleltest // mutates global OTel meter provider; MUST run serially
func TestEmitHelpers_NoopWhenInstrumentsNil(t *testing.T) {
	// Force the metric globals into the "init failed" shape. We bypass the
	// Once by using resetReadyzMetricsForTest and then nilling the globals
	// via a no-op init pass followed by direct assignment. Simpler: reset,
	// then assign nil before any emit (emit calls init, which will pick a
	// real meter but we then null things out mid-test).
	resetReadyzMetricsForTest()
	// Make the Once fire without us needing a meter — then we nil the
	// instruments to simulate the failure path.
	initReadyzMetrics()

	readyzCheckDurMs = nil
	readyzCheckStatus = nil
	readyzSelfProbeRes = nil

	ctx := context.Background()

	// None of these should panic or error — the nil-guard must trigger.
	assert.NotPanics(t, func() { emitCheckDuration(ctx, "postgres", "up", time.Millisecond) })
	assert.NotPanics(t, func() { emitCheckStatus(ctx, "postgres", "up") })
	assert.NotPanics(t, func() { emitSelfProbeResult(ctx, "postgres", true) })

	// Leave the package in a clean state for other tests.
	resetReadyzMetricsForTest()
}

// --- helpers ---.

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for si := range rm.ScopeMetrics {
		for mi := range rm.ScopeMetrics[si].Metrics {
			m := &rm.ScopeMetrics[si].Metrics[mi]
			if m.Name == name {
				return m
			}
		}
	}

	return nil
}

func findHistogramPoint(points []metricdata.HistogramDataPoint[float64], dep, status string) *metricdata.HistogramDataPoint[float64] {
	for i := range points {
		p := &points[i]
		if attrsMatch(p.Attributes.ToSlice(), dep, status) {
			return p
		}
	}

	return nil
}

func findSumPoint(points []metricdata.DataPoint[int64], dep, status string) *metricdata.DataPoint[int64] {
	for i := range points {
		p := &points[i]
		if attrsMatch(p.Attributes.ToSlice(), dep, status) {
			return p
		}
	}

	return nil
}

func attrsMatch(kvs []attribute.KeyValue, dep, status string) bool {
	var haveDep, haveStatus bool

	for _, kv := range kvs {
		k := string(kv.Key)
		v := kv.Value.AsString()

		if k == "dep" && v == dep {
			haveDep = true
		}

		if k == "status" && v == status {
			haveStatus = true
		}
	}

	return haveDep && haveStatus
}

// extractSelfProbeValue reads the last-recorded selfprobe value for the given
// dep from a gauge-shaped metric. Supports both Int64Gauge (DataPoints is a
// Gauge[int64]) and Int64UpDownCounter (Sum[int64]).
func extractSelfProbeValue(t *testing.T, m *metricdata.Metrics, dep string) int64 {
	t.Helper()

	switch d := m.Data.(type) {
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			for _, kv := range p.Attributes.ToSlice() {
				if string(kv.Key) == "dep" && kv.Value.AsString() == dep {
					return p.Value
				}
			}
		}
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			for _, kv := range p.Attributes.ToSlice() {
				if string(kv.Key) == "dep" && kv.Value.AsString() == dep {
					return p.Value
				}
			}
		}
	default:
		t.Fatalf("selfprobe_result must be a gauge-shaped metric; got %T", m.Data)
	}

	t.Fatalf("selfprobe_result had no datapoint for dep=%s", dep)

	return 0
}
