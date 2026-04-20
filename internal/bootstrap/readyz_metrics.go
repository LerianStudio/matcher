// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// Readyz metrics. The dev-readyz skill names these three exactly; the names
// are part of the public observability contract and must not be renamed
// without updating every downstream dashboard. Follows the matcher metric
// pattern established by db_metrics.go and initCleanupMetrics — direct
// otel.Meter() acquisition (lib-commons does not expose a meter accessor),
// sync.Once-guarded init, graceful partial init (individual instrument
// failures do not disable the others), and emit helpers that no-op when
// their instrument is nil.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// readyzBucketsMs is the explicit histogram bucket boundary set in
// milliseconds for readyz_check_duration_ms. Covers the p50..p99 range seen
// on infra health checks (1ms local Redis, 5-50ms DB ping, 100-500ms network
// hiccups) plus tail buckets that flag pathological timeouts (≥1s).
//
// The skill does not prescribe exact buckets. This set is chosen to resolve
// SRE questions like "what's the tail latency of our Postgres probe over the
// last hour" without burying interesting signal in one giant bucket.
var readyzBucketsMs = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000}

// Concurrency model for the readyz metric globals:
//
//   - initReadyzMetrics is guarded by sync.Once. The Go memory model guarantees
//     that after once.Do(fn) returns, every subsequent goroutine observes the
//     fully-initialised globals via happens-before on the Once's internal
//     synchronisation. No additional mutex is needed in the emit helpers.
//
//   - Production code only writes to these globals from inside once.Do. Reads
//     from emit* helpers always follow an initReadyzMetrics() call that itself
//     synchronises through the Once, so the reads see the final values.
//
//   - Tests that need to re-bind the globals against a freshly-registered
//     MeterProvider use a unit-tagged test helper (see the sibling
//     *_helpers_test.go file) so the reset path is not compiled into the
//     production binary. Tests touching this state run serially
//     (nolint:paralleltest).
var (
	readyzMetricsOnce  sync.Once
	readyzCheckDurMs   metric.Float64Histogram
	readyzCheckStatus  metric.Int64Counter
	readyzSelfProbeRes metric.Int64Gauge
)

// initReadyzMetrics initialises the three /readyz observability instruments
// against the global OTel meter provider. Idempotent via sync.Once; any
// individual instrument failure is logged via otel.Handle but does not
// abort the others (partial metrics beat no metrics).
func initReadyzMetrics() {
	readyzMetricsOnce.Do(func() {
		meter := otel.Meter("matcher.readyz")

		var err error

		readyzCheckDurMs, err = meter.Float64Histogram(
			"readyz_check_duration_ms",
			metric.WithDescription("Duration of /readyz per-dependency check, in milliseconds"),
			metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(readyzBucketsMs...),
		)
		if err != nil {
			otel.Handle(fmt.Errorf("create readyz_check_duration_ms histogram: %w", err))

			readyzCheckDurMs = nil
		}

		readyzCheckStatus, err = meter.Int64Counter(
			"readyz_check_status",
			metric.WithDescription("Count of /readyz per-dependency check outcomes by status"),
			metric.WithUnit("{check}"),
		)
		if err != nil {
			otel.Handle(fmt.Errorf("create readyz_check_status counter: %w", err))

			readyzCheckStatus = nil
		}

		readyzSelfProbeRes, err = meter.Int64Gauge(
			"selfprobe_result",
			metric.WithDescription("Startup self-probe result per dependency (1=up, 0=down)"),
		)
		if err != nil {
			otel.Handle(fmt.Errorf("create selfprobe_result gauge: %w", err))

			readyzSelfProbeRes = nil
		}
	})
}

// emitCheckDuration records a /readyz per-check latency data point.
func emitCheckDuration(ctx context.Context, dep, status string, duration time.Duration) {
	initReadyzMetrics()

	histogram := readyzCheckDurMs
	if histogram == nil {
		return
	}

	ms := float64(duration.Nanoseconds()) / float64(time.Millisecond)
	histogram.Record(ctx, ms, metric.WithAttributes(
		attribute.String("dep", dep),
		attribute.String("status", status),
	))
}

// emitCheckStatus increments the /readyz per-check outcome counter.
func emitCheckStatus(ctx context.Context, dep, status string) {
	initReadyzMetrics()

	c := readyzCheckStatus
	if c == nil {
		return
	}

	c.Add(ctx, 1, metric.WithAttributes(
		attribute.String("dep", dep),
		attribute.String("status", status),
	))
}

// emitSelfProbeResult records the startup self-probe result for a given dep.
// 1 means up; 0 means down. Modelled as Int64Gauge: the dev-readyz skill
// describes this as a gauge and we want "current value", not cumulative.
func emitSelfProbeResult(ctx context.Context, dep string, up bool) {
	initReadyzMetrics()

	gauge := readyzSelfProbeRes
	if gauge == nil {
		return
	}

	var v int64
	if up {
		v = 1
	}

	gauge.Record(ctx, v, metric.WithAttributes(attribute.String("dep", dep)))
}
