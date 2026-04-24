// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

// Direct OTel imports are required for infrastructure-level meter/tracer setup.
// otel.Meter() creates the named instrument for cleanup metrics. lib-commons
// does not abstract global provider accessors.
import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// cleanupMetrics holds initialized metrics for cleanup operations.
// Lazily initialized on first cleanup call.
var (
	cleanupMetrics     *cleanupMetricsCollector
	cleanupMetricsOnce sync.Once
)

// cleanupMetricsCollector tracks cleanup operation metrics.
type cleanupMetricsCollector struct {
	cleanupTotal    metric.Int64Counter
	cleanupDuration metric.Float64Histogram
}

// initCleanupMetrics initializes cleanup metrics (idempotent via sync.Once).
// Attempts to create both metrics independently; if one fails, the other may still succeed.
// Partial metrics are collected when possible rather than failing completely.
func initCleanupMetrics() *cleanupMetricsCollector {
	cleanupMetricsOnce.Do(func() {
		meter := otel.Meter("matcher.bootstrap.cleanup")

		var total metric.Int64Counter

		var duration metric.Float64Histogram

		var totalErr, durationErr error

		total, totalErr = meter.Int64Counter("bootstrap.cleanup.total",
			metric.WithDescription("Total cleanup operations by resource and status"),
			metric.WithUnit("{operation}"))
		if totalErr != nil {
			otel.Handle(fmt.Errorf("failed to create cleanup.total counter: %w", totalErr))
		}

		duration, durationErr = meter.Float64Histogram("bootstrap.cleanup.duration_seconds",
			metric.WithDescription("Duration of cleanup operations"),
			metric.WithUnit("s"))
		if durationErr != nil {
			otel.Handle(fmt.Errorf("failed to create cleanup.duration_seconds histogram: %w", durationErr))
		}

		// Construct collector with whatever metrics succeeded (nil values are handled by recordCleanup)
		cleanupMetrics = &cleanupMetricsCollector{
			cleanupTotal:    total,
			cleanupDuration: duration,
		}
	})

	return cleanupMetrics
}

// recordCleanup records a cleanup operation metric.
// Falls back to background context if the provided context is nil or cancelled,
// which is common during shutdown scenarios where metrics must still be recorded.
// Handles nil metric fields gracefully when partial metrics collection is in use.
//
//nolint:contextcheck // Intentional fallback to background context during shutdown
func recordCleanup(ctx context.Context, resource string, success bool, duration time.Duration) {
	metrics := initCleanupMetrics()
	if metrics == nil {
		return
	}

	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}

	status := statusSuccess
	if !success {
		status = statusError
	}

	attrs := []attribute.KeyValue{
		attribute.String("resource", resource),
		attribute.String("status", status),
	}

	if metrics.cleanupTotal != nil {
		metrics.cleanupTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	}

	if metrics.cleanupDuration != nil {
		metrics.cleanupDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	}
}
