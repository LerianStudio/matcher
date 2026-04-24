// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package metrics declares the OpenTelemetry business metrics emitted by
// the reporting bounded context. Limited to export-job lifecycle today —
// worker-cycle metrics (poll cadence, worker saturation) are a separate
// concern handled by later refactor tasks.
//
// Metric namespace: matcher.reporting.*
//
// Labels:
//   - format (all metrics): CSV | JSON | XML | PDF (upper-case, matches
//     ExportFormat values on the entity). Bounded set.
//   - status (export_jobs_total): one of the ExportJobStatus values
//     {QUEUED, RUNNING, SUCCEEDED, FAILED, EXPIRED, CANCELED}. Bounded set.
package metrics

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	sharedMetrics "github.com/LerianStudio/matcher/internal/shared/observability/metrics"
)

const (
	// MeterScope is the OTel meter scope used for reporting business metrics.
	MeterScope = "matcher.reporting"

	// AttrFormat labels the export format (CSV, JSON, XML, PDF).
	AttrFormat = "format"
	// AttrStatus labels the export job terminal/transition status. Applied
	// only to export_jobs_total — duration_ms does not carry status because
	// it is emitted only on terminal success.
	AttrStatus = "status"
)

type reportingMetrics struct {
	exportJobs     metric.Int64Counter
	exportDuration metric.Float64Histogram
}

var (
	instance *reportingMetrics
	once     sync.Once
)

func get() *reportingMetrics {
	once.Do(func() {
		meter := sharedMetrics.Meter(MeterScope)

		jobs, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.reporting.export_jobs_total",
			"Total export jobs by format and status (lifecycle transitions)",
		)

		duration, _ := sharedMetrics.Float64Histogram(
			meter,
			"matcher.reporting.export_duration_ms",
			"Duration of completed export jobs (queued → completed) in milliseconds",
			"ms",
		)

		instance = &reportingMetrics{
			exportJobs:     jobs,
			exportDuration: duration,
		}
	})

	return instance
}

// RecordExportJobTransition emits the export_jobs_total counter with the
// given format and status. One emission per lifecycle transition.
func RecordExportJobTransition(ctx context.Context, format, status string) {
	instruments := get()
	if instruments == nil {
		return
	}

	attrs := []attribute.KeyValue{
		sharedMetrics.Attr(AttrFormat, format),
		sharedMetrics.Attr(AttrStatus, status),
	}

	instruments.exportJobs.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordExportDuration emits the export_duration_ms histogram. Call-sites
// compute the delta from queued → completed (or queued → failed) themselves
// so this helper stays allocation-free.
func RecordExportDuration(ctx context.Context, format string, durationMs float64) {
	instruments := get()
	if instruments == nil || durationMs < 0 {
		return
	}

	instruments.exportDuration.Record(ctx, durationMs,
		metric.WithAttributes(sharedMetrics.Attr(AttrFormat, format)),
	)
}
