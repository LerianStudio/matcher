// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package metrics declares the OpenTelemetry business metrics emitted by
// the matching bounded context. Instruments are lazily constructed on first
// use via sync.Once so every call-site (commands, queries, workers) resolves
// to the same underlying meter without threading a dependency through
// service constructors.
//
// Metric namespace: matcher.matching.*
//
// Labels:
//   - outcome (matcher.matching.runs_total): "confirmed" | "manual" |
//     "dry_run" | "failed"
//   - context_id: UUID of the reconciliation context. Reconciliation
//     contexts are curated per-tenant and count in the low dozens at most,
//     so cardinality is bounded.
package metrics

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	sharedMetrics "github.com/LerianStudio/matcher/internal/shared/observability/metrics"
)

const (
	// MeterScope is the OTel meter scope used for matching business metrics.
	MeterScope = "matcher.matching"

	// OutcomeConfirmed labels a successful commit-mode run that produced
	// confirmed matches (or zero matches with no error).
	OutcomeConfirmed = "confirmed"
	// OutcomeManual labels a successful manual match creation.
	OutcomeManual = "manual"
	// OutcomeDryRun labels a successful dry-run that did not commit matches.
	OutcomeDryRun = "dry_run"
	// OutcomeFailed labels any run that returned an error.
	OutcomeFailed = "failed"

	// AttrOutcome is the label key for run outcome on runs_total and
	// run_duration_ms.
	AttrOutcome = "outcome"
	// AttrContextID is the label key for the reconciliation context UUID.
	AttrContextID = "context_id"
)

// matchingMetrics holds the constructed instruments. Populated exactly once
// by the sync.Once guard in get().
type matchingMetrics struct {
	runs          metric.Int64Counter
	runDurationMs metric.Float64Histogram
	confidence    metric.Float64Histogram
}

var (
	instance *matchingMetrics
	once     sync.Once
)

func get() *matchingMetrics {
	once.Do(func() {
		meter := sharedMetrics.Meter(MeterScope)

		runs, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.matching.runs_total",
			"Total number of match runs by outcome",
		)

		runDuration, _ := sharedMetrics.Float64Histogram(
			meter,
			"matcher.matching.run_duration_ms",
			"End-to-end duration of a match run in milliseconds",
			"ms",
		)

		confidence, _ := sharedMetrics.Float64Histogram(
			meter,
			"matcher.matching.confidence",
			"Confidence score distribution of matched groups (0-100)",
			"{score}",
		)

		instance = &matchingMetrics{
			runs:          runs,
			runDurationMs: runDuration,
			confidence:    confidence,
		}
	})

	return instance
}

// RecordRun emits the runs_total counter and run_duration_ms histogram.
// Both metrics share the same (outcome, context_id) attribute set so
// dashboards can slice by outcome for either.
func RecordRun(ctx context.Context, outcome, contextID string, durationMs float64) {
	instruments := get()
	if instruments == nil {
		return
	}

	attrs := []attribute.KeyValue{
		sharedMetrics.Attr(AttrOutcome, outcome),
		sharedMetrics.Attr(AttrContextID, contextID),
	}

	instruments.runs.Add(ctx, 1, metric.WithAttributes(attrs...))
	instruments.runDurationMs.Record(ctx, durationMs, metric.WithAttributes(attrs...))
}

// RecordConfidence emits one sample of the confidence histogram per matched
// group. context_id is attached so operators can see which contexts are
// producing low-confidence matches.
func RecordConfidence(ctx context.Context, contextID string, score float64) {
	instruments := get()
	if instruments == nil {
		return
	}

	instruments.confidence.Record(ctx, score,
		metric.WithAttributes(sharedMetrics.Attr(AttrContextID, contextID)),
	)
}
