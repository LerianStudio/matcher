// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package metrics declares the OpenTelemetry business metrics emitted by
// the ingestion bounded context.
//
// Metric namespace: matcher.ingestion.*
//
// Labels:
//   - format (all metrics): ingestion format string (csv | json | xml |
//     iso20022 | ...). Low-cardinality — bounded by the supported parser
//     set.
//   - error_type (parsing_errors_total): short kind string such as
//     "parse", "validate", "dedup". Low-cardinality by construction.
package metrics

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	sharedMetrics "github.com/LerianStudio/matcher/internal/shared/observability/metrics"
)

const (
	// MeterScope is the OTel meter scope used for ingestion business metrics.
	MeterScope = "matcher.ingestion"

	// AttrFormat is the label key for ingestion format (csv, json, xml,
	// iso20022). Attached to every ingestion metric emission.
	AttrFormat = "format"
	// AttrErrorType is the label key for parse/validate/dedup error kind.
	// Used by parsing_errors_total only.
	AttrErrorType = "error_type"

	// Error-type values. Exported so call-sites pick from a closed set.

	// ErrorTypeParse is emitted when the parser fails on malformed input.
	ErrorTypeParse = "parse"
	// ErrorTypeValidate is emitted when normalization/validation rejects a row.
	ErrorTypeValidate = "validate"
	// ErrorTypePipeline is emitted for pipeline-level failures (dedup write,
	// persistence, outbox) that terminate the run short of completion.
	ErrorTypePipeline = "pipeline"
)

type ingestionMetrics struct {
	rowsProcessed metric.Int64Counter
	dedupHitRate  metric.Float64Histogram
	parsingErrors metric.Int64Counter
}

var (
	instance *ingestionMetrics
	once     sync.Once
)

func get() *ingestionMetrics {
	once.Do(func() {
		meter := sharedMetrics.Meter(MeterScope)

		rows, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.ingestion.rows_processed_total",
			"Total rows inserted into the ingestion pipeline, by format",
		)

		dedupRate, _ := sharedMetrics.Float64Histogram(
			meter,
			"matcher.ingestion.dedup_hit_rate",
			"Ratio of duplicate rows detected during ingestion (0.0-1.0)",
			"1",
		)

		parseErrs, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.ingestion.parsing_errors_total",
			"Total ingestion errors, by format and error type",
		)

		instance = &ingestionMetrics{
			rowsProcessed: rows,
			dedupHitRate:  dedupRate,
			parsingErrors: parseErrs,
		}
	})

	return instance
}

// RecordRowsProcessed emits the rows_processed_total counter. inserted is the
// count of rows that made it through dedup and were persisted.
func RecordRowsProcessed(ctx context.Context, format string, inserted int) {
	instruments := get()
	if instruments == nil || inserted <= 0 {
		return
	}

	instruments.rowsProcessed.Add(ctx, int64(inserted),
		metric.WithAttributes(sharedMetrics.Attr(AttrFormat, format)),
	)
}

// RecordDedupRate emits one sample of the dedup_hit_rate histogram. The rate
// is computed from the caller's (parsed, inserted) counts: when parsed is 0
// the metric is skipped entirely, so downstream rate() queries aren't
// polluted by zero-denominator cycles.
func RecordDedupRate(ctx context.Context, format string, parsed, inserted int) {
	instruments := get()
	if instruments == nil || parsed <= 0 {
		return
	}

	duplicates := parsed - inserted
	if duplicates < 0 {
		duplicates = 0
	}

	rate := float64(duplicates) / float64(parsed)

	instruments.dedupHitRate.Record(ctx, rate,
		metric.WithAttributes(sharedMetrics.Attr(AttrFormat, format)),
	)
}

// RecordParsingError emits the parsing_errors_total counter. count is the
// number of errors observed in the batch; call-sites may emit once with
// count=N or N times with count=1 depending on how granular they need the
// per-row attribution to be.
func RecordParsingError(ctx context.Context, format, errorType string, count int) {
	instruments := get()
	if instruments == nil || count <= 0 {
		return
	}

	attrs := []attribute.KeyValue{
		sharedMetrics.Attr(AttrFormat, format),
		sharedMetrics.Attr(AttrErrorType, errorType),
	}

	instruments.parsingErrors.Add(ctx, int64(count), metric.WithAttributes(attrs...))
}
