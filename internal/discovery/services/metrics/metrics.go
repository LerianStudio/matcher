// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package metrics declares the OpenTelemetry business metrics emitted by
// the discovery bounded context. Instruments are lazily constructed on
// first use via sync.Once so every call-site (bridge worker, extraction
// commands, connection queries) resolves to the same underlying meter
// without threading a dependency through service constructors.
//
// Metric namespace: matcher.discovery.*
//
// Labels:
//   - outcome (fetcher_cycles_total): "success" | "failure" | "skipped"
//     where "skipped" covers lock-contention cycles that did not run work.
//   - state (extraction_states_total): one of the ExtractionStatus values
//     {PENDING, SUBMITTED, EXTRACTING, COMPLETE, FAILED, CANCELLED}. These
//     mirror the upstream Fetcher lifecycle states on ExtractionRequest.
//     Bounded set.
//
// Cardinality discipline: connection_id, source_id, schema_name, and
// scan_id are NOT metric labels — they are unbounded per tenant and belong
// on span attributes only. The closed enums above are the only permitted
// labels.
package metrics

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	sharedMetrics "github.com/LerianStudio/matcher/internal/shared/observability/metrics"
)

const (
	// MeterScope is the OTel meter scope used for discovery business metrics.
	MeterScope = "matcher.discovery"

	// AttrOutcome is the label key for fetcher-cycle outcome on
	// fetcher_cycles_total.
	AttrOutcome = "outcome"
	// AttrState is the label key for extraction lifecycle state on
	// extraction_states_total.
	AttrState = "state"

	// OutcomeSuccess labels a fetcher cycle that acquired the lock and
	// completed without error (zero or more extractions processed).
	OutcomeSuccess = "success"
	// OutcomeFailure labels a fetcher cycle that acquired the lock but
	// returned with a propagated error (e.g. tenant listing failed).
	OutcomeFailure = "failure"
	// OutcomeSkipped labels a fetcher cycle that did not run work — either
	// the distributed lock was held by another replica, or the lock
	// acquisition itself errored. Distinguished from failure so operators
	// can tell "this replica isn't winning cycles" from "cycles are
	// erroring".
	OutcomeSkipped = "skipped"
)

type discoveryMetrics struct {
	fetcherCycles        metric.Int64Counter
	fetcherCycleDuration metric.Float64Histogram
	extractionStates     metric.Int64Counter
	schemaCacheHits      metric.Int64Counter
	schemaCacheMisses    metric.Int64Counter
}

var (
	instance *discoveryMetrics
	once     sync.Once
)

func get() *discoveryMetrics {
	once.Do(func() {
		meter := sharedMetrics.Meter(MeterScope)

		cycles, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.discovery.fetcher_cycles_total",
			"Total fetcher bridge cycles by outcome",
		)

		cycleDuration, _ := sharedMetrics.Float64Histogram(
			meter,
			"matcher.discovery.fetcher_cycle_duration_ms",
			"Duration of a fetcher bridge poll cycle in milliseconds",
			"ms",
		)

		states, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.discovery.extraction_states_total",
			"Total extraction state transitions by state",
		)

		cacheHits, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.discovery.schema_cache_hits_total",
			"Total schema-cache hits on the connection-schema read path",
		)

		cacheMisses, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.discovery.schema_cache_misses_total",
			"Total schema-cache misses on the connection-schema read path",
		)

		instance = &discoveryMetrics{
			fetcherCycles:        cycles,
			fetcherCycleDuration: cycleDuration,
			extractionStates:     states,
			schemaCacheHits:      cacheHits,
			schemaCacheMisses:    cacheMisses,
		}
	})

	return instance
}

// RecordFetcherCycle emits the fetcher_cycles_total counter and the
// fetcher_cycle_duration_ms histogram. Both metrics share the same outcome
// label so dashboards can slice lock-contention cycles (outcome=skipped)
// against successful or failed cycles. Duration is recorded for every
// outcome (including skipped) so "cycle cadence" remains visible even
// when a replica is losing the lock.
func RecordFetcherCycle(ctx context.Context, outcome string, durationMs float64) {
	instruments := get()
	if instruments == nil {
		return
	}

	attrs := []attribute.KeyValue{sharedMetrics.Attr(AttrOutcome, outcome)}

	instruments.fetcherCycles.Add(ctx, 1, metric.WithAttributes(attrs...))
	instruments.fetcherCycleDuration.Record(ctx, durationMs, metric.WithAttributes(attrs...))
}

// RecordExtractionState emits the extraction_states_total counter for a
// single state transition. Call-sites emit once per successful state
// change — not per poll cycle — so the counter tracks actual lifecycle
// movement. state is expected to be one of the ExtractionStatus constants
// (PENDING, SUBMITTED, EXTRACTING, COMPLETE, FAILED, CANCELLED).
func RecordExtractionState(ctx context.Context, state string) {
	instruments := get()
	if instruments == nil || state == "" {
		return
	}

	instruments.extractionStates.Add(ctx, 1,
		metric.WithAttributes(sharedMetrics.Attr(AttrState, state)),
	)
}

// RecordSchemaCacheHit increments the schema_cache_hits_total counter.
// Emitted when GetConnectionSchema returns cached schemas without touching
// the database.
func RecordSchemaCacheHit(ctx context.Context) {
	instruments := get()
	if instruments == nil {
		return
	}

	instruments.schemaCacheHits.Add(ctx, 1)
}

// RecordSchemaCacheMiss increments the schema_cache_misses_total counter.
// Emitted when GetConnectionSchema falls through to the database because
// the cache was empty or errored. Error-path misses and genuine cold
// misses share the same counter because both represent "the cache did
// not help this request" — distinguishing them here would add a label
// that no dashboard currently alerts on.
func RecordSchemaCacheMiss(ctx context.Context) {
	instruments := get()
	if instruments == nil {
		return
	}

	instruments.schemaCacheMisses.Add(ctx, 1)
}
