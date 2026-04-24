// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package metrics declares the OpenTelemetry business metrics emitted by
// the configuration bounded context. Instruments are lazily constructed
// on first use via sync.Once so every call-site (scheduler worker)
// resolves to the same underlying meter without threading a dependency
// through service constructors.
//
// Metric namespace: matcher.configuration.*
//
// Labels:
//   - outcome (scheduler_firings_total): "fired" | "lock_contention" |
//     "error". "fired" means the scheduler acquired the lock and ran the
//     inner function; "lock_contention" means another replica held the
//     lock (redsync.ErrFailed / redsync.ErrTaken); "error" covers any
//     other infrastructure fault surfaced by WithLockOptions.
//
// Scope boundary (rule evaluation): rule_evaluations_total and
// rule_evaluation_duration_ms intentionally live in the matching
// bounded context's metrics package (matcher.matching.rule_*) rather
// than here. Configuration OWNS match rules (CRUD) but MATCHING
// EXECUTES them; the cross-context depguard blocks matching from
// importing configuration packages. Emitting rule-evaluation metrics
// from configuration would require either a scope violation or a
// contrived shim. Keeping the metric next to the code that measures
// it preserves that boundary.
//
// Cardinality discipline: rule_id, schedule_id, and context_id are NOT
// metric labels — they are unbounded per tenant and belong on span
// attributes only. The closed enums above are the only permitted
// labels.
package metrics

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/metric"

	sharedMetrics "github.com/LerianStudio/matcher/internal/shared/observability/metrics"
)

const (
	// MeterScope is the OTel meter scope used for configuration business metrics.
	MeterScope = "matcher.configuration"

	// AttrOutcome is the label key for outcome on scheduler_firings_total.
	AttrOutcome = "outcome"

	// OutcomeSchedulerFired labels a scheduler cycle that acquired the
	// distributed lock and executed the schedule's match trigger.
	OutcomeSchedulerFired = "fired"
	// OutcomeSchedulerLockContention labels a scheduler cycle where the
	// distributed lock was held by another replica. Distinguished from
	// OutcomeSchedulerError so operators can tell "this replica isn't
	// winning cycles" from "cycles are erroring".
	OutcomeSchedulerLockContention = "lock_contention"
	// OutcomeSchedulerError labels a scheduler cycle that failed on any
	// infrastructure fault surfaced by the lock manager or scheduler
	// repo.
	OutcomeSchedulerError = "error"
)

type configurationMetrics struct {
	schedulerFirings metric.Int64Counter
}

var (
	instance *configurationMetrics
	once     sync.Once
)

func get() *configurationMetrics {
	once.Do(func() {
		meter := sharedMetrics.Meter(MeterScope)

		firings, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.configuration.scheduler_firings_total",
			"Total scheduler cycle outcomes (fired vs lock_contention vs error)",
		)

		instance = &configurationMetrics{
			schedulerFirings: firings,
		}
	})

	return instance
}

// RecordSchedulerFiring emits the scheduler_firings_total counter with
// the given outcome. One emission per processed schedule per cycle.
func RecordSchedulerFiring(ctx context.Context, outcome string) {
	instruments := get()
	if instruments == nil {
		return
	}

	instruments.schedulerFirings.Add(ctx, 1,
		metric.WithAttributes(sharedMetrics.Attr(AttrOutcome, outcome)),
	)
}
