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
//   - rule_type (matcher.matching.rule_evaluations_total): one of the
//     shared RuleType values {EXACT, TOLERANCE, DATE_LAG}. Bounded set.
//   - outcome (matcher.matching.rule_evaluations_total): "matched" |
//     "unmatched" | "error". Emitted once per rule evaluated in a
//     rules-engine invocation.
//
// rule_evaluation_duration_ms measures the wall-clock of a single
// rules-engine invocation and carries NO rule_type label, because the
// engine does not surface per-rule timing. Dashboards needing per-rule
// cost should derive it from (duration_ms / evaluations_total) over the
// same time window.
//
// The rule_* metrics live here — not under matcher.configuration.* —
// because rule execution is matching's responsibility; configuration
// only owns rule CRUD. See
// internal/configuration/services/metrics/metrics.go for the boundary
// rationale.
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

	// OutcomeRuleMatched labels a rule evaluation that produced one or
	// more match proposals.
	OutcomeRuleMatched = "matched"
	// OutcomeRuleUnmatched labels a rule evaluation that ran without
	// error but produced zero proposals.
	OutcomeRuleUnmatched = "unmatched"
	// OutcomeRuleError labels a rule evaluation that returned an error
	// from the engine.
	OutcomeRuleError = "error"

	// AttrOutcome is the label key for run outcome on runs_total and
	// run_duration_ms, and for per-rule outcome on rule_evaluations_total.
	AttrOutcome = "outcome"
	// AttrContextID is the label key for the reconciliation context UUID.
	AttrContextID = "context_id"
	// AttrRuleType is the label key for rule_type on
	// rule_evaluations_total. Values come from shared.RuleType.
	AttrRuleType = "rule_type"
)

// matchingMetrics holds the constructed instruments. Populated exactly once
// by the sync.Once guard in get().
type matchingMetrics struct {
	runs                metric.Int64Counter
	runDurationMs       metric.Float64Histogram
	confidence          metric.Float64Histogram
	ruleEvaluations     metric.Int64Counter
	ruleEvaluationDurMs metric.Float64Histogram
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

		ruleEvals, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.matching.rule_evaluations_total",
			"Total rule evaluations by rule_type and outcome",
		)

		ruleEvalDuration, _ := sharedMetrics.Float64Histogram(
			meter,
			"matcher.matching.rule_evaluation_duration_ms",
			"Duration of a rules-engine invocation in milliseconds (covers all rules evaluated in the call)",
			"ms",
		)

		instance = &matchingMetrics{
			runs:                runs,
			runDurationMs:       runDuration,
			confidence:          confidence,
			ruleEvaluations:     ruleEvals,
			ruleEvaluationDurMs: ruleEvalDuration,
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

// RecordRuleEvaluation emits the rule_evaluations_total counter. One
// emission per rule evaluated in an engine invocation — call-sites loop
// over the rule definitions that were handed to the engine and emit
// once per rule with the outcome computed from the engine result.
// ruleType is expected to be one of the shared.RuleType constants
// (EXACT, TOLERANCE, DATE_LAG).
func RecordRuleEvaluation(ctx context.Context, ruleType, outcome string) {
	instruments := get()
	if instruments == nil {
		return
	}

	attrs := []attribute.KeyValue{
		sharedMetrics.Attr(AttrRuleType, ruleType),
		sharedMetrics.Attr(AttrOutcome, outcome),
	}

	instruments.ruleEvaluations.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordRuleEvaluationDuration emits the rule_evaluation_duration_ms
// histogram. One emission per rules-engine invocation covering the
// wall-clock of all rules evaluated in that call. Label-less because
// the engine does not surface per-rule timing — dashboards needing
// per-rule cost should derive it as (duration_ms / evaluations_total)
// in the same time window.
func RecordRuleEvaluationDuration(ctx context.Context, durationMs float64) {
	instruments := get()
	if instruments == nil || durationMs < 0 {
		return
	}

	instruments.ruleEvaluationDurMs.Record(ctx, durationMs)
}
