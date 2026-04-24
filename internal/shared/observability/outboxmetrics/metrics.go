// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package outboxmetrics declares the OpenTelemetry business metrics that
// matcher owns on the outbox dispatch path. Instruments are lazily
// constructed on first use via sync.Once so every call-site (the
// bootstrap handler-registration wrapper) resolves to the same
// underlying meter without threading a dependency through.
//
// # Metric namespace: matcher.outbox.*
//
// # Ownership boundary (what matcher owns vs. what lib-commons owns)
//
// The outbox dispatch loop lives in lib-commons/v5/commons/outbox. That
// package OWNS the following instruments under meter scope
// "commons.outbox.dispatcher":
//
//   - outbox.events.dispatched
//   - outbox.events.failed
//   - outbox.events.state_update_failed
//   - outbox.dispatch.latency
//   - outbox.queue.depth
//
// These are emitted by the dispatcher and are authoritative for
// "how many events the dispatcher moved" and "how long a dispatch
// cycle took". Matcher MUST NOT shadow them.
//
// Matcher owns the per-event-type handler closures registered on the
// HandlerRegistry in internal/bootstrap/outbox_wiring.go. When the
// dispatcher invokes a handler it hands control to a matcher closure
// that validates the payload, unmarshals it, and calls the right
// broker publisher. That handler step is invisible to the dispatcher's
// eventsDispatched counter — the dispatcher only sees "handler returned
// nil" or "handler returned error". The two instruments below capture
// what matcher spends INSIDE the handler closure:
//
//   - matcher.outbox.handler_invocations_total{event_type, outcome}
//   - matcher.outbox.handler_duration_ms{event_type}
//
// # Truncation counter (R-017)
//
// A separate counter, outbox_payload_truncated_total{entity_type}, is
// emitted from internal/shared/adapters/outboxtelemetry/truncation.go
// via the lib-commons MetricsFactory API (the bridge between matcher's
// OTel runtime and the shared counter registry). That counter predates
// this package and is intentionally left at its original name and
// meter scope so existing dashboards and alerts continue working.
// Renaming it would be a scope-creep breaking change orthogonal to the
// handler metrics added here.
//
// # Labels
//
//   - event_type: the canonical matcher event type string
//     ({matching,ingestion,governance}.{match_confirmed,match_unmatched,
//     completed,failed,audit_log_created}). Bounded to 5 today; if new
//     handlers are registered in outbox_wiring.go, the set grows with
//     them. Each is a compile-time constant, not user-supplied input.
//   - outcome: "success" | "failure" | "skipped". "skipped" is for
//     handler invocations where the payload was already non-retryable
//     (e.g. malformed JSON) so the handler refused to publish — the
//     dispatcher will mark the event invalid rather than retry.
package outboxmetrics

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	sharedMetrics "github.com/LerianStudio/matcher/internal/shared/observability/metrics"
)

const (
	// MeterScope is the OTel meter scope used for matcher-owned outbox metrics.
	MeterScope = "matcher.outbox"

	// AttrEventType is the label key for the canonical matcher event type.
	AttrEventType = "event_type"
	// AttrOutcome is the label key for handler outcome.
	AttrOutcome = "outcome"

	// OutcomeSuccess labels a handler invocation that returned nil.
	OutcomeSuccess = "success"
	// OutcomeFailure labels a handler invocation that returned an error
	// the dispatcher treats as retryable (transient broker fault, etc.).
	OutcomeFailure = "failure"
	// OutcomeSkipped labels a handler invocation that returned a
	// non-retryable error (malformed payload, missing publisher, etc.).
	// Distinguished from failure so operators can tell "broker is
	// struggling" from "producer is emitting garbage".
	OutcomeSkipped = "skipped"
)

type outboxMetrics struct {
	handlerInvocations metric.Int64Counter
	handlerDurationMs  metric.Float64Histogram
}

var (
	instance *outboxMetrics
	once     sync.Once
)

func get() *outboxMetrics {
	once.Do(func() {
		meter := sharedMetrics.Meter(MeterScope)

		invocations, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.outbox.handler_invocations_total",
			"Total matcher-owned outbox handler invocations by event_type and outcome",
		)

		duration, _ := sharedMetrics.Float64Histogram(
			meter,
			"matcher.outbox.handler_duration_ms",
			"Duration of a matcher-owned outbox handler invocation in milliseconds",
			"ms",
		)

		instance = &outboxMetrics{
			handlerInvocations: invocations,
			handlerDurationMs:  duration,
		}
	})

	return instance
}

// RecordHandlerInvocation emits the handler_invocations_total counter
// and the handler_duration_ms histogram for one matcher-owned outbox
// handler call. outcome is one of OutcomeSuccess / OutcomeFailure /
// OutcomeSkipped. eventType is the canonical matcher event type
// (sharedDomain.EventType* constants).
func RecordHandlerInvocation(ctx context.Context, eventType, outcome string, durationMs float64) {
	instruments := get()
	if instruments == nil {
		return
	}

	attrs := []attribute.KeyValue{
		sharedMetrics.Attr(AttrEventType, eventType),
		sharedMetrics.Attr(AttrOutcome, outcome),
	}

	instruments.handlerInvocations.Add(ctx, 1, metric.WithAttributes(attrs...))

	if durationMs >= 0 {
		instruments.handlerDurationMs.Record(ctx, durationMs,
			metric.WithAttributes(sharedMetrics.Attr(AttrEventType, eventType)),
		)
	}
}
