// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package workermetrics

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	sharedMetrics "github.com/LerianStudio/matcher/internal/shared/observability/metrics"
)

const (
	// MeterScope is the OTel meter scope used for matcher worker metrics.
	MeterScope = "matcher.worker"

	// AttrWorker is the label key for the worker name.
	AttrWorker = "worker"
	// AttrOutcome is the label key for cycle outcome on cycles_total.
	AttrOutcome = "outcome"

	// OutcomeSuccess labels a cycle that ran and completed at least
	// one unit of work (items processed or a successful no-backlog scan).
	OutcomeSuccess = "success"
	// OutcomeFailure labels a cycle that surfaced an error (DB, broker,
	// object store, tenant list, lock acquire, etc.). Items processed
	// so far in the cycle still count; the outcome is about the cycle.
	OutcomeFailure = "failure"
	// OutcomeSkipped labels a cycle that did no work — typically
	// because a distributed lock was held by a peer replica or
	// fetcher health check failed. Separated from failure so
	// operators can tell "peer owns the lock this tick" from
	// "something is actually broken".
	OutcomeSkipped = "skipped"
)

// workerInstruments holds the shared counters/histogram emitted by
// every registered worker. Populated exactly once by the sync.Once
// guard in get().
type workerInstruments struct {
	cycles          metric.Int64Counter
	cycleDurationMs metric.Float64Histogram
	itemsProcessed  metric.Int64Counter
	itemsFailed     metric.Int64Counter
	backlogSize     metric.Int64UpDownCounter
}

var (
	instance *workerInstruments
	once     sync.Once
)

func get() *workerInstruments {
	once.Do(func() {
		meter := sharedMetrics.Meter(MeterScope)

		cycles, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.worker.cycles_total",
			"Total number of worker cycles by worker and outcome",
		)

		cycleDuration, _ := sharedMetrics.Float64Histogram(
			meter,
			"matcher.worker.cycle_duration_ms",
			"Wall-clock duration of a worker cycle in milliseconds",
			"ms",
		)

		itemsProcessed, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.worker.items_processed_total",
			"Total items successfully processed by a worker cycle",
		)

		itemsFailed, _ := sharedMetrics.Int64Counter(
			meter,
			"matcher.worker.items_failed_total",
			"Total items a worker cycle attempted but failed to process",
		)

		backlog, _ := sharedMetrics.Int64UpDownCounter(
			meter,
			"matcher.worker.backlog_size",
			"Current worker backlog (items pending processing)",
		)

		instance = &workerInstruments{
			cycles:          cycles,
			cycleDurationMs: cycleDuration,
			itemsProcessed:  itemsProcessed,
			itemsFailed:     itemsFailed,
			backlogSize:     backlog,
		}
	})

	return instance
}

// Recorder is a per-worker handle over the shared instruments. Each
// worker constructs one at init (or once per Worker struct) with its
// canonical name — "archival_worker", "export_worker", etc. — and
// threads it through the cycle. The Recorder caches the worker-name
// attribute slice so hot paths (cycle-end, item processed) do not
// reallocate per call.
type Recorder struct {
	name           string
	workerAttr     attribute.KeyValue
	lastBacklog    int64
	backlogStarted bool
	backlogMu      sync.Mutex
}

// NewRecorder returns a Recorder bound to workerName. workerName is
// the stable, canonical worker identifier used as the `worker` label
// value — keep it snake_case and matching the worker type name
// (archival_worker, export_worker, cleanup_worker, scheduler_worker,
// bridge_worker, custody_retention_worker, discovery_worker,
// extraction_poller).
func NewRecorder(workerName string) *Recorder {
	return &Recorder{
		name:       workerName,
		workerAttr: sharedMetrics.Attr(AttrWorker, workerName),
	}
}

// Name returns the worker name bound to this recorder. Exposed so
// call-sites that need to mirror the label onto a span attribute do
// not need to remember the constant they passed at construction.
func (rec *Recorder) Name() string {
	if rec == nil {
		return ""
	}

	return rec.name
}

// RecordCycle emits cycles_total + cycle_duration_ms for one cycle.
// startedAt is the time the cycle began; outcome is one of
// OutcomeSuccess / OutcomeFailure / OutcomeSkipped.
//
// Typical usage at the top of a cycle method:
//
//	started := time.Now()
//	outcome := workermetrics.OutcomeSuccess
//	defer func() { recorder.RecordCycle(ctx, started, outcome) }()
//
// and then the cycle body flips outcome to OutcomeFailure /
// OutcomeSkipped on the relevant branches.
func (rec *Recorder) RecordCycle(ctx context.Context, startedAt time.Time, outcome string) {
	if rec == nil {
		return
	}

	instruments := get()
	if instruments == nil {
		return
	}

	durationMs := float64(time.Since(startedAt).Milliseconds())

	cycleAttrs := metric.WithAttributes(rec.workerAttr, sharedMetrics.Attr(AttrOutcome, outcome))
	durationAttrs := metric.WithAttributes(rec.workerAttr)

	instruments.cycles.Add(ctx, 1, cycleAttrs)
	instruments.cycleDurationMs.Record(ctx, durationMs, durationAttrs)
}

// RecordItems adds to items_processed_total and items_failed_total.
// Call once per cycle with the totals accumulated during the cycle.
// Passing zero for both is a valid no-op — a cycle that scanned but
// found nothing should still call this with (0, 0) for consistency,
// though nothing forces it.
func (rec *Recorder) RecordItems(ctx context.Context, processed, failed int) {
	if rec == nil {
		return
	}

	instruments := get()
	if instruments == nil {
		return
	}

	attrs := metric.WithAttributes(rec.workerAttr)

	if processed > 0 {
		instruments.itemsProcessed.Add(ctx, int64(processed), attrs)
	}

	if failed > 0 {
		instruments.itemsFailed.Add(ctx, int64(failed), attrs)
	}
}

// RecordBacklog reports the worker's current backlog size. It
// translates the caller's absolute value into a delta against the
// last-reported value so the underlying UpDownCounter converges to
// the absolute figure. Thread-safe; call from anywhere inside the
// cycle, though the tail is the usual spot.
//
// Workers that do not have a clean "backlog" count (e.g. workers
// whose work is driven by external event streams) simply skip this
// call. The instrument will stay at zero for their label set.
func (rec *Recorder) RecordBacklog(ctx context.Context, size int64) {
	if rec == nil {
		return
	}

	instruments := get()
	if instruments == nil {
		return
	}

	rec.backlogMu.Lock()
	defer rec.backlogMu.Unlock()

	var delta int64
	if rec.backlogStarted {
		delta = size - rec.lastBacklog
	} else {
		delta = size
		rec.backlogStarted = true
	}

	rec.lastBacklog = size

	if delta == 0 {
		return
	}

	instruments.backlogSize.Add(ctx, delta, metric.WithAttributes(rec.workerAttr))
}
