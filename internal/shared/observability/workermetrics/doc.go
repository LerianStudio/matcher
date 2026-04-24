// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package workermetrics declares the OpenTelemetry metrics emitted by
// matcher's background workers. Every ticker-based worker reuses this
// package so operators get one dashboard shape regardless of context —
// a single `worker` label slices cycles, items, and backlog the same
// way across archival, export, cleanup, scheduler, bridge, custody
// retention, and discovery workers.
//
// # Metric namespace: matcher.worker.*
//
// Five instruments cover the common worker shape:
//
//   - matcher.worker.cycles_total (Int64Counter) — one emission per
//     cycle (ticker tick) tagged with worker + outcome. A cycle is the
//     unit of "ticker fired, work attempted"; outcome distinguishes
//     success (work done), failure (error surfaced), and skipped
//     (no-op, usually because a distributed lock was held elsewhere
//     or the backlog was empty).
//
//   - matcher.worker.cycle_duration_ms (Float64Histogram) — wall-clock
//     of one cycle, regardless of outcome. Labelled by worker only so
//     operators can compare "how long does a cycle take" against
//     "how often does a cycle fail" without label-cardinality pain.
//
//   - matcher.worker.items_processed_total (Int64Counter) — rolling
//     count of successfully handled items (archived partitions,
//     exported jobs, deleted files, fired schedules, bridged
//     extractions, swept custody objects, synced connections,
//     completed extraction polls). "Item" is whatever the worker
//     treats as a unit of work inside a cycle. Labelled by worker.
//
//   - matcher.worker.items_failed_total (Int64Counter) — rolling
//     count of items the worker attempted but could not complete in
//     this cycle (retried next cycle unless terminal). Labelled by
//     worker.
//
//   - matcher.worker.backlog_size (Int64UpDownCounter) — current
//     backlog the worker is draining. Reported per cycle as an
//     absolute value via SetBacklog (implemented as "record delta to
//     reach the new value"). Labelled by worker. Not every worker
//     exposes a clean backlog count; those that do not simply omit
//     RecordBacklog calls.
//
// # Label discipline
//
// The `worker` label is bounded to the set of registered worker
// singletons (7 today: archival, export, cleanup, scheduler, bridge,
// custody_retention, discovery). The `outcome` label on cycles_total
// is a closed set of three values. No per-tenant, per-job, or
// per-item labels — those belong on spans, not metrics.
//
// # Usage
//
// Each worker constructs a Recorder once (typically as a package
// var or struct field) and calls RecordCycle at the tail of every
// cycle, RecordItems inside the cycle, and RecordBacklog when a
// fresh backlog count is cheap to compute. See
// internal/governance/services/worker/archival_worker.go and
// peers for call-site examples.
package workermetrics
