// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package metrics provides helpers for declaring OpenTelemetry business
// metrics under the matcher.<context>.* namespace.
//
// The package exposes a small, generic factory API (Counter, Histogram,
// UpDownCounter) that per-context metric packages build on. Each bounded
// context owns its own set of metric instruments — see
// internal/<context>/services/metrics/metrics.go — and reuses these helpers
// so the naming, unit conventions, and instrumentation errors stay uniform.
//
// Design choices:
//
//  1. Instruments are created via otel.Meter(scope) at package init of each
//     context-specific metrics package, wrapped in sync.Once so the first
//     call-site pays the setup cost and every subsequent emitter reuses the
//     singleton. This matches the lifecycle of the process-wide global meter
//     and avoids threading a meter dependency through every service
//     constructor.
//
//  2. Instrument construction errors never panic. A failed instrument
//     collapses to a nop so business logic is never gated by observability
//     plumbing. The error is returned so the per-context package can log it
//     at init if desired.
//
//  3. Attribute builders live here (BuildAttrs) so call-sites don't repeat
//     the attribute.KeyValue construction boilerplate. Attributes must be
//     low-cardinality — see individual metric packages for allow-listed
//     labels.
package metrics
