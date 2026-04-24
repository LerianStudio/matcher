// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package testutil — goleak helpers.
//
// Provides a shared ignore-list of known-safe goroutines so per-package
// TestMain functions can call LeakOptions() instead of duplicating
// boilerplate.
//
// Usage:
//
//	func TestMain(m *testing.M) {
//	    goleak.VerifyTestMain(m, testutil.LeakOptions()...)
//	}
//
// For per-test snapshots:
//
//	func TestSomething(t *testing.T) {
//	    defer goleak.VerifyNone(t, testutil.LeakOptions()...)
//	    // ...
//	}
package testutil

import "go.uber.org/goleak"

// LeakOptions returns the standard goleak ignore-list for Matcher packages.
//
// The entries cover goroutines that are intentionally long-lived across the
// process lifetime and cannot be terminated from within a normal test:
//
//   - OTel batch span processor — started by bootstrap, drains on Shutdown.
//   - database/sql connection opener/resetter — owned by the pool.
//   - OpenCensus stats worker — pulled in transitively by gRPC/OTel exporters.
//   - redis/go-redis maintnotifications circuit-breaker cleanup loop —
//     started when a go-redis client is constructed (e.g., miniredis-backed
//     tests) and only terminates on client Close.
//
// If a specific package has goroutines that MUST terminate, do not extend
// this list — let goleak fail the test so the leak is visible.
func LeakOptions() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreTopFunction("go.opentelemetry.io/otel/sdk/trace.(*batchSpanProcessor).processQueue"),
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionResetter"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/maintnotifications.(*CircuitBreakerManager).cleanupLoop"),
	}
}

// LeakOptionsWithSystemplane returns LeakOptions plus systemplane listener
// ignores. Use this in packages (e.g., bootstrap) that wire the lib-commons
// v5 systemplane runtime-config client — its Subscribe goroutine blocks on
// a pgx LISTEN connection and terminates only when the client's context
// is cancelled at shutdown.
func LeakOptionsWithSystemplane() []goleak.Option {
	return append(LeakOptions(),
		goleak.IgnoreAnyFunction("github.com/LerianStudio/lib-commons/v5/commons/systemplane/internal/postgres.(*Store).Subscribe"),
		goleak.IgnoreAnyFunction("github.com/LerianStudio/lib-commons/v5/commons/systemplane/internal/postgres.(*Store).listenLoop"),
		goleak.IgnoreAnyFunction("github.com/LerianStudio/lib-commons/v5/commons/systemplane.(*Client).Close.func2"),
	)
}

// LeakOptionsBootstrap extends LeakOptionsWithSystemplane with additional
// process-lifetime ignores for goroutines that the bootstrap wiring starts
// transitively. These are owned by third-party infrastructure libraries,
// not by Matcher code, and are meant to live for the full process
// lifetime:
//
//   - tenant-manager InMemoryCache cleanup loop (lib-commons).
//   - redis maintnotifications circuit-breaker cleanup loop.
//   - gRPC callback serializer worker.
//   - fasthttp server-date updater.
//   - OTel periodic metric reader + log batch processor.
//
// This is intentionally more permissive than LeakOptions so bootstrap
// tests remain signal-rich without drowning in third-party noise.
// Non-bootstrap packages should NOT import this list — they should use
// LeakOptions or LeakOptionsWithSystemplane.
func LeakOptionsBootstrap() []goleak.Option {
	return append(LeakOptionsWithSystemplane(),
		goleak.IgnoreAnyFunction("github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/cache.(*InMemoryCache).cleanupLoop"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/maintnotifications.(*CircuitBreakerManager).cleanupLoop"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreAnyFunction("github.com/valyala/fasthttp.updateServerDate.func1"),
		goleak.IgnoreAnyFunction("go.opentelemetry.io/otel/sdk/metric.(*PeriodicReader).run"),
		goleak.IgnoreAnyFunction("go.opentelemetry.io/otel/sdk/log.(*BatchProcessor).poll.func1"),
		goleak.IgnoreAnyFunction("go.opentelemetry.io/otel/sdk/log.exportSync.func1"),
	)
}
