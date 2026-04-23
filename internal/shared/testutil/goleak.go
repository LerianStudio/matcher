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
//
// If a specific package has goroutines that MUST terminate, do not extend
// this list — let goleak fail the test so the leak is visible.
func LeakOptions() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreTopFunction("go.opentelemetry.io/otel/sdk/trace.(*batchSpanProcessor).processQueue"),
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionResetter"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
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
