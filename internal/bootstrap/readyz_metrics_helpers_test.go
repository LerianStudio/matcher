// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

// Test-only helpers for the /readyz metric globals. Lives in a *_test.go file
// so it is NOT compiled into the production binary — per the Ring
// testing-anti-patterns skill, production files must not carry test-only
// symbols.
//
// Tests that re-bind the OTel MeterProvider (e.g., to register a
// ManualReader) must then re-run initReadyzMetrics so the package-level
// instrument handles point at the new provider. resetReadyzMetricsForTest
// clears the sync.Once and the handles so the next init call re-creates the
// instruments. Callers serialise via nolint:paralleltest — no lock needed.

import "sync"

// resetReadyzMetricsForTest clears the sync.Once and instrument handles so a
// subsequent initReadyzMetrics call binds against the currently-registered
// global MeterProvider.
func resetReadyzMetricsForTest() {
	readyzMetricsOnce = sync.Once{}
	readyzCheckDurMs = nil
	readyzCheckStatus = nil
	readyzSelfProbeRes = nil
}
