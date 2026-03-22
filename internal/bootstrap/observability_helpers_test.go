// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
)

func setInitTelemetryFnForTest(fn func(*Config, libLog.Logger) *libOpentelemetry.Telemetry) func() {
	initTelemetryFnMu.Lock()
	previous := initTelemetryFn
	initTelemetryFn = fn
	initTelemetryFnMu.Unlock()

	return func() {
		initTelemetryFnMu.Lock()
		initTelemetryFn = previous
		initTelemetryFnMu.Unlock()
	}
}
