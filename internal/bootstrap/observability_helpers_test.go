//go:build unit

package bootstrap

import (
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"
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
