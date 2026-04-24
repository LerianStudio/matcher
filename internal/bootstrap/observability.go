// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// defaultTelemetryTimeout bounds how long we wait for the OTEL collector
// to respond. Prevents startup from hanging when the collector is unreachable.
const defaultTelemetryTimeout = 10 * time.Second

const lateTelemetryCleanupTimeout = 30 * time.Second

var initTelemetryFn = InitTelemetry

var initTelemetryFnMu sync.RWMutex

func loadInitTelemetryFn() func(*Config, libLog.Logger) *libOpentelemetry.Telemetry {
	initTelemetryFnMu.RLock()
	defer initTelemetryFnMu.RUnlock()

	if initTelemetryFn == nil {
		return InitTelemetry
	}

	return initTelemetryFn
}

// InitTelemetry initializes OpenTelemetry with the provided configuration.
// It sets up the global TextMapPropagator for distributed trace context propagation
// across HTTP requests and message queues (RabbitMQ).
func InitTelemetry(cfg *Config, logger libLog.Logger) *libOpentelemetry.Telemetry {
	if cfg == nil {
		cfg = &Config{}
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	telemetry, err := libOpentelemetry.NewTelemetry(libOpentelemetry.TelemetryConfig{
		LibraryName:               cfg.Telemetry.LibraryName,
		ServiceName:               cfg.Telemetry.ServiceName,
		ServiceVersion:            cfg.Telemetry.ServiceVersion,
		DeploymentEnv:             cfg.Telemetry.DeploymentEnv,
		CollectorExporterEndpoint: cfg.Telemetry.CollectorEndpoint,
		EnableTelemetry:           cfg.Telemetry.Enabled,
		Logger:                    logger,
	})
	if err != nil {
		if logger != nil {
			libLog.SafeError(logger, context.Background(), "failed to initialize telemetry", err, runtime.IsProductionMode())
		}

		return nil
	}

	return telemetry
}

// telemetryInitResult holds the result of an async telemetry initialization.
type telemetryInitResult struct {
	telemetry *libOpentelemetry.Telemetry
}

// runTelemetryInit executes telemetry initialization inside a goroutine, sending
// the result (or nil on panic) to ch. Panics are recovered and logged.
func runTelemetryInit(
	ctx context.Context,
	ch chan telemetryInitResult,
	initializer func(*Config, libLog.Logger) *libOpentelemetry.Telemetry,
	cfg *Config,
	logger libLog.Logger,
) {
	result := telemetryInitResult{}

	defer func() {
		if recovered := recover(); recovered != nil {
			if logger != nil {
				logger.Log(
					ctx,
					libLog.LevelError,
					fmt.Sprintf("panic during telemetry initialization: %v", recovered),
				)
			}
		}

		select {
		case ch <- result:
		default:
			if result.telemetry != nil {
				result.telemetry.ShutdownTelemetry()
			}
		}
	}()

	result.telemetry = initializer(cfg, logger)
}

// deriveTelemetryTimeout returns the effective telemetry timeout, capped by any
// deadline already present on ctx.
func deriveTelemetryTimeout(ctx context.Context) time.Duration {
	timeout := defaultTelemetryTimeout

	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}

	return timeout
}

// InitTelemetryWithTimeout wraps InitTelemetry with a deadline.
// Returns a non-nil Telemetry in all cases: either the real telemetry instance,
// or a no-op instance if telemetry is disabled or initialization timed out.
// Callers can always invoke Telemetry methods without nil checks.
// lib-commons' NewTelemetry internally uses context.Background() for gRPC dials,
// so we can't propagate a deadline through the library. Instead, we run it in a
// goroutine and abandon the attempt if the deadline expires.
//
// If telemetry is disabled in config, this returns immediately (no gRPC dials occur).
// If the timeout fires, a no-op telemetry instance is created so the service starts
// without observability rather than hanging indefinitely.
//
// The abandoned goroutine is bounded: gRPC dials have their own internal timeouts,
// so the goroutine will eventually complete and be garbage collected.
func InitTelemetryWithTimeout(ctx context.Context, cfg *Config, logger libLog.Logger) *libOpentelemetry.Telemetry {
	if cfg == nil {
		cfg = &Config{}
	}

	// Fast path: disabled telemetry creates no-op providers instantly (no gRPC).
	if !cfg.Telemetry.Enabled {
		return InitTelemetry(cfg, logger) //nolint:contextcheck // InitTelemetry intentionally omits context; lib uses context.Background internally
	}

	timeout := deriveTelemetryTimeout(ctx)

	ch := make(chan telemetryInitResult, 1)
	capturedCfg := cfg
	capturedLogger := logger
	telemetryInitializer := loadInitTelemetryFn()

	runtime.SafeGoWithContextAndComponent(
		ctx, logger, constants.ApplicationName, "telemetry.init",
		runtime.KeepRunning,
		func(innerCtx context.Context) {
			runTelemetryInit(innerCtx, ch, telemetryInitializer, capturedCfg, capturedLogger)
		},
	)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-ch:
		return result.telemetry
	case <-timer.C:
		if logger != nil {
			logger.Log( //nolint:contextcheck // parent context may be canceled or unavailable; use background for this final log
				context.Background(),
				libLog.LevelWarn,
				fmt.Sprintf("telemetry initialization timed out after %v — starting with telemetry disabled", timeout),
			)
		}

		// Best-effort late-result cleanup: if the original telemetry init completes
		// after timeout, drain and shut it down to avoid orphaned exporters.
		runtime.SafeGoWithContextAndComponent(
			ctx,
			logger,
			constants.ApplicationName,
			"telemetry.init_late_cleanup",
			runtime.KeepRunning,
			func(_ context.Context) {
				cleanupTimer := time.NewTimer(lateTelemetryCleanupTimeout)
				defer cleanupTimer.Stop()

				select {
				case lateResult := <-ch:
					if lateResult.telemetry != nil {
						lateResult.telemetry.ShutdownTelemetry()
					}
				case <-cleanupTimer.C:
				}
			},
		)

		// Create a disabled-mode telemetry instance so callers always get
		// a valid struct (avoids nil checks scattered throughout the codebase).
		disabledCfg := *cfg
		disabledCfg.Telemetry.Enabled = false

		return InitTelemetry(&disabledCfg, logger) //nolint:contextcheck // InitTelemetry omits context; underlying lib uses context.Background
	}
}
