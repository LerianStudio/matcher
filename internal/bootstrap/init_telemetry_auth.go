// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-auth/v3/auth/middleware"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
)

// initTelemetryAndMetrics initializes OpenTelemetry with timeout protection and
// registers assertion/panic metrics if telemetry is available.
func initTelemetryAndMetrics(
	ctx context.Context,
	cfg *Config,
	logger libLog.Logger,
	connector InfraConnector,
) *libOpentelemetry.Telemetry {
	telemetryCtx, telemetryCancel := context.WithTimeout(ctx, cfg.InfraConnectTimeout()) //nolint:contextcheck // InfraConnectTimeout is a pure config accessor
	defer telemetryCancel()

	telemetry := InitTelemetryWithTimeout(telemetryCtx, cfg, logger, connector)

	if telemetry != nil {
		assert.InitAssertionMetrics(telemetry.MetricsFactory)
		runtime.InitPanicMetrics(telemetry.MetricsFactory)
	}

	return telemetry
}

// createAuthClient builds the authentication middleware client with a bridge logger
// for the auth boundary.
func createAuthClient(
	ctx context.Context,
	cfg *Config,
	logger libLog.Logger,
	connector InfraConnector,
) *middleware.AuthClient {
	authLogger, authLoggerErr := initializeAuthBoundaryLogger(connector)
	if authLoggerErr != nil {
		logger.Log(
			ctx,
			libLog.LevelWarn,
			fmt.Sprintf("failed to initialize auth boundary logger, using no-op logger: %v", authLoggerErr),
		)

		authLogger = libLog.NewNop()
	}

	return middleware.NewAuthClient(cfg.Auth.Host, cfg.Auth.Enabled, &authLogger)
}

func initializeAuthBoundaryLogger(connector InfraConnector) (libLog.Logger, error) {
	if connector == nil {
		connector = DefaultInfraConnector()
	}

	authLogger, authLoggerErr := connector.InitializeAuthBoundaryLogger()
	if authLoggerErr != nil {
		return nil, fmt.Errorf("initialize auth boundary logger: %w", authLoggerErr)
	}

	if authLogger == nil {
		return nil, fmt.Errorf("initialize auth boundary logger: %w", errAuthBoundaryLoggerNil)
	}

	return authLogger, nil
}
