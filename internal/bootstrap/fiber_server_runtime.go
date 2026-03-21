// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Server encapsulates the Fiber HTTP server with its runtime dependencies.
type Server struct {
	app       *fiber.App
	cfg       *Config
	logger    libLog.Logger
	telemetry *libOpentelemetry.Telemetry
	postgres  *libPostgres.Client
	redis     *libRedis.Client
	rabbitmq  *libRabbitmq.RabbitMQConnection
}

// NewServer creates a new Server instance with all required dependencies.
func NewServer(
	cfg *Config,
	app *fiber.App,
	logger libLog.Logger,
	telemetry *libOpentelemetry.Telemetry,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
) *Server {
	return &Server{
		app:       app,
		cfg:       cfg,
		logger:    logger,
		telemetry: telemetry,
		postgres:  postgres,
		redis:     redis,
		rabbitmq:  rabbitmq,
	}
}

// GetApp returns the underlying Fiber application for testing purposes.
// This allows integration tests to call app.Test() for in-process HTTP testing
// without starting a real network listener.
func (srv *Server) GetApp() *fiber.App {
	if srv == nil {
		return nil
	}

	return srv.app
}

// Run starts the HTTP server, implementing the libCommons.App interface.
func (srv *Server) Run(_ *libCommons.Launcher) error {
	if srv == nil {
		return errServerNotInitialized
	}

	if srv.cfg == nil {
		return errConfigNotInitialized
	}

	asserter := assert.New(
		context.Background(),
		srv.logger,
		constants.ApplicationName,
		"bootstrap.server_run",
	)

	if err := asserter.NotNil(context.Background(), srv.app, "server not initialized"); err != nil {
		return fmt.Errorf("server run: %w", err)
	}

	if strings.TrimSpace(srv.cfg.Server.TLSCertFile) != "" ||
		strings.TrimSpace(srv.cfg.Server.TLSKeyFile) != "" {
		if err := srv.app.ListenTLS(srv.cfg.Server.Address, srv.cfg.Server.TLSCertFile, srv.cfg.Server.TLSKeyFile); err != nil {
			return fmt.Errorf("server listen tls: %w", err)
		}

		return nil
	}

	if err := srv.app.Listen(srv.cfg.Server.Address); err != nil {
		return fmt.Errorf("server listen: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the HTTP server and flushes telemetry.
func (srv *Server) Shutdown(ctx context.Context) error {
	logger := libLog.Logger(&libLog.NopLogger{})
	if srv != nil && srv.logger != nil {
		logger = srv.logger
	}

	asserter := assert.New(ctx, logger, constants.ApplicationName, "bootstrap.server_shutdown")

	if err := asserter.NotNil(ctx, srv, "server not initialized"); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	if err := asserter.NotNil(ctx, srv.app, "server not initialized"); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	if err := srv.app.ShutdownWithContext(ctx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	if srv.telemetry != nil {
		srv.telemetry.ShutdownTelemetry()
	}

	return nil
}

// NewFiberApp creates and configures a new Fiber application with standard middleware.
