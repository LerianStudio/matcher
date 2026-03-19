// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// defaultShutdownGracePeriod is the default time to wait for background workers
// to finish after requesting stop, before closing infrastructure connections.
const defaultShutdownGracePeriod = 5 * time.Second

// Service is the main application container that orchestrates all components.
type Service struct {
	*Server
	libLog.Logger
	Config        *Config
	Routes        *Routes
	ConfigManager *ConfigManager

	outboxRunner       libCommons.App
	dbMetricsCollector *DBMetricsCollector
	workerManager      *WorkerManager
	connectionManager  connectionCloser
	cleanupFuncs       []func()
	readinessState     *readinessState

	// Systemplane components for centralized runtime configuration.
	// These are nil when systemplane initialization fails (graceful degradation).
	spComponents     *SystemplaneComponents
	snapshotReader   *SnapshotReader
	cancelChangeFeed context.CancelFunc
}

type readinessState struct {
	draining atomic.Bool
}

func (state *readinessState) beginDraining() {
	if state == nil {
		return
	}

	state.draining.Store(true)
}

func (state *readinessState) isDraining() bool {
	if state == nil {
		return false
	}

	return state.draining.Load()
}

type connectionCloser interface {
	Close() error
}

// stoppable is a lifecycle interface for components that can be stopped
// during graceful shutdown (e.g., the outbox dispatcher).
type stoppable interface {
	Stop()
}

// GetOutboxRunner returns the outbox dispatcher as a libCommons.App.
// This allows integration tests to extract the dispatcher for controlled event dispatch.
func (svc *Service) GetOutboxRunner() libCommons.App {
	if svc == nil {
		return nil
	}

	return svc.outboxRunner
}

// Run starts the service with all configured components using the launcher.
// Returns an error if the service is nil or encounters a fatal error during startup.
func (svc *Service) Run() error {
	if svc == nil {
		logger, logErr := libZap.New(libZap.Config{
			Environment:     ResolveLoggerEnvironment(os.Getenv("ENV_NAME")),
			Level:           ResolveLoggerLevel(os.Getenv("LOG_LEVEL")),
			OTelLibraryName: "github.com/LerianStudio/matcher",
		})
		if logErr == nil && logger != nil {
			logger.With(
				libLog.String("service.name", constants.ApplicationName),
				libLog.String("operation", "service.run"),
			).Log(context.Background(), libLog.LevelWarn, "Run invoked on nil *Service; skipping startup")
		}

		return nil
	}

	ctx := context.Background()

	defer runtime.RecoverAndLogWithContext(
		ctx,
		svc.Logger,
		constants.ApplicationName,
		"service.run",
	)

	if svc.dbMetricsCollector != nil {
		svc.dbMetricsCollector.Start(ctx)
	}

	activeCfg := svc.resolveActiveConfig()

	if svc.workerManager != nil {
		if err := svc.workerManager.Start(ctx, activeCfg); err != nil {
			return err
		}
	}

	opts := []libCommons.LauncherOption{
		libCommons.WithLogger(svc.Logger),
		libCommons.RunApp("Fiber HTTP Server", svc.Server),
	}
	if svc.outboxRunner != nil {
		opts = append(opts, libCommons.RunApp("Outbox Dispatcher", svc.outboxRunner))
	}

	libCommons.NewLauncher(opts...).Run()

	return nil
}

func (svc *Service) resolveActiveConfig() *Config {
	activeCfg := svc.Config
	if svc.ConfigManager == nil {
		return activeCfg
	}

	managedCfg := svc.ConfigManager.Get()
	if managedCfg == nil {
		return activeCfg
	}

	svc.Config = managedCfg

	return managedCfg
}

// Shutdown gracefully shuts down the service, including the HTTP server and telemetry.
func (svc *Service) Shutdown(ctx context.Context) error {
	if svc == nil {
		return nil
	}

	ctx = fallbackContext(ctx)

	logger := svc.Logger
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	svc.readinessState.beginDraining()

	svc.stopBackgroundWorkers(ctx, logger)

	// Allow background workers time to complete in-flight operations
	// before closing infrastructure connections they depend on.
	gracePeriod := defaultShutdownGracePeriod
	if svc.Config != nil && svc.Config.ShutdownGracePeriod > 0 {
		gracePeriod = svc.Config.ShutdownGracePeriod
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("waiting %v for background workers to complete", gracePeriod))

	timer := time.NewTimer(gracePeriod)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		logger.Log(ctx, libLog.LevelWarn, "shutdown context cancelled before grace period elapsed")
	case <-timer.C:
	}

	var shutdownErr error

	shutdownErr = svc.shutdownServerAndConnections(ctx, logger, shutdownErr)

	return shutdownErr
}

// stopBackgroundWorkers stops all background workers and the outbox runner.
//
// Shutdown ordering contract:
// 1. ConfigManager.Stop() — prevents config mutations during shutdown
// 2. Systemplane change feed — stop change feed before supervisor
// 3. Systemplane supervisor — stop config supervisory loop
// 4. WorkerManager.Stop() — stops all managed workers
// 5. Standalone workers — stops workers not yet migrated to WorkerManager
// This order is CRITICAL: stopping ConfigManager first prevents the
// config-change subscriber from restarting workers while we're shutting them down.
// Systemplane change feed stops before the supervisor to prevent reload triggers
// during shutdown.
func (svc *Service) stopBackgroundWorkers(ctx context.Context, logger libLog.Logger) {
	// Step 1: Stop ConfigManager first (see ordering contract above).
	if svc.ConfigManager != nil {
		svc.ConfigManager.Stop()
	}

	// Step 2: Stop systemplane change feed and supervisor.
	svc.stopSystemplane(ctx, logger)

	_ = svc.stopWorkerManager(ctx, logger)

	if svc.dbMetricsCollector != nil {
		svc.dbMetricsCollector.Stop()
	}

	if outbox := svc.outboxStoppable(); outbox != nil {
		outbox.Stop()
	}
}

// stopSystemplane gracefully shuts down the systemplane components.
// The ordering matters: change feed stops first to prevent triggering reloads,
// then the supervisor stops, and finally the backend connection is closed.
func (svc *Service) stopSystemplane(ctx context.Context, logger libLog.Logger) {
	if svc.cancelChangeFeed != nil {
		svc.cancelChangeFeed()
	}

	if svc.spComponents == nil {
		return
	}

	if svc.spComponents.Supervisor != nil {
		if err := svc.spComponents.Supervisor.Stop(ctx); err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("systemplane supervisor stop failed: %v", err))
		}
	}

	if svc.spComponents.Backend != nil {
		if err := svc.spComponents.Backend.Close(); err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("systemplane backend close failed: %v", err))
		}
	}
}

func (svc *Service) stopWorkerManager(ctx context.Context, logger libLog.Logger) bool {
	if svc.workerManager == nil {
		return false
	}

	if err := svc.workerManager.Stop(); err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to stop worker manager: %v", err))
	}

	return true
}

func (svc *Service) outboxStoppable() stoppable {
	if svc.outboxRunner == nil {
		return nil
	}

	s, ok := svc.outboxRunner.(stoppable)
	if !ok {
		return nil
	}

	return s
}

// shutdownServerAndConnections shuts down the HTTP server and closes all connections.
func (svc *Service) shutdownServerAndConnections(ctx context.Context, logger libLog.Logger, shutdownErr error) error {
	if svc.Server != nil {
		if err := svc.Server.Shutdown(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}

	if svc.connectionManager != nil {
		if err := svc.connectionManager.Close(); err != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close connection manager: %v", err))
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}

	if svc.Server != nil {
		cleanupConnections(ctx, svc.postgres, svc.redis, svc.rabbitmq, logger)
	}

	svc.runCleanupFuncs()

	return shutdownErr
}

func (svc *Service) runCleanupFuncs() {
	if svc == nil || len(svc.cleanupFuncs) == 0 {
		return
	}

	for i := len(svc.cleanupFuncs) - 1; i >= 0; i-- {
		if svc.cleanupFuncs[i] != nil {
			svc.cleanupFuncs[i]()
		}
	}

	svc.cleanupFuncs = nil
}
