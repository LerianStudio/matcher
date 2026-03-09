package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/LerianStudio/lib-auth/v2/auth/middleware"
	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"

	"github.com/LerianStudio/matcher/internal/auth"
	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// defaultShutdownGracePeriod is the default time to wait for background workers
// to finish after requesting stop, before closing infrastructure connections.
const defaultShutdownGracePeriod = 5 * time.Second

const defaultWorkerStartWaitTimeout = 30 * time.Second

var (
	errWorkerStartContextCanceled = errors.New("worker start canceled")
	errWorkerPanicked             = errors.New("worker panicked")
	errWorkerStartFuncNil         = errors.New("worker start function is nil")
	errWorkerStartTimeout         = errors.New("worker start timed out")
)

// Service is the main application container that orchestrates all components.
type Service struct {
	*Server
	libLog.Logger
	Config *Config
	Routes *Routes

	authClient         *middleware.AuthClient
	tenantExtractor    *auth.TenantExtractor
	outboxRunner       libCommons.App
	dbMetricsCollector *DBMetricsCollector
	exportWorker       *reportingWorker.ExportWorker
	cleanupWorker      *reportingWorker.CleanupWorker
	archivalWorker     *governanceWorker.ArchivalWorker
	schedulerWorker    *configWorker.SchedulerWorker
	connectionManager  connectionCloser
	cleanupFuncs       []func()
}

type connectionCloser interface {
	Close() error
}

type workerStartEntry struct {
	name          string
	start         func(context.Context) error
	stop          func() error
	critical      bool
	onSoftFailure func()
}

type workerStartResult struct {
	name     string
	err      error
	critical bool
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

	if err := svc.startWorkers(ctx); err != nil {
		return err
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

// startWorkers starts all background workers in parallel.
// Non-critical workers that fail to start are disabled with a warning.
// Critical workers (those explicitly enabled via config) cause a startup error.
//
// Workers are independent of each other, so starting them in parallel reduces
// the total startup time by the latency of the slowest worker rather than the sum.
func (svc *Service) startWorkers(ctx context.Context) error {
	// Collect workers to start — skip nil workers.
	var entries []workerStartEntry

	if svc.exportWorker != nil {
		entries = append(entries, workerStartEntry{
			name:          "export",
			start:         svc.exportWorker.Start,
			stop:          svc.exportWorker.Stop,
			critical:      svc.Config != nil && svc.Config.ExportWorker.Enabled,
			onSoftFailure: func() { svc.exportWorker = nil },
		})
	}

	if svc.cleanupWorker != nil {
		entries = append(entries, workerStartEntry{
			name:          "cleanup",
			start:         svc.cleanupWorker.Start,
			stop:          svc.cleanupWorker.Stop,
			critical:      svc.Config != nil && svc.Config.CleanupWorker.Enabled,
			onSoftFailure: func() { svc.cleanupWorker = nil },
		})
	}

	if svc.archivalWorker != nil {
		entries = append(entries, workerStartEntry{
			name:          "archival",
			start:         svc.archivalWorker.Start,
			stop:          svc.archivalWorker.Stop,
			critical:      svc.Config != nil && svc.Config.Archival.Enabled,
			onSoftFailure: func() { svc.archivalWorker = nil },
		})
	}

	if svc.schedulerWorker != nil {
		entries = append(entries, workerStartEntry{
			name:          "scheduler",
			start:         svc.schedulerWorker.Start,
			stop:          svc.schedulerWorker.Stop,
			critical:      false, // scheduler is always non-critical
			onSoftFailure: func() { svc.schedulerWorker = nil },
		})
	}

	if len(entries) == 0 {
		return nil
	}

	collected := startWorkerEntries(ctx, svc.Logger, entries)

	return svc.processWorkerStartResults(ctx, entries, collected)
}

func (svc *Service) processWorkerStartResults(
	ctx context.Context,
	entries []workerStartEntry,
	collected []workerStartResult,
) error {
	startedWorkers := collectStartedWorkers(collected)

	criticalFailures := collectCriticalWorkerFailures(collected)
	if len(criticalFailures) > 0 {
		rollbackErr := svc.stopStartedWorkers(entries, startedWorkers)

		criticalErrors := make([]error, 0, len(criticalFailures))
		for _, criticalFailure := range criticalFailures {
			criticalErrors = append(criticalErrors,
				fmt.Errorf("%s worker enabled but failed to start: %w", criticalFailure.name, criticalFailure.err),
			)
		}

		criticalErr := errors.Join(criticalErrors...)

		if rollbackErr != nil {
			svc.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to rollback started workers after critical startup failures: %v", rollbackErr))

			return errors.Join(criticalErr, fmt.Errorf("rollback started workers: %w", rollbackErr))
		}

		return criticalErr
	}

	// Process non-critical failures: log and disable.
	for _, result := range collected {
		if result.err == nil {
			continue
		}

		svc.Log(ctx, libLog.LevelWarn, fmt.Sprintf("%s worker failed to start (continuing without it): %v", result.name, result.err))

		// Find the matching entry and nil-out the worker.
		for _, entry := range entries {
			if entry.name == result.name {
				if entry.onSoftFailure != nil {
					entry.onSoftFailure()
				}

				break
			}
		}
	}

	return nil
}

func collectStartedWorkers(collected []workerStartResult) map[string]struct{} {
	startedWorkers := make(map[string]struct{}, len(collected))
	for _, result := range collected {
		if result.err == nil {
			startedWorkers[result.name] = struct{}{}
		}
	}

	return startedWorkers
}

func collectCriticalWorkerFailures(collected []workerStartResult) []workerStartResult {
	failures := make([]workerStartResult, 0)

	for _, result := range collected {
		if result.critical && result.err != nil {
			failures = append(failures, result)
		}
	}

	return failures
}

func (svc *Service) stopStartedWorkers(
	entries []workerStartEntry,
	startedWorkers map[string]struct{},
) error {
	var stopErrors []error

	for _, entry := range entries {
		if _, ok := startedWorkers[entry.name]; !ok {
			continue
		}

		if entry.stop != nil {
			if err := entry.stop(); err != nil {
				stopErrors = append(stopErrors, fmt.Errorf("stop %s worker after startup rollback: %w", entry.name, err))
			}
		}

		if entry.onSoftFailure != nil {
			entry.onSoftFailure()
		}
	}

	if len(stopErrors) > 0 {
		return errors.Join(stopErrors...)
	}

	return nil
}

func startWorkerEntries(
	ctx context.Context,
	logger libLog.Logger,
	entries []workerStartEntry,
) []workerStartResult {
	results := make(chan workerStartResult, len(entries))

	for _, entry := range entries {
		runtime.SafeGoWithContextAndComponent(
			ctx,
			logger,
			constants.ApplicationName,
			"worker.start."+entry.name,
			runtime.KeepRunning,
			func(workerCtx context.Context) {
				result := workerStartResult{
					name:     entry.name,
					critical: entry.critical,
				}

				defer func() {
					if recovered := recover(); recovered != nil {
						result.err = fmt.Errorf("panic starting %s worker (%v): %w", entry.name, recovered, errWorkerPanicked)
					}

					results <- result
				}()

				if entry.start == nil {
					result.err = errWorkerStartFuncNil

					return
				}

				result.err = entry.start(workerCtx)
			},
		)
	}

	timeout := resolveWorkerStartWaitTimeout(ctx)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	collected := make([]workerStartResult, 0, len(entries))
	for len(collected) < len(entries) {
		select {
		case result := <-results:
			collected = append(collected, result)
		case <-timer.C:
			return appendMissingWorkerResults(
				entries,
				collected,
				fmt.Errorf("worker start timed out after %v: %w", timeout, errWorkerStartTimeout),
			)
		case <-ctx.Done():
			return appendMissingWorkerResults(
				entries,
				collected,
				fmt.Errorf("%w: %w", errWorkerStartContextCanceled, ctx.Err()),
			)
		}
	}

	return collected
}

func resolveWorkerStartWaitTimeout(ctx context.Context) time.Duration {
	timeout := defaultWorkerStartWaitTimeout

	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}

	if timeout <= 0 {
		return time.Millisecond
	}

	return timeout
}

func appendMissingWorkerResults(
	entries []workerStartEntry,
	collected []workerStartResult,
	err error,
) []workerStartResult {
	if len(collected) >= len(entries) {
		return collected
	}

	received := make(map[string]struct{}, len(collected))
	for _, result := range collected {
		received[result.name] = struct{}{}
	}

	for _, entry := range entries {
		if _, ok := received[entry.name]; ok {
			continue
		}

		collected = append(collected, workerStartResult{
			name:     entry.name,
			critical: entry.critical,
			err:      err,
		})
	}

	return collected
}

// Shutdown gracefully shuts down the service, including the HTTP server and telemetry.
func (svc *Service) Shutdown(ctx context.Context) error {
	if svc == nil {
		return nil
	}

	logger := svc.Logger
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

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
func (svc *Service) stopBackgroundWorkers(ctx context.Context, logger libLog.Logger) {
	if svc.exportWorker != nil {
		if err := svc.exportWorker.Stop(); err != nil { //nolint:contextcheck // Stop() is defined without ctx in worker package
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to stop export worker: %v", err))
		}
	}

	if svc.cleanupWorker != nil {
		if err := svc.cleanupWorker.Stop(); err != nil { //nolint:contextcheck // Stop() is defined without ctx in worker package
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to stop cleanup worker: %v", err))
		}
	}

	if svc.archivalWorker != nil {
		if err := svc.archivalWorker.Stop(); err != nil { //nolint:contextcheck // Stop() is defined without ctx in worker package
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to stop archival worker: %v", err))
		}
	}

	if svc.schedulerWorker != nil {
		if err := svc.schedulerWorker.Stop(); err != nil { //nolint:contextcheck // Stop() is defined without ctx in worker package
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to stop scheduler worker: %v", err))
		}
	}

	if svc.dbMetricsCollector != nil {
		svc.dbMetricsCollector.Stop()
	}

	if svc.outboxRunner != nil {
		if stoppable, ok := svc.outboxRunner.(interface{ Stop() }); ok {
			stoppable.Stop()
		}
	}
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
