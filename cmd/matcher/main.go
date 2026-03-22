// Package main is the entry point for the matcher reconciliation service.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"

	"github.com/LerianStudio/matcher/internal/bootstrap"
)

const gracefulShutdownTimeout = 30 * time.Second

const loggerSyncTimeout = 2 * time.Second

type matcherService interface {
	Run() error
	Shutdown(ctx context.Context) error
}

var (
	newLogger = func(cfg libZap.Config) (libLog.Logger, error) {
		return libZap.New(cfg)
	}

	initLocalEnvConfig = func() {
		libCommons.InitLocalEnvConfig()
	}

	initMatcherService = func(opts *bootstrap.Options) (matcherService, error) {
		return bootstrap.InitServersWithOptions(opts)
	}

	notifySignalContext = signal.NotifyContext
)

// @title Matcher Reconciliation API
// @version v1.0.0
// @description Reconciliation engine for the Lerian Studio ecosystem.
// @description Provides automated transaction matching between Midaz ledger and external systems.
// @termsOfService http://swagger.io/terms/
// @contact.name Lerian Studio Support
// @contact.url https://discord.gg/DnhqKwkGv3
// @contact.email support@lerian.studio
// @license.name Lerian Studio General License
// @license.url https://github.com/LerianStudio/matcher/blob/main/LICENSE.md
// @host
// @BasePath /
// @schemes https
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description Bearer token authentication (format: "Bearer {token}")
//
// @tag.name Configuration Contexts
// @tag.description Reconciliation context management - create, update, delete contexts
// @tag.name Configuration Sources
// @tag.description Reconciliation source management - external data sources to match against
// @tag.name Configuration Field Maps
// @tag.description Field mapping configuration - define how source fields map to transactions
// @tag.name Configuration Match Rules
// @tag.description Match rule configuration - define matching algorithms and tolerances
// @tag.name Configuration Schedules
// @tag.description Scheduled reconciliation job management
// @tag.name Configuration Fee Schedules
// @tag.description Fee schedule and simulation configuration
// @tag.name Ingestion
// @tag.description File upload and import job management
// @tag.name Matching
// @tag.description Transaction matching execution and results
// @tag.name Exception
// @tag.description Exception lifecycle, disputes, evidence, and resolutions
// @tag.name Export Jobs
// @tag.description Async report export job management
// @tag.name Reporting
// @tag.description Dashboard analytics, metrics, and synchronous report exports
// @tag.name Governance
// @tag.description Immutable audit logs and archive retrieval
// @tag.name Health
// @tag.description Service health and readiness endpoints for Kubernetes probes
// @tag.name System Configs
// @tag.description Runtime configuration management - view, patch, and audit system configs
// @tag.name System Settings
// @tag.description Tenant-scoped settings management - view, patch, and audit system settings
func main() {
	os.Exit(run())
}

// run executes the main application logic and returns the exit code.
// This separation allows deferred functions in run() to execute before os.Exit.
func run() int {
	initLocalEnvConfig()

	ctx := context.Background()
	isProduction := bootstrap.IsProductionEnvironment(os.Getenv("ENV_NAME"))

	logger, err := newLogger(libZap.Config{
		Environment:     bootstrap.ResolveLoggerEnvironment(os.Getenv("ENV_NAME")),
		Level:           bootstrap.ResolveLoggerLevel(os.Getenv("LOG_LEVEL")),
		OTelLibraryName: "github.com/LerianStudio/matcher",
	})
	if err != nil {
		libLog.SafeError(nil, ctx, "Failed to initialize logger", err, isProduction)

		return 1
	}

	service, err := initMatcherService(&bootstrap.Options{
		Logger: logger,
	})
	if err != nil {
		libLog.SafeError(logger, ctx, "Failed to initialize matcher service", err, isProduction)

		syncCtx, syncCancel := context.WithTimeout(context.Background(), loggerSyncTimeout)
		defer syncCancel()

		_ = logger.Sync(syncCtx)

		return 1
	}

	// Create cancellable context for shutdown propagation
	ctx, cancel := notifySignalContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	errChan := make(chan error, 1)

	runtime.SafeGoWithContextAndComponent(
		ctx,
		logger,
		"bootstrap",
		"server-runner",
		runtime.CrashProcess,
		func(context.Context) {
			errChan <- service.Run()
		},
	)

	select {
	case <-ctx.Done():
		logger.Log(ctx, libLog.LevelInfo, "Received shutdown signal, initiating graceful shutdown...")
	case err := <-errChan:
		if err != nil {
			libLog.SafeError(logger, ctx, "Server error", err, isProduction)
		}
	}

	exitCode := 0

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		gracefulShutdownTimeout,
	)
	defer shutdownCancel()

	if err := service.Shutdown(shutdownCtx); err != nil {
		libLog.SafeError(logger, shutdownCtx, "Graceful shutdown failed", err, isProduction)

		exitCode = 1
	} else {
		logger.Log(ctx, libLog.LevelInfo, "Matcher service stopped gracefully")
	}

	syncCtx, syncCancel := context.WithTimeout(context.Background(), loggerSyncTimeout)
	defer syncCancel()

	_ = logger.Sync(syncCtx)

	return exitCode
}
