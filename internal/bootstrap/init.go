// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// Direct OTel imports are required for infrastructure-level meter/tracer setup.
// otel.Meter() and otel.Tracer() create named instruments for cleanup metrics
// and outbox/archival tracers. attribute/metric types are needed for metric
// recording. lib-commons does not abstract global provider accessors.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

const (
	healthConnMaxLifetime = 5 * time.Minute
	minPerServiceTimeout  = 5 * time.Second

	// archivalMaxOpenConns is the max open connections for the dedicated archival DB pool.
	// Low count because archival runs sequentially with long-lived transactions.
	archivalMaxOpenConns = 3

	// archivalMaxIdleConns is the max idle connections for the dedicated archival DB pool.
	archivalMaxIdleConns = 1

	// infraConnectTimeoutDivisor splits the total infra connect timeout evenly between
	// the two parallel infrastructure goroutines (Postgres and RabbitMQ).
	infraConnectTimeoutDivisor = 2

	// statusSuccess and statusError are metric attribute values for cleanup recording.
	statusSuccess = "success"
	statusError   = "error"
)

var (
	// ErrObjectStorageBucketRequired is returned when export worker is enabled but bucket is not configured.
	ErrObjectStorageBucketRequired = errors.New(
		"OBJECT_STORAGE_BUCKET is required when EXPORT_WORKER_ENABLED=true",
	)

	// ErrArchivalStorageRequired is returned when archival worker is enabled but storage is not configured.
	ErrArchivalStorageRequired = errors.New("archival storage is required when ARCHIVAL_WORKER_ENABLED=true")

	// ErrAuditPublisherRequired is returned when the system starts without audit publishing capability.
	// Audit events are compliance-critical (SOX) and must never be silently dropped.
	ErrAuditPublisherRequired = errors.New("audit publisher is required: compliance-critical audit events must not be dropped")

	errPostgresClientRequired   = errors.New("postgres client is required")
	errRabbitMQClientRequired   = errors.New("rabbitmq connection is required")
	errPostgresResolverRequired = errors.New("postgres resolver is nil")
	errAuthBoundaryLoggerNil    = errors.New("auth boundary logger is nil")

	// errSystemplanePrimaryUnavailable indicates the postgres primary handle
	// returned nil without a concrete error, blocking systemplane init in
	// production environments where runtime config is compliance-critical.
	errSystemplanePrimaryUnavailable = errors.New("systemplane init: postgres primary unavailable")
)

// tenantExtractorAdapter adapts auth.GetTenantID to the TenantExtractor interface.
type tenantExtractorAdapter struct{}

// GetTenantID extracts the tenant ID from context using the auth package.
func (t *tenantExtractorAdapter) GetTenantID(ctx context.Context) uuid.UUID {
	tenantIDStr := auth.GetTenantID(ctx)

	id, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return uuid.Nil
	}

	return id
}

func buildTenantExtractor(cfg *Config) (*auth.TenantExtractor, error) {
	if err := auth.SetDefaultTenantID(cfg.Tenancy.DefaultTenantID); err != nil {
		return nil, fmt.Errorf("set default tenant id: %w", err)
	}

	if err := auth.SetDefaultTenantSlug(cfg.Tenancy.DefaultTenantSlug); err != nil {
		return nil, fmt.Errorf("set default tenant slug: %w", err)
	}

	extractor, err := auth.NewTenantExtractor(
		cfg.Auth.Enabled,
		cfg.Tenancy.MultiTenantEnabled,
		cfg.Tenancy.DefaultTenantID,
		cfg.Tenancy.DefaultTenantSlug,
		cfg.Auth.TokenSecret,
		cfg.App.EnvName,
	)
	if err != nil {
		return nil, fmt.Errorf("create tenant extractor: %w", err)
	}

	return extractor, nil
}

// InitServersWithOptions initializes and returns the complete Matcher service with custom options.
//
//nolint:cyclop,gocyclo,gocognit // Bootstrap wiring exceeds complexity limits; keep explicit for readability.
func InitServersWithOptions(opts *Options) (*Service, error) {
	ctx := context.Background()
	timer := newStartupTimer()

	// Normalize factories on Options so every downstream helper receives a
	// non-nil InfraConnector / EventPublisherFactory. Tests may supply fakes;
	// production calls hit the default implementations.
	opts = opts.ensureDefaults()

	done := timer.track("logger")

	logger, err := initLogger(opts)
	if err != nil {
		return nil, fmt.Errorf("initialize logger: %w", err)
	}

	done()

	configDone := timer.track("config")

	cfg, err := LoadConfigWithLogger(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	configManager, err := NewConfigManager(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("initialize config manager: %w", err)
	}

	if managedCfg := configManager.Get(); managedCfg != nil {
		cfg = managedCfg
	}

	var (
		modules  *modulesResult
		wm       = NewWorkerManager(logger, configManager)
		spClient *systemplane.Client
	)

	wm.SetInfraConnector(opts.InfraConnector)

	// Configure runtime for production mode (redacts sensitive data in error reports)
	if IsProductionEnvironment(cfg.App.EnvName) {
		runtime.SetProductionMode(true)
	}

	// Emit a one-time startup warning when the process appears to run inside
	// a container without GOMEMLIMIT configured. Without GOMEMLIMIT the Go
	// runtime defaults its soft memory limit to math.MaxInt64 — which, on
	// cgroup-capped pods, lets the heap grow unbounded until the kernel OOM
	// killer intervenes. The Fetcher bridge sets GOMEMLIMIT itself when it
	// initializes; this warning is the companion for non-Fetcher deploys.
	warnOnMissingGOMEMLIMIT(ctx, logger, defaultMemoryLimitReader, os.Getenv("GOMEMLIMIT"))

	configDone()

	// Per-stack TLS enforcement. Runs BEFORE any connection opens so a stack
	// flagged X_TLS_REQUIRED=true but configured without TLS cannot produce a
	// silent insecure start. Stacks without the flag set are not enforced.
	// See tls_enforcement.go for the full contract.
	tlsRequiredDone := timer.track("tls_required_enforcement")

	if err := ValidateRequiredTLS(cfg); err != nil {
		// Close the span on the error path too; otherwise the phase record is
		// never emitted and startup-latency telemetry misses the failure case.
		tlsRequiredDone()

		return nil, fmt.Errorf("bootstrap: tls_required enforcement: %w", err)
	}

	tlsRequiredDone()

	done = timer.track("telemetry")

	asserter := assert.New(ctx, logger, constants.ApplicationName, "bootstrap")

	telemetry := initTelemetryAndMetrics(ctx, cfg, logger, opts.InfraConnector)

	done()

	done = timer.track("client_creation")

	postgresConnection, err := createPostgresConnection(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create postgres connection: %w", err)
	}

	rabbitMQConnection := createRabbitMQConnection(cfg, logger)

	done()

	infraCtx, infraCancel := context.WithTimeout(ctx, cfg.InfraConnectTimeout())
	defer infraCancel()

	done = timer.track("redis_connect")

	// Redis v2 New() connects immediately. Reuse the global infra startup context
	// so all connection phases share one deadline budget.
	redisConnection, err := createRedisConnection(infraCtx, cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create redis connection: %w", err)
	}

	done()

	// Cleanup accumulator: collects cleanup functions to run on failure
	var cleanups []func()

	var infraConnectionManager connectionCloser

	runCleanups := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	// Track success to skip cleanup on successful startup
	success := false

	defer func() {
		if success {
			return
		}

		runCleanups()

		if infraConnectionManager != nil {
			logCloseErr(ctx, logger, "failed to close connection manager", infraConnectionManager.Close)
		}

		cleanupConnections(ctx, postgresConnection, redisConnection, rabbitMQConnection, logger)
	}()

	done = timer.track("infra_connect")

	if err := connectInfrastructure(infraCtx, asserter, cfg, postgresConnection, rabbitMQConnection, logger, opts.InfraConnector); err != nil {
		return nil, err
	}

	done()

	// Initialize v5 systemplane client (register keys + start + subscribe).
	// Must happen before settings-resolver consumers (idempotency repo, rate limiter,
	// webhook timeout closure, etc.) to ensure runtime config reaches them.
	// Requires postgres to be connected. In production, failures are fatal because
	// runtime-config is compliance/operational-critical; in non-production we
	// continue with the static Config from env vars.
	done = timer.track("systemplane_init")

	primaryDB, dbErr := postgresConnection.Primary()
	switch {
	case dbErr != nil:
		if IsProductionEnvironment(cfg.App.EnvName) {
			return nil, fmt.Errorf("systemplane init: postgres primary unavailable: %w", dbErr)
		}

		logger.Log(ctx, libLog.LevelWarn, "systemplane skipped (no postgres primary); running with static config only")
	case primaryDB == nil:
		if IsProductionEnvironment(cfg.App.EnvName) {
			return nil, errSystemplanePrimaryUnavailable
		}

		logger.Log(ctx, libLog.LevelWarn, "systemplane skipped (no postgres primary); running with static config only")
	default:
		spClient, err = InitSystemplane(ctx, cfg, primaryDB, logger, telemetry)
		if err != nil {
			if IsProductionEnvironment(cfg.App.EnvName) {
				return nil, fmt.Errorf("systemplane initialization required: %w", err)
			}

			logger.Log(ctx, libLog.LevelWarn, "systemplane initialization failed, continuing with static config",
				libLog.String("error", err.Error()))

			spClient = nil
		}
	}

	// Wire OnChange to keep ConfigManager in sync with systemplane writes.
	if spClient != nil {
		if watchErr := configManager.WatchSystemplane(spClient); watchErr != nil {
			logger.Log(ctx, libLog.LevelWarn, "systemplane watch failed, runtime config hot-reload disabled",
				libLog.String("error", watchErr.Error()))
		}
	}

	settingsResolver := newRuntimeSettingsResolver(spClient)

	// Register systemplane client for graceful shutdown on startup failure.
	// Close is idempotent; the Service also closes spClient on regular shutdown.
	cleanups = append(cleanups, func() {
		if spClient != nil {
			if closeErr := spClient.Close(); closeErr != nil {
				logger.Log(ctx, libLog.LevelWarn, "systemplane close failed",
					libLog.String("error", closeErr.Error()))
			}
		}
	})

	done()

	done = timer.track("auth_and_routes")

	authClient := createAuthClient(ctx, cfg, logger, opts.InfraConnector)

	tenantExtractor, err := buildTenantExtractor(cfg)
	if err != nil {
		return nil, err
	}

	healthDeps, err := createHealthDependencies(
		ctx,
		cfg,
		logger,
		postgresConnection,
		redisConnection,
		rabbitMQConnection,
		&cleanups,
		opts.InfraConnector,
	)
	if err != nil {
		return nil, fmt.Errorf("create health dependencies: %w", err)
	}

	app := NewFiberApp(cfg, logger, telemetry, configManager.Get)

	rlProvider := newRateLimiterProvider(func() *libRedis.Client {
		return redisConnection
	}, logger)
	rateLimiterGetter := rlProvider.Get

	infraProvider, connectionManager, tenantDBHandler := createInfraProvider(
		cfg,
		configManager.Get,
		postgresConnection,
		redisConnection,
	)

	infraConnectionManager = connectionManager
	readiness := &readinessState{}

	connCloser := connectionManager

	idempotencyRepo := createIdempotencyRepository(cfg, configManager.Get, settingsResolver, infraProvider, logger)

	// Pass configManager.Get as the dynamic config getter when available.
	// This enables hot-reload of rate limits without service restart.
	routeConfigGetter := configGetterFuncFromManager(configManager)

	routes, err := RegisterRoutes(
		app,
		cfg,
		routeConfigGetter,
		settingsResolver,
		readiness,
		healthDeps,
		logger,
		authClient,
		tenantExtractor,
		rateLimiterGetter,
		idempotencyRepo,
		tenantDBHandler,
	)
	if err != nil {
		return nil, err
	}

	done()

	done = timer.track("modules")

	// Build a configGetter for dynamic rate limiters if ConfigManager is available.
	moduleConfigGetter := configGetterFuncFromManager(configManager)

	modules, err = initModulesAndMessaging(
		ctx,
		routes,
		cfg,
		moduleConfigGetter,
		settingsResolver,
		infraProvider,
		postgresConnection,
		rabbitMQConnection,
		rateLimiterGetter,
		logger,
		opts.InfraConnector,
		opts.EventPublishers,
	)
	if err != nil {
		return nil, err
	}

	archivalWorker, archivalErr := initArchivalComponents(routes, cfg, configManager.Get, settingsResolver, infraProvider, logger, &cleanups, IsProductionEnvironment(cfg.App.EnvName), opts.InfraConnector)
	if archivalErr != nil {
		if cfg.Archival.Enabled {
			return nil, fmt.Errorf("init archival components: %w", archivalErr)
		}

		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("archival components not available (continuing without them): %v", archivalErr))
	}

	modules.archivalWorker = archivalWorker

	done()

	done = timer.track("server_assembly")

	dbMetricsCollector, err := NewDBMetricsCollector(postgresConnection, cfg.DBMetricsInterval())
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Failed to create DB metrics collector: %v", err))
	} else if dbMetricsCollector != nil {
		dbMetricsCollector.SetResolverGetter(func(ctx context.Context) (dbresolver.DB, error) {
			if postgresConnection == nil {
				return nil, ErrNilResolverWithoutError
			}

			return postgresConnection.Resolver(ctx)
		})
	}

	// Redis pool metrics ride the same cadence as DB pool metrics — operators
	// inspect both together, so splitting the interval would be a knob with no
	// user. A connect failure at bootstrap is non-fatal: the collector is nil
	// and service.Start skips it, preserving the connection-optional contract
	// enforced for integration tests and standalone dev runs.
	redisMetricsCollector, err := NewRedisMetricsCollector(redisConnection, cfg.DBMetricsInterval())
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Failed to create Redis metrics collector: %v", err))
	}

	server := NewServer(
		cfg,
		app,
		logger,
		telemetry,
		postgresConnection,
		redisConnection,
		rabbitMQConnection,
	)

	done()

	// WorkerManager is created after modules; systemplane is already active
	// (initialized earlier, before module wiring). Runtime worker updates are
	// applied from the reload observer once the manager exists.
	done = timer.track("systemplane_runtime")

	wm = buildWorkerManager(modules, wm, configManager, logger)

	// Mount systemplane admin HTTP routes.
	// spClient was initialized earlier; if nil, MountSystemplaneAPI is a graceful
	// no-op. Any other failure (missing tenant extractor, nil app) is fatal —
	// we must not continue bootstrap with the admin plane partially wired or
	// the /system surface running without its guard chain.
	if mountErr := MountSystemplaneAPI(app, spClient, cfg, configManager.Get, settingsResolver, authClient, tenantExtractor, rateLimiterGetter, logger); mountErr != nil {
		return nil, fmt.Errorf("mount systemplane api: %w", mountErr)
	}

	done()

	infraStatus := buildInfraStatus(cfg, postgresConnection, redisConnection, rabbitMQConnection, modules, healthDeps, telemetry)
	logStartupInfo(logger, cfg, infraStatus)
	logStartupTiming(logger, timer)

	// Startup self-probe. Flips selfProbeOK atomically after confirming every
	// required dependency. A probe failure is logged but does NOT abort
	// startup — the /health endpoint returns 503 until the flag flips, and
	// the K8s livenessProbe restarts the pod if the condition persists.
	// Keeping startup non-abortive preserves log collection for post-mortem.
	selfProbeDone := timer.track("self_probe")

	runStartupSelfProbe(ctx, healthDeps, logger, RunSelfProbe)

	selfProbeDone()

	// Register ConfigManager Stop() in cleanups so resources are torn down on shutdown.
	if configManager != nil {
		cleanups = append(cleanups, configManager.Stop)
	}

	success = true

	return &Service{
		Server:                server,
		Logger:                logger,
		Config:                cfg,
		Routes:                routes,
		ConfigManager:         configManager,
		outboxRunner:          modules.outboxDispatcher,
		dbMetricsCollector:    dbMetricsCollector,
		redisMetricsCollector: redisMetricsCollector,
		workerManager:         wm,
		connectionManager:     connCloser,
		cleanupFuncs:          cleanups,
		readinessState:        readiness,
		spClient:              spClient,
	}, nil
}

// configGetterFuncFromManager returns the ConfigManager's Get function for use as
// a dynamic config getter, or nil if the manager is unavailable.
func configGetterFuncFromManager(configManager *ConfigManager) func() *Config {
	if configManager == nil {
		return nil
	}

	return configManager.Get
}

func initLogger(opts *Options) (libLog.Logger, error) {
	if opts != nil && opts.Logger != nil {
		return opts.Logger, nil
	}

	loggerBundle, err := buildLoggerBundle(os.Getenv("ENV_NAME"), os.Getenv("LOG_LEVEL"))
	if err != nil {
		return nil, fmt.Errorf("initialize logger: %w", err)
	}

	return loggerBundle.Logger, nil
}
