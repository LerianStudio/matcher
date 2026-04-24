// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Phase functions invoked by InitServersWithOptions. Each phase takes a
// *bootstrapState and advances it toward a fully wired *Service. Splitting
// the startup sequence into phases keeps InitServersWithOptions below the
// complexity ceiling and isolates the dependency surface of each stage.

package bootstrap

import (
	"context"
	"fmt"
	"os"

	"github.com/bxcodec/dbresolver/v2"
	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/lib-auth/v3/auth/middleware"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// bootstrapState carries values forward through the phases of
// InitServersWithOptions. Each phase reads the fields populated by earlier
// phases and writes the fields it produces. The struct is internal — phases
// are private and only InitServersWithOptions composes them.
type bootstrapState struct {
	// Inputs — set by initConfigStage.
	ctx           context.Context
	opts          *Options
	timer         *startupTimer
	logger        libLog.Logger
	cfg           *Config
	configManager *ConfigManager
	asserter      *assert.Asserter

	// Infrastructure — set by initInfrastructure.
	telemetry              *libOpentelemetry.Telemetry
	postgresConnection     *libPostgres.Client
	redisConnection        *libRedis.Client
	rabbitMQConnection     *libRabbitmq.RabbitMQConnection
	spClient               *systemplane.Client
	settingsResolver       *runtimeSettingsResolver
	cleanups               []func()
	infraConnectionManager connectionCloser

	// Routes / HTTP — set by initServers.
	routes            *Routes
	healthDeps        *HealthDependencies
	app               *fiber.App
	authClient        *middleware.AuthClient
	tenantExtractor   *auth.TenantExtractor
	rateLimiterGetter func() *ratelimit.RateLimiter
	idempotencyRepo   sharedPorts.IdempotencyRepository
	infraProvider     sharedPorts.InfrastructureProvider
	connectionManager connectionCloser
	tenantDBHandler   fiber.Handler

	// Modules — set by initServerModules.
	modules *modulesResult

	// Assembly — set by initServerAssembly.
	dbMetricsCollector    *DBMetricsCollector
	redisMetricsCollector *RedisMetricsCollector
	server                *Server
	workerManager         *WorkerManager
	readiness             *readinessState
}

// initConfigStage normalizes Options, creates the initial logger, loads the
// Config, and constructs the ConfigManager. Also applies process-level
// toggles (production mode, GOMEMLIMIT warning) that depend on Config.
func initConfigStage(opts *Options) (*bootstrapState, error) {
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

	return &bootstrapState{
		ctx:           ctx,
		opts:          opts,
		timer:         timer,
		logger:        logger,
		cfg:           cfg,
		configManager: configManager,
		asserter:      assert.New(ctx, logger, constants.ApplicationName, "bootstrap"),
	}, nil
}

// initInfrastructure runs the infra-connection phases: TLS enforcement,
// telemetry bring-up, Postgres/Redis/RabbitMQ connections, parallel infra
// connect, systemplane client init, and runtime-settings resolver wiring.
// All connection values and cleanup registrations land on the state.
func initInfrastructure(state *bootstrapState) error {
	// Per-stack TLS enforcement. Runs BEFORE any connection opens so a stack
	// flagged X_TLS_REQUIRED=true but configured without TLS cannot produce a
	// silent insecure start. Stacks without the flag set are not enforced.
	// See tls_enforcement.go for the full contract.
	tlsRequiredDone := state.timer.track("tls_required_enforcement")

	if err := ValidateRequiredTLS(state.cfg); err != nil {
		// Close the span on the error path too; otherwise the phase record is
		// never emitted and startup-latency telemetry misses the failure case.
		tlsRequiredDone()

		return fmt.Errorf("bootstrap: tls_required enforcement: %w", err)
	}

	tlsRequiredDone()

	done := state.timer.track("telemetry")

	state.telemetry = initTelemetryAndMetrics(state.ctx, state.cfg, state.logger, state.opts.InfraConnector)

	done()

	done = state.timer.track("client_creation")

	postgresConnection, err := createPostgresConnection(state.cfg, state.logger)
	if err != nil {
		return fmt.Errorf("create postgres connection: %w", err)
	}

	state.postgresConnection = postgresConnection
	state.rabbitMQConnection = createRabbitMQConnection(state.cfg, state.logger)

	done()

	if err := connectInfraWithRedis(state); err != nil {
		return err
	}

	return initSystemplaneStage(state)
}

// connectInfraWithRedis performs the redis + parallel infra connect stage.
// Extracted to keep initInfrastructure within the statement budget.
func connectInfraWithRedis(state *bootstrapState) error {
	infraCtx, infraCancel := context.WithTimeout(state.ctx, state.cfg.InfraConnectTimeout())
	defer infraCancel()

	done := state.timer.track("redis_connect")

	// Redis v2 New() connects immediately. Reuse the global infra startup context
	// so all connection phases share one deadline budget.
	redisConnection, err := createRedisConnection(infraCtx, state.cfg, state.logger)
	if err != nil {
		return fmt.Errorf("create redis connection: %w", err)
	}

	state.redisConnection = redisConnection

	done()

	done = state.timer.track("infra_connect")

	if err := connectInfrastructure(
		infraCtx,
		state.asserter,
		state.cfg,
		state.postgresConnection,
		state.rabbitMQConnection,
		state.logger,
		state.opts.InfraConnector,
	); err != nil {
		return err
	}

	done()

	return nil
}

// initSystemplaneStage wires the systemplane client, settings resolver, and
// its shutdown cleanup. On production environments a systemplane failure is
// fatal; non-production falls back to the static Config from env vars.
func initSystemplaneStage(state *bootstrapState) error {
	done := state.timer.track("systemplane_init")

	// Initialize v5 systemplane client (register keys + start + subscribe).
	// Must happen before settings-resolver consumers (idempotency repo, rate limiter,
	// webhook timeout closure, etc.) to ensure runtime config reaches them.
	// Requires postgres to be connected. In production, failures are fatal because
	// runtime-config is compliance/operational-critical; in non-production we
	// continue with the static Config from env vars.
	primaryDB, dbErr := state.postgresConnection.Primary()

	switch {
	case dbErr != nil:
		if IsProductionEnvironment(state.cfg.App.EnvName) {
			return fmt.Errorf("systemplane init: postgres primary unavailable: %w", dbErr)
		}

		state.logger.Log(state.ctx, libLog.LevelWarn,
			"systemplane skipped (no postgres primary); running with static config only")
	case primaryDB == nil:
		if IsProductionEnvironment(state.cfg.App.EnvName) {
			return errSystemplanePrimaryUnavailable
		}

		state.logger.Log(state.ctx, libLog.LevelWarn,
			"systemplane skipped (no postgres primary); running with static config only")
	default:
		spClient, err := InitSystemplane(state.ctx, state.cfg, primaryDB, state.logger, state.telemetry)
		if err != nil {
			if IsProductionEnvironment(state.cfg.App.EnvName) {
				return fmt.Errorf("systemplane initialization required: %w", err)
			}

			state.logger.Log(state.ctx, libLog.LevelWarn,
				"systemplane initialization failed, continuing with static config",
				libLog.String("error", err.Error()))
		} else {
			state.spClient = spClient
		}
	}

	// Wire OnChange to keep ConfigManager in sync with systemplane writes.
	if state.spClient != nil {
		if watchErr := state.configManager.WatchSystemplane(state.spClient); watchErr != nil {
			state.logger.Log(state.ctx, libLog.LevelWarn,
				"systemplane watch failed, runtime config hot-reload disabled",
				libLog.String("error", watchErr.Error()))
		}
	}

	state.settingsResolver = newRuntimeSettingsResolver(state.spClient)

	// Register systemplane client for graceful shutdown on startup failure.
	// Close is idempotent; the Service also closes spClient on regular shutdown.
	spClient := state.spClient
	logger := state.logger
	ctx := state.ctx
	state.cleanups = append(state.cleanups, func() {
		if spClient != nil {
			if closeErr := spClient.Close(); closeErr != nil {
				logger.Log(ctx, libLog.LevelWarn, "systemplane close failed",
					libLog.String("error", closeErr.Error()))
			}
		}
	})

	done()

	return nil
}

// initServers wires auth, tenant extraction, HTTP routes, modules, archival,
// server assembly (metrics collectors + Server), WorkerManager, and the
// systemplane admin mount. It is the largest phase because route and module
// registration share many shallow dependencies.
func initServers(state *bootstrapState) error {
	if err := initAuthAndRoutes(state); err != nil {
		return err
	}

	if err := initServerModules(state); err != nil {
		return err
	}

	return initServerAssembly(state)
}

// initAuthAndRoutes performs the auth_and_routes phase: authClient, tenant
// extractor, health deps, Fiber app, rate limiter, infra provider, and all
// route registration.
func initAuthAndRoutes(state *bootstrapState) error {
	done := state.timer.track("auth_and_routes")

	state.authClient = createAuthClient(state.ctx, state.cfg, state.logger, state.opts.InfraConnector)

	tenantExtractor, err := buildTenantExtractor(state.cfg)
	if err != nil {
		return err
	}

	state.tenantExtractor = tenantExtractor

	healthDeps, err := createHealthDependencies(
		state.ctx,
		state.cfg,
		state.logger,
		state.postgresConnection,
		state.redisConnection,
		state.rabbitMQConnection,
		&state.cleanups,
		state.opts.InfraConnector,
	)
	if err != nil {
		return fmt.Errorf("create health dependencies: %w", err)
	}

	state.healthDeps = healthDeps

	state.app = NewFiberApp(state.cfg, state.logger, state.telemetry, state.configManager.Get)

	redisConn := state.redisConnection
	rlProvider := newRateLimiterProvider(func() *libRedis.Client {
		return redisConn
	}, state.logger)
	state.rateLimiterGetter = rlProvider.Get

	infraProvider, connectionManager, tenantDBHandler := createInfraProvider(
		state.cfg,
		state.configManager.Get,
		state.postgresConnection,
		state.redisConnection,
	)

	state.infraProvider = infraProvider
	state.infraConnectionManager = connectionManager
	state.connectionManager = connectionManager
	state.tenantDBHandler = tenantDBHandler
	state.readiness = &readinessState{}

	state.idempotencyRepo = createIdempotencyRepository(
		state.cfg,
		state.configManager.Get,
		state.settingsResolver,
		state.infraProvider,
		state.logger,
	)

	// Pass configManager.Get as the dynamic config getter when available.
	// This enables hot-reload of rate limits without service restart.
	routes, err := RegisterRoutes(
		state.app,
		state.cfg,
		configGetterFuncFromManager(state.configManager),
		state.settingsResolver,
		state.readiness,
		state.healthDeps,
		state.logger,
		state.authClient,
		state.tenantExtractor,
		state.rateLimiterGetter,
		state.idempotencyRepo,
		state.tenantDBHandler,
	)
	if err != nil {
		return err
	}

	state.routes = routes

	done()

	return nil
}

// initServerModules runs the modules phase: per-context module wiring plus
// the optional archival components.
func initServerModules(state *bootstrapState) error {
	done := state.timer.track("modules")

	// Build a configGetter for dynamic rate limiters if ConfigManager is available.
	moduleConfigGetter := configGetterFuncFromManager(state.configManager)

	modules, err := initModulesAndMessaging(
		state.ctx,
		state.routes,
		state.cfg,
		moduleConfigGetter,
		state.settingsResolver,
		state.infraProvider,
		state.postgresConnection,
		state.rabbitMQConnection,
		state.rateLimiterGetter,
		state.logger,
		state.opts.InfraConnector,
		state.opts.EventPublishers,
	)
	if err != nil {
		return err
	}

	archivalWorker, archivalErr := initArchivalComponents(
		state.routes,
		state.cfg,
		state.configManager.Get,
		state.settingsResolver,
		state.infraProvider,
		state.logger,
		&state.cleanups,
		IsProductionEnvironment(state.cfg.App.EnvName),
		state.opts.InfraConnector,
	)
	if archivalErr != nil {
		if state.cfg.Archival.Enabled {
			return fmt.Errorf("init archival components: %w", archivalErr)
		}

		state.logger.Log(state.ctx, libLog.LevelWarn,
			fmt.Sprintf("archival components not available (continuing without them): %v", archivalErr))
	}

	modules.archivalWorker = archivalWorker
	state.modules = modules

	done()

	return nil
}

// initServerAssembly runs the server_assembly + systemplane_runtime phases:
// DB/Redis metrics collectors, Server construction, WorkerManager build, and
// the systemplane admin mount.
func initServerAssembly(state *bootstrapState) error {
	done := state.timer.track("server_assembly")

	dbMetricsCollector, err := NewDBMetricsCollector(state.postgresConnection, state.cfg.DBMetricsInterval())
	if err != nil {
		state.logger.Log(state.ctx, libLog.LevelWarn, fmt.Sprintf("Failed to create DB metrics collector: %v", err))
	} else if dbMetricsCollector != nil {
		pg := state.postgresConnection

		dbMetricsCollector.SetResolverGetter(func(ctx context.Context) (dbresolver.DB, error) {
			if pg == nil {
				return nil, ErrNilResolverWithoutError
			}

			return pg.Resolver(ctx)
		})
	}

	state.dbMetricsCollector = dbMetricsCollector

	// Redis pool metrics ride the same cadence as DB pool metrics — operators
	// inspect both together, so splitting the interval would be a knob with no
	// user. A connect failure at bootstrap is non-fatal: the collector is nil
	// and service.Start skips it, preserving the connection-optional contract
	// enforced for integration tests and standalone dev runs.
	redisMetricsCollector, err := NewRedisMetricsCollector(state.redisConnection, state.cfg.DBMetricsInterval())
	if err != nil {
		state.logger.Log(state.ctx, libLog.LevelWarn, fmt.Sprintf("Failed to create Redis metrics collector: %v", err))
	}

	state.redisMetricsCollector = redisMetricsCollector

	state.server = NewServer(
		state.cfg,
		state.app,
		state.logger,
		state.telemetry,
		state.postgresConnection,
		state.redisConnection,
		state.rabbitMQConnection,
	)

	done()

	// WorkerManager is created after modules; systemplane is already active
	// (initialized earlier, before module wiring). Runtime worker updates are
	// applied from the reload observer once the manager exists.
	done = state.timer.track("systemplane_runtime")

	wm := NewWorkerManager(state.logger, state.configManager)
	wm.SetInfraConnector(state.opts.InfraConnector)
	state.workerManager = buildWorkerManager(state.modules, wm, state.configManager, state.logger)

	// Mount systemplane admin HTTP routes.
	// spClient was initialized earlier; if nil, MountSystemplaneAPI is a graceful
	// no-op. Any other failure (missing tenant extractor, nil app) is fatal —
	// we must not continue bootstrap with the admin plane partially wired or
	// the /system surface running without its guard chain.
	if mountErr := MountSystemplaneAPI(
		state.app,
		state.spClient,
		state.cfg,
		state.configManager.Get,
		state.settingsResolver,
		state.authClient,
		state.tenantExtractor,
		state.rateLimiterGetter,
		state.logger,
	); mountErr != nil {
		return fmt.Errorf("mount systemplane api: %w", mountErr)
	}

	done()

	return nil
}

// assembleService composes the final *Service from the fully-populated
// bootstrapState. Runs after all phases succeed; does not return an error
// because there are no more fallible operations — only wiring.
func assembleService(state *bootstrapState) *Service {
	infraStatus := buildInfraStatus(
		state.cfg,
		state.postgresConnection,
		state.redisConnection,
		state.rabbitMQConnection,
		state.modules,
		state.healthDeps,
		state.telemetry,
	)
	logStartupInfo(state.logger, state.cfg, infraStatus)
	logStartupTiming(state.logger, state.timer)

	// Startup self-probe. Flips selfProbeOK atomically after confirming every
	// required dependency. A probe failure is logged but does NOT abort
	// startup — the /health endpoint returns 503 until the flag flips, and
	// the K8s livenessProbe restarts the pod if the condition persists.
	// Keeping startup non-abortive preserves log collection for post-mortem.
	selfProbeDone := state.timer.track("self_probe")

	runStartupSelfProbe(state.ctx, state.healthDeps, state.logger, RunSelfProbe)

	selfProbeDone()

	// Register ConfigManager Stop() in cleanups so resources are torn down on shutdown.
	if state.configManager != nil {
		state.cleanups = append(state.cleanups, state.configManager.Stop)
	}

	return &Service{
		Server:                state.server,
		Logger:                state.logger,
		Config:                state.cfg,
		Routes:                state.routes,
		ConfigManager:         state.configManager,
		outboxRunner:          state.modules.outboxDispatcher,
		dbMetricsCollector:    state.dbMetricsCollector,
		redisMetricsCollector: state.redisMetricsCollector,
		workerManager:         state.workerManager,
		connectionManager:     state.connectionManager,
		cleanupFuncs:          state.cleanups,
		readinessState:        state.readiness,
		spClient:              state.spClient,
	}
}
