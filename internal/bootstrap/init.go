// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	// Direct OTel imports required for infrastructure-level meter/tracer setup.
	// otel.Meter() and otel.Tracer() create named instruments for cleanup metrics
	// and outbox/archival tracers. attribute/metric types are needed for metric
	// recording. lib-commons does not abstract global provider accessors.
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/LerianStudio/lib-auth/v2/auth/middleware"
	libCommonsLog "github.com/LerianStudio/lib-commons/v2/commons/log"
	libCommonsZap "github.com/LerianStudio/lib-commons/v2/commons/zap"
	"github.com/LerianStudio/lib-commons/v4/commons/assert"
	"github.com/LerianStudio/lib-commons/v4/commons/errgroup"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/net/http/ratelimit"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"

	"github.com/LerianStudio/matcher/internal/auth"
	configAudit "github.com/LerianStudio/matcher/internal/configuration/adapters/audit"
	configHTTP "github.com/LerianStudio/matcher/internal/configuration/adapters/http"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configScheduleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/schedule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	configQuery "github.com/LerianStudio/matcher/internal/configuration/services/query"
	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	exceptionAdapters "github.com/LerianStudio/matcher/internal/exception/adapters"
	exceptionAudit "github.com/LerianStudio/matcher/internal/exception/adapters/audit"
	exceptionHTTP "github.com/LerianStudio/matcher/internal/exception/adapters/http"
	exceptionConnectors "github.com/LerianStudio/matcher/internal/exception/adapters/http/connectors"
	exceptionCommentRepo "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/comment"
	exceptionDisputeRepo "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/dispute"
	exceptionExceptionRepo "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/exception"
	exceptionRedis "github.com/LerianStudio/matcher/internal/exception/adapters/redis"
	exceptionResolution "github.com/LerianStudio/matcher/internal/exception/adapters/resolution"
	exceptionCommand "github.com/LerianStudio/matcher/internal/exception/services/command"
	exceptionQuery "github.com/LerianStudio/matcher/internal/exception/services/query"
	governanceAudit "github.com/LerianStudio/matcher/internal/governance/adapters/audit"
	governanceHTTP "github.com/LerianStudio/matcher/internal/governance/adapters/http"
	governancePostgres "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	actorMappingRepoAdapter "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/actor_mapping"
	archiveMetadataRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/archive_metadata"
	governanceCommand "github.com/LerianStudio/matcher/internal/governance/services/command"
	governanceQuery "github.com/LerianStudio/matcher/internal/governance/services/query"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	ingestionHTTP "github.com/LerianStudio/matcher/internal/ingestion/adapters/http"
	ingestionParser "github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTransactionRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	ingestionRedis "github.com/LerianStudio/matcher/internal/ingestion/adapters/redis"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	ingestionQuery "github.com/LerianStudio/matcher/internal/ingestion/services/query"
	matchingHTTP "github.com/LerianStudio/matcher/internal/matching/adapters/http"
	matchAdjustmentRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/adjustment"
	matchExceptionRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/exception_creator"
	matchFeeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	matchFeeVarianceRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_variance"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchItemRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_item"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchRateRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/rate"
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	matchLockManager "github.com/LerianStudio/matcher/internal/matching/adapters/redis"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	matchingQuery "github.com/LerianStudio/matcher/internal/matching/services/query"
	outboxPgRepo "github.com/LerianStudio/matcher/internal/outbox/adapters/postgres"
	outboxRepositories "github.com/LerianStudio/matcher/internal/outbox/domain/repositories"
	outboxServices "github.com/LerianStudio/matcher/internal/outbox/services"
	reportingHTTP "github.com/LerianStudio/matcher/internal/reporting/adapters/http"
	reportDashboard "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/dashboard"
	reportExportJob "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/export_job"
	reportRepo "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/report"
	reportingRedis "github.com/LerianStudio/matcher/internal/reporting/adapters/redis"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	reportingPorts "github.com/LerianStudio/matcher/internal/reporting/ports"
	reportingCommand "github.com/LerianStudio/matcher/internal/reporting/services/command"
	reportingQuery "github.com/LerianStudio/matcher/internal/reporting/services/query"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedHTTP "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
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

	// cleanupMetrics holds initialized metrics for cleanup operations.
	// Lazily initialized on first cleanup call.
	cleanupMetrics     *cleanupMetricsCollector
	cleanupMetricsOnce sync.Once

	runMigrationsFn = RunMigrations

	eventPublisherFnMu sync.RWMutex

	connectPostgresFn = func(ctx context.Context, postgres *libPostgres.Client) error {
		return postgres.Connect(ctx)
	}

	ensureRabbitChannelFn = func(rabbitmq *libRabbitmq.RabbitMQConnection) error {
		return rabbitmq.EnsureChannel()
	}

	openDedicatedChannelFn = openDedicatedChannel

	newMatchingEventPublisherFromChannelFn = matchingRabbitmq.NewEventPublisherFromChannel

	newIngestionEventPublisherFromChannelFn = ingestionRabbitmq.NewEventPublisherFromChannel

	closeAMQPChannelFn = func(ch *amqp.Channel) error {
		if ch == nil {
			return nil
		}

		return ch.Close()
	}

	closeMatchingEventPublisherFn = func(publisher *matchingRabbitmq.EventPublisher) error {
		if publisher == nil {
			return nil
		}

		return publisher.Close()
	}

	closeIngestionEventPublisherFn = func(publisher *ingestionRabbitmq.EventPublisher) error {
		if publisher == nil {
			return nil
		}

		return publisher.Close()
	}

	initializeAuthBoundaryLoggerFn = libCommonsZap.InitializeLoggerWithError

	newS3ClientFn = reportingStorage.NewS3Client
)

func loadOpenDedicatedChannelFn() func(*libRabbitmq.RabbitMQConnection) (*amqp.Channel, error) {
	eventPublisherFnMu.RLock()
	defer eventPublisherFnMu.RUnlock()

	if openDedicatedChannelFn == nil {
		return openDedicatedChannel
	}

	return openDedicatedChannelFn
}

func loadNewMatchingEventPublisherFromChannelFn() func(
	*amqp.Channel,
	...sharedRabbitmq.ConfirmablePublisherOption,
) (*matchingRabbitmq.EventPublisher, error) {
	eventPublisherFnMu.RLock()
	defer eventPublisherFnMu.RUnlock()

	if newMatchingEventPublisherFromChannelFn == nil {
		return matchingRabbitmq.NewEventPublisherFromChannel
	}

	return newMatchingEventPublisherFromChannelFn
}

func loadNewIngestionEventPublisherFromChannelFn() func(
	*amqp.Channel,
	...sharedRabbitmq.ConfirmablePublisherOption,
) (*ingestionRabbitmq.EventPublisher, error) {
	eventPublisherFnMu.RLock()
	defer eventPublisherFnMu.RUnlock()

	if newIngestionEventPublisherFromChannelFn == nil {
		return ingestionRabbitmq.NewEventPublisherFromChannel
	}

	return newIngestionEventPublisherFromChannelFn
}

func loadCloseAMQPChannelFn() func(*amqp.Channel) error {
	eventPublisherFnMu.RLock()
	defer eventPublisherFnMu.RUnlock()

	if closeAMQPChannelFn == nil {
		return func(ch *amqp.Channel) error {
			if ch == nil {
				return nil
			}

			return ch.Close()
		}
	}

	return closeAMQPChannelFn
}

func loadCloseMatchingEventPublisherFn() func(*matchingRabbitmq.EventPublisher) error {
	eventPublisherFnMu.RLock()
	defer eventPublisherFnMu.RUnlock()

	if closeMatchingEventPublisherFn == nil {
		return func(publisher *matchingRabbitmq.EventPublisher) error {
			if publisher == nil {
				return nil
			}

			return publisher.Close()
		}
	}

	return closeMatchingEventPublisherFn
}

func loadCloseIngestionEventPublisherFn() func(*ingestionRabbitmq.EventPublisher) error {
	eventPublisherFnMu.RLock()
	defer eventPublisherFnMu.RUnlock()

	if closeIngestionEventPublisherFn == nil {
		return func(publisher *ingestionRabbitmq.EventPublisher) error {
			if publisher == nil {
				return nil
			}

			return publisher.Close()
		}
	}

	return closeIngestionEventPublisherFn
}

// cleanupMetricsCollector tracks cleanup operation metrics.
type cleanupMetricsCollector struct {
	cleanupTotal    metric.Int64Counter
	cleanupDuration metric.Float64Histogram
}

// initCleanupMetrics initializes cleanup metrics (idempotent via sync.Once).
// Attempts to create both metrics independently; if one fails, the other may still succeed.
// Partial metrics are collected when possible rather than failing completely.
func initCleanupMetrics() *cleanupMetricsCollector {
	cleanupMetricsOnce.Do(func() {
		meter := otel.Meter("matcher.bootstrap.cleanup")

		var total metric.Int64Counter

		var duration metric.Float64Histogram

		var totalErr, durationErr error

		total, totalErr = meter.Int64Counter("bootstrap.cleanup.total",
			metric.WithDescription("Total cleanup operations by resource and status"),
			metric.WithUnit("{operation}"))
		if totalErr != nil {
			otel.Handle(fmt.Errorf("failed to create cleanup.total counter: %w", totalErr))
		}

		duration, durationErr = meter.Float64Histogram("bootstrap.cleanup.duration_seconds",
			metric.WithDescription("Duration of cleanup operations"),
			metric.WithUnit("s"))
		if durationErr != nil {
			otel.Handle(fmt.Errorf("failed to create cleanup.duration_seconds histogram: %w", durationErr))
		}

		// Construct collector with whatever metrics succeeded (nil values are handled by recordCleanup)
		cleanupMetrics = &cleanupMetricsCollector{
			cleanupTotal:    total,
			cleanupDuration: duration,
		}
	})

	return cleanupMetrics
}

// recordCleanup records a cleanup operation metric.
// Falls back to background context if the provided context is nil or cancelled,
// which is common during shutdown scenarios where metrics must still be recorded.
// Handles nil metric fields gracefully when partial metrics collection is in use.
//
//nolint:contextcheck // Intentional fallback to background context during shutdown
func recordCleanup(ctx context.Context, resource string, success bool, duration time.Duration) {
	metrics := initCleanupMetrics()
	if metrics == nil {
		return
	}

	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}

	status := "success"
	if !success {
		status = "error"
	}

	attrs := []attribute.KeyValue{
		attribute.String("resource", resource),
		attribute.String("status", status),
	}

	if metrics.cleanupTotal != nil {
		metrics.cleanupTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	}

	if metrics.cleanupDuration != nil {
		metrics.cleanupDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	}
}

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
	extractor, err := auth.NewTenantExtractor(
		cfg.Auth.Enabled,
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

	done := timer.track("logger")

	logger, err := initLogger(opts)
	if err != nil {
		return nil, fmt.Errorf("initialize logger: %w", err)
	}

	done()

	done = timer.track("config")

	cfg, err := LoadConfigWithLogger(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	configFilePath := resolveConfigFilePath()

	configManager, err := NewConfigManager(cfg, configFilePath, logger)
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

	done()

	done = timer.track("telemetry")

	asserter := assert.New(ctx, logger, constants.ApplicationName, "bootstrap")

	// Use timeout-protected telemetry init to prevent hanging when the
	// OTEL collector is unreachable. Falls back to no-op providers on timeout.
	telemetryCtx, telemetryCancel := context.WithTimeout(ctx, cfg.InfraConnectTimeout())

	telemetry := InitTelemetryWithTimeout(telemetryCtx, cfg, logger)

	telemetryCancel()

	if telemetry != nil {
		assert.InitAssertionMetrics(telemetry.MetricsFactory)
		runtime.InitPanicMetrics(telemetry.MetricsFactory)
	}

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

	var infraConnectionManager *tenantAdapters.TenantConnectionManager

	runCleanups := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	// Track success to skip cleanup on successful startup
	success := false

	defer func() {
		if !success {
			runCleanups()

			if infraConnectionManager != nil {
				if closeErr := infraConnectionManager.Close(); closeErr != nil {
					logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to close connection manager: %v", closeErr))
				}
			}

			cleanupConnections(ctx, postgresConnection, redisConnection, rabbitMQConnection, logger)
		}
	}()

	done = timer.track("infra_connect")

	if err := connectInfrastructure(infraCtx, asserter, cfg, postgresConnection, rabbitMQConnection, logger); err != nil {
		return nil, err
	}

	done()

	done = timer.track("auth_and_routes")

	// Bridge: lib-auth still depends on lib-commons types.
	// Create a separate lib-commons logger for the auth boundary until lib-auth migrates.
	authLogger, authLoggerErr := initializeAuthBoundaryLogger()
	if authLoggerErr != nil {
		logger.Log(
			ctx,
			libLog.LevelWarn,
			fmt.Sprintf("failed to initialize auth boundary logger, using no-op logger: %v", authLoggerErr),
		)

		authLogger = &libCommonsLog.NoneLogger{}
	}

	authClient := middleware.NewAuthClient(cfg.Auth.Host, cfg.Auth.Enabled, &authLogger)

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
	)
	if err != nil {
		return nil, fmt.Errorf("create health dependencies: %w", err)
	}

	app := NewFiberApp(cfg, logger, telemetry)

	rateLimitStorage := ratelimit.NewRedisStorage(redisConnection)

	infraProvider, connectionManager, err := createInfraProvider(
		cfg,
		postgresConnection,
		redisConnection,
	)
	if err != nil {
		return nil, fmt.Errorf("create infra provider: %w", err)
	}

	infraConnectionManager = connectionManager

	var connCloser connectionCloser
	if connectionManager != nil {
		connCloser = connectionManager
	}

	idempotencyRepo := createIdempotencyRepository(cfg, infraProvider, logger)

	routes, err := RegisterRoutes(
		app,
		cfg,
		configManager.Get,
		healthDeps,
		logger,
		authClient,
		tenantExtractor,
		rateLimitStorage,
		idempotencyRepo,
	)
	if err != nil {
		return nil, err
	}

	done()

	done = timer.track("modules")

	modules, err := initModulesAndMessaging(
		routes,
		cfg,
		configManager.Get,
		infraProvider,
		rabbitMQConnection,
		rateLimitStorage,
		logger,
	)
	if err != nil {
		return nil, err
	}

	archivalWorker, archivalErr := initArchivalComponents(routes, cfg, infraProvider, logger, &cleanups)
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

	done = timer.track("runtime_config_wiring")

	configAPIHandler, err := NewConfigAPIHandler(configManager, logger, IsProductionEnvironment(cfg.App.EnvName))
	if err != nil {
		return nil, fmt.Errorf("initialize config API handler: %w", err)
	}

	configAuditPublisher, err := NewConfigAuditPublisher(outboxPgRepo.NewRepository(infraProvider), logger)
	if err != nil {
		return nil, fmt.Errorf("initialize config audit publisher: %w", err)
	}

	configAPIHandler.SetAuditPublisher(configAuditPublisher)
	configAPIHandler.SetAuditRepository(governancePostgres.NewRepository(infraProvider))
	SetAuditCallback(configManager, configAuditPublisher, logger)

	if shouldEnableConfigAPIRoutes(cfg) {
		if err := RegisterConfigAPIRoutes(routes.Protected, configAPIHandler); err != nil {
			return nil, fmt.Errorf("register config API routes: %w", err)
		}

		logger.Log(context.Background(), libLog.LevelWarn,
			"system config API routes enabled; ensure auth policies grant system/config:read and system/config:write where appropriate")
	} else {
		logger.Log(context.Background(), libLog.LevelWarn,
			"system config API routes are disabled because AUTH_ENABLED=false")
	}

	workerMgr := NewWorkerManager(logger, configManager)
	registerWorkerManagerSlots(workerMgr, modules)

	configManager.StartWatcher()

	done()

	infraStatus := buildInfraStatus(cfg, postgresConnection, redisConnection, rabbitMQConnection, modules, healthDeps, telemetry)
	logStartupInfo(logger, cfg, infraStatus)
	logStartupTiming(logger, timer)

	success = true

	return &Service{
		Server:             server,
		Logger:             logger,
		Config:             cfg,
		Routes:             routes,
		ConfigManager:      configManager,
		authClient:         authClient,
		tenantExtractor:    tenantExtractor,
		outboxRunner:       modules.outboxDispatcher,
		dbMetricsCollector: dbMetricsCollector,
		exportWorker:       modules.exportWorker,
		cleanupWorker:      modules.cleanupWorker,
		archivalWorker:     modules.archivalWorker,
		schedulerWorker:    modules.schedulerWorker,
		workerManager:      workerMgr,
		connectionManager:  connCloser,
		cleanupFuncs:       cleanups,
	}, nil
}

func shouldEnableConfigAPIRoutes(cfg *Config) bool {
	return cfg != nil && cfg.Auth.Enabled
}

// registerWorkerManagerSlots registers all worker slots with the worker manager.
//
// IMPORTANT: Worker re-entrancy contract
// Each factory closure returns the SAME worker instance (captured from modules).
// The WorkerManager calls Stop() → UpdateRuntimeConfig() → Start() on the same
// instance during restarts. All workers MUST support this lifecycle by implementing
// prepareRunState() to reinitialize channels and sync primitives. Workers that do
// NOT support Stop→Start re-entrancy will fail silently on restart.
func registerWorkerManagerSlots(workerMgr *WorkerManager, modules *modulesResult) {
	if workerMgr == nil || modules == nil {
		return
	}

	registerExportWorkerSlot(workerMgr, modules)
	registerCleanupWorkerSlot(workerMgr, modules)
	registerArchivalWorkerSlot(workerMgr, modules)
	registerSchedulerWorkerSlot(workerMgr, modules)
}

func registerExportWorkerSlot(workerMgr *WorkerManager, modules *modulesResult) {
	workerMgr.Register(
		"export",
		func(cfg *Config) (WorkerLifecycle, error) {
			if modules.exportWorker == nil {
				return nil, errWorkerDependencyUnavailable
			}

			if cfg == nil {
				return nil, fmt.Errorf("worker factory: %w", ErrConfigNil)
			}

			return modules.exportWorker, nil
		},
		func(cfg *Config) bool { return cfg != nil && cfg.ExportWorker.Enabled },
		func(cfg *Config) bool { return cfg != nil && cfg.ExportWorker.Enabled },
	)
}

func registerCleanupWorkerSlot(workerMgr *WorkerManager, modules *modulesResult) {
	workerMgr.Register(
		"cleanup",
		func(cfg *Config) (WorkerLifecycle, error) {
			if modules.cleanupWorker == nil {
				return nil, errWorkerDependencyUnavailable
			}

			if cfg == nil {
				return nil, fmt.Errorf("worker factory: %w", ErrConfigNil)
			}

			return modules.cleanupWorker, nil
		},
		func(cfg *Config) bool { return cfg != nil && cfg.CleanupWorker.Enabled },
		func(cfg *Config) bool { return cfg != nil && cfg.CleanupWorker.Enabled },
	)
}

func registerArchivalWorkerSlot(workerMgr *WorkerManager, modules *modulesResult) {
	workerMgr.Register(
		"archival",
		func(cfg *Config) (WorkerLifecycle, error) {
			if modules.archivalWorker == nil {
				return nil, errWorkerDependencyUnavailable
			}

			if cfg == nil {
				return nil, fmt.Errorf("worker factory: %w", ErrConfigNil)
			}

			return modules.archivalWorker, nil
		},
		func(cfg *Config) bool { return cfg != nil && cfg.Archival.Enabled },
		func(cfg *Config) bool { return cfg != nil && cfg.Archival.Enabled },
	)
}

func registerSchedulerWorkerSlot(workerMgr *WorkerManager, modules *modulesResult) {
	workerMgr.Register(
		"scheduler",
		func(cfg *Config) (WorkerLifecycle, error) {
			if modules.schedulerWorker == nil {
				return nil, errWorkerDependencyUnavailable
			}

			if cfg == nil {
				return nil, fmt.Errorf("worker factory: %w", ErrConfigNil)
			}

			return modules.schedulerWorker, nil
		},
		func(cfg *Config) bool { return cfg != nil },
		func(_ *Config) bool { return false },
	)
}

func initLogger(opts *Options) (libLog.Logger, error) {
	if opts != nil && opts.Logger != nil {
		return opts.Logger, nil
	}

	logger, err := libZap.New(libZap.Config{
		Environment:     ResolveLoggerEnvironment(os.Getenv("ENV_NAME")),
		Level:           ResolveLoggerLevel(os.Getenv("LOG_LEVEL")),
		OTelLibraryName: constants.ApplicationName,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize logger: %w", err)
	}

	return logger, nil
}

// checkClientConnected returns the connected state of a client that exposes IsConnected.
// Returns false when the client is nil or IsConnected returns an error.
func checkClientConnected[T interface{ IsConnected() (bool, error) }](client T) bool {
	connected, err := client.IsConnected()
	if err != nil {
		return false
	}

	return connected
}

func buildInfraStatus(
	cfg *Config,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	modules *modulesResult,
	healthDeps *HealthDependencies,
	telemetry *libOpentelemetry.Telemetry,
) *InfraStatus {
	pgConnected := postgres != nil && checkClientConnected(postgres)
	redisConnected := redis != nil && checkClientConnected(redis)

	status := &InfraStatus{
		PostgresConnected:      pgConnected,
		RedisConnected:         redisConnected,
		RabbitMQConnected:      rabbitmq != nil && rabbitmq.Channel != nil,
		HasReplica:             cfg.Postgres.ReplicaHost != "" && cfg.Postgres.ReplicaHost != cfg.Postgres.PrimaryHost,
		ObjectStorageEnabled:   healthDeps != nil && healthDeps.ObjectStorage != nil,
		ExportWorkerEnabled:    modules != nil && modules.exportWorker != nil,
		CleanupWorkerEnabled:   modules != nil && modules.cleanupWorker != nil,
		ArchivalWorkerEnabled:  modules != nil && modules.archivalWorker != nil,
		SchedulerWorkerEnabled: modules != nil && modules.schedulerWorker != nil,
		TelemetryConfigured:    cfg.Telemetry.Enabled,
		TelemetryActive:        telemetry != nil && telemetry.EnableTelemetry,
	}

	status.TelemetryDegraded = status.TelemetryConfigured && !status.TelemetryActive

	if redis != nil {
		status.RedisMode = detectRedisMode(cfg)
	}

	return status
}

func detectRedisMode(cfg *Config) string {
	if cfg.Redis.MasterName != "" {
		return "sentinel"
	}

	if strings.Contains(cfg.Redis.Host, ",") {
		return "cluster"
	}

	return "standalone"
}

func createHealthDependencies(
	ctx context.Context,
	cfg *Config,
	logger libLog.Logger,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	cleanups *[]func(),
) (*HealthDependencies, error) {
	deps := NewHealthDependencies(postgres, nil, redis, rabbitmq, nil)

	// Redis is required for readiness.
	// Multiple critical paths depend on Redis (idempotency middleware,
	// matching locks, and rate limiting), so reporting ready while Redis is down
	// can route write traffic to an instance that cannot safely process it.
	deps.RedisOptional = false

	if cfg.Postgres.ReplicaHost != "" && cfg.Postgres.ReplicaHost != cfg.Postgres.PrimaryHost {
		//nolint:contextcheck // health check factory creates its own check closure that receives ctx at call time
		check, cleanup := createPostgresReplicaHealthCheck(cfg, logger)
		deps.PostgresReplicaCheck = check

		*cleanups = append(*cleanups, cleanup)
	}

	objectStorage, err := createObjectStorageForHealth(ctx, cfg, logger)
	if err != nil {
		if cfg.ExportWorker.Enabled {
			return nil, fmt.Errorf("object storage required when EXPORT_WORKER_ENABLED=true: %w", err)
		}

		logger.Log(ctx, libLog.LevelDebug, fmt.Sprintf("Object storage health check disabled: %v", err))
	} else if objectStorage != nil {
		deps.ObjectStorage = objectStorage
	}

	return deps, nil
}

func createPostgresConnection(cfg *Config, logger libLog.Logger) (*libPostgres.Client, error) {
	conn, err := libPostgres.New(libPostgres.Config{
		PrimaryDSN:         cfg.PrimaryDSN(),
		ReplicaDSN:         cfg.ReplicaDSN(),
		Logger:             logger,
		MaxOpenConnections: cfg.Postgres.MaxOpenConnections,
		MaxIdleConnections: cfg.Postgres.MaxIdleConnections,
	})
	if err != nil {
		return nil, fmt.Errorf("create postgres client: %w", err)
	}

	return conn, nil
}

func createPostgresReplicaHealthCheck(cfg *Config, logger libLog.Logger) (HealthCheckFunc, func()) {
	replicaDSN := cfg.ReplicaDSN()

	// Create a single connection for health checks to avoid connection leak.
	// The connection is lazily initialized on first health check.
	var (
		healthDB *sql.DB
		initOnce sync.Once
		initErr  error
	)

	check := func(ctx context.Context) error {
		initOnce.Do(func() {
			healthDB, initErr = sql.Open("pgx", replicaDSN)
			if initErr != nil {
				return
			}

			healthDB.SetMaxOpenConns(1)
			healthDB.SetMaxIdleConns(1)
			healthDB.SetConnMaxLifetime(healthConnMaxLifetime)
		})

		if initErr != nil {
			return fmt.Errorf("postgres replica health check: open failed: %w", initErr)
		}

		if err := healthDB.PingContext(ctx); err != nil {
			return fmt.Errorf("postgres replica health check: ping failed: %w", err)
		}

		return nil
	}

	cleanup := func() {
		if healthDB != nil {
			if err := healthDB.Close(); err != nil {
				logger.Log(context.Background(), libLog.LevelError, fmt.Sprintf("failed to close postgres replica health check connection: %v", err))
			}
		}
	}

	return check, cleanup
}

func createObjectStorageForHealth(
	ctx context.Context,
	cfg *Config,
	_ libLog.Logger,
) (ObjectStorageHealthChecker, error) {
	if cfg.ObjectStorage.Endpoint == "" {
		return nil, nil
	}

	if cfg.ObjectStorage.Bucket == "" {
		return nil, nil
	}

	s3Cfg := reportingStorage.S3Config{
		Endpoint:        cfg.ObjectStorage.Endpoint,
		Region:          cfg.ObjectStorage.Region,
		Bucket:          cfg.ObjectStorage.Bucket,
		AccessKeyID:     cfg.ObjectStorage.AccessKeyID,
		SecretAccessKey: cfg.ObjectStorage.SecretAccessKey,
		UsePathStyle:    cfg.ObjectStorage.UsePathStyle,
	}

	client, err := newS3ClientFn(ctx, s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 client for health check: %w", err)
	}

	return client, nil
}

func createInfraProvider(
	cfg *Config,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
) (sharedPorts.InfrastructureProvider, *tenantAdapters.TenantConnectionManager, error) {
	if !cfg.Tenancy.MultiTenantInfraEnabled {
		return tenantAdapters.NewSingleTenantInfrastructureProvider(postgres, redis), nil, nil
	}

	staticConfig := sharedPorts.TenantConfig{
		PostgresPrimaryDSN: cfg.PrimaryDSN(),
		PostgresReplicaDSN: cfg.ReplicaDSN(),
		PostgresPrimaryDB:  cfg.Postgres.PrimaryDB,
		PostgresReplicaDB:  cfg.Postgres.ReplicaDB,
		RedisAddresses:     strings.Split(cfg.Redis.Host, ","),
		RedisPassword:      cfg.Redis.Password,
		RedisDB:            cfg.Redis.DB,
		RedisMasterName:    cfg.Redis.MasterName,
		RedisProtocol:      cfg.Redis.Protocol,
		RedisUseTLS:        cfg.Redis.TLS,
		RedisCACert:        cfg.Redis.CACert,
		RedisReadTimeout:   cfg.RedisReadTimeout(),
		RedisWriteTimeout:  cfg.RedisWriteTimeout(),
		RedisDialTimeout:   cfg.RedisDialTimeout(),
		RedisPoolSize:      cfg.Redis.PoolSize,
		RedisMinIdleConns:  cfg.Redis.MinIdleConn,
	}

	configAdapter := tenantAdapters.NewStaticConfigurationAdapter(staticConfig)

	connectionManager, err := tenantAdapters.NewTenantConnectionManager(
		configAdapter,
		cfg.Postgres.MaxOpenConnections,
		cfg.Postgres.MaxIdleConnections,
		cfg.Postgres.ConnMaxLifetimeMins,
		cfg.Postgres.ConnMaxIdleTimeMins,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create tenant connection manager: %w", err)
	}

	return connectionManager, connectionManager, nil
}

func buildRedisConfig(cfg *Config, logger libLog.Logger) libRedis.Config {
	redisCfg := libRedis.Config{
		Auth: libRedis.Auth{
			StaticPassword: &libRedis.StaticPasswordAuth{
				Password: cfg.Redis.Password,
			},
		},
		Options: libRedis.ConnectionOptions{
			DB:           cfg.Redis.DB,
			Protocol:     cfg.Redis.Protocol,
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConn,
			ReadTimeout:  cfg.RedisReadTimeout(),
			WriteTimeout: cfg.RedisWriteTimeout(),
			DialTimeout:  cfg.RedisDialTimeout(),
		},
		Logger: logger,
	}

	// Build TLS config if enabled
	if cfg.Redis.TLS {
		redisCfg.TLS = &libRedis.TLSConfig{
			CACertBase64: cfg.Redis.CACert,
		}
	}

	// Determine topology from config
	rawAddresses := strings.Split(cfg.Redis.Host, ",")
	addresses := make([]string, 0, len(rawAddresses))

	for _, addr := range rawAddresses {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			addresses = append(addresses, trimmed)
		}
	}

	switch {
	case cfg.Redis.MasterName != "":
		redisCfg.Topology = libRedis.Topology{
			Sentinel: &libRedis.SentinelTopology{
				Addresses:  addresses,
				MasterName: cfg.Redis.MasterName,
			},
		}
	case len(addresses) > 1:
		redisCfg.Topology = libRedis.Topology{
			Cluster: &libRedis.ClusterTopology{
				Addresses: addresses,
			},
		}
	default:
		addr := strings.TrimSpace(cfg.Redis.Host)
		if addr == "" && !IsProductionEnvironment(cfg.App.EnvName) {
			addr = "localhost:6379"
		}

		redisCfg.Topology = libRedis.Topology{
			Standalone: &libRedis.StandaloneTopology{
				Address: addr,
			},
		}
	}

	return redisCfg
}

func createRedisConnection(ctx context.Context, cfg *Config, logger libLog.Logger) (*libRedis.Client, error) {
	redisCfg := buildRedisConfig(cfg, logger)

	conn, err := libRedis.New(ctx, redisCfg)
	if err != nil {
		return nil, fmt.Errorf("create redis client: %w", err)
	}

	return conn, nil
}

func createRabbitMQConnection(cfg *Config, logger libLog.Logger) *libRabbitmq.RabbitMQConnection {
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if cfg == nil {
		logger.Log(
			context.Background(),
			libLog.LevelError,
			"RabbitMQ connection configuration is nil; using empty defaults and disabling insecure health checks",
		)

		cfg = &Config{}
	}

	allowInsecureHealthCheck, denialReason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)
	if denialReason != "" {
		logger.Log(context.Background(), libLog.LevelWarn, denialReason)
	}

	if !allowInsecureHealthCheck && isInsecureHTTPHealthCheckURL(cfg.RabbitMQ.HealthURL) {
		logger.Log(
			context.Background(),
			libLog.LevelWarn,
			"RabbitMQ health URL uses HTTP while insecure checks are disabled; set RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK=true only for local/internal non-production environments",
		)
	}

	return &libRabbitmq.RabbitMQConnection{
		ConnectionStringSource:   cfg.RabbitMQDSN(),
		HealthCheckURL:           cfg.RabbitMQ.HealthURL,
		Host:                     cfg.RabbitMQ.Host,
		Port:                     cfg.RabbitMQ.Port,
		User:                     cfg.RabbitMQ.User,
		Pass:                     cfg.RabbitMQ.Password,
		Logger:                   logger,
		AllowInsecureHealthCheck: allowInsecureHealthCheck,
	}
}

func initializeAuthBoundaryLogger() (libCommonsLog.Logger, error) {
	authLogger, authLoggerErr := initializeAuthBoundaryLoggerFn()
	if authLoggerErr != nil {
		return nil, fmt.Errorf("initialize auth boundary logger: %w", authLoggerErr)
	}

	if authLogger == nil {
		return nil, fmt.Errorf("initialize auth boundary logger: %w", errAuthBoundaryLoggerNil)
	}

	return authLogger, nil
}

func evaluateInsecureRabbitMQHealthCheckPolicy(cfg *Config) (bool, string) {
	if cfg == nil {
		return false, "RabbitMQ health check insecure HTTP is disabled because configuration is nil"
	}

	if !cfg.RabbitMQ.AllowInsecureHealthCheck {
		return false, ""
	}

	if IsProductionEnvironment(cfg.App.EnvName) {
		return false, "RabbitMQ health check insecure HTTP is disabled in production"
	}

	if !isInsecureHTTPHealthCheckURL(cfg.RabbitMQ.HealthURL) {
		return false, "RabbitMQ insecure health check requires an HTTP health URL"
	}

	if !isAllowedInsecureHealthCheckHost(cfg.RabbitMQ.HealthURL, cfg.RabbitMQ.Host) {
		return false, "RabbitMQ insecure health check is restricted to local/internal hosts"
	}

	return true, ""
}

func isInsecureHTTPHealthCheckURL(healthURL string) bool {
	parsed, err := url.Parse(healthURL)
	if err != nil {
		return false
	}

	return strings.EqualFold(parsed.Scheme, "http")
}

func isAllowedInsecureHealthCheckHost(healthURL, configuredRabbitHost string) bool {
	parsed, err := url.Parse(healthURL)
	if err != nil {
		return false
	}

	hostname := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if hostname == "" {
		return false
	}

	if hostname == "localhost" {
		return true
	}

	ip := net.ParseIP(hostname)
	if ip != nil {
		return ip.IsLoopback() || ip.IsPrivate()
	}

	configuredHost := strings.ToLower(strings.TrimSpace(configuredRabbitHost))
	if configuredHost != "" && !strings.Contains(hostname, ".") && hostname == configuredHost {
		return true
	}

	return strings.HasSuffix(hostname, ".local") ||
		strings.HasSuffix(hostname, ".internal") ||
		strings.HasSuffix(hostname, ".cluster.local")
}

func cleanupConnections(
	ctx context.Context,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	logger libLog.Logger,
) {
	cleanupPostgres(ctx, postgres, logger)
	cleanupRedis(ctx, redis, logger)
	cleanupRabbitMQ(ctx, rabbitmq, logger)
}

func cleanupPostgres(ctx context.Context, postgres *libPostgres.Client, logger libLog.Logger) {
	if postgres == nil {
		return
	}

	start := time.Now()
	err := postgres.Close()
	duration := time.Since(start)

	if err != nil {
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to close postgres connection: %v", err))
		recordCleanup(ctx, "postgres", false, duration)

		return
	}

	recordCleanup(ctx, "postgres", true, duration)
}

func cleanupRedis(ctx context.Context, redis *libRedis.Client, logger libLog.Logger) {
	if redis == nil {
		return
	}

	start := time.Now()
	err := redis.Close()
	duration := time.Since(start)

	if err != nil {
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to close redis connection: %v", err))
		recordCleanup(ctx, "redis", false, duration)

		return
	}

	recordCleanup(ctx, "redis", true, duration)
}

func cleanupRabbitMQ(ctx context.Context, rabbitmq *libRabbitmq.RabbitMQConnection, logger libLog.Logger) {
	if rabbitmq == nil {
		return
	}

	start := time.Now()
	hasError := false

	if rabbitmq.Channel != nil {
		if err := rabbitmq.Channel.Close(); err != nil {
			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to close rabbitmq channel: %v", err))

			hasError = true
		}
	}

	if rabbitmq.Connection != nil {
		if err := rabbitmq.Connection.Close(); err != nil {
			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to close rabbitmq connection: %v", err))

			hasError = true
		}
	}

	recordCleanup(ctx, "rabbitmq", !hasError, time.Since(start))
}

// connectInfrastructure runs database migrations, then connects to PostgreSQL and
// RabbitMQ in parallel.
//
// Redis verification is intentionally omitted: createRedisConnection() already calls
// libRedis.New() which performs Connect() + Ping(). A second verification here would
// be redundant work.
//
// Dependency graph:
//
//	sequential:
//	  1) Migrations
//
//	errgroup (parallel after migrations):
//	  goroutine 1: Postgres Connect → Pool Settings
//	  goroutine 2: RabbitMQ Connect
//
// Running migrations before the errgroup prevents unrelated dependency failures
// (e.g., RabbitMQ outage) from canceling in-flight migrations and leaving dirty
// migration state.
func connectInfrastructure(
	ctx context.Context,
	asserter *assert.Asserter,
	cfg *Config,
	postgres *libPostgres.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	logger libLog.Logger,
) error {
	if postgres == nil {
		return errPostgresClientRequired
	}

	if rabbitmq == nil {
		return errRabbitMQClientRequired
	}

	// Each infrastructure service gets its own timeout budget to prevent
	// a single slow dependency from starving the others. The parent context
	// still enforces the overall deadline.
	perServiceTimeout := cfg.InfraConnectTimeout() / infraConnectTimeoutDivisor //nolint:contextcheck // 1/2 of total budget per service; InfraConnectTimeout is a pure config getter
	if perServiceTimeout < minPerServiceTimeout {
		perServiceTimeout = minPerServiceTimeout
	}

	allowDirtyRecovery := shouldAllowDirtyMigrationRecovery(cfg.App.EnvName)
	if err := runMigrationsFn(
		ctx,
		cfg.PrimaryDSN(),
		cfg.Postgres.PrimaryDB,
		cfg.Postgres.MigrationsPath,
		logger,
		allowDirtyRecovery,
	); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	infraGroup, groupCtx := errgroup.WithContext(ctx)
	infraGroup.SetLogger(logger)

	infraGroup.Go(func() error {
		pgCtx, pgCancel := context.WithTimeout(groupCtx, perServiceTimeout)
		defer pgCancel()

		if err := asserter.NoError(pgCtx, connectPostgresFn(pgCtx, postgres), "failed to connect postgres"); err != nil {
			return fmt.Errorf("connect postgres: %w", err)
		}

		return configurePostgresPoolSettings(groupCtx, cfg, postgres)
	})

	// Goroutine 2: RabbitMQ Connect (independent of migrations/postgres).
	infraGroup.Go(func() error {
		rabbitCtx, rabbitCancel := context.WithTimeout(groupCtx, perServiceTimeout)
		defer rabbitCancel()

		if err := asserter.NoError(rabbitCtx, ensureRabbitChannelFn(rabbitmq), "failed to connect rabbitmq"); err != nil {
			return fmt.Errorf("connect rabbitmq: %w", err)
		}

		return nil
	})

	if err := infraGroup.Wait(); err != nil {
		return fmt.Errorf("connect infrastructure: %w", err)
	}

	return nil
}

func shouldAllowDirtyMigrationRecovery(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "development", "local", "test":
		return true
	default:
		return false
	}
}

func configurePostgresPoolSettings(ctx context.Context, cfg *Config, postgres *libPostgres.Client) error {
	connected, connErr := postgres.IsConnected()
	if connErr != nil {
		return fmt.Errorf("check postgres connectivity for pool settings: %w", connErr)
	}

	if !connected {
		return nil
	}

	resolver, resolverErr := postgres.Resolver(ctx)
	if resolverErr != nil {
		return fmt.Errorf("resolve postgres for pool settings: %w", resolverErr)
	}

	if resolver == nil {
		return errPostgresResolverRequired
	}

	applySQLPoolSettings(resolver.PrimaryDBs(), cfg.ConnMaxLifetime(), cfg.ConnMaxIdleTime())
	applySQLPoolSettings(resolver.ReplicaDBs(), cfg.ConnMaxLifetime(), cfg.ConnMaxIdleTime())

	return nil
}

func applySQLPoolSettings(dbs []*sql.DB, maxLifetime, maxIdle time.Duration) {
	for _, db := range dbs {
		if db == nil {
			continue
		}

		db.SetConnMaxLifetime(maxLifetime)
		db.SetConnMaxIdleTime(maxIdle)
	}
}

type modulesResult struct {
	outboxDispatcher *outboxServices.Dispatcher
	exportWorker     *reportingWorker.ExportWorker
	cleanupWorker    *reportingWorker.CleanupWorker
	archivalWorker   *governanceWorker.ArchivalWorker
	schedulerWorker  *configWorker.SchedulerWorker
}

// errRabbitMQConnectionNil is returned when attempting to open a channel on a nil connection.
var errRabbitMQConnectionNil = errors.New("rabbitmq connection or underlying AMQP connection is nil")

// openDedicatedChannel opens a new AMQP channel from the underlying *amqp.Connection.
// Each ConfirmablePublisher MUST own a dedicated channel because AMQP publisher confirms
// are channel-scoped. Sharing a channel between publishers corrupts delivery tag tracking.
func openDedicatedChannel(conn *libRabbitmq.RabbitMQConnection) (*amqp.Channel, error) {
	if conn == nil || conn.Connection == nil {
		return nil, errRabbitMQConnectionNil
	}

	ch, err := conn.Connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("open dedicated AMQP channel: %w", err)
	}

	return ch, nil
}

// sharedRepositories holds repository instances that are used across multiple modules.
// Instantiating them once avoids redundant allocations and makes the dependency graph explicit.
type sharedRepositories struct {
	configContext      *configContextRepo.Repository
	configSource       *configSourceRepo.Repository
	configFieldMap     *configFieldMapRepo.Repository
	configMatchRule    *configMatchRuleRepo.Repository
	governanceAuditLog *governancePostgres.Repository
	ingestionTx        *ingestionTransactionRepo.Repository
	ingestionJob       *ingestionJobRepo.Repository
	feeSchedule        *matchFeeScheduleRepo.Repository
	adjustment         *matchAdjustmentRepo.Repository
}

// initSharedRepositories creates a single instance of every repository that is used
// by more than one bounded-context module. Callers receive the struct by value so
// there is no aliasing concern.
func initSharedRepositories(provider sharedPorts.InfrastructureProvider) (*sharedRepositories, error) {
	configSourceRepository, err := configSourceRepo.NewRepository(provider)
	if err != nil {
		return nil, fmt.Errorf("create shared source repository: %w", err)
	}

	auditLogRepo := governancePostgres.NewRepository(provider)

	return &sharedRepositories{
		configContext:      configContextRepo.NewRepository(provider),
		configSource:       configSourceRepository,
		configFieldMap:     configFieldMapRepo.NewRepository(provider),
		configMatchRule:    configMatchRuleRepo.NewRepository(provider),
		governanceAuditLog: auditLogRepo,
		ingestionTx:        ingestionTransactionRepo.NewRepository(provider),
		ingestionJob:       ingestionJobRepo.NewRepository(provider),
		feeSchedule:        matchFeeScheduleRepo.NewRepository(provider),
		adjustment:         matchAdjustmentRepo.NewRepository(provider, auditLogRepo),
	}, nil
}

//nolint:cyclop,gocyclo // module initialization requires sequential dependency setup
func initModulesAndMessaging(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	rabbitMQConnection *libRabbitmq.RabbitMQConnection,
	rateLimitStorage fiber.Storage,
	logger libLog.Logger,
) (*modulesResult, error) {
	ctx := context.Background()

	sharedOutboxRepository := outboxPgRepo.NewRepository(provider)

	sharedRepos, err := initSharedRepositories(provider)
	if err != nil {
		return nil, fmt.Errorf("init shared repositories: %w", err)
	}

	isProduction := IsProductionEnvironment(cfg.App.EnvName)

	if err := initConfigurationModule(routes, provider, sharedOutboxRepository, sharedRepos, isProduction); err != nil {
		return nil, err
	}

	matchingPublisher, ingestionPublisher, err := initEventPublishers(rabbitMQConnection, logger)
	if err != nil {
		return nil, err
	}

	matchingUseCase, err := initMatchingModule(routes, provider, sharedOutboxRepository, sharedRepos, isProduction)
	if err != nil {
		return nil, err
	}

	if err := initIngestionModule(routes, provider, sharedOutboxRepository, ingestionPublisher, matchingUseCase, sharedRepos, isProduction); err != nil {
		return nil, err
	}

	storage, err := createObjectStorage(cfg, logger)
	if err != nil {
		if cfg.ExportWorker.Enabled {
			return nil, fmt.Errorf("create object storage: %w", err)
		}

		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Object storage not available, export jobs disabled: %v", err))
	}

	exportWorker, cleanupWorker, err := initReportingModule(
		routes,
		cfg,
		configGetter,
		provider,
		storage,
		rateLimitStorage,
		logger,
		sharedRepos,
		isProduction,
	)
	if err != nil {
		return nil, err
	}

	if err := initGovernanceModule(routes, sharedRepos, provider, isProduction); err != nil {
		return nil, err
	}

	dispatchLimiter := NewDispatchRateLimiter(cfg, rateLimitStorage)
	if configGetter != nil {
		dispatchLimiter = NewDynamicDispatchRateLimiter(configGetter, rateLimitStorage)
	}

	if err := initExceptionModule(cfg, routes, provider, sharedOutboxRepository, dispatchLimiter, sharedRepos, isProduction); err != nil {
		return nil, err
	}

	// Create governance audit consumer for processing audit events from the outbox.
	// Audit publishing is compliance-critical (SOX) — the system MUST NOT start without it.
	// If the audit consumer fails to initialize, the entire startup is aborted to prevent
	// audit events from being silently dropped or retried indefinitely.
	auditLogRepository := sharedRepos.governanceAuditLog

	auditConsumer, err := governanceAudit.NewConsumer(auditLogRepository)
	if err != nil {
		return nil, fmt.Errorf("create governance audit consumer: %w", err)
	}

	// Defense-in-depth: reject startup if audit consumer is nil.
	// NewConsumer already validates its dependencies, but compliance requires an explicit guard
	// to prevent a nil publisher from reaching the outbox dispatcher.
	if auditConsumer == nil {
		return nil, ErrAuditPublisherRequired
	}

	outboxDispatcher, err := outboxServices.NewDispatcher(
		sharedOutboxRepository,
		ingestionPublisher,
		matchingPublisher,
		logger,
		otel.Tracer(constants.ApplicationName),
		outboxServices.WithAuditPublisher(auditConsumer),
		outboxServices.WithProduction(isProduction),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox dispatcher: %w", err)
	}

	outboxDispatcher.SetRetryWindow(cfg.IdempotencyRetryWindow())

	// Create scheduler worker for cron-based matching
	var schedulerWorker *configWorker.SchedulerWorker
	if matchingUseCase != nil {
		schedulerWorker = createSchedulerWorker(cfg, provider, matchingUseCase, logger)
	}

	return &modulesResult{
		outboxDispatcher: outboxDispatcher,
		exportWorker:     exportWorker,
		cleanupWorker:    cleanupWorker,
		schedulerWorker:  schedulerWorker,
	}, nil
}

// logCloseErr calls closeFn and logs any error at LevelError. It is a no-op
// when closeFn returns nil. This helper exists to flatten the nested
// "if err; if logger" pattern that otherwise inflates cognitive complexity.
func logCloseErr(ctx context.Context, logger libLog.Logger, msg string, closeFn func() error) {
	if err := closeFn(); err != nil {
		if logger != nil {
			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("%s: %v", msg, err))
		}
	}
}

// openChannelForPublisher opens a dedicated AMQP channel, checking for
// cancellation first. Extracted from initEventPublishers to reduce complexity.
func openChannelForPublisher(
	groupCtx context.Context,
	name string,
	conn *libRabbitmq.RabbitMQConnection,
	openFn func(*libRabbitmq.RabbitMQConnection) (*amqp.Channel, error),
) (*amqp.Channel, error) {
	select {
	case <-groupCtx.Done():
		return nil, fmt.Errorf("open dedicated channel for %s publisher: %w", name, groupCtx.Err())
	default:
	}

	ch, err := openFn(conn)
	if err != nil {
		return nil, fmt.Errorf("open dedicated channel for %s publisher: %w", name, err)
	}

	return ch, nil
}

// cleanupPublishersOnFailure closes publishers and channels when
// initEventPublishers encounters an error partway through setup.
func cleanupPublishersOnFailure(
	ctx context.Context,
	logger libLog.Logger,
	matchingPublisher *matchingRabbitmq.EventPublisher,
	ingestionPublisher *ingestionRabbitmq.EventPublisher,
	matchingChannel, ingestionChannel *amqp.Channel,
) {
	closeMatchingPublisherFn := loadCloseMatchingEventPublisherFn()
	closeIngestionPublisherFn := loadCloseIngestionEventPublisherFn()
	closeChannelFn := loadCloseAMQPChannelFn()

	logCloseErr(ctx, logger, "failed to close matching publisher during cleanup", func() error {
		return closeMatchingPublisherFn(matchingPublisher)
	})

	logCloseErr(ctx, logger, "failed to close ingestion publisher during cleanup", func() error {
		return closeIngestionPublisherFn(ingestionPublisher)
	})

	if matchingChannel != nil {
		logCloseErr(ctx, logger, "failed to close matching channel", func() error {
			return closeChannelFn(matchingChannel)
		})
	}

	if ingestionChannel != nil {
		logCloseErr(ctx, logger, "failed to close ingestion channel", func() error {
			return closeChannelFn(ingestionChannel)
		})
	}
}

// initEventPublishers creates dedicated AMQP channels and event publishers for
// matching and ingestion modules. Channels are opened in parallel since they are
// independent protocol exchanges on the same connection.
//
// Channel isolation: each publisher gets its own dedicated AMQP channel.
// AMQP publisher confirms are channel-scoped -- calling Confirm(false) on a
// channel resets the delivery tag counter and confirmation state for that channel.
// If two ConfirmablePublishers shared the same channel, the second Confirm call
// would corrupt the first publisher's tracking, leading to silent message loss.
//
// We open fresh channels from the underlying *amqp.Connection so each publisher
// has isolated delivery tags and confirm sequences.
func initEventPublishers(
	rabbitMQConnection *libRabbitmq.RabbitMQConnection,
	logger libLog.Logger,
) (*matchingRabbitmq.EventPublisher, *ingestionRabbitmq.EventPublisher, error) {
	ctx := context.Background()
	success := false
	openChannelFn := loadOpenDedicatedChannelFn()
	newMatchingPublisherFn := loadNewMatchingEventPublisherFromChannelFn()
	newIngestionPublisherFn := loadNewIngestionEventPublisherFromChannelFn()

	var matchingPublisher *matchingRabbitmq.EventPublisher

	var ingestionPublisher *ingestionRabbitmq.EventPublisher

	// Open both AMQP channels in parallel — independent protocol exchanges.
	var matchingChannel, ingestionChannel *amqp.Channel

	defer func() {
		if !success {
			cleanupPublishersOnFailure(ctx, logger, matchingPublisher, ingestionPublisher, matchingChannel, ingestionChannel)
		}
	}()

	channelGroup, groupCtx := errgroup.WithContext(ctx)
	channelGroup.SetLogger(logger)

	channelGroup.Go(func() error {
		ch, err := openChannelForPublisher(groupCtx, "matching", rabbitMQConnection, openChannelFn)
		if err != nil {
			return err
		}

		matchingChannel = ch

		return nil
	})

	channelGroup.Go(func() error {
		ch, err := openChannelForPublisher(groupCtx, "ingestion", rabbitMQConnection, openChannelFn)
		if err != nil {
			return err
		}

		ingestionChannel = ch

		return nil
	})

	if err := channelGroup.Wait(); err != nil {
		return nil, nil, fmt.Errorf("open AMQP channels: %w", err)
	}

	// Enable auto-recovery: if the AMQP channel dies (e.g., broker restart,
	// network partition), the publisher automatically reopens a new channel
	// from the underlying connection with exponential backoff.
	channelRecoveryProvider := sharedRabbitmq.WithAutoRecovery(func() (sharedRabbitmq.ConfirmableChannel, error) {
		ch, err := openChannelFn(rabbitMQConnection)
		if err != nil {
			return nil, fmt.Errorf("open dedicated channel for publisher recovery: %w", err)
		}

		return ch, nil
	})

	matchingPublisher, err := newMatchingPublisherFn(matchingChannel, channelRecoveryProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("create matching event publisher: %w", err)
	}

	ingestionPublisher, err = newIngestionPublisherFn(ingestionChannel, channelRecoveryProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("create ingestion event publisher: %w", err)
	}

	success = true

	return matchingPublisher, ingestionPublisher, nil
}

// createSchedulerWorker creates the scheduler worker for cron-based matching.
// Returns nil if any dependency fails to initialize (logged as warnings).
func createSchedulerWorker(
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	matchingUseCase *matchingCommand.UseCase,
	logger libLog.Logger,
) *configWorker.SchedulerWorker {
	ctx := context.Background()

	configScheduleRepository := configScheduleRepo.NewRepository(provider)

	matchTrigger, err := crossAdapters.NewMatchTriggerAdapter(matchingUseCase)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("scheduler worker not started: match trigger adapter unavailable: %v", err))

		return nil
	}

	workerCfg := configWorker.SchedulerWorkerConfig{
		Interval: cfg.SchedulerInterval(),
	}

	sw, err := configWorker.NewSchedulerWorker(
		configScheduleRepository,
		matchTrigger,
		provider,
		workerCfg,
		logger,
	)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("scheduler worker not available: %v", err))

		return nil
	}

	return sw
}

func createIdempotencyRepository(
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	logger libLog.Logger,
) sharedHTTP.IdempotencyRepository {
	ctx := context.Background()

	if provider == nil {
		logger.Log(ctx, libLog.LevelWarn, "idempotency repository: infrastructure provider is nil, idempotency disabled")

		return nil
	}

	exceptionIdempotencyRepo, err := exceptionRedis.NewIdempotencyRepositoryWithConfig(
		provider,
		cfg.IdempotencyRetryWindow(),
		cfg.IdempotencySuccessTTL(),
		cfg.Idempotency.HMACSecret,
	)
	if err != nil {
		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create idempotency repository (retryWindow=%v, successTTL=%v): %v",
			cfg.IdempotencyRetryWindow(), cfg.IdempotencySuccessTTL(), err))

		return nil
	}

	return sharedHTTP.NewIdempotencyRepositoryAdapter(exceptionIdempotencyRepo)
}

// createObjectStorage initialises the S3/MinIO client when a bucket is configured.
// The S3 client is created regardless of ExportWorker.Enabled — the WorkerManager
// controls worker lifecycle at the manager level. This allows dynamic enabling of
// the export worker via hot-reload without requiring a restart to initialise S3.
// Operators who do not have object storage should set OBJECT_STORAGE_BUCKET="" rather
// than relying on EXPORT_WORKER_ENABLED=false to skip S3 initialisation.
func createObjectStorage(
	cfg *Config,
	_ libLog.Logger,
) (reportingPorts.ObjectStorageClient, error) {
	if cfg.ObjectStorage.Bucket == "" {
		if !cfg.ExportWorker.Enabled {
			return nil, nil
		}

		return nil, ErrObjectStorageBucketRequired
	}

	s3Cfg := reportingStorage.S3Config{
		Endpoint:        cfg.ObjectStorage.Endpoint,
		Region:          cfg.ObjectStorage.Region,
		Bucket:          cfg.ObjectStorage.Bucket,
		AccessKeyID:     cfg.ObjectStorage.AccessKeyID,
		SecretAccessKey: cfg.ObjectStorage.SecretAccessKey,
		UsePathStyle:    cfg.ObjectStorage.UsePathStyle,
	}

	client, err := newS3ClientFn(context.Background(), s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}

	return client, nil
}

func initConfigurationModule(
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository outboxRepositories.OutboxRepository,
	repos *sharedRepositories,
	production bool,
) error {
	// Create outbox-based audit publisher for configuration module
	// This decouples configuration from governance via the outbox pattern
	auditPublisher, err := configAudit.NewOutboxPublisher(outboxRepository)
	if err != nil {
		return fmt.Errorf("create config audit publisher: %w", err)
	}

	scheduleRepository := configScheduleRepo.NewRepository(provider)

	configCommandUseCase, err := configCommand.NewUseCase(
		repos.configContext,
		repos.configSource,
		repos.configFieldMap,
		repos.configMatchRule,
		configCommand.WithAuditPublisher(auditPublisher),
		configCommand.WithFeeScheduleRepository(repos.feeSchedule),
		configCommand.WithScheduleRepository(scheduleRepository),
		configCommand.WithInfrastructureProvider(provider),
	)
	if err != nil {
		return fmt.Errorf("create config command use case: %w", err)
	}

	configQueryUseCase, err := configQuery.NewUseCase(
		repos.configContext,
		repos.configSource,
		repos.configFieldMap,
		repos.configMatchRule,
		configQuery.WithFeeScheduleRepository(repos.feeSchedule),
		configQuery.WithScheduleRepository(scheduleRepository),
	)
	if err != nil {
		return fmt.Errorf("create config query use case: %w", err)
	}

	configHandler, err := configHTTP.NewHandler(configCommandUseCase, configQueryUseCase, production)
	if err != nil {
		return fmt.Errorf("create config handler: %w", err)
	}

	if err := configHTTP.RegisterRoutes(routes.Protected, configHandler); err != nil {
		return fmt.Errorf("register configuration routes: %w", err)
	}

	return nil
}

func initIngestionModule(
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository outboxRepositories.OutboxRepository,
	publisher *ingestionRabbitmq.EventPublisher,
	matchingUseCase *matchingCommand.UseCase,
	repos *sharedRepositories,
	production bool,
) error {
	ingestionRegistry := ingestionParser.NewParserRegistry()
	ingestionRegistry.Register(ingestionParser.NewCSVParser())
	ingestionRegistry.Register(ingestionParser.NewJSONParser())
	ingestionRegistry.Register(ingestionParser.NewXMLParser())

	dedupeService := ingestionRedis.NewDedupeService(provider)

	fieldMapAdapter, err := crossAdapters.NewFieldMapRepositoryAdapter(repos.configFieldMap)
	if err != nil {
		return fmt.Errorf("create field map repository adapter: %w", err)
	}

	sourceAdapter, err := crossAdapters.NewSourceRepositoryAdapter(repos.configSource)
	if err != nil {
		return fmt.Errorf("create source repository adapter: %w", err)
	}

	contextAdapter := crossAdapters.NewIngestionContextProviderAdapter(repos.configContext)

	// Auto-match on upload: create adapters to check context config and trigger matching
	autoMatchContextProvider, err := crossAdapters.NewAutoMatchContextProviderAdapter(repos.configContext)
	if err != nil {
		return fmt.Errorf("create auto-match context provider adapter: %w", err)
	}

	var matchTriggerAdapter *crossAdapters.MatchTriggerAdapter

	if matchingUseCase != nil {
		var triggerErr error

		matchTriggerAdapter, triggerErr = crossAdapters.NewMatchTriggerAdapter(matchingUseCase)
		if triggerErr != nil {
			return fmt.Errorf("create match trigger adapter: %w", triggerErr)
		}
	}

	ingestionCommandUseCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
		JobRepo:         repos.ingestionJob,
		TransactionRepo: repos.ingestionTx,
		Dedupe:          dedupeService,
		Publisher:       publisher,
		OutboxRepo:      outboxRepository,
		Parsers:         ingestionRegistry,
		FieldMapRepo:    fieldMapAdapter,
		SourceRepo:      sourceAdapter,
		MatchTrigger:    matchTriggerAdapter,
		ContextProvider: autoMatchContextProvider,
	})
	if err != nil {
		return fmt.Errorf("create ingestion command use case: %w", err)
	}

	ingestionQueryUseCase, err := ingestionQuery.NewUseCase(
		repos.ingestionJob,
		repos.ingestionTx,
	)
	if err != nil {
		return fmt.Errorf("create ingestion query use case: %w", err)
	}

	ingestionHandler, err := ingestionHTTP.NewHandlers(
		ingestionCommandUseCase,
		ingestionQueryUseCase,
		contextAdapter,
		production,
	)
	if err != nil {
		return fmt.Errorf("create ingestion handler: %w", err)
	}

	if err := ingestionHTTP.RegisterRoutes(routes.Protected, ingestionHandler); err != nil {
		return fmt.Errorf("register ingestion routes: %w", err)
	}

	return nil
}

func initMatchingModule(
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepo outboxRepositories.OutboxRepository,
	repos *sharedRepositories,
	production bool,
) (*matchingCommand.UseCase, error) {
	contextAdapter, err := crossAdapters.NewContextProviderAdapter(repos.configContext)
	if err != nil {
		return nil, fmt.Errorf("create context provider adapter for matching: %w", err)
	}

	sourceAdapter, err := crossAdapters.NewSourceProviderAdapter(repos.configSource)
	if err != nil {
		return nil, fmt.Errorf("create source provider adapter for matching: %w", err)
	}

	ruleAdapter, err := crossAdapters.NewMatchRuleProviderAdapter(repos.configMatchRule)
	if err != nil {
		return nil, fmt.Errorf("create match rule provider adapter for matching: %w", err)
	}

	transactionAdapter, err := crossAdapters.NewTransactionRepositoryAdapterFromRepo(
		provider,
		repos.ingestionTx,
	)
	if err != nil {
		return nil, fmt.Errorf("create transaction adapter for matching: %w", err)
	}

	lockManager := matchLockManager.NewLockManager(provider)
	matchRunRepository := matchRunRepo.NewRepository(provider)
	matchGroupRepository := matchGroupRepo.NewRepository(provider)
	matchItemRepository := matchItemRepo.NewRepository(provider)
	exceptionCreator := matchExceptionRepo.NewRepository(provider)
	rateRepository := matchRateRepo.NewRepository(provider)
	feeVarianceRepository := matchFeeVarianceRepo.NewRepository(provider)

	useCase, err := matchingCommand.New(matchingCommand.UseCaseDeps{
		ContextProvider:  contextAdapter,
		SourceProvider:   sourceAdapter,
		RuleProvider:     ruleAdapter,
		TxRepo:           transactionAdapter,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepository,
		MatchGroupRepo:   matchGroupRepository,
		MatchItemRepo:    matchItemRepository,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		RateRepo:         rateRepository,
		FeeVarianceRepo:  feeVarianceRepository,
		AdjustmentRepo:   repos.adjustment,
		InfraProvider:    provider,
		AuditLogRepo:     repos.governanceAuditLog,
		FeeScheduleRepo:  repos.feeSchedule,
	})
	if err != nil {
		return nil, fmt.Errorf("create matching command use case: %w", err)
	}

	matchingQueryUseCase, err := matchingQuery.NewUseCase(matchRunRepository, matchGroupRepository, matchItemRepository)
	if err != nil {
		return nil, fmt.Errorf("create matching query use case: %w", err)
	}

	matchingHandler, err := matchingHTTP.NewHandler(
		useCase,
		matchingQueryUseCase,
		contextAdapter,
		production,
	)
	if err != nil {
		return nil, fmt.Errorf("create matching handler: %w", err)
	}

	if err := matchingHTTP.RegisterRoutes(routes.Protected, matchingHandler); err != nil {
		return nil, fmt.Errorf("register matching routes: %w", err)
	}

	return useCase, nil
}

func initReportingModule(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	storage reportingPorts.ObjectStorageClient,
	rateLimitStorage fiber.Storage,
	logger libLog.Logger,
	repos *sharedRepositories,
	production bool,
) (*reportingWorker.ExportWorker, *reportingWorker.CleanupWorker, error) {
	contextAdapter := crossAdapters.NewReportingContextProviderAdapter(repos.configContext)

	dashboardRepository := reportDashboard.NewRepository(provider)
	reportRepository := reportRepo.NewRepository(provider)
	exportJobRepository := reportExportJob.NewRepository(provider)
	cacheService := reportingRedis.NewCacheService(provider, 0)

	dashboardUseCase, err := reportingQuery.NewDashboardUseCase(dashboardRepository, cacheService)
	if err != nil {
		return nil, nil, fmt.Errorf("create dashboard use case: %w", err)
	}

	exportUseCase, err := reportingQuery.NewUseCase(reportRepository)
	if err != nil {
		return nil, nil, fmt.Errorf("create export use case: %w", err)
	}

	reportingHandler, err := reportingHTTP.NewHandlers(
		dashboardUseCase,
		contextAdapter,
		exportUseCase,
		production,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create reporting handler: %w", err)
	}

	exportLimiter := NewExportRateLimiter(cfg, rateLimitStorage)
	if configGetter != nil {
		exportLimiter = NewDynamicExportRateLimiter(configGetter, rateLimitStorage)
	}

	if err := reportingHTTP.RegisterRoutes(routes.Protected, reportingHandler, exportLimiter); err != nil {
		return nil, nil, fmt.Errorf("register reporting routes: %w", err)
	}

	if storage == nil {
		return nil, nil, nil
	}

	return initExportWorkers(
		routes,
		cfg,
		exportJobRepository,
		reportRepository,
		storage,
		contextAdapter,
		exportLimiter,
		logger,
	)
}

func initExportWorkers(
	routes *Routes,
	cfg *Config,
	exportJobRepository *reportExportJob.Repository,
	reportRepository *reportRepo.Repository,
	storage reportingPorts.ObjectStorageClient,
	contextAdapter *crossAdapters.ReportingContextProviderAdapter,
	exportLimiter fiber.Handler,
	logger libLog.Logger,
) (*reportingWorker.ExportWorker, *reportingWorker.CleanupWorker, error) {
	exportJobUseCase, err := reportingCommand.NewExportJobUseCase(exportJobRepository)
	if err != nil {
		return nil, nil, fmt.Errorf("create export job use case: %w", err)
	}

	exportJobQuerySvc, err := reportingQuery.NewExportJobQueryService(exportJobRepository)
	if err != nil {
		return nil, nil, fmt.Errorf("create export job query service: %w", err)
	}

	exportJobHandler, err := reportingHTTP.NewExportJobHandlers(
		exportJobUseCase,
		exportJobQuerySvc,
		storage,
		contextAdapter,
		cfg.ExportPresignExpiry(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create export job handler: %w", err)
	}

	if err := reportingHTTP.RegisterExportJobRoutes(routes.Protected, exportJobHandler, exportLimiter); err != nil {
		return nil, nil, fmt.Errorf("register export job routes: %w", err)
	}

	workerCfg := reportingWorker.ExportWorkerConfig{
		PollInterval: cfg.ExportWorkerPollInterval(),
		PageSize:     cfg.ExportWorker.PageSize,
	}

	exportWorker, err := reportingWorker.NewExportWorker(
		exportJobRepository,
		reportRepository,
		storage,
		workerCfg,
		logger,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create export worker: %w", err)
	}

	cleanupCfg := reportingWorker.CleanupWorkerConfig{
		Interval:              cfg.CleanupWorkerInterval(),
		BatchSize:             cfg.CleanupWorkerBatchSize(),
		FileDeleteGracePeriod: cfg.CleanupWorkerGracePeriod(),
	}

	cleanupWorker, err := reportingWorker.NewCleanupWorker(
		exportJobRepository,
		storage,
		cleanupCfg,
		logger,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create cleanup worker: %w", err)
	}

	return exportWorker, cleanupWorker, nil
}

func initGovernanceModule(routes *Routes, repos *sharedRepositories, provider sharedPorts.InfrastructureProvider, production bool) error {
	governanceHandler, err := governanceHTTP.NewHandler(repos.governanceAuditLog, production)
	if err != nil {
		return fmt.Errorf("create governance handler: %w", err)
	}

	if err := governanceHTTP.RegisterRoutes(routes.Protected, governanceHandler); err != nil {
		return fmt.Errorf("register governance routes: %w", err)
	}

	// Actor mapping CRUD
	actorMappingRepo := actorMappingRepoAdapter.NewRepository(provider)

	actorMappingCommandUC, err := governanceCommand.NewActorMappingUseCase(actorMappingRepo)
	if err != nil {
		return fmt.Errorf("create actor mapping command use case: %w", err)
	}

	actorMappingQueryUC, err := governanceQuery.NewActorMappingQueryUseCase(actorMappingRepo)
	if err != nil {
		return fmt.Errorf("create actor mapping query use case: %w", err)
	}

	actorMappingHandler, err := governanceHTTP.NewActorMappingHandler(actorMappingCommandUC, actorMappingQueryUC)
	if err != nil {
		return fmt.Errorf("create actor mapping handler: %w", err)
	}

	if err := governanceHTTP.RegisterActorMappingRoutes(routes.Protected, actorMappingHandler); err != nil {
		return fmt.Errorf("register actor mapping routes: %w", err)
	}

	return nil
}

func initExceptionModule(
	cfg *Config,
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository outboxRepositories.OutboxRepository,
	dispatchLimiter fiber.Handler,
	repos *sharedRepositories,
	production bool,
) error {
	// Exception-specific repositories (not shared across modules)
	exceptionRepository := exceptionExceptionRepo.NewRepository(provider)
	disputeRepository := exceptionDisputeRepo.NewRepository(provider)
	commentRepository := exceptionCommentRepo.NewRepository(provider)

	deps, err := initExceptionDependencies(provider, outboxRepository, exceptionRepository, repos)
	if err != nil {
		return err
	}

	useCases, err := initExceptionUseCases(
		cfg,
		provider,
		exceptionRepository,
		disputeRepository,
		commentRepository,
		deps,
		repos,
	)
	if err != nil {
		return err
	}

	callbackUseCase, err := initExceptionCallbackUseCase(cfg, provider, exceptionRepository, deps)
	if err != nil {
		return err
	}

	// HTTP Handlers
	exceptionHandlers, err := exceptionHTTP.NewHandlers(
		useCases.exception,
		useCases.dispute,
		useCases.query,
		useCases.dispatch,
		useCases.comment,
		useCases.commentQuery,
		callbackUseCase,
		exceptionRepository,
		disputeRepository,
		production,
	)
	if err != nil {
		return fmt.Errorf("create exception handlers: %w", err)
	}

	if err := exceptionHTTP.RegisterRoutes(routes.Protected, exceptionHandlers, dispatchLimiter); err != nil {
		return fmt.Errorf("register exception routes: %w", err)
	}

	return nil
}

// exceptionModuleDeps holds cross-cutting adapters used by exception use cases.
type exceptionModuleDeps struct {
	auditPublisher     *exceptionAudit.OutboxPublisher
	actorExtractor     *exceptionAdapters.AuthActorExtractor
	resolutionExecutor *exceptionResolution.Executor
}

// exceptionUseCases holds all exception use cases created during module initialization.
type exceptionUseCases struct {
	exception    *exceptionCommand.UseCase
	dispute      *exceptionCommand.DisputeUseCase
	query        *exceptionQuery.UseCase
	dispatch     *exceptionCommand.DispatchUseCase
	comment      *exceptionCommand.CommentUseCase
	commentQuery *exceptionQuery.CommentQueryUseCase
}

// initExceptionDependencies creates the cross-cutting adapters for the exception module:
// audit publisher, actor extractor, transaction context lookup, matching gateway, and resolution executor.
func initExceptionDependencies(
	provider sharedPorts.InfrastructureProvider,
	outboxRepository outboxRepositories.OutboxRepository,
	exceptionRepository *exceptionExceptionRepo.Repository,
	repos *sharedRepositories,
) (*exceptionModuleDeps, error) {
	transactionAdapter, err := crossAdapters.NewTransactionRepositoryAdapterFromRepo(
		provider,
		repos.ingestionTx,
	)
	if err != nil {
		return nil, fmt.Errorf("create transaction adapter for exception: %w", err)
	}

	auditPublisher, err := exceptionAudit.NewOutboxPublisher(outboxRepository)
	if err != nil {
		return nil, fmt.Errorf("create audit publisher: %w", err)
	}

	actorExtractor := exceptionAdapters.NewAuthActorExtractor()

	// TransactionContextLookup wraps the transaction and job repositories to look up context IDs.
	// The source finder provides a fallback path: Transaction.SourceID -> context_id
	// when the primary ingestion job lookup fails.
	transactionContextLookup, err := crossAdapters.NewTransactionContextLookup(
		repos.ingestionTx,
		repos.ingestionJob,
	)
	if err != nil {
		return nil, fmt.Errorf("create transaction context lookup: %w", err)
	}

	transactionContextLookup.WithSourceFinder(repos.configSource)

	matchingGateway, err := crossAdapters.NewExceptionMatchingGateway(
		repos.adjustment,
		transactionAdapter,
		transactionContextLookup,
	)
	if err != nil {
		return nil, fmt.Errorf("create matching gateway: %w", err)
	}

	resolutionExecutor, err := exceptionResolution.NewExecutor(
		exceptionRepository,
		matchingGateway,
		actorExtractor,
	)
	if err != nil {
		return nil, fmt.Errorf("create resolution executor: %w", err)
	}

	return &exceptionModuleDeps{
		auditPublisher:     auditPublisher,
		actorExtractor:     actorExtractor,
		resolutionExecutor: resolutionExecutor,
	}, nil
}

// initExceptionUseCases creates the core exception use cases (exception, dispute, query, dispatch, comment).
func initExceptionUseCases(
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	exceptionRepository *exceptionExceptionRepo.Repository,
	disputeRepository *exceptionDisputeRepo.Repository,
	commentRepository *exceptionCommentRepo.Repository,
	deps *exceptionModuleDeps,
	repos *sharedRepositories,
) (*exceptionUseCases, error) {
	exceptionUseCase, err := exceptionCommand.NewUseCase(
		exceptionRepository,
		deps.resolutionExecutor,
		deps.auditPublisher,
		deps.actorExtractor,
		provider,
	)
	if err != nil {
		return nil, fmt.Errorf("create exception use case: %w", err)
	}

	disputeUseCase, err := exceptionCommand.NewDisputeUseCase(
		disputeRepository,
		exceptionRepository,
		deps.auditPublisher,
		deps.actorExtractor,
		provider,
	)
	if err != nil {
		return nil, fmt.Errorf("create dispute use case: %w", err)
	}

	queryUseCase, err := exceptionQuery.NewUseCase(
		exceptionRepository,
		disputeRepository,
		repos.governanceAuditLog,
		&tenantExtractorAdapter{},
	)
	if err != nil {
		return nil, fmt.Errorf("create exception query use case: %w", err)
	}

	webhookTimeout := cfg.WebhookTimeout()

	httpConnector, err := exceptionConnectors.NewHTTPConnector(
		exceptionConnectors.ConnectorConfig{
			Webhook: &exceptionConnectors.WebhookConnectorConfig{
				BaseConnectorConfig: exceptionConnectors.BaseConnectorConfig{
					Timeout: &webhookTimeout,
				},
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create http connector: %w", err)
	}

	dispatchUseCase, err := exceptionCommand.NewDispatchUseCase(
		exceptionRepository,
		httpConnector,
		deps.auditPublisher,
		deps.actorExtractor,
		provider,
	)
	if err != nil {
		return nil, fmt.Errorf("create dispatch use case: %w", err)
	}

	commentUseCase, err := exceptionCommand.NewCommentUseCase(
		commentRepository,
		exceptionRepository,
		deps.actorExtractor,
	)
	if err != nil {
		return nil, fmt.Errorf("create comment use case: %w", err)
	}

	commentQueryUseCase, err := exceptionQuery.NewCommentQueryUseCase(commentRepository)
	if err != nil {
		return nil, fmt.Errorf("create comment query use case: %w", err)
	}

	return &exceptionUseCases{
		exception:    exceptionUseCase,
		dispute:      disputeUseCase,
		query:        queryUseCase,
		dispatch:     dispatchUseCase,
		comment:      commentUseCase,
		commentQuery: commentQueryUseCase,
	}, nil
}

// initExceptionCallbackUseCase creates the callback use case for processing external system webhooks.
func initExceptionCallbackUseCase(
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	exceptionRepository *exceptionExceptionRepo.Repository,
	deps *exceptionModuleDeps,
) (*exceptionCommand.CallbackUseCase, error) {
	callbackRateLimiter, err := exceptionRedis.NewCallbackRateLimiter(provider, 0, 0) // 0 values -> defaults: 60/min
	if err != nil {
		return nil, fmt.Errorf("create callback rate limiter: %w", err)
	}

	callbackIdempotencyRepo, err := exceptionRedis.NewIdempotencyRepositoryWithConfig(
		provider,
		cfg.IdempotencyRetryWindow(),
		cfg.IdempotencySuccessTTL(),
		cfg.Idempotency.HMACSecret,
	)
	if err != nil {
		return nil, fmt.Errorf("create callback idempotency repository: %w", err)
	}

	callbackUseCase, err := exceptionCommand.NewCallbackUseCase(
		callbackIdempotencyRepo,
		exceptionRepository,
		deps.auditPublisher,
		provider,
		callbackRateLimiter,
	)
	if err != nil {
		return nil, fmt.Errorf("create callback use case: %w", err)
	}

	return callbackUseCase, nil
}

// InfraStatus tracks the status of infrastructure components for consolidated logging.
type InfraStatus struct {
	PostgresConnected      bool
	RedisConnected         bool
	RedisMode              string
	RabbitMQConnected      bool
	ObjectStorageEnabled   bool
	HasReplica             bool
	ExportWorkerEnabled    bool
	CleanupWorkerEnabled   bool
	ArchivalWorkerEnabled  bool
	SchedulerWorkerEnabled bool
	TelemetryConfigured    bool
	TelemetryActive        bool
	TelemetryDegraded      bool
}

func shouldRedactInfraDetails(envName string) bool {
	return IsProductionEnvironment(envName)
}

func safeInfraTarget(envName, value string) string {
	if shouldRedactInfraDetails(envName) {
		return "configured"
	}

	return value
}

func logStartupInfo(logger libLog.Logger, cfg *Config, status *InfraStatus) {
	ctx := context.Background()

	banner := `
                    __       __             
   _________ ______/ /______/ /_  ___  _____
  / __  __ /  __ / __/  ___/ __ \/ _ \/ ___/
 / / / / / / /_/ / /_/ /__/ / / /  __/ /    
/_/ /_/ /_/\__,_/\__/\___/_/ /_/\___/_/     
                       by lerian studio         
`
	logger.Log(ctx, libLog.LevelInfo, banner)

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  🚀 SERVICE CONFIGURATION")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  Environment     : "+cfg.App.EnvName)
	logger.Log(ctx, libLog.LevelInfo, "  Server Address  : "+cfg.Server.Address)
	logger.Log(ctx, libLog.LevelInfo, "  Log Level       : "+cfg.App.LogLevel)
	logger.Log(ctx, libLog.LevelInfo, "")

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  📦 INFRASTRUCTURE")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	pgStatus := formatConnStatus(status.PostgresConnected)
	pgDisplay := safeInfraTarget(cfg.App.EnvName, cfg.Postgres.PrimaryHost+"/"+cfg.Postgres.PrimaryDB)

	if status.HasReplica {
		pgDisplay += " (+replica)"
	}

	logger.Log(ctx, libLog.LevelInfo, "  PostgreSQL      : "+pgDisplay+" "+pgStatus)

	redisDisplay := safeInfraTarget(cfg.App.EnvName, cfg.Redis.Host)

	if status.RedisMode != "" {
		redisDisplay = redisDisplay + " (" + status.RedisMode + ")"
	}

	logger.Log(ctx, libLog.LevelInfo, "  Redis           : "+redisDisplay+" "+formatConnStatus(status.RedisConnected))

	logger.Log(ctx, libLog.LevelInfo, "  RabbitMQ        : "+safeInfraTarget(cfg.App.EnvName, cfg.RabbitMQ.Host)+" "+formatConnStatus(status.RabbitMQConnected))

	if cfg.ObjectStorage.Endpoint != "" && cfg.ObjectStorage.Bucket != "" {
		objStatus := formatConnStatus(status.ObjectStorageEnabled)
		logger.Log(ctx, libLog.LevelInfo, "  Object Storage  : "+safeInfraTarget(cfg.App.EnvName, cfg.ObjectStorage.Endpoint+"/"+cfg.ObjectStorage.Bucket)+" "+objStatus)
	}

	telemetryStatus := formatFeatureStatus(status.TelemetryConfigured)
	if status.TelemetryDegraded {
		telemetryStatus = "degraded ⚠ (collector unavailable at startup)"
	}

	logger.Log(ctx, libLog.LevelInfo, "  Telemetry       : "+telemetryStatus)
	logger.Log(ctx, libLog.LevelInfo, "")

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  🔧 FEATURES")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  Authentication  : "+formatFeatureStatus(cfg.Auth.Enabled))
	logger.Log(ctx, libLog.LevelInfo, "  Multi-Tenant    : "+formatFeatureStatus(cfg.Tenancy.MultiTenantInfraEnabled))
	logger.Log(ctx, libLog.LevelInfo, "")

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  ⚙️  WORKERS")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  Export Worker   : "+formatWorkerStatus(status.ExportWorkerEnabled, cfg.ExportWorkerPollInterval()))
	logger.Log(ctx, libLog.LevelInfo, "  Cleanup Worker  : "+formatWorkerStatus(status.CleanupWorkerEnabled, time.Hour))
	logger.Log(ctx, libLog.LevelInfo, "  Archival Worker : "+formatWorkerStatus(status.ArchivalWorkerEnabled, cfg.ArchivalInterval()))
	logger.Log(ctx, libLog.LevelInfo, "  Scheduler Worker: "+formatWorkerStatus(status.SchedulerWorkerEnabled, time.Minute))
	logger.Log(ctx, libLog.LevelInfo, "")

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  ✅ Matcher service ready to accept connections")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

const statusDisabled = "disabled ✗"

func formatConnStatus(connected bool) string {
	if connected {
		return "✅"
	}

	return "❌"
}

func formatFeatureStatus(enabled bool) string {
	if enabled {
		return "enabled ✓"
	}

	return statusDisabled
}

func formatWorkerStatus(enabled bool, interval time.Duration) string {
	if enabled {
		return fmt.Sprintf("enabled (interval: %v)", interval)
	}

	return statusDisabled
}

// initArchivalComponents initializes the archival worker and archive retrieval routes.
// Archive routes are registered when archival storage is available (even if the worker is disabled),
// allowing users to query existing archives. The worker is only constructed when cfg.Archival.Enabled is true.
//
// A dedicated *sql.DB connection pool is created for archival operations because:
// 1. lib-commons postgres.Client.Resolver() returns dbresolver.DB (not *sql.DB)
// 2. Archival needs raw *sql.DB for DDL operations (CREATE/ALTER/DROP TABLE)
// 3. Isolating archival's long-running operations from the main pool prevents interference.
func initArchivalComponents(
	routes *Routes,
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	logger libLog.Logger,
	cleanups *[]func(),
) (*governanceWorker.ArchivalWorker, error) {
	ctx := context.Background()

	archiveRepo := archiveMetadataRepo.NewRepository(provider)

	archivalStorage, err := createArchivalStorage(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create archival storage: %w", err)
	}

	// Register archive retrieval routes when storage is available.
	// This allows querying existing archives even when the worker is paused.
	if archivalStorage != nil {
		archiveHandler, handlerErr := governanceHTTP.NewArchiveHandler(
			archiveRepo,
			archivalStorage,
			cfg.ArchivalPresignExpiry(),
		)
		if handlerErr != nil {
			return nil, fmt.Errorf("create archive handler: %w", handlerErr)
		}

		if routeErr := governanceHTTP.RegisterArchiveRoutes(routes.Protected, archiveHandler); routeErr != nil {
			return nil, fmt.Errorf("register archive routes: %w", routeErr)
		}
	}

	if !cfg.Archival.Enabled {
		return nil, nil
	}

	if archivalStorage == nil {
		return nil, ErrArchivalStorageRequired
	}

	// Open a dedicated *sql.DB for archival DDL operations.
	// Use cfg.PrimaryDSN() since v2 postgres.Client does not expose the connection string.
	archivalDB, dbErr := sql.Open("pgx", cfg.PrimaryDSN())
	if dbErr != nil {
		return nil, fmt.Errorf("open database for archival worker: %w", dbErr)
	}

	// Close archivalDB on error paths to prevent connection leak.
	success := false

	defer func() {
		if !success {
			if closeErr := archivalDB.Close(); closeErr != nil {
				logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close archival DB on error path: %v", closeErr))
			}
		}
	}()

	archivalDB.SetMaxOpenConns(archivalMaxOpenConns)
	archivalDB.SetMaxIdleConns(archivalMaxIdleConns)
	archivalDB.SetConnMaxLifetime(cfg.ConnMaxLifetime())
	archivalDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime())

	tracer := otel.Tracer(constants.ApplicationName)

	partitionMgr, pmErr := governanceCommand.NewPartitionManager(archivalDB, logger, tracer)
	if pmErr != nil {
		return nil, fmt.Errorf("create partition manager: %w", pmErr)
	}

	archivalWorkerCfg := governanceWorker.ArchivalWorkerConfig{
		Interval:            cfg.ArchivalInterval(),
		HotRetentionDays:    cfg.Archival.HotRetentionDays,
		WarmRetentionMonths: cfg.Archival.WarmRetentionMonths,
		ColdRetentionMonths: cfg.Archival.ColdRetentionMonths,
		BatchSize:           cfg.Archival.BatchSize,
		StorageBucket:       cfg.Archival.StorageBucket,
		StoragePrefix:       cfg.Archival.StoragePrefix,
		StorageClass:        cfg.Archival.StorageClass,
		PartitionLookahead:  cfg.Archival.PartitionLookahead,
		PresignExpiry:       cfg.ArchivalPresignExpiry(),
	}

	worker, workerErr := governanceWorker.NewArchivalWorker(
		archiveRepo,
		partitionMgr,
		archivalStorage,
		archivalDB,
		provider,
		archivalWorkerCfg,
		logger,
	)
	if workerErr != nil {
		return nil, fmt.Errorf("create archival worker: %w", workerErr)
	}

	success = true

	// Register archivalDB cleanup for shutdown. The ArchivalWorker.Stop() does not
	// close the DB, so we must track it for explicit cleanup.
	*cleanups = append(*cleanups, func() {
		if err := archivalDB.Close(); err != nil {
			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to close archival database: %v", err))
		}
	})

	return worker, nil
}

// createArchivalStorage creates an S3-compatible object storage client for the archival bucket.
// Returns (nil, nil) when archival storage is not configured (no bucket or no endpoint).
func createArchivalStorage(
	cfg *Config,
	_ libLog.Logger,
) (reportingPorts.ObjectStorageClient, error) {
	if cfg.Archival.StorageBucket == "" {
		return nil, nil
	}

	if cfg.ObjectStorage.Endpoint == "" {
		return nil, nil
	}

	s3Cfg := reportingStorage.S3Config{
		Endpoint:        cfg.ObjectStorage.Endpoint,
		Region:          cfg.ObjectStorage.Region,
		Bucket:          cfg.Archival.StorageBucket,
		AccessKeyID:     cfg.ObjectStorage.AccessKeyID,
		SecretAccessKey: cfg.ObjectStorage.SecretAccessKey,
		UsePathStyle:    cfg.ObjectStorage.UsePathStyle,
	}

	client, err := newS3ClientFn(context.Background(), s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create archival S3 client: %w", err)
	}

	return client, nil
}
