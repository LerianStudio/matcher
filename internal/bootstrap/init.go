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
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bxcodec/dbresolver/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/LerianStudio/lib-auth/v3/auth/middleware"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/errgroup"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	outboxpg "github.com/LerianStudio/lib-commons/v5/commons/outbox/postgres"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
	tmclient "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/client"
	tmmiddleware "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/middleware"
	tmpostgres "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/postgres"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/rabbitmq"
	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/tenantcache"
	libZap "github.com/LerianStudio/lib-commons/v5/commons/zap"

	"github.com/LerianStudio/matcher/internal/auth"
	configAudit "github.com/LerianStudio/matcher/internal/configuration/adapters/audit"
	configHTTP "github.com/LerianStudio/matcher/internal/configuration/adapters/http"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFeeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configScheduleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/schedule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configCommand "github.com/LerianStudio/matcher/internal/configuration/services/command"
	configQuery "github.com/LerianStudio/matcher/internal/configuration/services/query"
	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
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
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	matchLockManager "github.com/LerianStudio/matcher/internal/matching/adapters/redis"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	matchingQuery "github.com/LerianStudio/matcher/internal/matching/services/query"
	reportingHTTP "github.com/LerianStudio/matcher/internal/reporting/adapters/http"
	reportDashboard "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/dashboard"
	reportExportJob "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/export_job"
	reportRepo "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/report"
	reportingRedis "github.com/LerianStudio/matcher/internal/reporting/adapters/redis"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	reportingCommand "github.com/LerianStudio/matcher/internal/reporting/services/command"
	reportingQuery "github.com/LerianStudio/matcher/internal/reporting/services/query"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
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

	initializeAuthBoundaryLoggerFn = func() (libLog.Logger, error) {
		return libZap.New(libZap.Config{})
	}

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

	status := statusSuccess
	if !success {
		status = statusError
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

	done := timer.track("logger")

	logger, err := initLogger(opts)
	if err != nil {
		return nil, fmt.Errorf("initialize logger: %w", err)
	}

	logger = NewSwappableLogger(logger)

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

	done = timer.track("telemetry")

	asserter := assert.New(ctx, logger, constants.ApplicationName, "bootstrap")

	telemetry := initTelemetryAndMetrics(ctx, cfg, logger)

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

	if err := connectInfrastructure(infraCtx, asserter, cfg, postgresConnection, rabbitMQConnection, logger); err != nil {
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
	case dbErr != nil || primaryDB == nil:
		if IsProductionEnvironment(cfg.App.EnvName) {
			return nil, fmt.Errorf("systemplane init: postgres primary unavailable: %w", dbErr)
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

	authClient := createAuthClient(ctx, cfg, logger)

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
	)
	if err != nil {
		return nil, err
	}

	archivalWorker, archivalErr := initArchivalComponents(routes, cfg, configManager.Get, settingsResolver, infraProvider, logger, &cleanups, IsProductionEnvironment(cfg.App.EnvName))
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

	// Register ConfigManager Stop() in cleanups so resources are torn down on shutdown.
	if configManager != nil {
		cleanups = append(cleanups, configManager.Stop)
	}

	success = true

	return &Service{
		Server:             server,
		Logger:             logger,
		Config:             cfg,
		Routes:             routes,
		ConfigManager:      configManager,
		outboxRunner:       modules.outboxDispatcher,
		dbMetricsCollector: dbMetricsCollector,
		workerManager:      wm,
		connectionManager:  connCloser,
		cleanupFuncs:       cleanups,
		readinessState:     readiness,
		spClient:           spClient,
	}, nil
}

// IMPORTANT: Worker re-entrancy contract
// Each factory closure returns the SAME worker instance (captured from modules).
// The WorkerManager calls Stop() -> UpdateRuntimeConfig() -> Start() on the same
// instance during restarts. All workers MUST support this lifecycle by implementing
// prepareRunState() to reinitialize channels and sync primitives. Workers that do
// NOT support Stop -> Start re-entrancy may hang or panic on restart because
// they can retain closed channels or stale synchronization state.
// registerCriticalWorkers registers workers that are critical when explicitly enabled
// via config (export, cleanup, archival).
func registerCriticalWorkers(wm *WorkerManager, modules *modulesResult) {
	if modules.exportWorker != nil {
		w := modules.exportWorker

		wm.Register("export",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.ExportWorker.Enabled },
			func(cfg *Config) bool { return cfg != nil && cfg.ExportWorker.Enabled },
		)
	}

	if modules.cleanupWorker != nil {
		w := modules.cleanupWorker

		wm.Register("cleanup",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.CleanupWorker.Enabled },
			func(cfg *Config) bool { return cfg != nil && cfg.CleanupWorker.Enabled },
		)
	}

	if modules.archivalWorker != nil {
		w := modules.archivalWorker

		wm.Register("archival",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.Archival.Enabled },
			func(cfg *Config) bool { return cfg != nil && cfg.Archival.Enabled },
		)
	}
}

// buildWorkerManager creates a WorkerManager and registers all workers from the
// init-time module results. Each worker is wrapped in a factory closure that
// returns the pre-built instance. The factory's cfg parameter is available for
// future hot-reload support where workers can be reconstructed from new config.
func buildWorkerManager(modules *modulesResult, existing *WorkerManager, configManager *ConfigManager, logger libLog.Logger) *WorkerManager {
	wm := existing
	if wm == nil {
		wm = NewWorkerManager(logger, configManager)
	}

	if modules == nil {
		return wm
	}

	registerCriticalWorkers(wm, modules)

	// Scheduler worker — always non-critical.
	if modules.schedulerWorker != nil {
		w := modules.schedulerWorker

		wm.Register("scheduler",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(_ *Config) bool { return true }, // always enabled when present
			nil,                                  // never critical
		)
	}

	// Discovery worker — always non-critical.
	if modules.discoveryWorker != nil {
		w := modules.discoveryWorker

		wm.Register("discovery",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.Fetcher.Enabled },
			nil, // never critical
		)
	}

	// Fetcher bridge worker (T-003) — runs only when Fetcher is enabled
	// AND the verified-artifact pipeline is operational. Non-critical:
	// startup failure to Start does NOT abort matcher boot because the
	// bridge worker's absence only affects Fetcher-sourced data; other
	// reconciliation flows continue.
	if modules.bridgeWorker != nil {
		w := modules.bridgeWorker

		wm.Register("fetcher_bridge",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.Fetcher.Enabled },
			nil, // never critical
		)
	}

	// Custody retention sweep worker (T-006) — runs only when Fetcher is
	// enabled AND the verified-artifact pipeline is operational (which
	// means the custody store is available to delete from). Non-critical
	// because retention is a background housekeeping task: orphan
	// accumulation rates are bounded by happy-path bridge throughput so a
	// short sweep outage is operationally tolerable.
	if modules.custodyRetentionWorker != nil {
		w := modules.custodyRetentionWorker

		wm.Register("custody_retention",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.Fetcher.Enabled },
			nil, // never critical
		)
	}

	return wm
}

// initTelemetryAndMetrics initializes OpenTelemetry with timeout protection and
// registers assertion/panic metrics if telemetry is available.
func initTelemetryAndMetrics(ctx context.Context, cfg *Config, logger libLog.Logger) *libOpentelemetry.Telemetry {
	telemetryCtx, telemetryCancel := context.WithTimeout(ctx, cfg.InfraConnectTimeout()) //nolint:contextcheck // InfraConnectTimeout is a pure config accessor
	defer telemetryCancel()

	telemetry := InitTelemetryWithTimeout(telemetryCtx, cfg, logger)

	if telemetry != nil {
		assert.InitAssertionMetrics(telemetry.MetricsFactory)
		runtime.InitPanicMetrics(telemetry.MetricsFactory)
	}

	return telemetry
}

// createAuthClient builds the authentication middleware client with a bridge logger
// for the auth boundary.
func createAuthClient(ctx context.Context, cfg *Config, logger libLog.Logger) *middleware.AuthClient {
	authLogger, authLoggerErr := initializeAuthBoundaryLogger()
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

// checkClientConnected returns the connected state of a client that exposes IsConnected.
// Returns false when the client is nil or IsConnected returns an error.
func checkClientConnected[T interface{ IsConnected() (bool, error) }](client T) bool {
	connected, err := client.IsConnected()
	if err != nil {
		return false
	}

	return connected
}

func buildWorkerStatus(cfg *Config, modules *modulesResult) (export, cleanup, archival, scheduler, discovery bool) {
	if modules == nil {
		return false, false, false, false, false
	}

	return modules.exportWorker != nil && cfg.ExportWorker.Enabled,
		modules.cleanupWorker != nil && cfg.CleanupWorker.Enabled,
		modules.archivalWorker != nil && cfg.Archival.Enabled,
		modules.schedulerWorker != nil,
		modules.discoveryWorker != nil && cfg.Fetcher.Enabled
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
	exportEnabled, cleanupEnabled, archivalEnabled, schedulerEnabled, discoveryEnabled := buildWorkerStatus(cfg, modules)

	status := &InfraStatus{
		PostgresConnected:      pgConnected,
		RedisConnected:         redisConnected,
		RabbitMQConnected:      rabbitmq != nil && rabbitmq.Channel != nil,
		HasReplica:             cfg.Postgres.ReplicaHost != "" && cfg.Postgres.ReplicaHost != cfg.Postgres.PrimaryHost,
		ObjectStorageEnabled:   healthDeps != nil && healthDeps.ObjectStorage != nil,
		ExportWorkerEnabled:    exportEnabled,
		CleanupWorkerEnabled:   cleanupEnabled,
		ArchivalWorkerEnabled:  archivalEnabled,
		SchedulerWorkerEnabled: schedulerEnabled,
		DiscoveryWorkerEnabled: discoveryEnabled,
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

func detachedContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.TODO()
	}

	return context.WithoutCancel(ctx)
}

func appendCleanup(cleanups *[]func(), cleanup func()) {
	if cleanups == nil || cleanup == nil {
		return
	}

	*cleanups = append(*cleanups, cleanup)
}

func assignReplicaHealthCheck(
	ctx context.Context,
	cfg *Config,
	logger libLog.Logger,
	deps *HealthDependencies,
	cleanups *[]func(),
) {
	if cfg.Postgres.ReplicaHost == "" || cfg.Postgres.ReplicaHost == cfg.Postgres.PrimaryHost {
		return
	}

	check, cleanup := createPostgresReplicaHealthCheck(ctx, cfg, logger)
	deps.PostgresReplicaCheck = check

	appendCleanup(cleanups, cleanup)
}

func resolvePrimaryDB(checkCtx context.Context, postgres *libPostgres.Client) (*sql.DB, error) {
	resolver, err := postgres.Resolver(checkCtx)
	if err != nil {
		return nil, fmt.Errorf("postgres health check: get primary db failed: %w", err)
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		return nil, errPostgresPrimaryNil
	}

	return primaryDBs[0], nil
}

func resolveReplicaDB(checkCtx context.Context, postgres *libPostgres.Client) (*sql.DB, error) {
	resolver, err := postgres.Resolver(checkCtx)
	if err != nil {
		return nil, fmt.Errorf("postgres replica health check: get db failed: %w", err)
	}

	replicaDBs := resolver.ReplicaDBs()
	if len(replicaDBs) == 0 || replicaDBs[0] == nil {
		return nil, errNoReplicasConfigured
	}

	return replicaDBs[0], nil
}

func pingSQLDB(checkCtx context.Context, db *sql.DB, operation string) error {
	if err := db.PingContext(checkCtx); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}

	return nil
}

func newPostgresHealthCheck(
	postgres *libPostgres.Client,
) HealthCheckFunc {
	return func(checkCtx context.Context) error {
		if postgres == nil {
			return errPostgresPrimaryNil
		}

		primaryDB, err := resolvePrimaryDB(checkCtx, postgres)
		if err != nil {
			return err
		}

		return pingSQLDB(checkCtx, primaryDB, "postgres health check: ping primary db")
	}
}

func newRedisHealthCheck(
	redis *libRedis.Client,
) HealthCheckFunc {
	return func(checkCtx context.Context) error {
		if redis == nil {
			return errRedisClientNil
		}

		client, err := redis.GetClient(checkCtx)
		if err != nil {
			return fmt.Errorf("redis health check: get client failed: %w", err)
		}

		if client == nil {
			return errRedisClientNil
		}

		if err := client.Ping(checkCtx).Err(); err != nil {
			return fmt.Errorf("redis health check: ping failed: %w", err)
		}

		return nil
	}
}

func newPostgresReplicaHealthCheck(
	postgres *libPostgres.Client,
) HealthCheckFunc {
	return func(checkCtx context.Context) error {
		if postgres == nil {
			return errNoReplicasConfigured
		}

		replicaDB, err := resolveReplicaDB(checkCtx, postgres)
		if err != nil {
			return err
		}

		return pingSQLDB(checkCtx, replicaDB, "postgres replica health check: ping replica db")
	}
}

func newRabbitMQHealthCheck(
	rabbitmq *libRabbitmq.RabbitMQConnection,
) HealthCheckFunc {
	return func(checkCtx context.Context) error {
		conn := rabbitmq
		if conn == nil {
			return errRabbitMQConnectionNil
		}

		if conn.HealthCheckURL != "" &&
			(conn.AllowInsecureHealthCheck || !isInsecureHTTPHealthCheckURL(conn.HealthCheckURL)) {
			if err := checkRabbitMQHTTPHealth(checkCtx, conn.HealthCheckURL); err == nil {
				return nil
			}
		}

		if err := conn.EnsureChannel(); err != nil {
			return fmt.Errorf("rabbitmq health check: ensure channel: %w", err)
		}

		return nil
	}
}

func attachBundleHealthChecks(
	deps *HealthDependencies,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
) {
	deps.PostgresCheck = newPostgresHealthCheck(postgres)
	deps.RedisCheck = newRedisHealthCheck(redis)
	deps.PostgresReplicaCheck = newPostgresReplicaHealthCheck(postgres)
	deps.RabbitMQCheck = newRabbitMQHealthCheck(rabbitmq)
}

func configureObjectStorageHealthChecks(
	ctx context.Context,
	cfg *Config,
	deps *HealthDependencies,
	logger libLog.Logger,
) error {
	objectStorage, err := createObjectStorageForHealth(ctx, cfg)
	if err != nil {
		if cfg.ExportWorker.Enabled {
			return fmt.Errorf("object storage required when EXPORT_WORKER_ENABLED=true: %w", err)
		}

		logger.Log(ctx, libLog.LevelDebug, fmt.Sprintf("Object storage health check disabled: %v", err))
	} else if objectStorage != nil {
		deps.ObjectStorage = objectStorage
	}

	deps.ObjectStorageCheck = func(checkCtx context.Context) error {
		if deps.ObjectStorage == nil {
			return nil
		}

		_, existsErr := deps.ObjectStorage.Exists(checkCtx, ".health-check")
		if existsErr != nil {
			return fmt.Errorf("object storage health check: exists: %w", existsErr)
		}

		return nil
	}

	return nil
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

	assignReplicaHealthCheck(ctx, cfg, logger, deps, cleanups)
	attachBundleHealthChecks(deps, postgres, redis, rabbitmq)

	if err := configureObjectStorageHealthChecks(ctx, cfg, deps, logger); err != nil {
		return nil, err
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

func createPostgresReplicaHealthCheck(
	ctx context.Context,
	cfg *Config,
	logger libLog.Logger,
) (HealthCheckFunc, func()) {
	replicaDSN := cfg.ReplicaDSN()
	logCtx := detachedContext(ctx)

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
				logger.Log(logCtx, libLog.LevelError, fmt.Sprintf("failed to close postgres replica health check connection: %v", err))
			}
		}
	}

	return check, cleanup
}

func createObjectStorageForHealth(
	ctx context.Context,
	cfg *Config,
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
		AllowInsecure:   allowInsecureObjectStorageEndpoint(cfg),
	}

	client, err := newS3ClientFn(detachedContext(ctx), s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 client for health check: %w", err)
	}

	return client, nil
}

func createInfraProvider(
	cfg *Config,
	configGetter func() *Config,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
) (sharedPorts.InfrastructureProvider, connectionCloser, fiber.Handler) {
	mtEnabled := multiTenantModeEnabled(cfg)

	metrics, metricsErr := NewMultiTenantMetrics(mtEnabled)
	if metricsErr != nil && cfg.Logger != nil {
		cfg.Logger.Log(context.Background(), libLog.LevelWarn,
			fmt.Sprintf("multi-tenant metrics not available: %v", metricsErr))
	}

	provider := newDynamicInfrastructureProvider(cfg, configGetter, postgres, redis, cfg.Logger, metrics)

	// Create the canonical TenantMiddleware when multi-tenant mode is enabled.
	// The middleware resolves per-tenant database connections from the lib-commons
	// tenant-manager and stores them in context for downstream handlers/repositories.
	// In single-tenant mode, tenantDBHandler is nil and WhenEnabled makes it a no-op.
	tenantDBHandler := initMultiTenantDBHandler(cfg, configGetter, provider)

	return provider, provider, tenantDBHandler
}

// initMultiTenantDBHandler creates a Fiber middleware handler for multi-tenant database
// resolution when multi-tenant mode is enabled. Returns nil in single-tenant mode.
//
// The middleware is built once at startup (or lazily on first request) with TenantCache
// and TenantLoader for cache-first tenant resolution. It is rebuilt only when the
// underlying pgManager changes (e.g., systemplane config reload), not on every request.
// This avoids per-request heap allocation while preserving dynamic manager swapping.
func initMultiTenantDBHandler(
	cfg *Config,
	configGetter func() *Config,
	provider *dynamicInfrastructureProvider,
) fiber.Handler {
	if !multiTenantModeEnabled(cfg) {
		return nil
	}

	logMultiTenantRedisStatus(cfg)

	tmClient, pgManager := initTenantManagerAtStartup(cfg, provider)

	tCache, tLoader := buildTenantCacheAndLoader(cfg, tmClient)

	// buildMiddleware constructs a TenantMiddleware with PG manager + cache/loader.
	// Called once at startup and again only when the pgManager changes.
	buildMiddleware := func(mgr *tmpostgres.Manager) *tmmiddleware.TenantMiddleware {
		opts := []tmmiddleware.TenantMiddlewareOption{
			tmmiddleware.WithPG(mgr),
			tmmiddleware.WithTenantCache(tCache),
		}

		if tLoader != nil {
			opts = append(opts, tmmiddleware.WithTenantLoader(tLoader))
		}

		return tmmiddleware.NewTenantMiddleware(opts...)
	}

	return newCachedTenantDBHandler(provider, configGetter, pgManager, buildMiddleware)
}

// logMultiTenantRedisStatus logs a warning when the multi-tenant Redis host is not
// configured, indicating that event-driven tenant discovery is inactive.
func logMultiTenantRedisStatus(cfg *Config) {
	if cfg.Tenancy.MultiTenantRedisHost == "" && cfg.Logger != nil {
		cfg.Logger.Log(context.Background(), libLog.LevelInfo,
			"MULTI_TENANT_REDIS_HOST not configured; event-driven tenant discovery not active (TTL-based cache only)")
	}
}

// initTenantManagerAtStartup builds the canonical tenant manager and shares it with
// the dynamic infrastructure provider. Returns (tmClient, pgManager); either may be
// nil if the tenant manager is not available at startup (will retry lazily).
func initTenantManagerAtStartup(
	cfg *Config,
	provider *dynamicInfrastructureProvider,
) (*tmclient.Client, *tmpostgres.Manager) {
	tmClient, pgManager, err := buildCanonicalTenantManager(cfg, cfg.Logger)
	if err != nil && cfg.Logger != nil {
		cfg.Logger.Log(context.Background(), libLog.LevelWarn,
			fmt.Sprintf("multi-tenant PG manager not available at startup (will retry lazily): %v", err))
	}

	if pgManager != nil {
		provider.mu.Lock()
		provider.pgManager = pgManager
		provider.tmClient = tmClient
		provider.multiTenantKey = dynamicMultiTenantKey(cfg)
		provider.mu.Unlock()
	}

	return tmClient, pgManager
}

// buildTenantCacheAndLoader creates the TenantCache and optional TenantLoader for
// cache-first tenant resolution. On cache hit the middleware skips the Tenant Manager
// API call; on miss or expiry the loader fetches fresh config and caches it.
func buildTenantCacheAndLoader(
	cfg *Config,
	tmClient *tmclient.Client,
) (*tenantcache.TenantCache, *tenantcache.TenantLoader) {
	tCache := tenantcache.NewTenantCache()

	var tLoader *tenantcache.TenantLoader
	if tmClient != nil {
		tLoader = tenantcache.NewTenantLoader(
			tmClient, tCache, constants.ApplicationName,
			cfg.MultiTenantCacheTTL(), cfg.Logger,
		)
	}

	return tCache, tLoader
}

// newCachedTenantDBHandler returns a Fiber handler that caches the TenantMiddleware
// and rebuilds it only when the pgManager pointer changes (runtime config reload).
// RWMutex allows concurrent reads on the hot path (steady state) while serialising
// writes on the cold path (pgManager swap after config reload).
func newCachedTenantDBHandler(
	provider *dynamicInfrastructureProvider,
	configGetter func() *Config,
	pgManager *tmpostgres.Manager,
	buildMiddleware func(*tmpostgres.Manager) *tmmiddleware.TenantMiddleware,
) fiber.Handler {
	var (
		mu      sync.RWMutex
		lastMgr *tmpostgres.Manager
		lastMid *tmmiddleware.TenantMiddleware
	)

	if pgManager != nil {
		lastMgr = pgManager
		lastMid = buildMiddleware(pgManager)
	}

	return func(fiberCtx *fiber.Ctx) error {
		mgr, mgrErr := provider.currentPGManager(fiberCtx.UserContext(), configGetter())
		if mgrErr != nil {
			return fmt.Errorf("resolve tenant postgres manager: %w", mgrErr)
		}

		// Fast path: read lock (hot path, no contention under normal operation).
		mu.RLock()

		if mgr == lastMgr && lastMid != nil {
			mid := lastMid

			mu.RUnlock()

			return mid.WithTenantDB(fiberCtx)
		}

		mu.RUnlock()

		// Slow path: write lock (cold path, only on pgManager change).
		mu.Lock()

		// Double-check after acquiring write lock — another goroutine may
		// have already completed the rebuild between RUnlock and Lock.
		if mgr != lastMgr || lastMid == nil {
			lastMgr = mgr
			lastMid = buildMiddleware(mgr)
		}

		mid := lastMid

		mu.Unlock()

		return mid.WithTenantDB(fiberCtx)
	}
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

func initializeAuthBoundaryLogger() (libLog.Logger, error) {
	authLogger, authLoggerErr := initializeAuthBoundaryLoggerFn()
	if authLoggerErr != nil {
		return nil, fmt.Errorf("initialize auth boundary logger: %w", authLoggerErr)
	}

	if authLogger == nil {
		return nil, fmt.Errorf("initialize auth boundary logger: %w", errAuthBoundaryLoggerNil)
	}

	return authLogger, nil
}

func schedulerInterval(cfg *Config) time.Duration {
	return cfg.SchedulerInterval()
}

func runtimeConfigOrFallback(cfg *Config, configGetter func() *Config) *Config {
	if configGetter != nil {
		if runtimeCfg := configGetter(); runtimeCfg != nil {
			return runtimeCfg
		}
	}

	return cfg
}

func resolveRuntimeDurationSetting(
	_ context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	fallbackFn func(*Config) time.Duration,
	resolverFn func(time.Duration) time.Duration,
) time.Duration {
	runtimeCfg := runtimeConfigOrFallback(cfg, configGetter)

	fallback := fallbackFn(cfg)
	if runtimeCfg != nil {
		fallback = fallbackFn(runtimeCfg)
	}

	if settingsResolver == nil {
		return fallback
	}

	return resolverFn(fallback)
}

func resolveRuntimeIntSetting(
	_ context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	fallbackFn func(*Config) int,
	resolverFn func(int) int,
) int {
	runtimeCfg := runtimeConfigOrFallback(cfg, configGetter)

	fallback := fallbackFn(cfg)
	if runtimeCfg != nil {
		fallback = fallbackFn(runtimeCfg)
	}

	if settingsResolver == nil {
		return fallback
	}

	return resolverFn(fallback)
}

func resolveIdempotencyRetryWindow(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return current.IdempotencyRetryWindow() },
		settingsResolver.idempotencyRetryWindow,
	)
}

func resolveIdempotencySuccessTTL(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return current.IdempotencySuccessTTL() },
		settingsResolver.idempotencySuccessTTL,
	)
}

func resolveCallbackRateLimit(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) int {
	return resolveRuntimeIntSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) int { return current.CallbackRateLimitPerMinute() },
		settingsResolver.callbackRateLimitPerMinute,
	)
}

func resolveWebhookTimeout(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return configuredWebhookTimeout(ctx, current) },
		settingsResolver.webhookTimeout,
	)
}

func resolveDedupeTTL(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return current.DedupeTTL() },
		settingsResolver.dedupeTTL,
	)
}

func resolveExportPresignExpiry(ctx context.Context, cfg *Config, configGetter func() *Config, settingsResolver *runtimeSettingsResolver) time.Duration {
	return resolveRuntimeDurationSetting(ctx, cfg, configGetter, settingsResolver,
		func(current *Config) time.Duration { return configuredExportPresignExpiry(ctx, current) },
		settingsResolver.exportPresignExpiry,
	)
}

func configuredWebhookTimeout(ctx context.Context, cfg *Config) time.Duration {
	return normalizedWebhookTimeout(ctx, cfg)
}

func configuredExportPresignExpiry(ctx context.Context, cfg *Config) time.Duration {
	return normalizedExportPresignExpiry(ctx, cfg)
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
	case defaultEnvName, envLocalName, envTestName:
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
	outboxDispatcher       *outbox.Dispatcher
	exportWorker           *reportingWorker.ExportWorker
	cleanupWorker          *reportingWorker.CleanupWorker
	archivalWorker         *governanceWorker.ArchivalWorker
	schedulerWorker        *configWorker.SchedulerWorker
	discoveryWorker        *discoveryWorker.DiscoveryWorker
	bridgeWorker           *discoveryWorker.BridgeWorker
	custodyRetentionWorker *discoveryWorker.CustodyRetentionWorker
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
	configFeeRule      *configFeeRuleRepo.Repository
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
		configFeeRule:      configFeeRuleRepo.NewRepository(provider),
		adjustment:         matchAdjustmentRepo.NewRepository(provider, auditLogRepo),
	}, nil
}

// buildAndAttachRabbitMQTenantManager constructs the RabbitMQ tenant manager
// when multi-tenant mode is enabled and attaches its resources to the
// infrastructure provider so they're cleaned up on provider.Close().
// Returns nil when multi-tenancy is disabled.
//
// Extracted from initModulesAndMessaging to keep the orchestration function
// under the gocognit complexity ceiling.
func buildAndAttachRabbitMQTenantManager(
	ctx context.Context,
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	logger libLog.Logger,
) *tmrabbitmq.Manager {
	if !multiTenantModeEnabled(cfg) {
		return nil
	}

	rmqTmClient, rmqMgr := buildRabbitMQTenantManagerWithClient(ctx, cfg, logger)

	// Store the RabbitMQ tenant-manager resources on the infrastructure provider
	// so they are cleaned up on provider.Close(). Without this, the tmClient and
	// Manager created by buildRabbitMQTenantManagerWithClient would be leaked.
	if dynProvider, ok := provider.(*dynamicInfrastructureProvider); ok && rmqMgr != nil {
		dynProvider.mu.Lock()
		dynProvider.rmqManager = rmqMgr
		dynProvider.rmqTmClient = rmqTmClient
		dynProvider.mu.Unlock()
	}

	return rmqMgr
}

//nolint:cyclop,gocyclo // module initialization requires sequential dependency setup for all bounded contexts.
func initModulesAndMessaging(
	ctx context.Context,
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	provider sharedPorts.InfrastructureProvider,
	postgresConnection *libPostgres.Client,
	rabbitMQConnection *libRabbitmq.RabbitMQConnection,
	rateLimiterGetter func() *ratelimit.RateLimiter,
	logger libLog.Logger,
) (*modulesResult, error) {
	// Build canonical outbox repository using the lib-commons outbox/postgres package.
	// SchemaResolver provides both TenantResolver and TenantDiscoverer for schema-per-tenant.
	// WithAllowEmptyTenant permits the default tenant (public schema) to operate without a UUID schema.
	// WithDefaultTenantID maps the default tenant to public schema for dispatch.
	schemaResolver, err := outboxpg.NewSchemaResolver(
		postgresConnection,
		outboxpg.WithAllowEmptyTenant(),
		outboxpg.WithDefaultTenantID(auth.GetDefaultTenantID()),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox schema resolver: %w", err)
	}

	sharedOutboxRepository, err := outboxpg.NewRepository(
		postgresConnection,
		schemaResolver,
		&defaultTenantDiscoverer{inner: schemaResolver},
		outboxpg.WithLogger(logger),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox repository: %w", err)
	}

	sharedRepos, err := initSharedRepositories(provider)
	if err != nil {
		return nil, fmt.Errorf("init shared repositories: %w", err)
	}

	isProduction := IsProductionEnvironment(cfg.App.EnvName)

	if err := initConfigurationModule(routes, provider, sharedOutboxRepository, sharedRepos, isProduction); err != nil {
		return nil, err
	}

	// Create RabbitMQ tenant manager when multi-tenant is enabled.
	// This provides Layer 1 (vhost isolation) for event publishers.
	rmqManager := buildAndAttachRabbitMQTenantManager(ctx, cfg, provider, logger)

	matchingPublisher, ingestionPublisher, err := initEventPublishers(ctx, rabbitMQConnection, logger, rmqManager)
	if err != nil {
		return nil, err
	}

	matchingUseCase, err := initMatchingModule(routes, provider, sharedOutboxRepository, sharedRepos, isProduction)
	if err != nil {
		return nil, err
	}

	ingestionUseCase, err := initIngestionModule(cfg, configGetter, settingsResolver, routes, provider, sharedOutboxRepository, ingestionPublisher, matchingUseCase, sharedRepos, isProduction)
	if err != nil {
		return nil, err
	}

	storage, err := createObjectStorage(ctx, cfg)
	if err != nil {
		if reportingStorageRequired(cfg) {
			return nil, fmt.Errorf("create object storage: %w", err)
		}

		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Object storage not available, reporting background workers disabled: %v", err))
	}

	// Wrap storage in a dynamic client that always exposes a non-nil interface,
	// so reporting handlers/workers can be constructed even when object storage
	// is unconfigured (e.g. in tests with EXPORT_WORKER_ENABLED=false). Actual
	// calls on an unconfigured client return ErrObjectStorageUnavailable at
	// invocation time.
	storage = newRuntimeReportingStorageClient(storage)

	//nolint:contextcheck // Reporting config accessors are not context-aware.
	exportWorker, cleanupWorker, err := initReportingModule(
		routes,
		cfg,
		configGetter,
		settingsResolver,
		provider,
		storage,
		rateLimiterGetter,
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

	dispatchLimiter := NewDispatchRateLimit(rateLimiterGetter, cfg, configGetter, settingsResolver)

	if err := initExceptionModule(ctx, cfg, configGetter, settingsResolver, routes, provider, sharedOutboxRepository, dispatchLimiter, sharedRepos, isProduction); err != nil {
		return nil, err
	}

	// Single extraction repo instance shared across the discovery module
	// and the Fetcher-to-ingestion bridge. Constructed once so any future
	// stateful change (connection pool, cache) does not silently diverge
	// between the two consumers.
	extractionRepo := discoveryExtractionRepo.NewRepository(provider)

	// Discovery module (optional — non-critical, gated by FETCHER_ENABLED).
	discWorker, err := initOptionalDiscoveryWorker(routes, cfg, configGetter, provider, sharedOutboxRepository, extractionRepo, logger, initDiscoveryModule)
	if err != nil {
		return nil, fmt.Errorf("init optional discovery worker: %w", err)
	}

	// Fetcher-to-ingestion trusted bridge (T-001 intake + T-002 verified
	// artifact pipeline + T-003 automatic bridging). Wired here so the
	// adapters are reachable once the ingestion command use case,
	// discovery extraction repository, and object storage all exist.
	//
	// T-003: when all preconditions are met (Fetcher enabled, bridge
	// bundle operational, source resolver available), the bridge worker
	// is constructed. Otherwise, the bundle is kept around for the
	// intake path but the bridge worker is not registered — the
	// verified-artifact pipeline is soft-disabled when APP_ENC_KEY is
	// empty or when object storage is unavailable.
	bridgeBundle, err := initFetcherBridgeAdapters(ctx, FetcherBridgeDeps{
		Config:           cfg,
		IngestionUseCase: ingestionUseCase,
		ExtractionRepo:   extractionRepo,
		ObjectStorage:    storage,
		Logger:           logger,
	})
	if err != nil {
		return nil, fmt.Errorf("init fetcher bridge adapters: %w", err)
	}

	// Interim memory guard: the verified-artifact verifier currently
	// materializes plaintext in memory (~512 MiB per concurrent artifact).
	// Reject boot when the pod memory budget is below the safe floor so
	// operators see the misconfiguration instead of a silent OOMKill
	// later. On dev/macOS (no cgroup files) this is a no-op.
	if err := EnsureBridgeMemoryBudget(cfg); err != nil {
		return nil, fmt.Errorf("ensure fetcher bridge memory budget: %w", err)
	}

	// Companion to the guard: set GOMEMLIMIT to 85% of the detected
	// cgroup limit so the Go runtime garbage collector works harder
	// before we hit the cgroup ceiling. Skips when GOMEMLIMIT is
	// already set explicitly by the operator.
	applyGOMEMLIMIT(ctx, cfg, logger, defaultMemoryLimitReader)

	bridgeWorker, err := initFetcherBridgeWorker(
		ctx,
		cfg,
		configGetter,
		provider,
		extractionRepo,
		sharedOutboxRepository,
		bridgeBundle,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("init fetcher bridge worker: %w", err)
	}

	// T-006 custody retention sweep worker. Runs only when Fetcher is
	// enabled AND the verified-artifact pipeline is operational (the
	// custody store is part of the same bundle). Sweeps orphan custody
	// objects left behind by terminally-failed bridge attempts and by
	// happy-path cleanupCustody hook failures.
	custodyRetentionWorker, err := initCustodyRetentionWorker(
		ctx,
		cfg,
		extractionRepo,
		sharedOutboxRepository,
		provider,
		bridgeBundle,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("init custody retention worker: %w", err)
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

	// Build canonical outbox HandlerRegistry with event-type handlers.
	// Each handler dispatches a single event type published via the outbox.
	handlers := outbox.NewHandlerRegistry()

	if err := registerOutboxHandlers(handlers, ingestionPublisher, matchingPublisher, auditConsumer); err != nil {
		return nil, fmt.Errorf("register outbox handlers: %w", err)
	}

	// Build retry classifier: marks validation / payload errors as non-retryable.
	classifier := outbox.RetryClassifierFunc(isNonRetryableOutboxError)

	dispatcher, err := outbox.NewDispatcher(
		sharedOutboxRepository,
		handlers,
		logger,
		otel.Tracer(constants.ApplicationName),
		outbox.WithDispatchInterval(cfg.OutboxDispatchInterval()),
		outbox.WithRetryWindow(cfg.OutboxRetryWindow()),
		outbox.WithRetryClassifier(classifier),
		outbox.WithPriorityEventTypes(sharedDomain.EventTypeAuditLogCreated),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox dispatcher: %w", err)
	}

	// Create scheduler worker for cron-based matching
	var schedulerWorker *configWorker.SchedulerWorker
	if matchingUseCase != nil {
		schedulerWorker = createSchedulerWorker(ctx, cfg, provider, matchingUseCase, logger)
	}

	// Surface any errors that occurred during Protected route group creation.
	// The Protected closure collects errors instead of panicking so that all
	// modules finish registration before we fail, giving a complete error picture.
	if err := routes.RegistrationErr(); err != nil {
		return nil, fmt.Errorf("route registration: %w", err)
	}

	return &modulesResult{
		outboxDispatcher:       dispatcher,
		exportWorker:           exportWorker,
		cleanupWorker:          cleanupWorker,
		schedulerWorker:        schedulerWorker,
		discoveryWorker:        discWorker,
		bridgeWorker:           bridgeWorker,
		custodyRetentionWorker: custodyRetentionWorker,
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
	ctx context.Context,
	rabbitMQConnection *libRabbitmq.RabbitMQConnection,
	logger libLog.Logger,
	rmqManager *tmrabbitmq.Manager,
) (*matchingRabbitmq.EventPublisher, *ingestionRabbitmq.EventPublisher, error) {
	// Multi-tenant mode: use per-tenant vhost channels via tmrabbitmq.Manager.
	// No dedicated AMQP channels or confirmable publishers are needed because
	// each publish call resolves a tenant-specific channel on demand.
	if rmqManager != nil {
		matchingPublisher, err := matchingRabbitmq.NewMultiTenantEventPublisher(rmqManager)
		if err != nil {
			return nil, nil, fmt.Errorf("create multi-tenant matching event publisher: %w", err)
		}

		ingestionPublisher, err := ingestionRabbitmq.NewMultiTenantEventPublisher(rmqManager)
		if err != nil {
			return nil, nil, fmt.Errorf("create multi-tenant ingestion event publisher: %w", err)
		}

		return matchingPublisher, ingestionPublisher, nil
	}

	// Single-tenant mode: existing behavior with dedicated AMQP channels.
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
	ctx context.Context,
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	matchingUseCase *matchingCommand.UseCase,
	logger libLog.Logger,
) *configWorker.SchedulerWorker {
	ctx = detachedContext(ctx)

	configScheduleRepository := configScheduleRepo.NewRepository(provider)

	matchTrigger, err := crossAdapters.NewMatchTriggerAdapter(matchingUseCase)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("scheduler worker not started: match trigger adapter unavailable: %v", err))

		return nil
	}

	// Use a provider-backed lock manager that resolves Redis lazily per lock
	// attempt. This ensures the scheduler survives transient Redis outages at
	// boot and benefits from runtime infrastructure bundle swaps.
	lockManager := newProviderBackedLockManager(provider)

	workerCfg := configWorker.SchedulerWorkerConfig{
		Interval: schedulerInterval(cfg),
	}

	sw, err := configWorker.NewSchedulerWorker(
		configScheduleRepository,
		matchTrigger,
		lockManager,
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
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	provider sharedPorts.InfrastructureProvider,
	logger libLog.Logger,
) sharedPorts.IdempotencyRepository {
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

	if configGetter != nil || settingsResolver != nil {
		exceptionIdempotencyRepo.SetRuntimeConfigResolvers(
			func(ctx context.Context) time.Duration {
				return resolveIdempotencyRetryWindow(ctx, cfg, configGetter, settingsResolver)
			},
			func(ctx context.Context) time.Duration {
				return resolveIdempotencySuccessTTL(ctx, cfg, configGetter, settingsResolver)
			},
			nil,
		)
	}

	return exceptionIdempotencyRepo
}

// createObjectStorage initialises the S3/MinIO client only when the reporting
// background workers actually need it at startup.
func createObjectStorage(
	ctx context.Context,
	cfg *Config,
) (sharedPorts.ObjectStorageClient, error) {
	if !reportingStorageRequired(cfg) {
		return nil, nil
	}

	if cfg.ObjectStorage.Bucket == "" {
		return nil, ErrObjectStorageBucketRequired
	}

	s3Cfg := reportingStorage.S3Config{
		Endpoint:        cfg.ObjectStorage.Endpoint,
		Region:          cfg.ObjectStorage.Region,
		Bucket:          cfg.ObjectStorage.Bucket,
		AccessKeyID:     cfg.ObjectStorage.AccessKeyID,
		SecretAccessKey: cfg.ObjectStorage.SecretAccessKey,
		UsePathStyle:    cfg.ObjectStorage.UsePathStyle,
		AllowInsecure:   allowInsecureObjectStorageEndpoint(cfg),
	}

	client, err := newS3ClientFn(detachedContext(ctx), s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}

	return client, nil
}

func reportingStorageRequired(cfg *Config) bool {
	if cfg == nil {
		return false
	}

	return cfg.ExportWorker.Enabled || cfg.CleanupWorker.Enabled
}

// newRuntimeReportingStorageClient wraps the startup-time reporting storage
// client in a dynamic delegate that always exposes a non-nil interface to the
// reporting handlers and workers. When no storage is configured, calls fail at
// invocation time with ErrObjectStorageUnavailable rather than preventing
// startup.
//
// This mirrors newRuntimeArchivalStorageClient and allows the reporting module
// to register its routes and workers unconditionally, even when the export and
// cleanup workers are disabled or the object-storage endpoint is empty.
func newRuntimeReportingStorageClient(
	fallback sharedPorts.ObjectStorageClient,
) sharedPorts.ObjectStorageClient {
	// A trivial non-nil getter guarantees newDynamicObjectStorageClient returns
	// a non-nil wrapper. The wrapper's current() method defers to the fallback
	// and, if that is also nil, returns ErrObjectStorageUnavailable at call
	// time instead of panicking.
	return newDynamicObjectStorageClient(func() sharedPorts.ObjectStorageClient {
		return fallback
	}, fallback)
}

func initConfigurationModule(
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository sharedPorts.OutboxRepository,
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
		configCommand.WithFeeRuleRepository(repos.configFeeRule),
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
		configQuery.WithFeeRuleRepository(repos.configFeeRule),
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
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository sharedPorts.OutboxRepository,
	publisher sharedPorts.IngestionEventPublisher,
	matchingUseCase *matchingCommand.UseCase,
	repos *sharedRepositories,
	production bool,
) (*ingestionCommand.UseCase, error) {
	ingestionRegistry := ingestionParser.NewParserRegistry()
	ingestionRegistry.Register(ingestionParser.NewCSVParser())
	ingestionRegistry.Register(ingestionParser.NewJSONParser())
	ingestionRegistry.Register(ingestionParser.NewXMLParser())

	dedupeService := ingestionRedis.NewDedupeService(provider)

	fieldMapAdapter, err := crossAdapters.NewFieldMapRepositoryAdapter(repos.configFieldMap)
	if err != nil {
		return nil, fmt.Errorf("create field map repository adapter: %w", err)
	}

	sourceAdapter, err := crossAdapters.NewSourceRepositoryAdapter(repos.configSource)
	if err != nil {
		return nil, fmt.Errorf("create source repository adapter: %w", err)
	}

	contextAdapter := crossAdapters.NewContextAccessProviderAdapter(repos.configContext)

	// Auto-match on upload: create adapters to check context config and trigger matching
	autoMatchContextProvider, err := crossAdapters.NewAutoMatchContextProviderAdapter(repos.configContext)
	if err != nil {
		return nil, fmt.Errorf("create auto-match context provider adapter: %w", err)
	}

	var matchTriggerAdapter *crossAdapters.MatchTriggerAdapter

	if matchingUseCase != nil {
		var triggerErr error

		matchTriggerAdapter, triggerErr = crossAdapters.NewMatchTriggerAdapter(matchingUseCase)
		if triggerErr != nil {
			return nil, fmt.Errorf("create match trigger adapter: %w", triggerErr)
		}
	}

	ingestionCommandUseCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
		JobRepo:         repos.ingestionJob,
		TransactionRepo: repos.ingestionTx,
		Dedupe:          dedupeService,
		DedupeTTL:       cfg.DedupeTTL(),
		DedupeTTLResolver: func(ctx context.Context) time.Duration {
			return resolveDedupeTTL(ctx, cfg, configGetter, settingsResolver)
		},
		DedupeTTLGetter: func() time.Duration {
			runtimeCfg := configGetter()
			if runtimeCfg == nil {
				return cfg.DedupeTTL()
			}

			return runtimeCfg.DedupeTTL()
		},
		Publisher:       publisher,
		OutboxRepo:      outboxRepository,
		Parsers:         ingestionRegistry,
		FieldMapRepo:    fieldMapAdapter,
		SourceRepo:      sourceAdapter,
		MatchTrigger:    matchTriggerAdapter,
		ContextProvider: autoMatchContextProvider,
	})
	if err != nil {
		return nil, fmt.Errorf("create ingestion command use case: %w", err)
	}

	ingestionQueryUseCase, err := ingestionQuery.NewUseCase(
		repos.ingestionJob,
		repos.ingestionTx,
	)
	if err != nil {
		return nil, fmt.Errorf("create ingestion query use case: %w", err)
	}

	ingestionHandler, err := ingestionHTTP.NewHandlers(
		ingestionCommandUseCase,
		ingestionQueryUseCase,
		contextAdapter,
		production,
	)
	if err != nil {
		return nil, fmt.Errorf("create ingestion handler: %w", err)
	}

	if err := ingestionHTTP.RegisterRoutes(routes.Protected, ingestionHandler); err != nil {
		return nil, fmt.Errorf("register ingestion routes: %w", err)
	}

	return ingestionCommandUseCase, nil
}

func initMatchingModule(
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepo sharedPorts.OutboxRepository,
	repos *sharedRepositories,
	production bool,
) (*matchingCommand.UseCase, error) {
	configProvider, err := crossAdapters.NewMatchingConfigurationProvider(
		repos.configContext,
		repos.configSource,
		repos.configMatchRule,
		repos.configFeeRule,
	)
	if err != nil {
		return nil, fmt.Errorf("create matching configuration provider: %w", err)
	}

	sourceAdapter, err := crossAdapters.NewSourceProviderAdapter(repos.configSource)
	if err != nil {
		return nil, fmt.Errorf("create source provider adapter for matching: %w", err)
	}

	feeRuleAdapter, err := crossAdapters.NewFeeRuleProviderAdapter(repos.configFeeRule)
	if err != nil {
		return nil, fmt.Errorf("create fee rule provider adapter for matching: %w", err)
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
	feeVarianceRepository := matchFeeVarianceRepo.NewRepository(provider)

	useCase, err := matchingCommand.New(matchingCommand.UseCaseDeps{
		ContextProvider:  configProvider.ContextProvider(),
		SourceProvider:   sourceAdapter,
		RuleProvider:     configProvider.MatchRuleProvider(),
		TxRepo:           transactionAdapter,
		LockManager:      lockManager,
		MatchRunRepo:     matchRunRepository,
		MatchGroupRepo:   matchGroupRepository,
		MatchItemRepo:    matchItemRepository,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  feeVarianceRepository,
		AdjustmentRepo:   repos.adjustment,
		InfraProvider:    provider,
		AuditLogRepo:     repos.governanceAuditLog,
		FeeScheduleRepo:  repos.feeSchedule,
		FeeRuleProvider:  feeRuleAdapter,
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
		configProvider.ContextProvider(),
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
	settingsResolver *runtimeSettingsResolver,
	provider sharedPorts.InfrastructureProvider,
	storage sharedPorts.ObjectStorageClient,
	rateLimiterGetter func() *ratelimit.RateLimiter,
	logger libLog.Logger,
	repos *sharedRepositories,
	production bool,
) (*reportingWorker.ExportWorker, *reportingWorker.CleanupWorker, error) {
	contextAdapter := crossAdapters.NewContextAccessProviderAdapter(repos.configContext)

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

	exportLimiter := NewExportRateLimit(rateLimiterGetter, cfg, configGetter, settingsResolver)

	if err := reportingHTTP.RegisterRoutes(routes.Protected, reportingHandler, exportLimiter); err != nil {
		return nil, nil, fmt.Errorf("register reporting routes: %w", err)
	}

	return initExportWorkers(
		routes,
		cfg,
		configGetter,
		settingsResolver,
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
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	exportJobRepository *reportExportJob.Repository,
	reportRepository *reportRepo.Repository,
	storage sharedPorts.ObjectStorageClient,
	contextAdapter sharedPorts.ContextAccessProvider,
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
		configuredExportPresignExpiry(context.Background(), cfg),
		IsProductionEnvironment(cfg.App.EnvName),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create export job handler: %w", err)
	}

	exportJobHandler.SetRuntimeEnabled(cfg.ExportWorker.Enabled)

	if configGetter != nil || settingsResolver != nil {
		exportJobHandler.SetRuntimeConfigResolver(func(ctx context.Context) reportingHTTP.ExportJobRuntimeConfig {
			runtimeCfg := runtimeConfigOrFallback(cfg, configGetter)

			enabled := cfg.ExportWorker.Enabled
			if runtimeCfg != nil {
				enabled = runtimeCfg.ExportWorker.Enabled
			}

			return reportingHTTP.ExportJobRuntimeConfig{
				Enabled:       &enabled,
				PresignExpiry: resolveExportPresignExpiry(ctx, cfg, configGetter, settingsResolver),
			}
		})
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

	actorMappingHandler, err := governanceHTTP.NewActorMappingHandler(actorMappingCommandUC, actorMappingQueryUC, production)
	if err != nil {
		return fmt.Errorf("create actor mapping handler: %w", err)
	}

	if err := governanceHTTP.RegisterActorMappingRoutes(routes.Protected, actorMappingHandler); err != nil {
		return fmt.Errorf("register actor mapping routes: %w", err)
	}

	return nil
}

func initExceptionModule(
	ctx context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	routes *Routes,
	provider sharedPorts.InfrastructureProvider,
	outboxRepository sharedPorts.OutboxRepository,
	dispatchLimiter fiber.Handler,
	repos *sharedRepositories,
	production bool,
) error {
	// Exception-specific repositories (not shared across modules)
	exceptionRepository := exceptionExceptionRepo.NewRepository(provider)
	disputeRepository := exceptionDisputeRepo.NewRepository(provider)
	commentRepository := exceptionCommentRepo.NewRepository(provider)

	deps, err := initExceptionDependencies(outboxRepository, exceptionRepository, repos)
	if err != nil {
		return err
	}

	useCases, err := initExceptionUseCases(
		ctx,
		cfg,
		configGetter,
		settingsResolver,
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

	callbackUseCase, err := initExceptionCallbackUseCase(cfg, configGetter, settingsResolver, provider, exceptionRepository, deps)
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
// audit publisher, actor extractor, merged exception-matching bridge, and resolution executor.
func initExceptionDependencies(
	outboxRepository sharedPorts.OutboxRepository,
	exceptionRepository *exceptionExceptionRepo.Repository,
	repos *sharedRepositories,
) (*exceptionModuleDeps, error) {
	auditPublisher, err := exceptionAudit.NewOutboxPublisher(outboxRepository)
	if err != nil {
		return nil, fmt.Errorf("create audit publisher: %w", err)
	}

	actorExtractor := exceptionAdapters.NewAuthActorExtractor()

	matchingGateway, err := crossAdapters.NewExceptionMatchingGateway(
		repos.adjustment,
		repos.ingestionTx,
		repos.ingestionJob,
		repos.configSource,
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
	ctx context.Context,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
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

	webhookDispatchTimeout := configuredWebhookTimeout(ctx, cfg)

	// SEC-27: RequireSignedPayloads defaults to true in production so an
	// unsigned webhook configuration fails validation at startup rather
	// than silently dispatching unsigned payloads to downstream systems.
	// Development and test environments retain the permissive default
	// (false) so local tooling can exercise the dispatch path without a
	// shared secret. Operators who deploy webhook dispatch to production
	// without a signing secret must explicitly opt out in code; there is
	// no runtime toggle because the whole point of the default is that
	// misconfiguration is visible from the first run.
	isProduction := IsProductionEnvironment(cfg.App.EnvName)

	httpConnector, err := exceptionConnectors.NewHTTPConnector(
		exceptionConnectors.ConnectorConfig{
			Webhook: &exceptionConnectors.WebhookConnectorConfig{
				BaseConnectorConfig: exceptionConnectors.BaseConnectorConfig{
					Timeout: &webhookDispatchTimeout,
				},
				RequireSignedPayloads: isProduction,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create http connector: %w", err)
	}

	if configGetter != nil || settingsResolver != nil {
		httpConnector.SetWebhookTimeoutResolver(func(ctx context.Context) time.Duration {
			return resolveWebhookTimeout(ctx, cfg, configGetter, settingsResolver)
		})
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
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	provider sharedPorts.InfrastructureProvider,
	exceptionRepository *exceptionExceptionRepo.Repository,
	deps *exceptionModuleDeps,
) (*exceptionCommand.CallbackUseCase, error) {
	callbackRateLimiter, err := exceptionRedis.NewCallbackRateLimiter(
		provider,
		cfg.CallbackRateLimitPerMinute(),
		time.Minute,
	)
	if err != nil {
		return nil, fmt.Errorf("create callback rate limiter: %w", err)
	}

	if configGetter != nil || settingsResolver != nil {
		callbackRateLimiter.SetRuntimeLimitResolver(func(ctx context.Context) int {
			return resolveCallbackRateLimit(ctx, cfg, configGetter, settingsResolver)
		})
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

	if configGetter != nil || settingsResolver != nil {
		callbackIdempotencyRepo.SetRuntimeConfigResolvers(
			func(ctx context.Context) time.Duration {
				return resolveIdempotencyRetryWindow(ctx, cfg, configGetter, settingsResolver)
			},
			func(ctx context.Context) time.Duration {
				return resolveIdempotencySuccessTTL(ctx, cfg, configGetter, settingsResolver)
			},
			nil,
		)
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
