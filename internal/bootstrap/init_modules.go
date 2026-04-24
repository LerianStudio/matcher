// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	outboxpg "github.com/LerianStudio/lib-commons/v5/commons/outbox/postgres"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFeeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	governanceAudit "github.com/LerianStudio/matcher/internal/governance/adapters/audit"
	governancePostgres "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTransactionRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	matchAdjustmentRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/adjustment"
	matchFeeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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

//nolint:cyclop,gocyclo,funlen // module initialization requires sequential dependency setup for all bounded contexts.
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
	connector InfraConnector,
	publishers EventPublisherFactory,
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

	matchingPublisher, ingestionPublisher, err := initEventPublishers(ctx, rabbitMQConnection, logger, rmqManager, publishers)
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

	storageBackend, err := createObjectStorage(ctx, cfg, connector)
	if err != nil {
		if reportingStorageRequired(cfg) {
			return nil, fmt.Errorf("create object storage: %w", err)
		}

		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Object storage not available, reporting background workers disabled: %v", err))
	}

	// Wrap the startup-time backend in the hot-reloadable *objectstorage.Client
	// so reporting handlers/workers can be constructed even when object storage
	// is unconfigured (e.g. in tests with EXPORT_WORKER_ENABLED=false). Actual
	// calls on an unconfigured client return ErrObjectStorageUnavailable at
	// invocation time. The Client's resolver re-reads object_storage.* config
	// on each call, so /system changes propagate without a restart; the swap
	// itself uses atomic.Pointer so in-flight operations never race.
	storage := newRuntimeReportingStorageClient(cfg, configGetter, storageBackend, connector)

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
