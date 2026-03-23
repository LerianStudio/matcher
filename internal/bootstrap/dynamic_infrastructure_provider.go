// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/client"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/core"
	tmpostgres "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/postgres"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for infrastructure provider operations.
var (
	// ErrPostgresConnectionNotConfigured is returned when no postgres connection was provided.
	ErrPostgresConnectionNotConfigured = errors.New("postgres connection not configured")

	// ErrRedisConnectionNotConfigured is returned when no redis connection was provided.
	ErrRedisConnectionNotConfigured = errors.New("redis connection not configured")

	// ErrNoPrimaryDatabaseConfigured is returned when no primary database is available for transactions.
	ErrNoPrimaryDatabaseConfigured = errors.New(
		"no primary database configured for multi-tenant transaction",
	)

	// errDynamicInfrastructureConfigUnavailable is returned when the provider's config is nil.
	errDynamicInfrastructureConfigUnavailable = errors.New("dynamic infrastructure provider config unavailable")

	// ErrNilDBResolver is returned when a tenant connection returns a nil db resolver.
	ErrNilDBResolver = errors.New("tenant connection returned nil db resolver")
)

// dynamicInfrastructureProvider resolves connections for the tenant in ctx.
// In single-tenant mode, it delegates to singleton PG/Redis connections.
// In multi-tenant mode, it delegates to a canonical tmpostgres.Manager from
// lib-commons/v4/commons/tenant-manager/postgres.
type dynamicInfrastructureProvider struct {
	mu           sync.Mutex
	initialCfg   *Config
	configGetter func() *Config
	bundleState  *activeMatcherBundleState
	postgres     *libPostgres.Client
	redis        *libRedis.Client
	logger       libLog.Logger
	metrics      *MultiTenantMetrics

	multiTenantKey string
	pgManager      *tmpostgres.Manager
	tmClient       *client.Client

	// RabbitMQ tenant-manager resources for multi-tenant vhost isolation.
	// Stored here so provider.Close() can release them on shutdown.
	rmqManager  *tmrabbitmq.Manager
	rmqTmClient *client.Client
}

var _ sharedPorts.InfrastructureProvider = (*dynamicInfrastructureProvider)(nil)

func newDynamicInfrastructureProvider(
	initialCfg *Config,
	configGetter func() *Config,
	bundleState *activeMatcherBundleState,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	logger libLog.Logger,
	metrics *MultiTenantMetrics,
) *dynamicInfrastructureProvider {
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &dynamicInfrastructureProvider{
		initialCfg:   initialCfg,
		configGetter: configGetter,
		bundleState:  bundleState,
		postgres:     postgres,
		redis:        redis,
		logger:       logger,
		metrics:      metrics,
	}
}

// GetPostgresConnection returns the active PostgreSQL connection lease.
func (provider *dynamicInfrastructureProvider) GetPostgresConnection(ctx context.Context) (*sharedPorts.PostgresConnectionLease, error) {
	if currentCfg := provider.currentConfig(); currentCfg != nil && multiTenantModeEnabled(currentCfg) {
		pgConn, release, err := provider.resolveMultiTenantPostgres(ctx, currentCfg)
		if err != nil {
			return nil, fmt.Errorf("resolve tenant postgres connection: %w", err)
		}

		return sharedPorts.NewPostgresConnectionLease(pgConn, release), nil
	}

	postgres := provider.currentPostgres()
	if postgres == nil {
		return nil, ErrPostgresConnectionNotConfigured
	}

	return sharedPorts.NewPostgresConnectionLease(postgres, nil), nil
}

// GetRedisConnection returns the active Redis connection lease.
//
// TODO(multi-tenant): Add per-tenant Redis routing when lib-commons tenant-manager/redis is available.
// Currently Redis uses a singleton connection with key prefixing via valkey.GetKeyFromContext.
// Both single-tenant and multi-tenant modes share the same Redis connection; multi-tenant
// isolation is achieved at the key level, not the connection level.
func (provider *dynamicInfrastructureProvider) GetRedisConnection(_ context.Context) (*sharedPorts.RedisConnectionLease, error) {
	redisClient := provider.currentRedis()
	if redisClient == nil {
		return nil, ErrRedisConnectionNotConfigured
	}

	return sharedPorts.NewRedisConnectionLease(redisClient, nil), nil
}

// BeginTx starts a write transaction against the active primary database.
func (provider *dynamicInfrastructureProvider) BeginTx(ctx context.Context) (*sharedPorts.TxLease, error) {
	if currentCfg := provider.currentConfig(); currentCfg != nil && multiTenantModeEnabled(currentCfg) {
		return provider.beginMultiTenantTx(ctx, currentCfg)
	}

	postgres := provider.currentPostgres()
	if postgres == nil {
		return nil, ErrPostgresConnectionNotConfigured
	}

	resolver, err := postgres.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres connection: %w", err)
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		return nil, ErrNoPrimaryDatabaseConfigured
	}

	tx, err := primaryDBs[0].BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin postgres transaction: %w", err)
	}

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return nil, fmt.Errorf("apply tenant schema: %w",
				errors.Join(err, fmt.Errorf("rollback: %w", rollbackErr)))
		}

		return nil, fmt.Errorf("apply tenant schema: %w", err)
	}

	return sharedPorts.NewTxLease(tx, nil), nil
}

// beginMultiTenantTx resolves a tenant-specific database connection and begins a
// transaction with tenant schema isolation. Extracted from BeginTx to reduce cognitive
// complexity.
func (provider *dynamicInfrastructureProvider) beginMultiTenantTx(ctx context.Context, cfg *Config) (*sharedPorts.TxLease, error) {
	pgConn, release, err := provider.resolveMultiTenantPostgres(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant manager for transaction: %w", err)
	}

	resolver, err := pgConn.Resolver(ctx)
	if err != nil {
		safeRelease(release)

		return nil, fmt.Errorf("resolve postgres connection: %w", err)
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		safeRelease(release)

		return nil, ErrNoPrimaryDatabaseConfigured
	}

	tx, err := primaryDBs[0].BeginTx(ctx, nil)
	if err != nil {
		safeRelease(release)

		return nil, fmt.Errorf("begin postgres transaction: %w", err)
	}

	if err := auth.ApplyTenantSchema(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			safeRelease(release)

			return nil, fmt.Errorf("apply tenant schema: %w",
				errors.Join(err, fmt.Errorf("rollback: %w", rollbackErr)))
		}

		safeRelease(release)

		return nil, fmt.Errorf("apply tenant schema: %w", err)
	}

	return sharedPorts.NewTxLease(tx, release), nil
}

// safeRelease calls the release function if it is non-nil.
func safeRelease(release func()) {
	if release != nil {
		release()
	}
}

// GetReplicaDB returns the active replica database lease when configured.
func (provider *dynamicInfrastructureProvider) GetReplicaDB(ctx context.Context) (*sharedPorts.ReplicaDBLease, error) {
	if currentCfg := provider.currentConfig(); currentCfg != nil && multiTenantModeEnabled(currentCfg) {
		return provider.resolveMultiTenantReplicaDB(ctx, currentCfg)
	}

	postgres := provider.currentPostgres()
	if postgres == nil {
		return nil, ErrPostgresConnectionNotConfigured
	}

	resolver, err := postgres.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres connection for replica db: %w", err)
	}

	replicas := resolver.ReplicaDBs()
	if len(replicas) == 0 || replicas[0] == nil {
		return nil, nil
	}

	return sharedPorts.NewReplicaDBLease(replicas[0], nil), nil
}

// resolveMultiTenantReplicaDB resolves a tenant-specific replica database.
// Extracted from GetReplicaDB to reduce nesting complexity.
func (provider *dynamicInfrastructureProvider) resolveMultiTenantReplicaDB(ctx context.Context, cfg *Config) (*sharedPorts.ReplicaDBLease, error) {
	pgConn, release, err := provider.resolveMultiTenantPostgres(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant manager for replica db: %w", err)
	}

	resolver, err := pgConn.Resolver(ctx)
	if err != nil {
		safeRelease(release)

		return nil, fmt.Errorf("resolve postgres connection for replica db: %w", err)
	}

	replicas := resolver.ReplicaDBs()
	if len(replicas) == 0 || replicas[0] == nil {
		safeRelease(release)

		return nil, nil
	}

	return sharedPorts.NewReplicaDBLease(replicas[0], release), nil
}

// Close releases the active multi-tenant manager, if present.
func (provider *dynamicInfrastructureProvider) Close() error {
	if provider == nil {
		return nil
	}

	provider.mu.Lock()
	defer provider.mu.Unlock()

	var errs []error

	if provider.pgManager != nil {
		if err := provider.pgManager.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("close postgres manager: %w", err))
		}

		provider.pgManager = nil
	}

	if provider.tmClient != nil {
		if err := provider.tmClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close tenant manager client: %w", err))
		}

		provider.tmClient = nil
	}

	if provider.rmqManager != nil {
		if err := provider.rmqManager.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("close rabbitmq tenant manager: %w", err))
		}

		provider.rmqManager = nil
	}

	if provider.rmqTmClient != nil {
		if err := provider.rmqTmClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close rabbitmq tenant manager client: %w", err))
		}

		provider.rmqTmClient = nil
	}

	provider.multiTenantKey = ""

	return errors.Join(errs...)
}

// resolveMultiTenantPostgres extracts the tenant ID from context and returns the
// tenant-specific libPostgres.Client from the canonical tmpostgres.Manager.
//
//nolint:unparam // cleanup func reserved for future use (e.g., connection pool release)
func (provider *dynamicInfrastructureProvider) resolveMultiTenantPostgres(
	ctx context.Context,
	cfg *Config,
) (*libPostgres.Client, func(), error) {
	manager, err := provider.currentPGManager(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve pg manager: %w", err)
	}

	tenantID := core.GetTenantID(ctx)
	if tenantID == "" {
		// Fall back to auth package tenant extraction for backward compatibility.
		// Use LookupTenantID (not GetTenantID) to avoid silently resolving to the
		// default tenant — multi-tenant mode must fail closed when tenant context
		// is genuinely absent.
		if explicit, ok := auth.LookupTenantID(ctx); ok {
			tenantID = explicit
		}
	}

	if tenantID == "" {
		return nil, nil, core.ErrTenantContextRequired
	}

	conn, err := manager.GetConnection(ctx, tenantID)
	if err != nil {
		provider.metrics.RecordConnectionError(ctx, tenantID, "connection_failed")
		return nil, nil, fmt.Errorf("get tenant connection (tenant=%s): %w", tenantID, err)
	}

	// The PostgresConnection from tmpostgres.Manager holds a .client field that is
	// a *libPostgres.Client — but it's unexported. Instead, we use conn.GetDB() to
	// get the dbresolver.DB, then wrap it in a synthetic libPostgres.Client that the
	// lease system can use. However, the tmpostgres.PostgresConnection exposes a
	// .client field only internally. We need to work with what we have.
	//
	// The PostgresConnection struct has an exported ConnectionDB field (*dbresolver.DB)
	// and an unexported client field (*libPostgres.Client). Since we can't access the
	// unexported field directly, we create a new libPostgres.Client from the connection's
	// resolver. However, the manager already creates and caches a libPostgres.Client
	// internally when PostgresConnection.Connect is called. We can get the resolver
	// from the connection's GetDB() method.
	//
	// Since the InfrastructureProvider interface returns *PostgresConnectionLease which
	// wraps *libPostgres.Client, and the tmpostgres.PostgresConnection doesn't expose
	// its internal client, we need to create a wrapper. We use the
	// infraTestutil.NewClientWithResolver pattern to wrap the dbresolver.DB.
	//
	// However, to avoid a dependency on testutil in production code, we'll use a
	// different approach: we construct a libPostgres.Client using the connection
	// information from the Manager's PostgresConnection.

	// The tmpostgres.PostgresConnection stores its libPostgres.Client in an unexported
	// field. We need the dbresolver.DB from GetDB() and must wrap it into what the
	// lease system expects. Looking at the lease callers: they call
	// lease.Resolver(ctx) which calls conn.Resolver(ctx). So we need a *libPostgres.Client
	// that can return a dbresolver.DB.
	//
	// Since we cannot access the unexported client field, we create a synthetic
	// client wrapper. This is a temporary bridge pattern that will be cleaned up
	// when repositories are migrated to use core.ResolvePostgres directly (Gate 5+).

	db, err := conn.GetDB()
	if err != nil {
		return nil, nil, fmt.Errorf("get db resolver for tenant: %w", err)
	}

	if db == nil {
		return nil, nil, fmt.Errorf("tenant=%s: %w", tenantID, ErrNilDBResolver)
	}

	pgClient := newLibPostgresClientFromResolver(db)

	provider.metrics.RecordConnection(ctx, tenantID, "success")

	return pgClient, nil, nil
}

func (provider *dynamicInfrastructureProvider) currentConfig() *Config {
	if provider == nil {
		return nil
	}

	if provider.configGetter != nil {
		if runtimeCfg := provider.configGetter(); runtimeCfg != nil {
			return runtimeCfg
		}
	}

	return provider.initialCfg
}

func (provider *dynamicInfrastructureProvider) currentPostgres() *libPostgres.Client {
	if provider.bundleState != nil {
		if bundle := provider.bundleState.Current(); bundle != nil && bundle.DB() != nil {
			return bundle.DB()
		}
	}

	return provider.postgres
}

func (provider *dynamicInfrastructureProvider) currentRedis() *libRedis.Client {
	if provider.bundleState != nil {
		if bundle := provider.bundleState.Current(); bundle != nil && bundle.RedisClient() != nil {
			return bundle.RedisClient()
		}
	}

	return provider.redis
}

// currentPGManager returns the canonical tmpostgres.Manager, lazily creating it.
// If config changes (detected via cache key), the previous manager is closed and a new one is created.
func (provider *dynamicInfrastructureProvider) currentPGManager(ctx context.Context, cfg *Config) (*tmpostgres.Manager, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()

	if cfg == nil {
		return nil, fmt.Errorf("current pg manager: %w", errDynamicInfrastructureConfigUnavailable)
	}

	key := dynamicMultiTenantKey(cfg)
	if provider.pgManager != nil && provider.multiTenantKey == key {
		return provider.pgManager, nil
	}

	tmClient, pgManager, err := buildCanonicalTenantManager(cfg, provider.logger)
	if err != nil {
		return nil, err
	}

	// Close previous manager and client if config changed
	if provider.pgManager != nil {
		if closeErr := provider.pgManager.Close(ctx); closeErr != nil && provider.logger != nil {
			provider.logger.Log(ctx, libLog.LevelWarn, "dynamic multi-tenant provider cleanup failed",
				libLog.String("error", closeErr.Error()))
		}
	}

	if provider.tmClient != nil {
		if closeErr := provider.tmClient.Close(); closeErr != nil && provider.logger != nil {
			provider.logger.Log(ctx, libLog.LevelWarn, "previous tenant manager client cleanup failed",
				libLog.String("error", closeErr.Error()))
		}
	}

	provider.pgManager = pgManager
	provider.tmClient = tmClient
	provider.multiTenantKey = key

	return provider.pgManager, nil
}

// dynamicMultiTenantKey builds the cache key that determines whether the
// current tenant connection manager can be reused. Any field included here is
// considered manager-shaping: if it changes, the provider rebuilds the manager
// and closes the previous one. Keep this list aligned with
// buildCanonicalTenantManager.
func dynamicMultiTenantKey(cfg *Config) string {
	if cfg == nil {
		return ""
	}

	// Hash the API key so it is never stored verbatim in the cache key held in
	// provider.multiTenantKey. A truncated SHA-256 (first 8 bytes / 16 hex chars)
	// is sufficient for change detection without leaking the secret.
	apiKeyHash := sha256.Sum256([]byte(cfg.Tenancy.MultiTenantServiceAPIKey))
	apiKeyFingerprint := hex.EncodeToString(apiKeyHash[:8])

	return fmt.Sprintf("%t|%s|%s|%s|%s|%d|%d|%d|%d|%d|%d",
		cfg.Tenancy.MultiTenantEnabled,
		cfg.Tenancy.MultiTenantURL,
		apiKeyFingerprint,
		cfg.effectiveMultiTenantEnvironment(),
		cfg.App.EnvName,
		cfg.Postgres.MaxOpenConnections,
		cfg.Postgres.MaxIdleConnections,
		cfg.Tenancy.MultiTenantMaxTenantPools,
		cfg.Tenancy.MultiTenantIdleTimeoutSec,
		cfg.Tenancy.MultiTenantCircuitBreakerThreshold,
		cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec)
}

func multiTenantModeEnabled(cfg *Config) bool {
	return cfg != nil && cfg.Tenancy.MultiTenantEnabled
}

// buildCanonicalTenantManager creates the canonical lib-commons tenant-manager
// client and tmpostgres.Manager from the service config.
func buildCanonicalTenantManager(cfg *Config, logger libLog.Logger) (*client.Client, *tmpostgres.Manager, error) {
	// Build client options
	clientOpts := []client.ClientOption{
		client.WithServiceAPIKey(cfg.Tenancy.MultiTenantServiceAPIKey),
		client.WithCircuitBreaker(
			cfg.Tenancy.MultiTenantCircuitBreakerThreshold,
			time.Duration(cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec)*time.Second,
		),
	}

	// Allow insecure HTTP only for local development (http:// URLs).
	// Staging, pre-production, and other non-production environments must still use
	// HTTPS to protect tenant-manager credentials in transit.
	if isLocalDevelopmentEnvironment(cfg.App.EnvName) {
		clientOpts = append(clientOpts, client.WithAllowInsecureHTTP())
	}

	tmClient, err := client.NewClient(cfg.Tenancy.MultiTenantURL, logger, clientOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("create tenant manager client: %w", err)
	}

	// Build postgres manager options
	pgOpts := []tmpostgres.Option{
		tmpostgres.WithLogger(logger),
		tmpostgres.WithMaxTenantPools(cfg.Tenancy.MultiTenantMaxTenantPools),
		tmpostgres.WithIdleTimeout(time.Duration(cfg.Tenancy.MultiTenantIdleTimeoutSec) * time.Second),
		tmpostgres.WithConnectionLimits(cfg.Postgres.MaxOpenConnections, cfg.Postgres.MaxIdleConnections),
	}

	pgManager := tmpostgres.NewManager(tmClient, constants.ApplicationName, pgOpts...)

	return tmClient, pgManager, nil
}

// buildRabbitMQTenantManager creates a tmrabbitmq.Manager for per-tenant
// RabbitMQ vhost isolation (Layer 1). It is a convenience wrapper that discards
// the tenant-manager client. Use buildRabbitMQTenantManagerWithClient when the
// caller needs to store the client for shutdown cleanup.
func buildRabbitMQTenantManager(ctx context.Context, cfg *Config, logger libLog.Logger) *tmrabbitmq.Manager {
	_, mgr := buildRabbitMQTenantManagerWithClient(ctx, cfg, logger)
	return mgr
}

// buildRabbitMQTenantManagerWithClient creates a tmrabbitmq.Manager for per-tenant
// RabbitMQ vhost isolation (Layer 1). It reuses the same tenant-manager client
// configuration as buildCanonicalTenantManager but creates a separate client
// instance to avoid lifecycle coupling.
//
// Returns (nil, nil) if the client cannot be created (logged as warning, non-fatal).
// When nil is returned, the bootstrap falls back to single-tenant publishing.
// The returned *client.Client must be stored for shutdown cleanup; see
// dynamicInfrastructureProvider.rmqTmClient.
func buildRabbitMQTenantManagerWithClient(ctx context.Context, cfg *Config, logger libLog.Logger) (*client.Client, *tmrabbitmq.Manager) {
	clientOpts := []client.ClientOption{
		client.WithServiceAPIKey(cfg.Tenancy.MultiTenantServiceAPIKey),
		client.WithCircuitBreaker(
			cfg.Tenancy.MultiTenantCircuitBreakerThreshold,
			time.Duration(cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec)*time.Second,
		),
	}

	// Allow insecure HTTP only for local development — see buildCanonicalTenantManager.
	if isLocalDevelopmentEnvironment(cfg.App.EnvName) {
		clientOpts = append(clientOpts, client.WithAllowInsecureHTTP())
	}

	tmClient, err := client.NewClient(cfg.Tenancy.MultiTenantURL, logger, clientOpts...)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("rabbitmq tenant manager not available (falling back to single-tenant publishing): %v", err))
		return nil, nil
	}

	opts := []tmrabbitmq.Option{
		tmrabbitmq.WithLogger(logger),
		tmrabbitmq.WithMaxTenantPools(cfg.Tenancy.MultiTenantMaxTenantPools),
		tmrabbitmq.WithIdleTimeout(time.Duration(cfg.Tenancy.MultiTenantIdleTimeoutSec) * time.Second),
	}

	return tmClient, tmrabbitmq.NewManager(tmClient, constants.ApplicationName, opts...)
}
