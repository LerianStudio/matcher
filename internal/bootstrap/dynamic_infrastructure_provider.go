// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bxcodec/dbresolver/v2"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/client"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/core"
	tmpostgres "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/postgres"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
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
	lease, err := provider.resolveMultiTenantPrimaryDB(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant manager for transaction: %w", err)
	}

	if lease == nil || lease.DB() == nil {
		return nil, ErrNoPrimaryDatabaseConfigured
	}

	tx, err := lease.DB().BeginTx(ctx, nil)
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

	return sharedPorts.NewTxLease(tx, lease.Release), nil
}

// GetReplicaDB returns the active replica database lease when configured.
func (provider *dynamicInfrastructureProvider) GetReplicaDB(ctx context.Context) (*sharedPorts.DBLease, error) {
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

	return sharedPorts.NewDBLease(replicas[0], nil), nil
}

// GetPrimaryDB returns the active primary database lease.
func (provider *dynamicInfrastructureProvider) GetPrimaryDB(ctx context.Context) (*sharedPorts.DBLease, error) {
	if currentCfg := provider.currentConfig(); currentCfg != nil && multiTenantModeEnabled(currentCfg) {
		return provider.resolveMultiTenantPrimaryDB(ctx, currentCfg)
	}

	postgres := provider.currentPostgres()
	if postgres == nil {
		return nil, ErrPostgresConnectionNotConfigured
	}

	resolver, err := postgres.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres connection for primary db: %w", err)
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		return nil, ErrNoPrimaryDatabaseConfigured
	}

	return sharedPorts.NewDBLease(primaryDBs[0], nil), nil
}

// resolveMultiTenantReplicaDB resolves a tenant-specific replica database.
// Extracted from GetReplicaDB to reduce nesting complexity.
func (provider *dynamicInfrastructureProvider) resolveMultiTenantReplicaDB(ctx context.Context, cfg *Config) (*sharedPorts.DBLease, error) {
	resolver, tenantID, err := provider.resolveMultiTenantResolver(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant manager for replica db: %w", err)
	}

	replicas := resolver.ReplicaDBs()
	if len(replicas) == 0 || replicas[0] == nil {
		return nil, nil
	}

	provider.metrics.RecordConnection(ctx, tenantID, "success")

	return sharedPorts.NewDBLease(replicas[0], nil), nil
}

func (provider *dynamicInfrastructureProvider) resolveMultiTenantPrimaryDB(ctx context.Context, cfg *Config) (*sharedPorts.DBLease, error) {
	resolver, tenantID, err := provider.resolveMultiTenantResolver(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant manager for primary db: %w", err)
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		return nil, ErrNoPrimaryDatabaseConfigured
	}

	provider.metrics.RecordConnection(ctx, tenantID, "success")

	return sharedPorts.NewDBLease(primaryDBs[0], nil), nil
}

func (provider *dynamicInfrastructureProvider) resolveMultiTenantResolver(
	ctx context.Context,
	cfg *Config,
) (dbresolver.DB, string, error) {
	manager, err := provider.currentPGManager(ctx, cfg)
	if err != nil {
		return nil, "", fmt.Errorf("resolve pg manager: %w", err)
	}

	tenantID := core.GetTenantID(ctx)
	if tenantID == "" {
		if explicit, ok := auth.LookupTenantID(ctx); ok {
			tenantID = explicit
		}
	}

	if tenantID == "" {
		return nil, "", core.ErrTenantContextRequired
	}

	conn, err := manager.GetConnection(ctx, tenantID)
	if err != nil {
		provider.metrics.RecordConnectionError(ctx, tenantID, "connection_failed")
		return nil, tenantID, fmt.Errorf("get tenant connection (tenant=%s): %w", tenantID, err)
	}

	resolver, err := conn.GetDB()
	if err != nil {
		return nil, tenantID, fmt.Errorf("get db resolver for tenant: %w", err)
	}

	if resolver == nil {
		return nil, tenantID, fmt.Errorf("tenant=%s: %w", tenantID, ErrNilDBResolver)
	}

	return resolver, tenantID, nil
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
