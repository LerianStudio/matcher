// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v4/commons/circuitbreaker"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/shared/constants"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var errDynamicInfrastructureConfigUnavailable = errors.New("dynamic infrastructure provider config unavailable")

type dynamicInfrastructureProvider struct {
	mu           sync.Mutex
	initialCfg   *Config
	configGetter func() *Config
	bundleState  *activeMatcherBundleState
	postgres     *libPostgres.Client
	redis        *libRedis.Client
	logger       libLog.Logger

	multiTenantKey string
	multiTenantMgr *tenantAdapters.TenantConnectionManager
}

var _ sharedPorts.InfrastructureProvider = (*dynamicInfrastructureProvider)(nil)

func newDynamicInfrastructureProvider(
	initialCfg *Config,
	configGetter func() *Config,
	bundleState *activeMatcherBundleState,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	logger libLog.Logger,
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
	}
}

// GetPostgresConnection returns the active PostgreSQL connection lease.
func (provider *dynamicInfrastructureProvider) GetPostgresConnection(ctx context.Context) (*sharedPorts.PostgresConnectionLease, error) {
	if currentCfg := provider.currentConfig(); currentCfg != nil && multiTenantModeEnabled(currentCfg) {
		manager, err := provider.currentMultiTenantManager(ctx, currentCfg)
		if err != nil {
			return nil, fmt.Errorf("resolve tenant manager for postgres connection: %w", err)
		}

		lease, err := manager.GetPostgresConnection(ctx)
		if err != nil {
			return nil, fmt.Errorf("get postgres connection from tenant manager: %w", err)
		}

		return lease, nil
	}

	postgres := provider.currentPostgres()
	if postgres == nil {
		return nil, tenantAdapters.ErrPostgresConnectionNotConfigured
	}

	return sharedPorts.NewPostgresConnectionLease(postgres, nil), nil
}

// GetRedisConnection returns the active Redis connection lease.
func (provider *dynamicInfrastructureProvider) GetRedisConnection(ctx context.Context) (*sharedPorts.RedisConnectionLease, error) {
	if currentCfg := provider.currentConfig(); currentCfg != nil && multiTenantModeEnabled(currentCfg) {
		manager, err := provider.currentMultiTenantManager(ctx, currentCfg)
		if err != nil {
			return nil, fmt.Errorf("resolve tenant manager for redis connection: %w", err)
		}

		lease, err := manager.GetRedisConnection(ctx)
		if err != nil {
			return nil, fmt.Errorf("get redis connection from tenant manager: %w", err)
		}

		return lease, nil
	}

	redisClient := provider.currentRedis()
	if redisClient == nil {
		return nil, tenantAdapters.ErrRedisConnectionNotConfigured
	}

	return sharedPorts.NewRedisConnectionLease(redisClient, nil), nil
}

// BeginTx starts a write transaction against the active primary database.
func (provider *dynamicInfrastructureProvider) BeginTx(ctx context.Context) (*sharedPorts.TxLease, error) {
	if currentCfg := provider.currentConfig(); currentCfg != nil && multiTenantModeEnabled(currentCfg) {
		manager, err := provider.currentMultiTenantManager(ctx, currentCfg)
		if err != nil {
			return nil, fmt.Errorf("resolve tenant manager for transaction: %w", err)
		}

		lease, err := manager.BeginTx(ctx)
		if err != nil {
			return nil, fmt.Errorf("begin transaction with tenant manager: %w", err)
		}

		return lease, nil
	}

	postgres := provider.currentPostgres()
	if postgres == nil {
		return nil, tenantAdapters.ErrPostgresConnectionNotConfigured
	}

	resolver, err := postgres.Resolver(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres connection: %w", err)
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		return nil, tenantAdapters.ErrNoPrimaryDatabaseConfigured
	}

	tx, err := primaryDBs[0].BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin postgres transaction: %w", err)
	}

	return sharedPorts.NewTxLease(tx, func() {
		_ = tx.Rollback()
	}), nil
}

// GetReplicaDB returns the active replica database lease when configured.
func (provider *dynamicInfrastructureProvider) GetReplicaDB(ctx context.Context) (*sharedPorts.ReplicaDBLease, error) {
	if currentCfg := provider.currentConfig(); currentCfg != nil && multiTenantModeEnabled(currentCfg) {
		manager, err := provider.currentMultiTenantManager(ctx, currentCfg)
		if err != nil {
			return nil, fmt.Errorf("resolve tenant manager for replica db: %w", err)
		}

		lease, err := manager.GetReplicaDB(ctx)
		if err != nil {
			return nil, fmt.Errorf("get replica db from tenant manager: %w", err)
		}

		return lease, nil
	}

	postgres := provider.currentPostgres()
	if postgres == nil {
		return nil, tenantAdapters.ErrPostgresConnectionNotConfigured
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

// Close releases the active multi-tenant manager, if present.
func (provider *dynamicInfrastructureProvider) Close() error {
	provider.mu.Lock()
	defer provider.mu.Unlock()

	if provider.multiTenantMgr == nil {
		return nil
	}

	err := provider.multiTenantMgr.Close()
	provider.multiTenantMgr = nil
	provider.multiTenantKey = ""

	if err != nil {
		return fmt.Errorf("close tenant connection manager: %w", err)
	}

	return nil
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

func (provider *dynamicInfrastructureProvider) currentMultiTenantManager(ctx context.Context, cfg *Config) (*tenantAdapters.TenantConnectionManager, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()

	if cfg == nil {
		return nil, fmt.Errorf("current multi-tenant manager: %w", errDynamicInfrastructureConfigUnavailable)
	}

	key := dynamicMultiTenantKey(cfg)
	if provider.multiTenantMgr != nil && provider.multiTenantKey == key {
		return provider.multiTenantMgr, nil
	}

	manager, err := buildTenantConnectionManagerFromConfig(cfg, provider.logger)
	if err != nil {
		return nil, err
	}

	previous := provider.multiTenantMgr
	provider.multiTenantMgr = manager
	provider.multiTenantKey = key

	if previous != nil {
		if closeErr := previous.Close(); closeErr != nil && provider.logger != nil {
			provider.logger.Log(ctx, libLog.LevelWarn, "dynamic multi-tenant provider cleanup failed",
				libLog.String("error", closeErr.Error()))
		}
	}

	return provider.multiTenantMgr, nil
}

func dynamicMultiTenantKey(cfg *Config) string {
	if cfg == nil {
		return ""
	}

	return fmt.Sprintf("%t|%t|%s|%s|%s|%s|%d|%d|%d|%d|%d|%d|%d|%d", cfg.Tenancy.MultiTenantEnabled, cfg.Tenancy.MultiTenantInfraEnabled, cfg.Tenancy.MultiTenantURL, cfg.Tenancy.MultiTenantServiceAPIKey, cfg.effectiveMultiTenantEnvironment(), cfg.App.EnvName, cfg.Postgres.MaxOpenConnections, cfg.Postgres.MaxIdleConnections, cfg.Postgres.ConnMaxLifetimeMins, cfg.Postgres.ConnMaxIdleTimeMins, cfg.Tenancy.MultiTenantMaxTenantPools, cfg.Tenancy.MultiTenantIdleTimeoutSec, cfg.Tenancy.MultiTenantCircuitBreakerThreshold, cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec)
}

func multiTenantModeEnabled(cfg *Config) bool {
	return cfg != nil && (cfg.Tenancy.MultiTenantEnabled || cfg.Tenancy.MultiTenantInfraEnabled)
}

func buildTenantConnectionManagerFromConfig(cfg *Config, logger libLog.Logger) (*tenantAdapters.TenantConnectionManager, error) {
	remoteConfigTimeout := time.Duration(cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec) * time.Second
	if remoteConfigTimeout < minPerServiceTimeout {
		remoteConfigTimeout = minPerServiceTimeout
	}

	configAdapter, err := tenantAdapters.NewRemoteConfigurationAdapter(tenantAdapters.RemoteConfigurationConfig{
		BaseURL:            cfg.Tenancy.MultiTenantURL,
		ServiceName:        constants.ApplicationName,
		ServiceAPIKey:      cfg.Tenancy.MultiTenantServiceAPIKey,
		RequestTimeout:     remoteConfigTimeout,
		EnvironmentName:    cfg.effectiveMultiTenantEnvironment(),
		RuntimeEnvironment: cfg.App.EnvName,
		BreakerConfig: circuitbreaker.Config{
			ConsecutiveFailures: safePositiveUint32(cfg.Tenancy.MultiTenantCircuitBreakerThreshold),
			Timeout:             remoteConfigTimeout,
		},
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create tenant configuration adapter: %w", err)
	}

	manager, err := tenantAdapters.NewTenantConnectionManager(
		configAdapter,
		cfg.Postgres.MaxOpenConnections,
		cfg.Postgres.MaxIdleConnections,
		cfg.Postgres.ConnMaxLifetimeMins,
		cfg.Postgres.ConnMaxIdleTimeMins,
		tenantAdapters.WithCachePolicy(
			cfg.Tenancy.MultiTenantMaxTenantPools,
			time.Duration(cfg.Tenancy.MultiTenantIdleTimeoutSec)*time.Second,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create tenant connection manager: %w", err)
	}

	return manager, nil
}
