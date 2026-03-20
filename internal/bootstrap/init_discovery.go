// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	discoveryFetcher "github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	discoveryHTTP "github.com/LerianStudio/matcher/internal/discovery/adapters/http"
	discoveryConnRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/connection"
	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoverySchemaRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/schema"
	discoveryRedis "github.com/LerianStudio/matcher/internal/discovery/adapters/redis"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryQuery "github.com/LerianStudio/matcher/internal/discovery/services/query"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	defaultFetcherClientMaxRetries     = 3
	defaultFetcherClientRetryBaseDelay = 500 * time.Millisecond
)

func fetcherHTTPClientConfig(cfg *Config) discoveryFetcher.HTTPClientConfig {
	clientCfg := discoveryFetcher.DefaultConfig()
	clientCfg.BaseURL = cfg.Fetcher.URL
	clientCfg.AllowPrivateIPs = cfg.Fetcher.AllowPrivateIPs
	clientCfg.HealthTimeout = cfg.FetcherHealthTimeout()
	clientCfg.RequestTimeout = cfg.FetcherRequestTimeout()
	clientCfg.MaxRetries = defaultFetcherClientMaxRetries
	clientCfg.RetryBaseDelay = defaultFetcherClientRetryBaseDelay

	return clientCfg
}

type discoveryModuleInitFunc func(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	tenantLister sharedPorts.TenantLister,
	logger libLog.Logger,
) (*discoveryWorker.DiscoveryWorker, error)

func initOptionalDiscoveryWorker(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	tenantLister sharedPorts.TenantLister,
	logger libLog.Logger,
	initFn discoveryModuleInitFunc,
) (*discoveryWorker.DiscoveryWorker, error) {
	if cfg == nil {
		return nil, nil
	}
	if initFn == nil {
		return nil, nil
	}

	worker, err := initFn(routes, cfg, configGetter, provider, tenantLister, logger)
	if err != nil {
		return nil, fmt.Errorf("initialize discovery module: %w", err)
	}

	return worker, nil
}

// initDiscoveryModule initializes the Fetcher discovery module including HTTP handlers,
// PG repositories, the Fetcher HTTP client, command/query use cases, and the background
// discovery worker. This module is non-critical: failures are logged but do not prevent startup.
func initDiscoveryModule(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	tenantLister sharedPorts.TenantLister,
	logger libLog.Logger,
) (*discoveryWorker.DiscoveryWorker, error) {
	fetcherClient := newDynamicFetcherClient(cfg, configGetter)

	connRepo := discoveryConnRepo.NewRepository(provider)
	schemaRepo := discoverySchemaRepo.NewRepository(provider)
	extractionRepo := discoveryExtractionRepo.NewRepository(provider)

	cmdUseCase, err := discoveryCommand.NewUseCase(fetcherClient, connRepo, schemaRepo, extractionRepo, logger)
	if err != nil {
		return nil, fmt.Errorf("create discovery command use case: %w", err)
	}

	queryUseCase, err := discoveryQuery.NewUseCase(fetcherClient, connRepo, schemaRepo, extractionRepo, logger)
	if err != nil {
		return nil, fmt.Errorf("create discovery query use case: %w", err)
	}

	handler, err := discoveryHTTP.NewHandler(cmdUseCase, queryUseCase, IsProductionEnvironment(cfg.App.EnvName))
	if err != nil {
		return nil, fmt.Errorf("create discovery handler: %w", err)
	}

	if cfg.Auth.Enabled {
		logger.Log(context.Background(), libLog.LevelWarn,
			"discovery: auth is enabled; ensure RBAC resource 'discovery' with actions 'discovery:read' and 'discovery:write' is provisioned before exposing discovery routes")
	}

	if err := discoveryHTTP.RegisterRoutes(routes.Protected, handler); err != nil {
		return nil, fmt.Errorf("register discovery routes: %w", err)
	}

	extractionPoller := newDynamicExtractionPoller(fetcherClient, extractionRepo, func() discoveryWorker.ExtractionPollerConfig {
		runtimeCfg := cfg
		if configGetter != nil {
			if currentCfg := configGetter(); currentCfg != nil {
				runtimeCfg = currentCfg
			}
		}

		return discoveryWorker.ExtractionPollerConfig{
			PollInterval: runtimeCfg.FetcherExtractionPollInterval(),
			Timeout:      runtimeCfg.FetcherExtractionTimeout(),
		}
	}, logger)
	if extractionPoller != nil {
		cmdUseCase.WithExtractionPoller(extractionPoller)
		logger.Log(context.Background(), libLog.LevelInfo,
			fmt.Sprintf("discovery: extraction poller wired into command use case (poll: %s, timeout: %s)",
				cfg.FetcherExtractionPollInterval(), cfg.FetcherExtractionTimeout()))
	}

	cmdUseCase.WithTenantContextRequirement(cfg.Auth.Enabled)
	cmdUseCase.WithDiscoveryRefreshLock(provider, cfg.FetcherDiscoveryInterval())
	cmdUseCase.WithDiscoveryRefreshLockGetter(func() time.Duration {
		runtimeCfg := cfg
		if configGetter != nil {
			if currentCfg := configGetter(); currentCfg != nil {
				runtimeCfg = currentCfg
			}
		}

		return 2 * runtimeCfg.FetcherDiscoveryInterval()
	})

	worker, err := discoveryWorker.NewDiscoveryWorker(
		fetcherClient,
		connRepo,
		schemaRepo,
		tenantLister,
		provider,
		discoveryWorker.DiscoveryWorkerConfig{Interval: cfg.FetcherDiscoveryInterval()},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create discovery worker: %w", err)
	}

	wireDiscoverySchemaCacheFromRedis(provider, cmdUseCase, queryUseCase, worker, cfg, configGetter, logger)

	return worker, nil
}

// wireDiscoverySchemaCacheFromRedis attempts to create a Redis-backed schema cache and
// wire it into the query use case. This is non-critical: failures are logged as warnings.
func wireDiscoverySchemaCacheFromRedis(
	provider sharedPorts.InfrastructureProvider,
	cmdUseCase *discoveryCommand.UseCase,
	queryUseCase *discoveryQuery.UseCase,
	worker *discoveryWorker.DiscoveryWorker,
	cfg *Config,
	configGetter func() *Config,
	logger libLog.Logger,
) {
	ctx := context.Background()

	redisLease, redisErr := provider.GetRedisConnection(ctx)
	if redisErr != nil {
		logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("discovery: failed to get redis connection for schema cache: %v", redisErr))

		return
	}
	defer redisLease.Release()

	redisConn := redisLease.Connection()
	if redisConn == nil {
		return
	}

	redisClient, clientErr := redisConn.GetClient(ctx)
	if clientErr != nil {
		logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("discovery: failed to get redis client for schema cache: %v", clientErr))

		return
	}

	if _, cacheErr := discoveryRedis.NewSchemaCache(redisClient, !cfg.Auth.Enabled); cacheErr != nil {
		logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("discovery: failed to create schema cache: %v", cacheErr))

		return
	}

	ttlGetter := func() time.Duration {
		runtimeCfg := cfg
		if configGetter != nil {
			if currentCfg := configGetter(); currentCfg != nil {
				runtimeCfg = currentCfg
			}
		}

		return runtimeCfg.FetcherSchemaCacheTTL()
	}
	dynamicCache := newDynamicSchemaCache(newProviderBackedSchemaCache(provider, !cfg.Auth.Enabled), ttlGetter)
	ttl := ttlGetter()
	queryUseCase.WithSchemaCache(dynamicCache, ttl)
	cmdUseCase.WithSchemaCache(dynamicCache, ttl)
	worker.WithSchemaCache(dynamicCache, ttl)

	logger.Log(ctx, libLog.LevelInfo,
		"discovery: schema cache wired into discovery module (TTL: "+ttl.String()+")")
}
