// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	discoveryFetcher "github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	discoveryHTTP "github.com/LerianStudio/matcher/internal/discovery/adapters/http"
	discoveryM2M "github.com/LerianStudio/matcher/internal/discovery/adapters/m2m"
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
	discoveryRefreshLockMultiplier     = 2
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
	m2mProvider ...sharedPorts.M2MProvider,
) (*discoveryWorker.DiscoveryWorker, error)

func initOptionalDiscoveryWorker(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	tenantLister sharedPorts.TenantLister,
	logger libLog.Logger,
	initFn discoveryModuleInitFunc,
	m2mProvider ...sharedPorts.M2MProvider,
) (*discoveryWorker.DiscoveryWorker, error) {
	if cfg == nil {
		return nil, nil
	}

	if initFn == nil {
		return nil, nil
	}

	worker, err := initFn(routes, cfg, configGetter, provider, tenantLister, logger, m2mProvider...)
	if err != nil {
		return nil, fmt.Errorf("initialize discovery module: %w", err)
	}

	return worker, nil
}

// wireDiscoveryTokenExchanger installs the OAuth2 Bearer-token auth flow on the
// Fetcher HTTP client when cfg.Auth.Enabled is true.
//
// Lifecycle note: The TokenExchanger is created once with cfg.Auth.Host as its
// authURL. This URL is bootstrap-only (ApplyBootstrapOnly in systemplane keys,
// see systemplane_keys_runtime_http.go), so the exchanger remains valid across
// dynamic fetcher client reinjections. If Auth.Host ever becomes
// runtime-mutable, this function must be extended to recreate the exchanger
// on config change.
//
// Failures (missing host, exchanger creation error) are logged as warnings and
// fall back to BasicAuth, letting startup succeed.
func wireDiscoveryTokenExchanger(fetcherClient sharedPorts.FetcherClient, cfg *Config, logger libLog.Logger) {
	if !cfg.Auth.Enabled || cfg.Auth.Host == "" {
		return
	}

	var teOpts []discoveryM2M.TokenExchangerOption

	if strings.HasPrefix(cfg.Auth.Host, "http://") {
		teOpts = append(teOpts, discoveryM2M.WithInsecureHTTP())
	}

	te, teErr := discoveryM2M.NewTokenExchanger(cfg.Auth.Host, teOpts...)
	if teErr != nil {
		logger.Log(context.Background(), libLog.LevelWarn,
			fmt.Sprintf("discovery: failed to create token exchanger: %v -- falling back to BasicAuth", teErr))

		return
	}

	dfc, ok := fetcherClient.(*dynamicFetcherClient)
	if !ok {
		logger.Log(context.Background(), libLog.LevelWarn,
			"discovery: token exchanger not wired — FetcherClient is not *dynamicFetcherClient")

		return
	}

	dfc.tokenExchanger = te

	logger.Log(context.Background(), libLog.LevelInfo, "discovery: token exchanger wired for Bearer auth")
}

// wireDiscoveryExtractionPoller creates the extraction poller and wires it into the
// command use case. The poller monitors extraction job completion asynchronously.
func wireDiscoveryExtractionPoller(
	fetcherClient sharedPorts.FetcherClient,
	extractionRepo *discoveryExtractionRepo.Repository,
	cmdUseCase *discoveryCommand.UseCase,
	cfg *Config,
	configGetter func() *Config,
	logger libLog.Logger,
) {
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

	if extractionPoller == nil {
		return
	}

	cmdUseCase.WithExtractionPoller(extractionPoller)

	logger.Log(context.Background(), libLog.LevelInfo,
		fmt.Sprintf("discovery: extraction poller wired into command use case (poll: %s, timeout: %s)",
			cfg.FetcherExtractionPollInterval(), cfg.FetcherExtractionTimeout()))
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
	m2mProvider ...sharedPorts.M2MProvider,
) (*discoveryWorker.DiscoveryWorker, error) {
	var m2m sharedPorts.M2MProvider
	if len(m2mProvider) > 0 {
		m2m = m2mProvider[0]
	}

	fetcherClient := newDynamicFetcherClient(cfg, configGetter, logger, m2m)

	wireDiscoveryTokenExchanger(fetcherClient, cfg, logger)

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

	wireDiscoveryExtractionPoller(fetcherClient, extractionRepo, cmdUseCase, cfg, configGetter, logger)

	cmdUseCase.WithDiscoveryRefreshLock(provider, cfg.FetcherDiscoveryInterval())
	cmdUseCase.WithDiscoveryRefreshLockGetter(func() time.Duration {
		runtimeCfg := cfg

		if configGetter != nil {
			if currentCfg := configGetter(); currentCfg != nil {
				runtimeCfg = currentCfg
			}
		}

		return discoveryRefreshLockMultiplier * runtimeCfg.FetcherDiscoveryInterval()
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
