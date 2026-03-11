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
	return discoveryFetcher.HTTPClientConfig{
		BaseURL:         cfg.Fetcher.URL,
		AllowPrivateIPs: cfg.Fetcher.AllowPrivateIPs,
		HealthTimeout:   cfg.FetcherHealthTimeout(),
		RequestTimeout:  cfg.FetcherRequestTimeout(),
		MaxRetries:      defaultFetcherClientMaxRetries,
		RetryBaseDelay:  defaultFetcherClientRetryBaseDelay,
	}
}

type discoveryModuleInitFunc func(
	routes *Routes,
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	tenantLister sharedPorts.TenantLister,
	logger libLog.Logger,
) (*discoveryWorker.DiscoveryWorker, error)

func initOptionalDiscoveryWorker(
	ctx context.Context,
	routes *Routes,
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	tenantLister sharedPorts.TenantLister,
	logger libLog.Logger,
	initFn discoveryModuleInitFunc,
) *discoveryWorker.DiscoveryWorker {
	if cfg == nil || !cfg.Fetcher.Enabled {
		if logger != nil {
			logger.Log(ctx, libLog.LevelInfo, "discovery module disabled (FETCHER_ENABLED=false)")
		}

		return nil
	}

	worker, err := initFn(routes, cfg, provider, tenantLister, logger)
	if err != nil {
		if logger != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("discovery module failed to initialize (continuing without it): %v", err))
		}

		return nil
	}

	return worker
}

// initDiscoveryModule initializes the Fetcher discovery module including HTTP handlers,
// PG repositories, the Fetcher HTTP client, command/query use cases, and the background
// discovery worker. This module is non-critical: failures are logged but do not prevent startup.
func initDiscoveryModule(
	routes *Routes,
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	tenantLister sharedPorts.TenantLister,
	logger libLog.Logger,
) (*discoveryWorker.DiscoveryWorker, error) {
	fetcherClient, err := discoveryFetcher.NewHTTPFetcherClient(fetcherHTTPClientConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("create fetcher client: %w", err)
	}

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

	if err := discoveryHTTP.RegisterRoutes(routes.Protected, handler); err != nil {
		return nil, fmt.Errorf("register discovery routes: %w", err)
	}

	extractionPoller, pollerErr := discoveryWorker.NewExtractionPoller(
		fetcherClient,
		extractionRepo,
		discoveryWorker.ExtractionPollerConfig{
			PollInterval: cfg.FetcherExtractionPollInterval(),
			Timeout:      cfg.FetcherExtractionTimeout(),
		},
		logger,
	)
	if pollerErr != nil {
		logger.Log(context.Background(), libLog.LevelWarn,
			fmt.Sprintf("discovery: failed to create extraction poller: %v", pollerErr))
	} else {
		cmdUseCase.WithExtractionPoller(extractionPoller)
		logger.Log(context.Background(), libLog.LevelInfo,
			fmt.Sprintf("discovery: extraction poller wired into command use case (poll: %s, timeout: %s)",
				cfg.FetcherExtractionPollInterval(), cfg.FetcherExtractionTimeout()))
	}

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

	wireDiscoverySchemaCacheFromRedis(provider, cmdUseCase, queryUseCase, worker, cfg, logger)

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
	logger libLog.Logger,
) {
	ctx := context.Background()

	redisConn, redisErr := provider.GetRedisConnection(ctx)
	if redisErr != nil {
		logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("discovery: failed to get redis connection for schema cache: %v", redisErr))

		return
	}

	if redisConn == nil {
		return
	}

	redisClient, clientErr := redisConn.GetClient(ctx)
	if clientErr != nil {
		logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("discovery: failed to get redis client for schema cache: %v", clientErr))

		return
	}

	schemaCache, cacheErr := discoveryRedis.NewSchemaCache(redisClient)
	if cacheErr != nil {
		logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("discovery: failed to create schema cache: %v", cacheErr))

		return
	}

	ttl := cfg.FetcherSchemaCacheTTL()
	queryUseCase.WithSchemaCache(schemaCache, ttl)
	cmdUseCase.WithSchemaCache(schemaCache, ttl)
	worker.WithSchemaCache(schemaCache, ttl)

	logger.Log(ctx, libLog.LevelInfo,
		"discovery: schema cache wired into discovery module (TTL: "+ttl.String()+")")
}
