// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	tmevent "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/event"
	tmpostgres "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/postgres"
	tmredis "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/redis"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/tenantcache"
	"github.com/redis/go-redis/v9"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// buildTenantPubSubRedisConfig converts TenancyConfig fields into the
// lib-commons TenantPubSubRedisConfig struct used by the Pub/Sub Redis client.
func buildTenantPubSubRedisConfig(cfg *Config) tmredis.TenantPubSubRedisConfig {
	return tmredis.TenantPubSubRedisConfig{
		Host:     cfg.Tenancy.MultiTenantRedisHost,
		Port:     cfg.Tenancy.MultiTenantRedisPort,
		Password: cfg.Tenancy.MultiTenantRedisPassword,
		TLS:      cfg.Tenancy.MultiTenantRedisTLS,
	}
}

// initTenantEventDiscovery creates the TenantCache, Pub/Sub Redis client, EventDispatcher,
// and TenantEventListener for event-driven tenant discovery when multi-tenant mode is
// enabled and a Redis host is configured.
//
// Returns:
//   - listener: the running TenantEventListener (nil if not applicable)
//   - cache: the shared TenantCache (nil if not applicable)
//   - cleanup: a function that stops the listener and closes the Pub/Sub client; always non-nil
//
// When MULTI_TENANT_ENABLED=false or MULTI_TENANT_REDIS_HOST is empty, this is a no-op.
func initTenantEventDiscovery(
	cfg *Config,
	pgManager *tmpostgres.Manager,
	logger libLog.Logger,
) (*tmevent.TenantEventListener, *tenantcache.TenantCache, func()) {
	noop := func() {}

	if !multiTenantModeEnabled(cfg) {
		return nil, nil, noop
	}

	if cfg.Tenancy.MultiTenantRedisHost == "" {
		if logger != nil {
			logger.Log(context.Background(), libLog.LevelInfo,
				"tenant event discovery disabled: MULTI_TENANT_REDIS_HOST not configured")
		}

		return nil, nil, noop
	}

	// 1. Create TenantCache for in-memory tenant config caching.
	cache := tenantcache.NewTenantCache()

	// 2. Create Pub/Sub Redis client for receiving tenant lifecycle events.
	redisCfg := buildTenantPubSubRedisConfig(cfg)

	pubsubClient, err := tmredis.NewTenantPubSubRedisClient(context.Background(), redisCfg)
	if err != nil {
		if logger != nil {
			logger.Log(context.Background(), libLog.LevelWarn,
				fmt.Sprintf("tenant event discovery: Redis Pub/Sub connection failed (event-driven discovery disabled): %v", err))
		}

		return nil, cache, noop
	}

	// 3. Create EventDispatcher that handles tenant lifecycle events
	//    by updating the cache and optionally closing stale infrastructure connections.
	dispatcherOpts := []tmevent.DispatcherOption{
		tmevent.WithDispatcherLogger(logger),
	}

	if pgManager != nil {
		dispatcherOpts = append(dispatcherOpts, tmevent.WithPostgres(pgManager))
	}

	dispatcher := tmevent.NewEventDispatcher(cache, nil, constants.ApplicationName, dispatcherOpts...)

	// 4. Create TenantEventListener that subscribes to Redis Pub/Sub
	//    and dispatches parsed events to the dispatcher.
	listener, err := tmevent.NewTenantEventListener(
		pubsubClient,
		dispatcher.HandleEvent,
		tmevent.WithListenerLogger(logger),
		tmevent.WithService(constants.ApplicationName),
	)
	if err != nil {
		if logger != nil {
			logger.Log(context.Background(), libLog.LevelWarn,
				fmt.Sprintf("tenant event discovery: failed to create listener: %v", err))
		}

		closeRedisClient(pubsubClient, logger)

		return nil, cache, noop
	}

	// 5. Start the listener (spawns a background goroutine for Pub/Sub reads).
	if startErr := listener.Start(context.Background()); startErr != nil {
		if logger != nil {
			logger.Log(context.Background(), libLog.LevelWarn,
				fmt.Sprintf("tenant event discovery: failed to start listener: %v", startErr))
		}

		closeRedisClient(pubsubClient, logger)

		return nil, cache, noop
	}

	// 6. Build cleanup function that stops the listener and closes the Redis client.
	cleanup := func() {
		if stopErr := listener.Stop(); stopErr != nil && logger != nil {
			logger.Log(context.Background(), libLog.LevelWarn,
				fmt.Sprintf("tenant event discovery: listener stop error: %v", stopErr))
		}

		closeRedisClient(pubsubClient, logger)
	}

	if logger != nil {
		logger.Log(context.Background(), libLog.LevelInfo,
			fmt.Sprintf("tenant event discovery: listener started (redis=%s:%s, service=%s)",
				cfg.Tenancy.MultiTenantRedisHost, cfg.Tenancy.MultiTenantRedisPort, constants.ApplicationName))
	}

	return listener, cache, cleanup
}

// closeRedisClient closes a Redis Pub/Sub client, logging any errors.
func closeRedisClient(client redis.UniversalClient, logger libLog.Logger) {
	if client == nil {
		return
	}

	if err := client.Close(); err != nil && logger != nil {
		logger.Log(context.Background(), libLog.LevelWarn,
			fmt.Sprintf("tenant event discovery: failed to close Redis Pub/Sub client: %v", err))
	}
}
