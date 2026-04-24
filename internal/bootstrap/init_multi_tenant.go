// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"fmt"
	"sync"

	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	tmclient "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/client"
	tmmiddleware "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/middleware"
	tmpostgres "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/postgres"
	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/tenantcache"

	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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
