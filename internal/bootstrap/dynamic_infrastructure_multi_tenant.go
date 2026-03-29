// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	tmcache "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/cache"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/client"
	tmpostgres "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/postgres"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/rabbitmq"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

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

	return fmt.Sprintf("%t|%s|%s|%s|%s|%d|%d|%d|%d|%d|%d|%d|%d|%d",
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
		cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec,
		cfg.Tenancy.MultiTenantCacheTTLSec,
		cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec,
		cfg.Tenancy.MultiTenantTimeout)
}

func multiTenantModeEnabled(cfg *Config) bool {
	return cfg != nil && cfg.Tenancy.MultiTenantEnabled
}

// buildCanonicalTenantManager creates the canonical lib-commons tenant-manager
// client and tmpostgres.Manager from the service config.
func buildCanonicalTenantManager(cfg *Config, logger libLog.Logger) (*client.Client, *tmpostgres.Manager, error) {
	// Build client options with cache + timeout for HTTP response caching.
	// The InMemoryCache reduces redundant HTTP calls to the Tenant Manager API;
	// WithCacheTTL controls how long responses are cached before refetching;
	// WithTimeout sets the HTTP client deadline for tenant-manager API calls.
	clientOpts := []client.ClientOption{
		client.WithServiceAPIKey(cfg.Tenancy.MultiTenantServiceAPIKey),
		client.WithCircuitBreaker(
			cfg.Tenancy.MultiTenantCircuitBreakerThreshold,
			time.Duration(cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec)*time.Second,
		),
		client.WithCache(tmcache.NewInMemoryCache()),
		client.WithCacheTTL(cfg.MultiTenantCacheTTL()),
		client.WithTimeout(cfg.MultiTenantTimeoutDuration()),
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

	// Build postgres manager options.
	// WithConnectionsCheckInterval enables async settings revalidation: the pgManager
	// periodically fetches fresh config from the Tenant Manager and applies updated
	// pool settings (maxOpenConns, maxIdleConns, statementTimeout) without recreating
	// the connection. If the interval is zero, revalidation is disabled.
	pgOpts := []tmpostgres.Option{
		tmpostgres.WithLogger(logger),
		tmpostgres.WithMaxTenantPools(cfg.Tenancy.MultiTenantMaxTenantPools),
		tmpostgres.WithIdleTimeout(cfg.MultiTenantIdleTimeout()),
		tmpostgres.WithConnectionLimits(cfg.Postgres.MaxOpenConnections, cfg.Postgres.MaxIdleConnections),
		tmpostgres.WithConnectionsCheckInterval(cfg.MultiTenantConnectionsCheckInterval()),
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
		client.WithCache(tmcache.NewInMemoryCache()),
		client.WithCacheTTL(cfg.MultiTenantCacheTTL()),
		client.WithTimeout(cfg.MultiTenantTimeoutDuration()),
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
		tmrabbitmq.WithIdleTimeout(cfg.MultiTenantIdleTimeout()),
	}

	return tmClient, tmrabbitmq.NewManager(tmClient, constants.ApplicationName, opts...)
}
