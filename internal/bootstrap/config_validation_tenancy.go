// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
)

func (cfg *Config) validateTenancyConfig(ctx context.Context, asserter *assert.Asserter) error {
	if !multiTenantModeEnabled(cfg) {
		return nil
	}

	if err := validateMultiTenantURL(ctx, asserter, cfg.Tenancy.MultiTenantURL); err != nil {
		return err
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Tenancy.MultiTenantServiceAPIKey), "MULTI_TENANT_SERVICE_API_KEY is required when multi-tenant mode is enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantMaxTenantPools > 0, "MULTI_TENANT_MAX_TENANT_POOLS must be positive", "multi_tenant_max_tenant_pools", cfg.Tenancy.MultiTenantMaxTenantPools); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantIdleTimeoutSec > 0, "MULTI_TENANT_IDLE_TIMEOUT_SEC must be positive", "multi_tenant_idle_timeout_sec", cfg.Tenancy.MultiTenantIdleTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantTimeout > 0, "MULTI_TENANT_TIMEOUT must be positive", "multi_tenant_timeout", cfg.Tenancy.MultiTenantTimeout); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantCircuitBreakerThreshold > 0, "MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD must be positive", "multi_tenant_circuit_breaker_threshold", cfg.Tenancy.MultiTenantCircuitBreakerThreshold); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec > 0, "MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC must be positive", "multi_tenant_circuit_breaker_timeout_sec", cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantCacheTTLSec > 0, "MULTI_TENANT_CACHE_TTL_SEC must be positive", "multi_tenant_cache_ttl_sec", cfg.Tenancy.MultiTenantCacheTTLSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec > 0, "MULTI_TENANT_CONNECTIONS_CHECK_INTERVAL_SEC must be positive", "multi_tenant_connections_check_interval_sec", cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.NotEmpty(ctx, cfg.effectiveMultiTenantEnvironment(), "MULTI_TENANT_ENVIRONMENT is required when multi-tenant mode is enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func validateMultiTenantURL(ctx context.Context, asserter *assert.Asserter, rawURL string) error {
	if err := asserter.NotEmpty(ctx, strings.TrimSpace(rawURL), "MULTI_TENANT_URL is required when multi-tenant mode is enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	parsedTenantURL, parseErr := url.Parse(strings.TrimSpace(rawURL))
	if err := asserter.NoError(ctx, parseErr, "MULTI_TENANT_URL must be a valid absolute URL"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(
		ctx,
		parsedTenantURL != nil && parsedTenantURL.IsAbs() && parsedTenantURL.Host != "",
		"MULTI_TENANT_URL must be an absolute URL with scheme and host",
		"multi_tenant_url", rawURL,
	); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(
		ctx,
		strings.EqualFold(parsedTenantURL.Scheme, "http") || strings.EqualFold(parsedTenantURL.Scheme, "https"),
		"MULTI_TENANT_URL must use http or https",
		"multi_tenant_url", rawURL,
	); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func validateTrustedProxies(ctx context.Context, asserter *assert.Asserter, trustedProxies string) error {
	for _, candidate := range strings.Split(trustedProxies, ",") {
		proxy := strings.TrimSpace(candidate)
		if proxy == "" {
			continue
		}

		if err := asserter.That(
			ctx,
			proxy != "*" && proxy != "0.0.0.0/0" && proxy != "::/0",
			"TRUSTED_PROXIES must not trust all addresses",
			"trusted_proxy", proxy,
		); err != nil {
			return fmt.Errorf("config validation: %w", err)
		}
	}

	return nil
}
