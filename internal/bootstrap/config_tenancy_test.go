// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig_TenancyPrimaryFields(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.False(t, cfg.Tenancy.MultiTenantEnabled)
	assert.Empty(t, cfg.Tenancy.MultiTenantEnvironment)
	assert.Empty(t, cfg.Tenancy.MultiTenantURL)
	assert.Empty(t, cfg.Tenancy.MultiTenantServiceAPIKey)

	// Redis event-driven discovery fields.
	assert.Empty(t, cfg.Tenancy.MultiTenantRedisHost)
	assert.Equal(t, "6379", cfg.Tenancy.MultiTenantRedisPort)
	assert.Empty(t, cfg.Tenancy.MultiTenantRedisPassword)
	assert.False(t, cfg.Tenancy.MultiTenantRedisTLS)

	// Pool and resilience fields.
	assert.Equal(t, 100, cfg.Tenancy.MultiTenantMaxTenantPools)
	assert.Equal(t, 300, cfg.Tenancy.MultiTenantIdleTimeoutSec)
	assert.Equal(t, 30, cfg.Tenancy.MultiTenantTimeout)
	assert.Equal(t, 5, cfg.Tenancy.MultiTenantCircuitBreakerThreshold)
	assert.Equal(t, 30, cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec)
	assert.Equal(t, 120, cfg.Tenancy.MultiTenantCacheTTLSec)
	assert.Equal(t, 30, cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec)
}

func TestConfigValidate_MultiTenantRequiresTenantManagerSettings(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MULTI_TENANT_URL is required when multi-tenant mode is enabled")

	cfg.Tenancy.MultiTenantURL = "http://tenant-manager:4003"
	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MULTI_TENANT_SERVICE_API_KEY is required when multi-tenant mode is enabled")

	cfg.Tenancy.MultiTenantServiceAPIKey = "service-api-key"
	// MULTI_TENANT_REDIS_HOST is optional (event-driven tenant discovery not yet consumed).
	// Validation should pass without it.
	require.NoError(t, cfg.Validate())

	cfg.Tenancy.MultiTenantURL = "tenant-manager"
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_URL must be an absolute URL with scheme and host")

	cfg.Tenancy.MultiTenantURL = "ftp://tenant-manager:4003"
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_URL must use http or https")

	cfg.Tenancy.MultiTenantURL = "://bad-url"
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_URL must be a valid absolute URL")

	cfg.Tenancy.MultiTenantURL = "https://tenant-manager:4003"
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate_MultiTenantFieldConstraints(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "http://tenant-manager:4003"
	cfg.Tenancy.MultiTenantServiceAPIKey = "service-api-key"
	cfg.Tenancy.MultiTenantRedisHost = "redis"

	cfg.Tenancy.MultiTenantEnvironment = ""
	err := cfg.Validate()
	require.NoError(t, err)
	assert.Equal(t, "development", cfg.effectiveMultiTenantEnvironment())

	cfg.Tenancy.MultiTenantEnvironment = "staging"
	cfg.Tenancy.MultiTenantMaxTenantPools = 0
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_MAX_TENANT_POOLS must be positive")

	cfg.Tenancy.MultiTenantMaxTenantPools = 10
	cfg.Tenancy.MultiTenantIdleTimeoutSec = 0
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_IDLE_TIMEOUT_SEC must be positive")

	cfg.Tenancy.MultiTenantIdleTimeoutSec = 10
	cfg.Tenancy.MultiTenantTimeout = 0
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_TIMEOUT must be positive")

	cfg.Tenancy.MultiTenantTimeout = 30
	cfg.Tenancy.MultiTenantCircuitBreakerThreshold = 0
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD must be positive")

	cfg.Tenancy.MultiTenantCircuitBreakerThreshold = 1
	cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = 0
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC must be positive")

	cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = 30
	cfg.Tenancy.MultiTenantCacheTTLSec = 0
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_CACHE_TTL_SEC must be positive")

	cfg.Tenancy.MultiTenantCacheTTLSec = 120
	cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec = 0
	err = cfg.Validate()
	require.ErrorContains(t, err, "MULTI_TENANT_CONNECTIONS_CHECK_INTERVAL_SEC must be positive")
}
