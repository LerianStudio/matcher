//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"

	tmredis "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/redis"
)

// --- GAP 1: MULTI_TENANT_REDIS_TLS env var ---

func TestTenancyConfig_MultiTenantRedisTLS_FieldExists(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// The field must exist and default to false.
	assert.False(t, cfg.Tenancy.MultiTenantRedisTLS,
		"MultiTenantRedisTLS must default to false")
}

func TestTenancyConfig_MultiTenantRedisHost_FieldExists(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// The field must exist and default to empty string.
	assert.Empty(t, cfg.Tenancy.MultiTenantRedisHost,
		"MultiTenantRedisHost must default to empty string")
}

func TestTenancyConfig_MultiTenantRedisPort_FieldExists(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// The field must exist and default to "6379".
	assert.Equal(t, "6379", cfg.Tenancy.MultiTenantRedisPort,
		"MultiTenantRedisPort must default to 6379")
}

func TestTenancyConfig_MultiTenantRedisPassword_FieldExists(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// The field must exist and default to empty string.
	assert.Empty(t, cfg.Tenancy.MultiTenantRedisPassword,
		"MultiTenantRedisPassword must default to empty string")
}

// --- GAP 2: Event-Driven Tenant Discovery wiring ---

func TestBuildTenantPubSubRedisConfig_ConvertsFromTenancyConfig(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Tenancy.MultiTenantRedisHost = "redis.example.com"
	cfg.Tenancy.MultiTenantRedisPort = "6380"
	cfg.Tenancy.MultiTenantRedisPassword = "secret"
	cfg.Tenancy.MultiTenantRedisTLS = true

	result := buildTenantPubSubRedisConfig(cfg)

	assert.Equal(t, tmredis.TenantPubSubRedisConfig{
		Host:     "redis.example.com",
		Port:     "6380",
		Password: "secret",
		TLS:      true,
	}, result)
}

func TestBuildTenantPubSubRedisConfig_DefaultsFromEmptyConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	result := buildTenantPubSubRedisConfig(cfg)

	assert.Empty(t, result.Host)
	assert.Equal(t, "6379", result.Port)
	assert.Empty(t, result.Password)
	assert.False(t, result.TLS)
}

func TestInitTenantEventDiscovery_DisabledWhenSingleTenant(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	listener, cache, cleanup := initTenantEventDiscovery(cfg, nil, nil)

	assert.Nil(t, listener, "listener must be nil in single-tenant mode")
	assert.Nil(t, cache, "cache must be nil in single-tenant mode")
	assert.NotNil(t, cleanup, "cleanup must always return a non-nil func")

	// Cleanup should be a no-op and not panic.
	cleanup()
}

func TestInitTenantEventDiscovery_DisabledWhenRedisHostEmpty(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantRedisHost = "" // no Redis host configured

	listener, cache, cleanup := initTenantEventDiscovery(cfg, nil, nil)

	assert.Nil(t, listener, "listener must be nil when Redis host is empty")
	assert.Nil(t, cache, "cache must be nil when Redis host is empty")
	assert.NotNil(t, cleanup, "cleanup must always return a non-nil func")
	cleanup()
}
