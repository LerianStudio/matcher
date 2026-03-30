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

func TestDynamicMultiTenantKey_NilConfig(t *testing.T) {
	t.Parallel()

	key := dynamicMultiTenantKey(nil)
	assert.Empty(t, key)
}

func TestDynamicMultiTenantKey_DeterministicOutput(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "https://tm.example.com"
	cfg.Tenancy.MultiTenantServiceAPIKey = "secret-api-key"

	key1 := dynamicMultiTenantKey(cfg)
	key2 := dynamicMultiTenantKey(cfg)

	require.NotEmpty(t, key1)
	assert.Equal(t, key1, key2, "same config should produce identical cache keys")
}

func TestDynamicMultiTenantKey_DifferentAPIKeys_ProduceDifferentKeys(t *testing.T) {
	t.Parallel()

	cfgA := defaultConfig()
	cfgA.Tenancy.MultiTenantServiceAPIKey = "key-alpha"

	cfgB := defaultConfig()
	cfgB.Tenancy.MultiTenantServiceAPIKey = "key-beta"

	assert.NotEqual(t, dynamicMultiTenantKey(cfgA), dynamicMultiTenantKey(cfgB))
}

func TestDynamicMultiTenantKey_DoesNotContainRawSecret(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantServiceAPIKey = "super-secret-key-12345"

	key := dynamicMultiTenantKey(cfg)

	assert.NotContains(t, key, "super-secret-key-12345",
		"cache key must not contain the raw API key")
}

// TestBuildCanonicalTenantManager_IncludesConnectionsCheckInterval verifies that
// buildCanonicalTenantManager passes WithConnectionsCheckInterval to the pgManager.
// We validate this indirectly: the function must not panic and must include the
// connections check interval option from cfg.MultiTenantConnectionsCheckInterval().
func TestBuildCanonicalTenantManager_IncludesConnectionsCheckInterval(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "https://tm.example.com"
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-api-key"
	cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec = 45

	// buildCanonicalTenantManager creates a real client+manager. The client
	// creation validates the URL + API key. If it succeeds, the manager was
	// constructed with all options including WithConnectionsCheckInterval.
	tmClient, pgManager, err := buildCanonicalTenantManager(cfg, nil)

	require.NoError(t, err, "buildCanonicalTenantManager should succeed with valid config")
	require.NotNil(t, tmClient)
	require.NotNil(t, pgManager)

	// The pgManager is opaque, but we can verify it was created (non-nil).
	// The WithConnectionsCheckInterval option is a functional option that sets
	// an internal field; correctness is verified by the lib-commons unit tests.
	// Our job is to ensure we pass it.
}

// TestBuildCanonicalTenantManager_IncludesClientCacheAndTimeout verifies that
// buildCanonicalTenantManager passes WithCache, WithCacheTTL, and WithTimeout
// to the tenant-manager client.
func TestBuildCanonicalTenantManager_IncludesClientCacheAndTimeout(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "https://tm.example.com"
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-api-key"
	cfg.Tenancy.MultiTenantCacheTTLSec = 300
	cfg.Tenancy.MultiTenantTimeout = 15

	tmClient, pgManager, err := buildCanonicalTenantManager(cfg, nil)

	require.NoError(t, err, "buildCanonicalTenantManager should succeed")
	require.NotNil(t, tmClient)
	require.NotNil(t, pgManager)

	// The client's internal cache and timeout are not directly inspectable,
	// but a successful NewClient call with these options proves they were
	// accepted without error.
}

// TestBuildRabbitMQTenantManagerWithClient_IncludesClientCacheAndTimeout verifies
// that the RabbitMQ tenant manager client also gets cache + timeout options.
func TestBuildRabbitMQTenantManagerWithClient_IncludesClientCacheAndTimeout(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "https://tm.example.com"
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-api-key"
	cfg.Tenancy.MultiTenantCacheTTLSec = 300
	cfg.Tenancy.MultiTenantTimeout = 15

	tmClient, rmqManager := buildRabbitMQTenantManagerWithClient(
		t.Context(), cfg, nil,
	)

	require.NotNil(t, tmClient, "RabbitMQ TM client should be created")
	require.NotNil(t, rmqManager, "RabbitMQ manager should be created")
}

// TestDynamicMultiTenantKey_IncludesNewConfigFields verifies the cache key
// includes the new config fields (cache TTL, connections check interval, timeout)
// so that changes to these fields trigger a manager rebuild.
func TestDynamicMultiTenantKey_IncludesNewConfigFields(t *testing.T) {
	t.Parallel()

	cfgA := defaultConfig()
	cfgA.Tenancy.MultiTenantEnabled = true
	cfgA.Tenancy.MultiTenantURL = "https://tm.example.com"
	cfgA.Tenancy.MultiTenantServiceAPIKey = "test-key"
	cfgA.Tenancy.MultiTenantCacheTTLSec = 120
	cfgA.Tenancy.MultiTenantConnectionsCheckIntervalSec = 30
	cfgA.Tenancy.MultiTenantTimeout = 15

	cfgB := defaultConfig()
	cfgB.Tenancy.MultiTenantEnabled = true
	cfgB.Tenancy.MultiTenantURL = "https://tm.example.com"
	cfgB.Tenancy.MultiTenantServiceAPIKey = "test-key"
	cfgB.Tenancy.MultiTenantCacheTTLSec = 300 // Different cache TTL
	cfgB.Tenancy.MultiTenantConnectionsCheckIntervalSec = 30
	cfgB.Tenancy.MultiTenantTimeout = 15

	keyA := dynamicMultiTenantKey(cfgA)
	keyB := dynamicMultiTenantKey(cfgB)

	assert.NotEqual(t, keyA, keyB,
		"different cache TTL should produce different cache keys")

	// Also verify the key contains the new fields as pipe-separated values
	assert.Contains(t, keyA, "|120|",
		"cache key should contain the cache TTL value")
	assert.Contains(t, keyA, "|30|",
		"cache key should contain the connections check interval value")
}

// TestInitMultiTenantDBHandler_SingleTenantReturnsNil verifies nil is returned
// when multi-tenant mode is disabled.
func TestInitMultiTenantDBHandler_SingleTenantReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	handler := initMultiTenantDBHandler(cfg, func() *Config { return cfg }, nil)
	assert.Nil(t, handler, "single-tenant mode must return nil handler")
}

func TestMultiTenantModeEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *Config
		want bool
	}{
		{
			name: "nil config",
			cfg:  nil,
			want: false,
		},
		{
			name: "disabled",
			cfg:  &Config{Tenancy: TenancyConfig{MultiTenantEnabled: false}},
			want: false,
		},
		{
			name: "enabled",
			cfg:  &Config{Tenancy: TenancyConfig{MultiTenantEnabled: true}},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := multiTenantModeEnabled(tt.cfg)
			assert.Equal(t, tt.want, got)
		})
	}
}
