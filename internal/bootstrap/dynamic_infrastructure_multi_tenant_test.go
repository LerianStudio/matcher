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
