// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestMultiTenant_BackwardCompatibility(t *testing.T) {
	t.Parallel()

	postgresConn := sharedTestutil.NewClientWithResolver(nil)
	redisConn := sharedTestutil.NewRedisClientWithMock(nil)
	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false
	cfg.Tenancy.MultiTenantInfraEnabled = false

	provider, manager := createInfraProviderWithBundleState(cfg, nil, postgresConn, redisConn, nil)
	require.NotNil(t, manager)

	resolvedPostgres, err := provider.GetPostgresConnection(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resolvedPostgres)
	assert.Same(t, postgresConn, resolvedPostgres.Connection())

	resolvedRedis, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resolvedRedis)
	assert.Same(t, redisConn, resolvedRedis.Connection())

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	txProvider, ok := provider.(sharedPorts.InfrastructureProvider)
	require.True(t, ok)

	_, err = txProvider.GetPostgresConnection(ctx)
	require.NoError(t, err)
	_, err = txProvider.GetRedisConnection(ctx)
	require.NoError(t, err)
}

func TestCreateInfraProvider_MultiTenantEnabled_ReturnsTenantConnectionManager(t *testing.T) {
	t.Parallel()

	requested := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = true
		assert.Equal(t, "/tenants/tenant-a/services/matcher/settings", r.URL.Path)
		assert.Equal(t, "staging", r.URL.Query().Get("environment"))
		assert.Equal(t, "staging", r.Header.Get("X-Tenant-Environment"))
		assert.Equal(t, "service-api-key", r.Header.Get("X-API-Key"))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"config": sharedPorts.TenantConfig{PostgresPrimaryDSN: "postgres://tenant-a", RedisAddresses: []string{"redis:6379"}}}))
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantInfraEnabled = true
	cfg.Tenancy.MultiTenantURL = server.URL
	cfg.Tenancy.MultiTenantEnvironment = "staging"
	cfg.Tenancy.MultiTenantServiceAPIKey = "service-api-key"
	cfg.Tenancy.MultiTenantMaxTenantPools = 10
	cfg.Tenancy.MultiTenantIdleTimeoutSec = 300
	cfg.Tenancy.MultiTenantCircuitBreakerThreshold = 5
	cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = 30

	provider, manager := createInfraProviderWithBundleState(cfg, nil, nil, nil, nil)
	require.NotNil(t, manager)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	_, err := provider.GetPostgresConnection(ctx)
	require.Error(t, err)
	assert.True(t, requested, "multi-tenant provider should resolve tenant settings through the remote adapter")
}
