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

	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestMultiTenant_BackwardCompatibility(t *testing.T) {
	t.Parallel()

	postgresConn := testPostgresClient(t)
	redisConn := sharedTestutil.NewRedisClientWithMock(nil)
	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	provider, manager, tenantDBHandler := createInfraProvider(cfg, nil, postgresConn, redisConn)
	require.NotNil(t, manager)
	assert.Nil(t, tenantDBHandler, "single-tenant mode should not create tenant DB middleware")

	resolvedPostgres, err := provider.GetPrimaryDB(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resolvedPostgres)
	primaryDB, resolveErr := resolvePrimaryDB(context.Background(), postgresConn)
	require.NoError(t, resolveErr)
	assert.Same(t, primaryDB, resolvedPostgres.DB())

	resolvedRedis, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resolvedRedis)
	assert.Same(t, redisConn, resolvedRedis.Connection())

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
	_, err = provider.GetPrimaryDB(ctx)
	require.NoError(t, err)
	_, err = provider.GetRedisConnection(ctx)
	require.NoError(t, err)
}

func TestMultiTenant_BackwardCompatibility_ConfigLoadsWithoutMultiTenantVars(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.False(t, cfg.Tenancy.MultiTenantEnabled, "multi-tenant should be disabled by default")
	assert.Empty(t, cfg.Tenancy.MultiTenantURL, "multi-tenant URL should be empty by default")
	assert.Equal(t, "11111111-1111-1111-1111-111111111111", cfg.Tenancy.DefaultTenantID,
		"default tenant ID should be populated")
	assert.Equal(t, "default", cfg.Tenancy.DefaultTenantSlug,
		"default tenant slug should be populated")

	err := cfg.Validate()
	require.NoError(t, err, "default config must validate without any multi-tenant vars")
}

func TestMultiTenant_BackwardCompatibility_NoTenantManagerRequired(t *testing.T) {
	t.Parallel()

	postgresConn := testPostgresClient(t)
	redisConn := sharedTestutil.NewRedisClientWithMock(nil)
	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	provider, manager, tenantDBHandler := createInfraProvider(cfg, nil, postgresConn, redisConn)
	require.NotNil(t, manager, "config manager should be created in single-tenant mode")
	assert.Nil(t, tenantDBHandler, "no tenant DB handler in single-tenant mode")

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

	resolvedPG, err := provider.GetPrimaryDB(ctx)
	require.NoError(t, err)
	require.NotNil(t, resolvedPG, "postgres should resolve without tenant manager")

	resolvedRedis, err := provider.GetRedisConnection(ctx)
	require.NoError(t, err)
	require.NotNil(t, resolvedRedis, "redis should resolve without tenant manager")
}

func TestMultiTenant_BackwardCompatibility_ConnectionResolutionWithoutTenantContext(t *testing.T) {
	t.Parallel()

	postgresConn := testPostgresClient(t)
	redisConn := sharedTestutil.NewRedisClientWithMock(nil)
	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	provider, _, _ := createInfraProvider(cfg, nil, postgresConn, redisConn)

	tests := []struct {
		name string
		ctx  context.Context
	}{
		{
			name: "with default tenant in context",
			ctx:  context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID),
		},
		{
			name: "with bare background context",
			ctx:  context.Background(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pg, err := provider.GetPrimaryDB(tt.ctx)
			require.NoError(t, err, "postgres should work %s", tt.name)
			require.NotNil(t, pg)

			redis, err := provider.GetRedisConnection(tt.ctx)
			require.NoError(t, err, "redis should work %s", tt.name)
			require.NotNil(t, redis)
		})
	}
}

func TestMultiTenant_BackwardCompatibility_MetricsNoopWhenDisabled(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(false)
	require.NoError(t, err, "creating noop metrics should not fail")
	require.NotNil(t, m, "noop metrics struct should be non-nil")

	ctx := context.Background()

	assert.NotPanics(t, func() {
		m.RecordConnection(ctx, "tenant-a", "success")
		m.RecordConnectionError(ctx, "tenant-a", "timeout")
		m.SetActiveConsumers(ctx, "tenant-a", "events", 5)
		m.RecordMessageProcessed(ctx, "tenant-a", "events", "success")
	}, "all metric methods should be no-op without panic when disabled")
}

func TestCreateInfraProvider_MultiTenantEnabled_CreatesCanonicalManager(t *testing.T) {
	t.Parallel()

	requested := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = true
		// The canonical lib-commons client calls: /v1/tenants/{tenantID}/associations/{service}/connections
		assert.Contains(t, r.URL.Path, "/v1/tenants/tenant-a/associations/matcher/connections")
		assert.Equal(t, "service-api-key", r.Header.Get("X-API-Key"))

		// Return a valid TenantConfig response
		resp := core.TenantConfig{
			ID:            "tenant-a",
			IsolationMode: "isolated",
			Databases: map[string]core.DatabaseConfig{
				"matcher": {
					PostgreSQL: &core.PostgreSQLConfig{
						Host:     "localhost",
						Port:     5432,
						Database: "tenant_a",
						Username: "user",
						Password: "pass",
					},
				},
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = server.URL
	cfg.Tenancy.MultiTenantServiceAPIKey = "service-api-key"
	cfg.Tenancy.MultiTenantMaxTenantPools = 10
	cfg.Tenancy.MultiTenantIdleTimeoutSec = 300
	cfg.Tenancy.MultiTenantCircuitBreakerThreshold = 5
	cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = 30

	provider, manager, tenantDBHandler := createInfraProvider(cfg, nil, nil, nil)
	require.NotNil(t, manager)
	assert.NotNil(t, tenantDBHandler, "multi-tenant mode should create tenant DB middleware")

	// Use the core context key for tenant ID (canonical approach)
	ctx := core.ContextWithTenantID(context.Background(), "tenant-a")
	_, err := provider.GetPrimaryDB(ctx)
	// The connection will fail because we can't actually connect to "localhost:5432"
	// in a unit test, but the request to the tenant manager should have been made.
	require.Error(t, err)
	assert.True(t, requested, "multi-tenant provider should resolve tenant config through the canonical client")
}
