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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/valkey"

	"github.com/LerianStudio/matcher/internal/auth"
)

// Section 1: Redis Key Isolation — valkey.GetKeyContext produces distinct keys.

func TestRedisKeyIsolation_DifferentTenants_DifferentKeys(t *testing.T) {
	t.Parallel()

	ctxA := core.SetTenantIDInContext(context.Background(), "tenant-a-uuid")
	ctxB := core.SetTenantIDInContext(context.Background(), "tenant-b-uuid")
	ctxNone := context.Background()

	logicalKey := "matcher:dashboard:550e8400:volume:2024-01-01:2024-01-31:all"

	keyA, errA := valkey.GetKeyContext(ctxA, logicalKey)
	keyB, errB := valkey.GetKeyContext(ctxB, logicalKey)
	keyNone, errNone := valkey.GetKeyContext(ctxNone, logicalKey)

	require.NoError(t, errA)
	require.NoError(t, errB)
	require.NoError(t, errNone)

	assert.NotEqual(t, keyA, keyB,
		"tenant A and tenant B must produce different Redis keys for the same logical key")
	assert.NotEqual(t, keyA, keyNone,
		"tenant A key must differ from unprefixed key")
	assert.NotEqual(t, keyB, keyNone,
		"tenant B key must differ from unprefixed key")

	// Unprefixed key should be the raw logical key itself.
	assert.Equal(t, logicalKey, keyNone,
		"without tenant context, key should be returned as-is")

	// Prefixed keys must contain tenant prefix.
	assert.Contains(t, keyA, "tenant-a-uuid",
		"tenant A key must contain tenant-a-uuid")
	assert.Contains(t, keyB, "tenant-b-uuid",
		"tenant B key must contain tenant-b-uuid")
}

func TestRedisKeyIsolation_SameTenant_SameKey(t *testing.T) {
	t.Parallel()

	ctxA1 := core.SetTenantIDInContext(context.Background(), "tenant-x")
	ctxA2 := core.SetTenantIDInContext(context.Background(), "tenant-x")

	logicalKey := "matcher:dedupe:ctx-id:hash"

	key1, err1 := valkey.GetKeyContext(ctxA1, logicalKey)
	key2, err2 := valkey.GetKeyContext(ctxA2, logicalKey)

	require.NoError(t, err1)
	require.NoError(t, err2)

	assert.Equal(t, key1, key2,
		"same tenant ID must produce identical keys for the same logical key")
}

func TestRedisKeyIsolation_DefaultTenant_GetsPrefixed(t *testing.T) {
	t.Parallel()

	defaultTID := "11111111-1111-1111-1111-111111111111"
	ctxDefault := core.SetTenantIDInContext(context.Background(), defaultTID)

	logicalKey := "matcher:callback:ratelimit:JIRA"

	key, err := valkey.GetKeyContext(ctxDefault, logicalKey)
	require.NoError(t, err)

	// Even the default tenant should get a prefixed key when set explicitly.
	assert.Contains(t, key, defaultTID,
		"default tenant ID in context should produce a prefixed key")
	assert.NotEqual(t, logicalKey, key,
		"default tenant key should differ from raw logical key")
}

// Section 2: Redis Key Isolation — Multiple domain prefixes.

func TestRedisKeyIsolation_CrossDomainPrefixes(t *testing.T) {
	t.Parallel()

	tenantID := "tenant-cross-domain"
	ctx := core.SetTenantIDInContext(context.Background(), tenantID)

	domainKeys := []struct {
		name string
		key  string
	}{
		{name: "dashboard", key: "matcher:dashboard:ctx:volume:2024-01-01:2024-01-31:all"},
		{name: "dedupe", key: "matcher:dedupe:ctx:hash123"},
		{name: "ratelimit", key: "matcher:callback:ratelimit:JIRA"},
		{name: "export", key: "matcher:export:job-id"},
		{name: "idempotency", key: "matcher:idempotency:req-hash"},
	}

	for _, dk := range domainKeys {
		t.Run(dk.name, func(t *testing.T) {
			t.Parallel()

			key, err := valkey.GetKeyContext(ctx, dk.key)
			require.NoError(t, err)
			assert.Contains(t, key, tenantID,
				"tenant-prefixed key for %s must contain tenant ID", dk.name)
		})
	}
}

func TestRedisKeyIsolation_TwoTenants_AllDomainKeys_FullyIsolated(t *testing.T) {
	t.Parallel()

	ctxA := core.SetTenantIDInContext(context.Background(), "tenant-a")
	ctxB := core.SetTenantIDInContext(context.Background(), "tenant-b")

	logicalKeys := []string{
		"matcher:dashboard:ctx:volume:2024-01-01:2024-01-31:all",
		"matcher:dedupe:ctx:hash123",
		"matcher:callback:ratelimit:JIRA",
	}

	for _, lk := range logicalKeys {
		keyA, errA := valkey.GetKeyContext(ctxA, lk)
		keyB, errB := valkey.GetKeyContext(ctxB, lk)

		require.NoError(t, errA)
		require.NoError(t, errB)

		assert.NotEqual(t, keyA, keyB,
			"key %q must be different for tenant-a vs tenant-b", lk)
	}
}

// Section 3: Redis Key Isolation — Edge cases.

func TestRedisKeyIsolation_EmptyLogicalKey(t *testing.T) {
	t.Parallel()

	ctx := core.SetTenantIDInContext(context.Background(), "tenant-empty")

	key, err := valkey.GetKeyContext(ctx, "")
	require.NoError(t, err)

	// Even with empty logical key, the tenant prefix should be applied.
	assert.Contains(t, key, "tenant-empty")
}

func TestRedisKeyIsolation_SpecialCharactersInTenantID(t *testing.T) {
	t.Parallel()

	// UUID-format tenant IDs (the normal case).
	uuidTenant := "550e8400-e29b-41d4-a716-446655440000"
	ctx := core.SetTenantIDInContext(context.Background(), uuidTenant)

	key, err := valkey.GetKeyContext(ctx, "matcher:test:key")
	require.NoError(t, err)
	assert.Contains(t, key, uuidTenant)
}

// Section 4: Provider-Level Isolation — Single-tenant resolves same connection.

func TestProviderIsolation_SingleTenant_AlwaysSameConnection(t *testing.T) {
	t.Parallel()

	pg := testPostgresClient(t)
	redis := testRedisClient(t)
	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	provider := newDynamicInfrastructureProvider(cfg, nil, nil, pg, redis, &libLog.NopLogger{}, nil)

	// Multiple calls should always return the same underlying connection.
	for i := 0; i < 5; i++ {
		pgLease, err := provider.GetPrimaryDB(context.Background())
		require.NoError(t, err)
		primaryDB, resolveErr := resolvePrimaryDB(context.Background(), pg)
		require.NoError(t, resolveErr)
		assert.Same(t, primaryDB, pgLease.DB(),
			"single-tenant mode must always return the same postgres connection")

		redisLease, err := provider.GetRedisConnection(context.Background())
		require.NoError(t, err)
		assert.Same(t, redis, redisLease.Connection(),
			"single-tenant mode must always return the same redis connection")
	}
}

func TestProviderIsolation_SingleTenant_ContextDoesNotAffectConnection(t *testing.T) {
	t.Parallel()

	pg := testPostgresClient(t)
	redis := testRedisClient(t)
	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	provider := newDynamicInfrastructureProvider(cfg, nil, nil, pg, redis, &libLog.NopLogger{}, nil)

	// Use the proper typed context key from the auth package to avoid SA1029.
	contexts := []struct {
		name string
		ctx  context.Context
	}{
		{"bare context", context.Background()},
		{"with core tenant-a", core.SetTenantIDInContext(context.Background(), "tenant-a")},
		{"with core tenant-b", core.SetTenantIDInContext(context.Background(), "tenant-b")},
		{"with auth tenant-x", context.WithValue(context.Background(), auth.TenantIDKey, "tenant-x")},
	}

	for _, tc := range contexts {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pgLease, err := provider.GetPrimaryDB(tc.ctx)
			require.NoError(t, err)
			primaryDB, resolveErr := resolvePrimaryDB(tc.ctx, pg)
			require.NoError(t, resolveErr)
			assert.Same(t, primaryDB, pgLease.DB(),
				"single-tenant mode: %s should still resolve to same connection", tc.name)

			redisLease, err := provider.GetRedisConnection(tc.ctx)
			require.NoError(t, err)
			assert.Same(t, redis, redisLease.Connection())
		})
	}
}

// Section 5: Multi-tenant provider — Two tenants resolve through different paths.

func TestProviderIsolation_MultiTenant_TwoTenantsCallTenantManager(t *testing.T) {
	t.Parallel()

	tenantsRequested := make(map[string]int)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Track which tenant IDs are being resolved.
		path := r.URL.Path
		for _, tid := range []string{"tenant-alpha", "tenant-beta"} {
			if strings.Contains(path, tid) {
				tenantsRequested[tid]++
			}
		}

		resp := core.TenantConfig{
			ID:            "tenant",
			IsolationMode: "isolated",
			Databases: map[string]core.DatabaseConfig{
				"matcher": {
					PostgreSQL: &core.PostgreSQLConfig{
						Host: "localhost", Port: 5432,
						Database: "db", Username: "u", Password: "p",
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
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, nil, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	ctxA := core.ContextWithTenantID(context.Background(), "tenant-alpha")
	ctxB := core.ContextWithTenantID(context.Background(), "tenant-beta")

	// Both will fail with connection errors (no real DB), but should reach tenant manager.
	_, _ = provider.GetPrimaryDB(ctxA)
	_, _ = provider.GetPrimaryDB(ctxB)

	assert.Positive(t, tenantsRequested["tenant-alpha"],
		"tenant-alpha should have been resolved through tenant manager")
	assert.Positive(t, tenantsRequested["tenant-beta"],
		"tenant-beta should have been resolved through tenant manager")
}

// Section 6: Close Behavior.

func TestDynamicInfrastructureProvider_Close_Idempotent(t *testing.T) {
	t.Parallel()

	provider := newDynamicInfrastructureProvider(defaultConfig(), nil, nil, nil, nil, &libLog.NopLogger{}, nil)

	// First close should succeed.
	require.NoError(t, provider.Close())

	// Second close should also succeed (idempotent).
	require.NoError(t, provider.Close())
}

func TestDynamicInfrastructureProvider_Close_WithActiveManager(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(core.TenantConfig{}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = server.URL
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, nil, &libLog.NopLogger{}, nil)

	// Force creation of pgManager.
	_, err := provider.currentPGManager(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, provider.pgManager, "pgManager should be created")

	// Close should clean up manager.
	require.NoError(t, provider.Close())
	assert.Nil(t, provider.pgManager, "pgManager should be nil after close")
	assert.Nil(t, provider.tmClient, "tmClient should be nil after close")
	assert.Empty(t, provider.multiTenantKey, "multiTenantKey should be empty after close")
}
