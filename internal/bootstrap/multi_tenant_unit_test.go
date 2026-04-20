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

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// Section 1: Dynamic Infrastructure Provider — Multi-Tenant Error Paths.

func TestDynamicInfrastructureProvider_MultiTenant_MissingTenantInContext(t *testing.T) {
	t.Parallel()

	// When no tenant is in context, multi-tenant mode fails closed:
	// 1. core.GetTenantIDContext(ctx) => "" (empty)
	// 2. auth.LookupTenantID(ctx) => ("", false) — no explicit tenant set
	// 3. tenantID remains "" => returns core.ErrTenantContextRequired
	//
	// This is intentional: multi-tenant mode must NOT silently fall back to the
	// default tenant. The caller must provide explicit tenant context.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		resp := core.TenantConfig{
			ID:            auth.DefaultTenantID,
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

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	// Bare context: no tenant set => fail closed with ErrTenantContextRequired.
	_, err := provider.GetPrimaryDB(context.Background())
	require.Error(t, err, "expected error because no tenant context is set")
	require.ErrorContains(t, err, "tenant context required")
	require.ErrorIs(t, err, core.ErrTenantContextRequired,
		"multi-tenant mode must fail closed when tenant context is genuinely absent")
}

func TestDynamicInfrastructureProvider_MultiTenant_FallbackToAuthGetTenantID(t *testing.T) {
	t.Parallel()

	// The provider falls back from core.GetTenantID → auth.GetTenantID.
	// Inject tenant via the auth package context key (not core).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "tenant-from-auth")

		resp := core.TenantConfig{
			ID:            "tenant-from-auth",
			IsolationMode: "isolated",
			Databases: map[string]core.DatabaseConfig{
				"matcher": {
					PostgreSQL: &core.PostgreSQLConfig{
						Host: "localhost", Port: 5432,
						Database: "tenant_db", Username: "u", Password: "p",
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

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	// Use auth context key (not core)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-from-auth")

	// The connection will fail (can't connect to localhost:5432 in unit test) but
	// the key point is that the fallback to auth.GetTenantID works: the request
	// reaches the mock server with the correct tenant ID.
	_, err := provider.GetPrimaryDB(ctx)
	require.Error(t, err, "expected connection failure in unit test, but fallback path should have been taken")
	require.ErrorContains(t, err, "get tenant connection")
}

func TestDynamicInfrastructureProvider_MultiTenant_EmptyTenantIDInContext(t *testing.T) {
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

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	// Set empty string for both core and auth tenant ID contexts
	ctx := core.ContextWithTenantID(context.Background(), "")
	ctx = context.WithValue(ctx, auth.TenantIDKey, "")

	_, err := provider.GetPrimaryDB(ctx)
	require.Error(t, err, "expected error for empty tenant fallback chain")
	require.ErrorContains(t, err, "tenant context required")
	// Empty tenant context now fails closed for primary DB resolution.
}

// Section 2: Dynamic Infrastructure Provider — BeginTx Multi-Tenant Error Paths.

func TestDynamicInfrastructureProvider_BeginTx_MissingTenantContext(t *testing.T) {
	t.Parallel()

	// Same fallback chain as primary DB resolution: bare context => explicit tenant required.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(core.TenantConfig{
			ID:            auth.DefaultTenantID,
			IsolationMode: "isolated",
			Databases: map[string]core.DatabaseConfig{
				"matcher": {
					PostgreSQL: &core.PostgreSQLConfig{
						Host: "localhost", Port: 5432,
						Database: "db", Username: "u", Password: "p",
					},
				},
			},
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = server.URL
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	_, err := provider.BeginTx(context.Background())
	require.Error(t, err, "expected error because mock tenant manager can't provide a real DB for transactions")
	require.ErrorContains(t, err, "resolve tenant manager for transaction")
}

func TestDynamicInfrastructureProvider_BeginTx_SingleTenant_NoPostgres(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	provider := newDynamicInfrastructureProvider(cfg, nil, nil, nil, &libLog.NopLogger{}, nil)

	_, err := provider.BeginTx(context.Background())
	require.ErrorIs(t, err, ErrPostgresConnectionNotConfigured)
}

// Section 3: Dynamic Infrastructure Provider — GetReplicaDB Multi-Tenant Error Paths.

func TestDynamicInfrastructureProvider_GetReplicaDB_MissingTenantContext(t *testing.T) {
	t.Parallel()

	// Same fallback chain: bare context => default tenant ID.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(core.TenantConfig{
			ID:            auth.DefaultTenantID,
			IsolationMode: "isolated",
			Databases: map[string]core.DatabaseConfig{
				"matcher": {
					PostgreSQL: &core.PostgreSQLConfig{
						Host: "localhost", Port: 5432,
						Database: "db", Username: "u", Password: "p",
					},
				},
			},
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = server.URL
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	_, err := provider.GetReplicaDB(context.Background())
	require.Error(t, err, "expected error because mock tenant manager can't provide a real DB for replica")
	require.ErrorContains(t, err, "resolve tenant manager for replica db")
}

func TestDynamicInfrastructureProvider_GetReplicaDB_SingleTenant_NoPostgres(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	provider := newDynamicInfrastructureProvider(cfg, nil, nil, nil, &libLog.NopLogger{}, nil)

	_, err := provider.GetReplicaDB(context.Background())
	require.ErrorIs(t, err, ErrPostgresConnectionNotConfigured)
}

func TestDynamicInfrastructureProvider_GetReplicaDB_SingleTenant_NilReplica(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	pg := testPostgresClient(t)
	provider := newDynamicInfrastructureProvider(cfg, nil, pg, nil, &libLog.NopLogger{}, nil)

	lease, err := provider.GetReplicaDB(context.Background())
	// The test postgres client only has primary, no replica => should return nil, nil.
	require.NoError(t, err)
	assert.Nil(t, lease)
}

// Section 4: Dynamic Infrastructure Provider — Redis in Multi-Tenant Mode.

func TestDynamicInfrastructureProvider_Redis_MultiTenantMode_UsesSingleton(t *testing.T) {
	t.Parallel()

	// Even in multi-tenant mode, Redis uses singleton connection with key prefixing.
	redisConn := sharedTestutil.NewRedisClientWithMock(nil)

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "http://localhost:9999"
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, redisConn, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	lease, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Same(t, redisConn, lease.Connection())
}

func TestDynamicInfrastructureProvider_Redis_MultiTenantMode_NilRedis(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "http://localhost:9999"
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	_, err := provider.GetRedisConnection(context.Background())
	require.ErrorIs(t, err, ErrRedisConnectionNotConfigured)
}

// Section 5: Dynamic Infrastructure Provider — Config Nil / Getter Paths.

func TestDynamicInfrastructureProvider_NilConfigGetter_FallsBackToInitial(t *testing.T) {
	t.Parallel()

	initialCfg := defaultConfig()
	provider := newDynamicInfrastructureProvider(initialCfg, nil, nil, nil, &libLog.NopLogger{}, nil)

	got := provider.currentConfig()
	assert.Same(t, initialCfg, got)
}

func TestDynamicInfrastructureProvider_ConfigGetterReturnsNil_FallsBackToInitial(t *testing.T) {
	t.Parallel()

	initialCfg := defaultConfig()
	provider := newDynamicInfrastructureProvider(initialCfg, func() *Config { return nil }, nil, nil, &libLog.NopLogger{}, nil)

	got := provider.currentConfig()
	assert.Same(t, initialCfg, got)
}

func TestDynamicInfrastructureProvider_NilProvider_CurrentConfigReturnsNil(t *testing.T) {
	t.Parallel()

	var provider *dynamicInfrastructureProvider

	got := provider.currentConfig()
	assert.Nil(t, got)
}

// Section 6: buildRabbitMQTenantManager.

func TestBuildRabbitMQTenantManager_ValidConfig(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantURL = server.URL
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"
	cfg.Tenancy.MultiTenantMaxTenantPools = 5
	cfg.Tenancy.MultiTenantIdleTimeoutSec = 60

	_, mgr := buildRabbitMQTenantManagerWithClient(context.Background(), cfg, &libLog.NopLogger{})
	require.NotNil(t, mgr, "valid config should create a non-nil RabbitMQ tenant manager")
}

func TestBuildRabbitMQTenantManager_InvalidURL_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	// Empty URL will cause client creation to fail
	cfg.Tenancy.MultiTenantURL = ""
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	_, mgr := buildRabbitMQTenantManagerWithClient(context.Background(), cfg, &libLog.NopLogger{})
	assert.Nil(t, mgr, "invalid config should return nil RabbitMQ tenant manager (fallback to single-tenant)")
}

func TestBuildRabbitMQTenantManager_NonProductionAllowsInsecureHTTP(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.App.EnvName = "development"
	cfg.Tenancy.MultiTenantURL = server.URL // http:// (not https://)
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	_, mgr := buildRabbitMQTenantManagerWithClient(context.Background(), cfg, &libLog.NopLogger{})
	require.NotNil(t, mgr, "non-production env should allow insecure HTTP")
}

// Section 7: buildCanonicalTenantManager.

func TestBuildCanonicalTenantManager_Success(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantURL = server.URL
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"
	cfg.Tenancy.MultiTenantMaxTenantPools = 10
	cfg.Tenancy.MultiTenantIdleTimeoutSec = 60
	cfg.Tenancy.MultiTenantCircuitBreakerThreshold = 3
	cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = 15

	tmClient, pgManager, err := buildCanonicalTenantManager(cfg, &libLog.NopLogger{})
	require.NoError(t, err)
	require.NotNil(t, tmClient, "tenant-manager client should be created")
	require.NotNil(t, pgManager, "postgres manager should be created")

	// Cleanup
	require.NoError(t, tmClient.Close())
	require.NoError(t, pgManager.Close(context.Background()))
}

func TestBuildCanonicalTenantManager_EmptyURL_Fails(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantURL = ""
	cfg.Tenancy.MultiTenantServiceAPIKey = "test-key"

	_, _, err := buildCanonicalTenantManager(cfg, &libLog.NopLogger{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "create tenant manager client")
}

// Section 8: createInfraProvider — Integration of all pieces.

func TestCreateInfraProvider_SingleTenant_ReturnsNilTenantDBHandler(t *testing.T) {
	t.Parallel()

	pg := sharedTestutil.NewClientWithResolver(nil)
	redis := sharedTestutil.NewRedisClientWithMock(nil)
	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = false

	provider, closer, tenantDBHandler := createInfraProvider(cfg, nil, pg, redis)
	require.NotNil(t, provider)
	require.NotNil(t, closer)
	assert.Nil(t, tenantDBHandler, "single-tenant mode must return nil tenant DB handler")
}

func TestCreateInfraProvider_MultiTenant_ReturnsTenantDBHandler(t *testing.T) {
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

	provider, closer, tenantDBHandler := createInfraProvider(cfg, nil, nil, nil)
	require.NotNil(t, provider)
	require.NotNil(t, closer)
	assert.NotNil(t, tenantDBHandler, "multi-tenant mode must return non-nil tenant DB handler")
}

// Section 9: Metrics — Enabled vs Disabled context behavior.

func TestMultiTenantMetrics_AllMethods_WithVariousTenantIDs(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(true)
	require.NoError(t, err)

	tenantIDs := []string{
		"tenant-a",
		"tenant-b",
		"11111111-1111-1111-1111-111111111111",
		"",
	}

	for _, tid := range tenantIDs {
		t.Run("tenant_"+tid, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			if tid != "" {
				ctx = core.ContextWithTenantID(ctx, tid)
			}

			assert.NotPanics(t, func() {
				m.RecordConnection(ctx, tid, "success")
				m.RecordConnection(ctx, tid, "failure")
				m.RecordConnectionError(ctx, tid, "timeout")
				m.RecordConnectionError(ctx, tid, "refused")
				m.SetActiveConsumers(ctx, tid, "events.queue", 3)
				m.SetActiveConsumers(ctx, tid, "events.queue", 0)
				m.RecordMessageProcessed(ctx, tid, "events.queue", "success")
				m.RecordMessageProcessed(ctx, tid, "events.queue", "error")
			}, "all metric methods must be safe for tenant ID %q", tid)
		})
	}
}

func TestMultiTenantMetrics_EnabledAndDisabled_SameAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := NewMultiTenantMetrics(tt.enabled)
			require.NoError(t, err)
			require.NotNil(t, m)

			// Both enabled and disabled metrics must expose the same non-nil interface
			assert.NotNil(t, m.connectionsTotal)
			assert.NotNil(t, m.connectionErrors)
			assert.NotNil(t, m.consumersActive)
			assert.NotNil(t, m.messagesProcessed)

			ctx := core.ContextWithTenantID(context.Background(), "tenant-x")

			assert.NotPanics(t, func() {
				m.RecordConnection(ctx, "tenant-x", "success")
				m.RecordConnectionError(ctx, "tenant-x", "timeout")
				m.SetActiveConsumers(ctx, "tenant-x", "q", 1)
				m.RecordMessageProcessed(ctx, "tenant-x", "q", "ok")
			})
		})
	}
}

// Section 10: Dynamic Multi-Tenant Key — Additional edge cases.

func TestDynamicMultiTenantKey_IncludesAllConfigFields(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "http://tm:4003"
	cfg.Tenancy.MultiTenantServiceAPIKey = "key-1"
	cfg.Tenancy.MultiTenantMaxTenantPools = 50
	cfg.Tenancy.MultiTenantIdleTimeoutSec = 120
	cfg.Tenancy.MultiTenantCircuitBreakerThreshold = 3
	cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = 15
	cfg.Postgres.MaxOpenConnections = 25
	cfg.Postgres.MaxIdleConnections = 10

	key1 := dynamicMultiTenantKey(cfg)

	// Changing each field must produce a different key
	modifications := []struct {
		name   string
		modify func(*Config)
	}{
		{"url", func(c *Config) { c.Tenancy.MultiTenantURL = "http://tm:4004" }},
		{"apiKey", func(c *Config) { c.Tenancy.MultiTenantServiceAPIKey = "key-2" }},
		{"maxPools", func(c *Config) { c.Tenancy.MultiTenantMaxTenantPools = 99 }},
		{"idleTimeout", func(c *Config) { c.Tenancy.MultiTenantIdleTimeoutSec = 999 }},
		{"cbThreshold", func(c *Config) { c.Tenancy.MultiTenantCircuitBreakerThreshold = 10 }},
		{"cbTimeout", func(c *Config) { c.Tenancy.MultiTenantCircuitBreakerTimeoutSec = 60 }},
		{"pgMaxOpen", func(c *Config) { c.Postgres.MaxOpenConnections = 50 }},
		{"pgMaxIdle", func(c *Config) { c.Postgres.MaxIdleConnections = 20 }},
		{"enabled", func(c *Config) { c.Tenancy.MultiTenantEnabled = false }},
		{"envName", func(c *Config) { c.App.EnvName = "staging" }},
	}

	for _, mod := range modifications {
		t.Run(mod.name, func(t *testing.T) {
			t.Parallel()

			modified := *cfg
			mod.modify(&modified)
			key2 := dynamicMultiTenantKey(&modified)

			assert.NotEqual(t, key1, key2,
				"changing %s must produce a different multi-tenant key", mod.name)
		})
	}
}

func TestDynamicMultiTenantKey_SameConfig_SameKey(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = "http://tm:4003"
	cfg.Tenancy.MultiTenantServiceAPIKey = "key-1"

	key1 := dynamicMultiTenantKey(cfg)
	key2 := dynamicMultiTenantKey(cfg)

	assert.Equal(t, key1, key2, "same config must produce same key")
}

// Section 11: multiTenantModeEnabled — Table-driven completeness.

func TestMultiTenantModeEnabled_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      *Config
		expected bool
	}{
		{
			name:     "nil config",
			cfg:      nil,
			expected: false,
		},
		{
			name: "disabled explicitly",
			cfg: &Config{
				Tenancy: TenancyConfig{MultiTenantEnabled: false},
			},
			expected: false,
		},
		{
			name: "enabled with defaults",
			cfg: &Config{
				Tenancy: TenancyConfig{MultiTenantEnabled: true},
			},
			expected: true,
		},
		{
			name:     "default config (disabled by default)",
			cfg:      defaultConfig(),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := multiTenantModeEnabled(tc.cfg)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// Section 12: PG Manager Rebuild on Config Change.

func TestDynamicInfrastructureProvider_PGManager_RebuildOnServiceAPIKeyChange(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(core.TenantConfig{ID: "t1"}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}))
	defer server.Close()

	cfg := defaultConfig()
	cfg.Tenancy.MultiTenantEnabled = true
	cfg.Tenancy.MultiTenantURL = server.URL
	cfg.Tenancy.MultiTenantServiceAPIKey = "key-v1"

	provider := newDynamicInfrastructureProvider(cfg, func() *Config { return cfg }, nil, nil, &libLog.NopLogger{}, nil)
	defer func() { _ = provider.Close() }()

	first, err := provider.currentPGManager(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, first)

	// Same config => same manager (cached)
	second, err := provider.currentPGManager(context.Background(), cfg)
	require.NoError(t, err)
	assert.Same(t, first, second, "same config should return cached manager")

	// Change API key => new manager
	changedCfg := *cfg
	changedCfg.Tenancy.MultiTenantServiceAPIKey = "key-v2"

	third, err := provider.currentPGManager(context.Background(), &changedCfg)
	require.NoError(t, err)
	assert.NotSame(t, first, third, "changed API key should rebuild manager")
}

func TestDynamicInfrastructureProvider_PGManager_NilConfig_ReturnsError(t *testing.T) {
	t.Parallel()

	provider := newDynamicInfrastructureProvider(defaultConfig(), nil, nil, nil, &libLog.NopLogger{}, nil)

	_, err := provider.currentPGManager(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errDynamicInfrastructureConfigUnavailable)
}
