//go:build unit

package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestDynamicInfrastructureProvider_RebuildsManagerWhenKeyChanges(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
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

	activeCfg := defaultConfig()
	activeCfg.Tenancy.MultiTenantEnabled = true
	activeCfg.Tenancy.MultiTenantURL = server.URL
	activeCfg.Tenancy.MultiTenantServiceAPIKey = "service-api-key"
	activeCfg.Tenancy.MultiTenantEnvironment = "staging"

	provider := newDynamicInfrastructureProvider(activeCfg, func() *Config { return activeCfg }, nil, nil, &libLog.NopLogger{}, nil)

	first, err := provider.currentPGManager(context.Background(), activeCfg)
	require.NoError(t, err)
	second, err := provider.currentPGManager(context.Background(), activeCfg)
	require.NoError(t, err)
	assert.Same(t, first, second)

	updatedCfg := *activeCfg
	updatedCfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = activeCfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec + 5
	activeCfg = &updatedCfg

	third, err := provider.currentPGManager(context.Background(), activeCfg)
	require.NoError(t, err)
	assert.NotSame(t, first, third)
	require.NoError(t, provider.Close())
}

func TestDynamicInfrastructureProvider_SingleTenantUsesDirectConnections(t *testing.T) {
	t.Parallel()

	bootstrapCfg := defaultConfig()
	pg := testPostgresClient(t)
	redis := testRedisClient(t)
	provider := newDynamicInfrastructureProvider(bootstrapCfg, nil, pg, redis, &libLog.NopLogger{}, nil)

	pgLease, err := provider.GetPrimaryDB(context.Background())
	require.NoError(t, err)
	require.NotNil(t, pgLease)

	redisLease, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	require.NotNil(t, redisLease)
}

func TestDynamicInfrastructureProvider_MultiTenantModeEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      *Config
		expected bool
	}{
		{
			name:     "nil config returns false",
			cfg:      nil,
			expected: false,
		},
		{
			name: "disabled returns false",
			cfg: &Config{
				Tenancy: TenancyConfig{MultiTenantEnabled: false},
			},
			expected: false,
		},
		{
			name: "enabled returns true",
			cfg: &Config{
				Tenancy: TenancyConfig{MultiTenantEnabled: true},
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, multiTenantModeEnabled(tc.cfg))
		})
	}
}

func TestDynamicInfrastructureProvider_DynamicMultiTenantKey(t *testing.T) {
	t.Parallel()

	t.Run("nil config returns empty", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", dynamicMultiTenantKey(nil))
	})

	t.Run("different configs produce different keys", func(t *testing.T) {
		t.Parallel()

		cfg1 := defaultConfig()
		cfg1.Tenancy.MultiTenantURL = "http://url-a"
		cfg1.Tenancy.MultiTenantServiceAPIKey = "key-a"

		cfg2 := defaultConfig()
		cfg2.Tenancy.MultiTenantURL = "http://url-b"
		cfg2.Tenancy.MultiTenantServiceAPIKey = "key-b"

		assert.NotEqual(t, dynamicMultiTenantKey(cfg1), dynamicMultiTenantKey(cfg2))
	})
}

func TestDynamicInfrastructureProvider_SingleTenantPostgresNotConfigured(t *testing.T) {
	t.Parallel()

	bootstrapCfg := defaultConfig()
	provider := newDynamicInfrastructureProvider(bootstrapCfg, nil, nil, nil, &libLog.NopLogger{}, nil)

	_, err := provider.GetPrimaryDB(context.Background())
	require.ErrorIs(t, err, ErrPostgresConnectionNotConfigured)
}

func TestDynamicInfrastructureProvider_SingleTenantRedisNotConfigured(t *testing.T) {
	t.Parallel()

	bootstrapCfg := defaultConfig()
	provider := newDynamicInfrastructureProvider(bootstrapCfg, nil, nil, nil, &libLog.NopLogger{}, nil)

	_, err := provider.GetRedisConnection(context.Background())
	require.ErrorIs(t, err, ErrRedisConnectionNotConfigured)
}

func TestDynamicInfrastructureProvider_CloseNilProvider(t *testing.T) {
	t.Parallel()

	var provider *dynamicInfrastructureProvider
	require.NoError(t, provider.Close())
}

func TestDynamicInfrastructureProvider_CloseWithNoManager(t *testing.T) {
	t.Parallel()

	provider := newDynamicInfrastructureProvider(defaultConfig(), nil, nil, nil, &libLog.NopLogger{}, nil)
	require.NoError(t, provider.Close())
}

func testPostgresClient(t *testing.T) *libPostgres.Client {
	t.Helper()
	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(new(sql.DB)))
	return infraTestutil.NewClientWithResolver(resolver)
}

func testRedisClient(t *testing.T) *libRedis.Client {
	t.Helper()
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	return infraTestutil.NewRedisClientWithMock(client)
}
