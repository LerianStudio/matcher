//go:build unit

package bootstrap

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestDynamicInfrastructureProvider_RebuildsMultiTenantManagerWhenKeyChanges(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"config":{"postgres_primary_dsn":"postgres://tenant-a","redis_addresses":["redis:6379"]}}`))
	}))
	defer server.Close()

	activeCfg := defaultConfig()
	activeCfg.Tenancy.MultiTenantEnabled = true
	activeCfg.Tenancy.MultiTenantInfraEnabled = true
	activeCfg.Tenancy.MultiTenantURL = server.URL
	activeCfg.Tenancy.MultiTenantServiceAPIKey = "service-api-key"
	activeCfg.Tenancy.MultiTenantEnvironment = "staging"

	provider := newDynamicInfrastructureProvider(activeCfg, func() *Config { return activeCfg }, nil, nil, nil, &libLog.NopLogger{})

	first, err := provider.currentMultiTenantManager(context.Background(), activeCfg)
	require.NoError(t, err)
	second, err := provider.currentMultiTenantManager(context.Background(), activeCfg)
	require.NoError(t, err)
	assert.Same(t, first, second)

	updatedCfg := *activeCfg
	updatedCfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = activeCfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec + 5
	activeCfg = &updatedCfg

	third, err := provider.currentMultiTenantManager(context.Background(), activeCfg)
	require.NoError(t, err)
	assert.NotSame(t, first, third)
	require.NoError(t, provider.Close())
}

func TestDynamicInfrastructureProvider_SingleTenantUsesActiveBundleConnections(t *testing.T) {
	t.Parallel()

	bootstrapCfg := defaultConfig()
	bundleState := newActiveMatcherBundleState()
	provider := newDynamicInfrastructureProvider(bootstrapCfg, nil, bundleState, nil, nil, &libLog.NopLogger{})

	bundleState.Update(&MatcherBundle{Infra: &InfraBundle{Postgres: testPostgresClient(t), Redis: testRedisClient(t)}})

	pgLease, err := provider.GetPostgresConnection(context.Background())
	require.NoError(t, err)
	assert.Same(t, bundleState.Current().DB(), pgLease.Connection())

	redisLease, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	assert.Same(t, bundleState.Current().RedisClient(), redisLease.Connection())
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
