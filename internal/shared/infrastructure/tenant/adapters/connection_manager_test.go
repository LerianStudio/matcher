//go:build unit

package adapters

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

var errConfigFetchFailed = errors.New("config fetch failed")

type mockConfigurationPort struct {
	config *ports.TenantConfig
	err    error
}

type mapConfigurationPort struct {
	configs map[string]*ports.TenantConfig
}

type libRedisConnectionOptionsExpectation struct {
	PoolSize     int
	MinIdleConns int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	DialTimeout  time.Duration
}

type setupContextKey struct{}

func (m *mockConfigurationPort) GetTenantConfig(
	_ context.Context,
	_ string,
) (*ports.TenantConfig, error) {
	if m.err != nil {
		return nil, m.err
	}

	return m.config, nil
}

func (m *mapConfigurationPort) GetTenantConfig(_ context.Context, tenantID string) (*ports.TenantConfig, error) {
	if cfg, ok := m.configs[tenantID]; ok {
		return cfg, nil
	}

	return nil, errConfigFetchFailed
}

func defaultTestConfig() *ports.TenantConfig {
	return &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://user:pass@primary:5432/db",
		PostgresReplicaDSN: "postgres://user:pass@replica:5432/db",
		PostgresPrimaryDB:  "testdb",
		PostgresReplicaDB:  "testdb",
		RedisAddresses:     []string{"redis:6379"},
		RedisPassword:      "secret",
		RedisDB:            0,
		RedisMasterName:    "",
		RedisProtocol:      3,
		RedisUseTLS:        false,
		RedisCACert:        "",
	}
}

func TestNewTenantConnectionManager(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}

	manager, err := NewTenantConnectionManager(mockConfig, 0, 0, 0, 0)

	require.NoError(t, err)
	require.NotNil(t, manager)
	assert.Equal(t, 25, manager.maxOpenConns)
	assert.Equal(t, 5, manager.maxIdleConns)
	assert.Equal(t, 30, manager.connMaxLifetimeMins)
	assert.Equal(t, 5, manager.connMaxIdleTimeMins)
	assert.NotNil(t, manager.postgresCache)
	assert.NotNil(t, manager.redisCache)
	assert.False(t, manager.closed)
}

func TestNewTenantConnectionManager_NilConfigPort(t *testing.T) {
	t.Parallel()

	manager, err := NewTenantConnectionManager(nil, 25, 5, 30, 5)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrConfigurationPortRequired)
	assert.Nil(t, manager)
}

func TestNewTenantConnectionManager_CustomPoolSizes(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}

	manager, err := NewTenantConnectionManager(mockConfig, 50, 10, 60, 10)

	require.NoError(t, err)
	require.NotNil(t, manager)
	assert.Equal(t, 50, manager.maxOpenConns)
	assert.Equal(t, 10, manager.maxIdleConns)
	assert.Equal(t, 60, manager.connMaxLifetimeMins)
	assert.Equal(t, 10, manager.connMaxIdleTimeMins)
}

func TestNewTenantConnectionManager_ZeroPoolSizes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		maxOpenConns     int
		maxIdleConns     int
		expectedOpenConn int
		expectedIdleConn int
	}{
		{
			name:             "zero values use defaults",
			maxOpenConns:     0,
			maxIdleConns:     0,
			expectedOpenConn: 25,
			expectedIdleConn: 5,
		},
		{
			name:             "negative values use defaults",
			maxOpenConns:     -1,
			maxIdleConns:     -5,
			expectedOpenConn: 25,
			expectedIdleConn: 5,
		},
		{
			name:             "mixed zero and positive",
			maxOpenConns:     0,
			maxIdleConns:     15,
			expectedOpenConn: 25,
			expectedIdleConn: 15,
		},
		{
			name:             "mixed negative and positive",
			maxOpenConns:     100,
			maxIdleConns:     -1,
			expectedOpenConn: 100,
			expectedIdleConn: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockConfig := &mockConfigurationPort{config: defaultTestConfig()}

			manager, err := NewTenantConnectionManager(
				mockConfig,
				tc.maxOpenConns,
				tc.maxIdleConns,
				0,
				0,
			)

			require.NoError(t, err)
			assert.Equal(t, tc.expectedOpenConn, manager.maxOpenConns)
			assert.Equal(t, tc.expectedIdleConn, manager.maxIdleConns)
		})
	}
}

func TestGetPostgresConnection_ClosedManager(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()
	require.NoError(t, err)

	conn, err := manager.GetPostgresConnection(context.Background())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection manager is closed")
}

func TestGetRedisConnection_ClosedManager(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()
	require.NoError(t, err)

	conn, err := manager.GetRedisConnection(context.Background())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection manager is closed")
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err1 := manager.Close()
	require.NoError(t, err1)

	err2 := manager.Close()
	require.NoError(t, err2)

	err3 := manager.Close()
	require.NoError(t, err3)

	assert.True(t, manager.closed)
}

func TestClose_NilConnections(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()

	require.NoError(t, err)
	assert.True(t, manager.closed)
	assert.Empty(t, manager.postgresCache)
	assert.Empty(t, manager.redisCache)
}

func TestPostgresInfraKey_Deterministic(t *testing.T) {
	t.Parallel()

	cfg := defaultTestConfig()

	key1 := postgresInfraKey(cfg)
	key2 := postgresInfraKey(cfg)
	key3 := postgresInfraKey(cfg)

	assert.Equal(t, key1, key2)
	assert.Equal(t, key2, key3)
	assert.NotEmpty(t, key1)
}

func TestPostgresInfraKey_DifferentConfigs(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary1:5432/db",
		PostgresReplicaDSN: "postgres://replica1:5432/db",
		PostgresPrimaryDB:  "db1",
		PostgresReplicaDB:  "db1",
	}

	cfg2 := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary2:5432/db",
		PostgresReplicaDSN: "postgres://replica2:5432/db",
		PostgresPrimaryDB:  "db2",
		PostgresReplicaDB:  "db2",
	}

	cfg3 := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary1:5432/db",
		PostgresReplicaDSN: "postgres://replica3:5432/db",
		PostgresPrimaryDB:  "db1",
		PostgresReplicaDB:  "db1",
	}

	key1 := postgresInfraKey(cfg1)
	key2 := postgresInfraKey(cfg2)
	key3 := postgresInfraKey(cfg3)

	assert.NotEqual(t, key1, key2)
	assert.NotEqual(t, key1, key3)
	assert.NotEqual(t, key2, key3)
}

func TestPostgresInfraKey_IgnoresDBMetadata(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		PostgresReplicaDSN: "postgres://replica:5432/db",
		PostgresPrimaryDB:  "db-name-a",
		PostgresReplicaDB:  "db-name-a-replica",
	}
	cfg2 := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		PostgresReplicaDSN: "postgres://replica:5432/db",
		PostgresPrimaryDB:  "db-name-b",
		PostgresReplicaDB:  "db-name-b-replica",
	}

	assert.Equal(t, postgresInfraKey(cfg1), postgresInfraKey(cfg2))
}

func TestRedisInfraKey_Deterministic(t *testing.T) {
	t.Parallel()

	cfg := defaultTestConfig()

	key1 := redisInfraKey(cfg)
	key2 := redisInfraKey(cfg)
	key3 := redisInfraKey(cfg)

	assert.Equal(t, key1, key2)
	assert.Equal(t, key2, key3)
	assert.NotEmpty(t, key1)
}

func TestRedisInfraKey_AddressOrder(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		RedisAddresses:  []string{"redis1:6379", "redis2:6379", "redis3:6379"},
		RedisPassword:   "secret",
		RedisDB:         0,
		RedisMasterName: "mymaster",
		RedisProtocol:   3,
		RedisUseTLS:     false,
		RedisCACert:     "",
	}

	cfg2 := &ports.TenantConfig{
		RedisAddresses:  []string{"redis3:6379", "redis1:6379", "redis2:6379"},
		RedisPassword:   "secret",
		RedisDB:         0,
		RedisMasterName: "mymaster",
		RedisProtocol:   3,
		RedisUseTLS:     false,
		RedisCACert:     "",
	}

	cfg3 := &ports.TenantConfig{
		RedisAddresses:  []string{"redis2:6379", "redis3:6379", "redis1:6379"},
		RedisPassword:   "secret",
		RedisDB:         0,
		RedisMasterName: "mymaster",
		RedisProtocol:   3,
		RedisUseTLS:     false,
		RedisCACert:     "",
	}

	key1 := redisInfraKey(cfg1)
	key2 := redisInfraKey(cfg2)
	key3 := redisInfraKey(cfg3)

	assert.Equal(t, key1, key2, "address order should not affect key")
	assert.Equal(t, key2, key3, "address order should not affect key")
}

func TestRedisInfraKey_DifferentConfigs(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		RedisAddresses:  []string{"redis1:6379"},
		RedisPassword:   "secret1",
		RedisDB:         0,
		RedisMasterName: "",
		RedisProtocol:   3,
		RedisUseTLS:     false,
		RedisCACert:     "",
	}

	cfg2 := &ports.TenantConfig{
		RedisAddresses:  []string{"redis2:6379"},
		RedisPassword:   "secret2",
		RedisDB:         1,
		RedisMasterName: "",
		RedisProtocol:   3,
		RedisUseTLS:     false,
		RedisCACert:     "",
	}

	key1 := redisInfraKey(cfg1)
	key2 := redisInfraKey(cfg2)

	assert.NotEqual(t, key1, key2)
}

func TestHashKey(t *testing.T) {
	t.Parallel()

	result1 := hashKey("test-data")
	result2 := hashKey("test-data")
	result3 := hashKey("different-data")

	assert.Equal(t, result1, result2)
	assert.NotEqual(t, result1, result3)
	assert.Len(t, result1, 64)
}

func TestHashKey_EmptyString(t *testing.T) {
	t.Parallel()

	result := hashKey("")

	assert.NotEmpty(t, result)
	assert.Len(t, result, 64)
}

func TestGetPostgresConnection_ConfigError(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{err: errConfigFetchFailed}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	conn, err := manager.GetPostgresConnection(context.Background())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get tenant config for postgres")
}

func TestGetRedisConnection_ConfigError(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{err: errConfigFetchFailed}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	conn, err := manager.GetRedisConnection(context.Background())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get tenant config for redis")
}

func TestGetPostgresConnection_NilConfig(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: nil}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	conn, err := manager.GetPostgresConnection(context.Background())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant config is nil")
}

func TestGetRedisConnection_NilConfig(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: nil}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	conn, err := manager.GetRedisConnection(context.Background())

	assert.Nil(t, conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant config is nil")
}

func TestConcurrentGetPostgresConnection_ClosedState(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()
	require.NoError(t, err)

	var (
		wg         sync.WaitGroup
		errorCount atomic.Int32
	)

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, connErr := manager.GetPostgresConnection(context.Background())
			if connErr != nil {
				errorCount.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.True(t, manager.closed)
	assert.Equal(t, int32(100), errorCount.Load(), "all goroutines should get closed error")
}

func TestConcurrentGetRedisConnection_ClosedState(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()
	require.NoError(t, err)

	var (
		wg         sync.WaitGroup
		errorCount atomic.Int32
	)

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, connErr := manager.GetRedisConnection(context.Background())
			if connErr != nil {
				errorCount.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.True(t, manager.closed)
	assert.Equal(t, int32(100), errorCount.Load(), "all goroutines should get closed error")
}

func TestTenantConnectionManager_EvictsIdleEntriesAtSoftLimit(t *testing.T) {
	t.Parallel()

	configPort := &mapConfigurationPort{configs: map[string]*ports.TenantConfig{
		"tenant-a": {PostgresPrimaryDSN: "postgres://tenant-a", RedisAddresses: []string{"redis-a:6379"}},
		"tenant-b": {PostgresPrimaryDSN: "postgres://tenant-b", RedisAddresses: []string{"redis-b:6379"}},
	}}

	manager, err := NewTenantConnectionManager(configPort, 25, 5, 30, 5, WithCachePolicy(1, time.Second))
	require.NoError(t, err)

	manager.postgresClientFactory = func(cfg libPostgres.Config) (*libPostgres.Client, error) { return &libPostgres.Client{}, nil }
	manager.postgresConnector = func(context.Context, *libPostgres.Client) error { return nil }

	ctxA := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	ctxB := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-b")

	leaseA, err := manager.GetPostgresConnection(ctxA)
	require.NoError(t, err)
	require.Len(t, manager.postgresCache, 1)
	leaseA.Release()

	for _, entry := range manager.postgresCache {
		entry.idleSince = time.Now().Add(-2 * time.Second)
	}

	leaseB, err := manager.GetPostgresConnection(ctxB)
	require.NoError(t, err)
	leaseB.Release()
	require.Len(t, manager.postgresCache, 1)

	keyB := postgresInfraKey(configPort.configs["tenant-b"])
	_, ok := manager.postgresCache[keyB]
	assert.True(t, ok)
}

func TestTenantConnectionManager_RejectsNewPoolWhenCapReachedAndEntryStillLeased(t *testing.T) {
	t.Parallel()

	configPort := &mapConfigurationPort{configs: map[string]*ports.TenantConfig{
		"tenant-a": {PostgresPrimaryDSN: "postgres://tenant-a", RedisAddresses: []string{"redis-a:6379"}},
		"tenant-b": {PostgresPrimaryDSN: "postgres://tenant-b", RedisAddresses: []string{"redis-b:6379"}},
	}}

	manager, err := NewTenantConnectionManager(configPort, 25, 5, 30, 5, WithCachePolicy(1, time.Hour))
	require.NoError(t, err)

	manager.postgresClientFactory = func(cfg libPostgres.Config) (*libPostgres.Client, error) { return &libPostgres.Client{}, nil }
	manager.postgresConnector = func(context.Context, *libPostgres.Client) error { return nil }

	ctxA := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	ctxB := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-b")

	leaseA, err := manager.GetPostgresConnection(ctxA)
	require.NoError(t, err)
	defer leaseA.Release()

	_, err = manager.GetPostgresConnection(ctxB)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTenantPoolLimitReached)

	require.Len(t, manager.postgresCache, 1)
}

func TestTenantConnectionManager_RejectsNewRedisPoolWhenCapReachedAndEntryStillLeased(t *testing.T) {
	t.Parallel()

	configPort := &mapConfigurationPort{configs: map[string]*ports.TenantConfig{
		"tenant-a": {PostgresPrimaryDSN: "postgres://tenant-a", RedisAddresses: []string{"redis-a:6379"}},
		"tenant-b": {PostgresPrimaryDSN: "postgres://tenant-b", RedisAddresses: []string{"redis-b:6379"}},
	}}

	manager, err := NewTenantConnectionManager(configPort, 25, 5, 30, 5, WithCachePolicy(1, time.Hour))
	require.NoError(t, err)

	manager.redisClientFactory = func(_ context.Context, _ libRedis.Config) (*libRedis.Client, error) {
		return testutil.NewRedisClientWithMock(redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})), nil
	}

	ctxA := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	ctxB := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-b")

	leaseA, err := manager.GetRedisConnection(ctxA)
	require.NoError(t, err)
	defer leaseA.Release()

	_, err = manager.GetRedisConnection(ctxB)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTenantPoolLimitReached)
	require.Len(t, manager.redisCache, 1)
}

func TestTenantConnectionManager_EvictsReleasedIdleRedisEntryAtHardCap(t *testing.T) {
	t.Parallel()

	configPort := &mapConfigurationPort{configs: map[string]*ports.TenantConfig{
		"tenant-a": {PostgresPrimaryDSN: "postgres://tenant-a", RedisAddresses: []string{"redis-a:6379"}},
		"tenant-b": {PostgresPrimaryDSN: "postgres://tenant-b", RedisAddresses: []string{"redis-b:6379"}},
	}}

	manager, err := NewTenantConnectionManager(configPort, 25, 5, 30, 5, WithCachePolicy(1, time.Second))
	require.NoError(t, err)

	manager.redisClientFactory = func(_ context.Context, _ libRedis.Config) (*libRedis.Client, error) {
		return testutil.NewRedisClientWithMock(redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})), nil
	}

	ctxA := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	ctxB := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-b")

	leaseA, err := manager.GetRedisConnection(ctxA)
	require.NoError(t, err)
	require.Len(t, manager.redisCache, 1)
	leaseA.Release()

	for _, entry := range manager.redisCache {
		entry.idleSince = time.Now().Add(-2 * time.Second)
	}

	leaseB, err := manager.GetRedisConnection(ctxB)
	require.NoError(t, err)
	leaseB.Release()
	require.Len(t, manager.redisCache, 1)

	keyB := redisInfraKey(configPort.configs["tenant-b"])
	_, ok := manager.redisCache[keyB]
	assert.True(t, ok)
}

func TestConcurrentClose(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	var (
		wg              sync.WaitGroup
		closeErrorCount atomic.Int32
	)

	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if closeErr := manager.Close(); closeErr != nil {
				closeErrorCount.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.True(t, manager.closed)
	assert.Equal(t, int32(0), closeErrorCount.Load(), "concurrent closes should not return errors")
}

func TestTenantConnectionManager_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ports.InfrastructureProvider = (*TenantConnectionManager)(nil)
}

func TestPostgresInfraKey_CollisionResistance(t *testing.T) {
	t.Parallel()

	configs := []*ports.TenantConfig{
		{
			PostgresPrimaryDSN: "postgres://a:pass@host:5432/db",
			PostgresReplicaDSN: "",
			PostgresPrimaryDB:  "db1",
			PostgresReplicaDB:  "db1",
		},
		{
			PostgresPrimaryDSN: "postgres://b:pass@host:5432/db",
			PostgresReplicaDSN: "",
			PostgresPrimaryDB:  "db1",
			PostgresReplicaDB:  "db1",
		},
		{
			PostgresPrimaryDSN: "postgres://c:pass@host:5432/db",
			PostgresReplicaDSN: "",
			PostgresPrimaryDB:  "db2",
			PostgresReplicaDB:  "db2",
		},
		{
			PostgresPrimaryDSN: "postgres://a:pass@host:5432/db",
			PostgresReplicaDSN: "postgres://replica:5432/db",
			PostgresPrimaryDB:  "db1",
			PostgresReplicaDB:  "db1",
		},
		{
			PostgresPrimaryDSN: "postgres://a:pass@host:5432/db",
			PostgresReplicaDSN: "postgres://replica-2:5432/db",
			PostgresPrimaryDB:  "db1",
			PostgresReplicaDB:  "db1",
		},
	}

	keys := make(map[string]int)

	for i, cfg := range configs {
		key := postgresInfraKey(cfg)

		if existing, ok := keys[key]; ok {
			t.Errorf("collision between config %d and %d: both produce key %s", existing, i, key)
		}

		keys[key] = i
	}

	assert.Len(t, keys, len(configs), "all configs should produce unique keys")
}

func TestRedisInfraKey_CollisionResistance(t *testing.T) {
	t.Parallel()

	configs := []*ports.TenantConfig{
		{
			RedisAddresses:  []string{"redis:6379"},
			RedisPassword:   "pass1",
			RedisDB:         0,
			RedisMasterName: "",
			RedisProtocol:   3,
			RedisUseTLS:     false,
		},
		{
			RedisAddresses:  []string{"redis:6379"},
			RedisPassword:   "pass2",
			RedisDB:         0,
			RedisMasterName: "",
			RedisProtocol:   3,
			RedisUseTLS:     false,
		},
		{
			RedisAddresses:  []string{"redis:6379"},
			RedisPassword:   "pass1",
			RedisDB:         1,
			RedisMasterName: "",
			RedisProtocol:   3,
			RedisUseTLS:     false,
		},
		{
			RedisAddresses:  []string{"redis:6380"},
			RedisPassword:   "pass1",
			RedisDB:         0,
			RedisMasterName: "",
			RedisProtocol:   3,
			RedisUseTLS:     false,
		},
		{
			RedisAddresses:  []string{"redis:6379"},
			RedisPassword:   "pass1",
			RedisDB:         0,
			RedisMasterName: "mymaster",
			RedisProtocol:   3,
			RedisUseTLS:     false,
		},
		{
			RedisAddresses:  []string{"redis:6379"},
			RedisPassword:   "pass1",
			RedisDB:         0,
			RedisMasterName: "",
			RedisProtocol:   2,
			RedisUseTLS:     false,
		},
		{
			RedisAddresses:  []string{"redis:6379"},
			RedisPassword:   "pass1",
			RedisDB:         0,
			RedisMasterName: "",
			RedisProtocol:   3,
			RedisUseTLS:     true,
		},
		{
			RedisAddresses:  []string{"redis:6379", "redis2:6379"},
			RedisPassword:   "pass1",
			RedisDB:         0,
			RedisMasterName: "",
			RedisProtocol:   3,
			RedisUseTLS:     false,
		},
	}

	keys := make(map[string]int)

	for i, cfg := range configs {
		key := redisInfraKey(cfg)

		if existing, ok := keys[key]; ok {
			t.Errorf("collision between config %d and %d: both produce key %s", existing, i, key)
		}

		keys[key] = i
	}

	assert.Len(t, keys, len(configs), "all configs should produce unique keys")
}

func TestHashKey_LengthConsistency(t *testing.T) {
	t.Parallel()

	testCases := []string{
		"",
		"a",
		"short",
		"a very long string that should still produce a consistent length hash output regardless of input size",
		"unicode: 日本語 emoji: 🎉 special: @#$%^&*()",
	}

	for _, tc := range testCases {
		key := hashKey(tc)
		assert.Len(t, key, 64, "hash key should always be 64 characters (32 bytes hex encoded)")
	}
}

func TestConnectionManager_ErrorMessages(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrConnectionManagerClosed",
			err:      ErrConnectionManagerClosed,
			expected: "connection manager is closed",
		},
		{
			name:     "ErrCloseConnections",
			err:      ErrCloseConnections,
			expected: "errors closing connections",
		},
		{
			name:     "ErrConfigurationPortRequired",
			err:      ErrConfigurationPortRequired,
			expected: "configuration port is required",
		},
		{
			name:     "ErrNoPrimaryDatabaseConfigured",
			err:      ErrNoPrimaryDatabaseConfigured,
			expected: "no primary database configured for multi-tenant transaction",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, tc.err.Error())
		})
	}
}

func TestNewTenantConnectionManager_NegativeTimeouts(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}

	manager, err := NewTenantConnectionManager(mockConfig, -10, -20, -30, -40)

	require.NoError(t, err)
	require.NotNil(t, manager)
	assert.Equal(t, 25, manager.maxOpenConns)
	assert.Equal(t, 5, manager.maxIdleConns)
	assert.Equal(t, 30, manager.connMaxLifetimeMins)
	assert.Equal(t, 5, manager.connMaxIdleTimeMins)
}

func TestRedisInfraKey_EmptyAddresses(t *testing.T) {
	t.Parallel()

	cfg := &ports.TenantConfig{
		RedisAddresses:  nil,
		RedisPassword:   "secret",
		RedisDB:         0,
		RedisMasterName: "",
		RedisProtocol:   3,
		RedisUseTLS:     false,
	}

	key := redisInfraKey(cfg)

	assert.NotEmpty(t, key)
	assert.Len(t, key, 64)
}

func TestRedisInfraKey_WithTimeoutValues(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		RedisAddresses:    []string{"redis:6379"},
		RedisPassword:     "secret",
		RedisDB:           0,
		RedisReadTimeout:  1000,
		RedisWriteTimeout: 2000,
		RedisDialTimeout:  3000,
		RedisPoolSize:     10,
		RedisMinIdleConns: 2,
	}

	cfg2 := &ports.TenantConfig{
		RedisAddresses:    []string{"redis:6379"},
		RedisPassword:     "secret",
		RedisDB:           0,
		RedisReadTimeout:  5000,
		RedisWriteTimeout: 6000,
		RedisDialTimeout:  7000,
		RedisPoolSize:     20,
		RedisMinIdleConns: 5,
	}

	key1 := redisInfraKey(cfg1)
	key2 := redisInfraKey(cfg2)

	assert.NotEqual(t, key1, key2, "different timeout values should produce different keys")
}

func TestPostgresInfraKey_EmptyStrings(t *testing.T) {
	t.Parallel()

	cfg := &ports.TenantConfig{
		PostgresPrimaryDSN: "",
		PostgresReplicaDSN: "",
		PostgresPrimaryDB:  "",
		PostgresReplicaDB:  "",
	}

	key := postgresInfraKey(cfg)

	assert.NotEmpty(t, key)
	assert.Len(t, key, 64)
}

func TestBeginTx_ClosedManager(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()
	require.NoError(t, err)

	tx, err := manager.BeginTx(context.Background())

	assert.Nil(t, tx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection manager is closed")
}

func TestGetReplicaDB_ClosedManager(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()
	require.NoError(t, err)

	db, err := manager.GetReplicaDB(context.Background())

	assert.Nil(t, db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection manager is closed")
}

func TestBeginTx_ConfigError(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{err: errConfigFetchFailed}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	tx, err := manager.BeginTx(context.Background())

	assert.Nil(t, tx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get postgres connection")
}

func TestGetReplicaDB_ConfigError(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{err: errConfigFetchFailed}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	db, err := manager.GetReplicaDB(context.Background())

	assert.Nil(t, db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get postgres connection")
}

func TestConnectionManagerBeginTx_ApplyTenantSchemaError(t *testing.T) {
	t.Parallel()

	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: defaultTestConfig()}, 25, 5, 30, 5)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	manager.postgresClientFactory = func(_ libPostgres.Config) (*libPostgres.Client, error) {
		return testutil.NewClientWithResolver(resolver), nil
	}
	manager.postgresConnector = func(_ context.Context, _ *libPostgres.Client) error {
		return nil
	}

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	tx, beginErr := manager.BeginTx(ctx)

	require.Error(t, beginErr)
	assert.Nil(t, tx)
	assert.Contains(t, beginErr.Error(), "failed to apply tenant schema")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConnectionManagerBeginTx_ApplyTenantSchemaError_RollbackFails(t *testing.T) {
	t.Parallel()

	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: defaultTestConfig()}, 25, 5, 30, 5)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	manager.postgresClientFactory = func(_ libPostgres.Config) (*libPostgres.Client, error) {
		return testutil.NewClientWithResolver(resolver), nil
	}
	manager.postgresConnector = func(_ context.Context, _ *libPostgres.Client) error {
		return nil
	}

	errRollback := errors.New("rollback failed")
	mock.ExpectBegin()
	mock.ExpectRollback().WillReturnError(errRollback)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	tx, beginErr := manager.BeginTx(ctx)

	require.Error(t, beginErr)
	assert.Nil(t, tx)
	assert.Contains(t, beginErr.Error(), "failed to apply tenant schema")
	assert.Contains(t, beginErr.Error(), "rollback transaction")
	assert.ErrorIs(t, beginErr, errRollback)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConcurrentBeginTx_ClosedState(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()
	require.NoError(t, err)

	var (
		wg         sync.WaitGroup
		errorCount atomic.Int32
	)

	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, txErr := manager.BeginTx(context.Background())
			if txErr != nil {
				errorCount.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(50), errorCount.Load(), "all goroutines should get closed error")
}

func TestConcurrentGetReplicaDB_ClosedState(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	err = manager.Close()
	require.NoError(t, err)

	var (
		wg         sync.WaitGroup
		errorCount atomic.Int32
	)

	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, dbErr := manager.GetReplicaDB(context.Background())
			if dbErr != nil {
				errorCount.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(50), errorCount.Load(), "all goroutines should get closed error")
}

func TestRedisInfraKey_TLSAndCACert(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisUseTLS:    true,
		RedisCACert:    "cert-content-1",
	}

	cfg2 := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisUseTLS:    true,
		RedisCACert:    "cert-content-2",
	}

	cfg3 := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisUseTLS:    false,
		RedisCACert:    "cert-content-1",
	}

	key1 := redisInfraKey(cfg1)
	key2 := redisInfraKey(cfg2)
	key3 := redisInfraKey(cfg3)

	assert.NotEqual(t, key1, key2, "different CA certs should produce different keys")
	assert.NotEqual(t, key1, key3, "different TLS settings should produce different keys")
}

func TestHashKey_Determinism(t *testing.T) {
	t.Parallel()

	input := "pg:postgres://user:pass@primary:5432/db:postgres://user:pass@replica:5432/db:testdb:testdb"

	results := make([]string, 100)
	for i := 0; i < 100; i++ {
		results[i] = hashKey(input)
	}

	for i := 1; i < len(results); i++ {
		assert.Equal(t, results[0], results[i], "hash should be deterministic")
	}
}

func TestClose_ClearsAllMaps(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	assert.NotNil(t, manager.postgresCache)
	assert.NotNil(t, manager.redisCache)

	err = manager.Close()
	require.NoError(t, err)

	assert.Empty(t, manager.postgresCache)
	assert.Empty(t, manager.redisCache)
}

func TestErrorWrapping_ConnectionManager(t *testing.T) {
	t.Parallel()

	assert.True(t, errors.Is(ErrConnectionManagerClosed, ErrConnectionManagerClosed))
	assert.True(t, errors.Is(ErrCloseConnections, ErrCloseConnections))
	assert.True(t, errors.Is(ErrConfigurationPortRequired, ErrConfigurationPortRequired))
	assert.True(t, errors.Is(ErrNoPrimaryDatabaseConfigured, ErrNoPrimaryDatabaseConfigured))

	assert.False(t, errors.Is(ErrConnectionManagerClosed, ErrCloseConnections))
	assert.False(t, errors.Is(ErrConfigurationPortRequired, ErrNoPrimaryDatabaseConfigured))
}

// delayingConfigurationPort wraps a config port and adds a delay to simulate slow config fetches.
type delayingConfigurationPort struct {
	inner  ports.ConfigurationPort
	delay  time.Duration
	called *atomic.Int32
	start  chan struct{}
}

func (d *delayingConfigurationPort) GetTenantConfig(
	ctx context.Context,
	tenantID string,
) (*ports.TenantConfig, error) {
	d.called.Add(1)
	if d.start != nil {
		select {
		case <-d.start:
			// already signaled
		default:
			close(d.start)
		}
	}
	time.Sleep(d.delay)

	return d.inner.GetTenantConfig(ctx, tenantID)
}

func TestConcurrentGetPostgresConnection_ConfigError(t *testing.T) {
	t.Parallel()

	callCount := &atomic.Int32{}
	delayingConfig := &delayingConfigurationPort{
		inner:  &mockConfigurationPort{err: errConfigFetchFailed},
		delay:  5 * time.Millisecond,
		called: callCount,
	}

	manager, err := NewTenantConnectionManager(delayingConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	var (
		wg         sync.WaitGroup
		errorCount atomic.Int32
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, connErr := manager.GetPostgresConnection(context.Background())
			if connErr != nil {
				errorCount.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.True(t, callCount.Load() >= 1, "config should be fetched at least once")
	assert.Equal(t, int32(10), errorCount.Load(), "all concurrent calls should error")
}

func TestConcurrentGetRedisConnection_ConfigError(t *testing.T) {
	t.Parallel()

	callCount := &atomic.Int32{}
	delayingConfig := &delayingConfigurationPort{
		inner:  &mockConfigurationPort{err: errConfigFetchFailed},
		delay:  5 * time.Millisecond,
		called: callCount,
	}

	manager, err := NewTenantConnectionManager(delayingConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	var (
		wg         sync.WaitGroup
		errorCount atomic.Int32
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, connErr := manager.GetRedisConnection(context.Background())
			if connErr != nil {
				errorCount.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.True(t, callCount.Load() >= 1, "config should be fetched at least once")
	assert.Equal(t, int32(10), errorCount.Load(), "all concurrent calls should error")
}

func TestGetPostgresConnection_ClosedDuringConfigFetch(t *testing.T) {
	t.Parallel()

	callCount := &atomic.Int32{}
	start := make(chan struct{})
	delayingConfig := &delayingConfigurationPort{
		inner:  &mockConfigurationPort{err: errConfigFetchFailed},
		delay:  30 * time.Millisecond,
		called: callCount,
		start:  start,
	}

	manager, err := NewTenantConnectionManager(delayingConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	var (
		wg       sync.WaitGroup
		gotError atomic.Bool
	)

	wg.Add(1)

	go func() {
		defer wg.Done()

		_, fetchErr := manager.GetPostgresConnection(context.Background())
		gotError.Store(fetchErr != nil)
	}()

	<-start

	err = manager.Close()
	require.NoError(t, err)

	wg.Wait()
	assert.True(t, manager.closed)
	assert.True(t, gotError.Load(), "connection fetch should fail during close/config fetch race")
}

func TestGetRedisConnection_ClosedDuringConfigFetch(t *testing.T) {
	t.Parallel()

	callCount := &atomic.Int32{}
	start := make(chan struct{})
	delayingConfig := &delayingConfigurationPort{
		inner:  &mockConfigurationPort{err: errConfigFetchFailed},
		delay:  30 * time.Millisecond,
		called: callCount,
		start:  start,
	}

	manager, err := NewTenantConnectionManager(delayingConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	var (
		wg       sync.WaitGroup
		gotError atomic.Bool
	)

	wg.Add(1)

	go func() {
		defer wg.Done()

		_, fetchErr := manager.GetRedisConnection(context.Background())
		gotError.Store(fetchErr != nil)
	}()

	<-start

	err = manager.Close()
	require.NoError(t, err)

	wg.Wait()
	assert.True(t, manager.closed)
	assert.True(t, gotError.Load(), "connection fetch should fail during close/config fetch race")
}

func TestRedisConnectionOptions_CustomTimeouts(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		cfg             *ports.TenantConfig
		expectedOptions libRedisConnectionOptionsExpectation
	}{
		{
			name: "custom timeout values",
			cfg: &ports.TenantConfig{
				RedisAddresses:    []string{"redis:6379"},
				RedisReadTimeout:  5 * time.Second,
				RedisWriteTimeout: 5 * time.Second,
				RedisDialTimeout:  10 * time.Second,
				RedisPoolSize:     50,
				RedisMinIdleConns: 10,
			},
			expectedOptions: libRedisConnectionOptionsExpectation{
				PoolSize:     50,
				MinIdleConns: 10,
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 5 * time.Second,
				DialTimeout:  10 * time.Second,
			},
		},
		{
			name: "zero timeout values use defaults",
			cfg: &ports.TenantConfig{
				RedisAddresses:    []string{"redis:6379"},
				RedisReadTimeout:  0,
				RedisWriteTimeout: 0,
				RedisDialTimeout:  0,
				RedisPoolSize:     0,
				RedisMinIdleConns: 0,
			},
			expectedOptions: libRedisConnectionOptionsExpectation{
				PoolSize:     defaultRedisPoolSize,
				MinIdleConns: defaultRedisMinIdleConns,
				ReadTimeout:  defaultRedisReadTimeout,
				WriteTimeout: defaultRedisWriteTimeout,
				DialTimeout:  defaultRedisDialTimeout,
			},
		},
		{
			name: "mixed custom and default values",
			cfg: &ports.TenantConfig{
				RedisAddresses:    []string{"redis:6379"},
				RedisReadTimeout:  10 * time.Second,
				RedisWriteTimeout: 0,
				RedisDialTimeout:  0,
				RedisPoolSize:     25,
				RedisMinIdleConns: 0,
			},
			expectedOptions: libRedisConnectionOptionsExpectation{
				PoolSize:     25,
				MinIdleConns: defaultRedisMinIdleConns,
				ReadTimeout:  10 * time.Second,
				WriteTimeout: defaultRedisWriteTimeout,
				DialTimeout:  defaultRedisDialTimeout,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			options := redisConnectionOptions(tc.cfg)

			assert.Equal(t, tc.expectedOptions.PoolSize, options.PoolSize)
			assert.Equal(t, tc.expectedOptions.MinIdleConns, options.MinIdleConns)
			assert.Equal(t, tc.expectedOptions.ReadTimeout, options.ReadTimeout)
			assert.Equal(t, tc.expectedOptions.WriteTimeout, options.WriteTimeout)
			assert.Equal(t, tc.expectedOptions.DialTimeout, options.DialTimeout)
		})
	}
}

func TestCreatePostgresConnection_ReplicaFallback(t *testing.T) {
	t.Parallel()

	cfg := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://user:pass@primary:5432/db",
		PostgresReplicaDSN: "",
		PostgresPrimaryDB:  "testdb",
		PostgresReplicaDB:  "",
	}

	key := postgresInfraKey(cfg)
	assert.NotEmpty(t, key)
}

func TestRedisInfraKey_AllFieldsCombined(t *testing.T) {
	t.Parallel()

	cfg := &ports.TenantConfig{
		RedisAddresses:    []string{"redis1:6379", "redis2:6380"},
		RedisPassword:     "complex_password!@#$%",
		RedisDB:           15,
		RedisMasterName:   "sentinel-master",
		RedisProtocol:     2,
		RedisUseTLS:       true,
		RedisCACert:       "-----BEGIN CERTIFICATE-----\nMIIC...",
		RedisReadTimeout:  5 * time.Second,
		RedisWriteTimeout: 5 * time.Second,
		RedisDialTimeout:  10 * time.Second,
		RedisPoolSize:     100,
		RedisMinIdleConns: 20,
	}

	key := redisInfraKey(cfg)
	assert.NotEmpty(t, key)
	assert.Len(t, key, 64)

	key2 := redisInfraKey(cfg)
	assert.Equal(t, key, key2, "same config should produce same key")
}

func TestNewTenantConnectionManager_PartialDefaultValues(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                 string
		maxOpenConns         int
		maxIdleConns         int
		connMaxLifetimeMins  int
		connMaxIdleTimeMins  int
		expectedOpenConns    int
		expectedIdleConns    int
		expectedLifetimeMins int
		expectedIdleTimeMins int
	}{
		{
			name:                 "only lifetime is zero",
			maxOpenConns:         50,
			maxIdleConns:         10,
			connMaxLifetimeMins:  0,
			connMaxIdleTimeMins:  10,
			expectedOpenConns:    50,
			expectedIdleConns:    10,
			expectedLifetimeMins: 30,
			expectedIdleTimeMins: 10,
		},
		{
			name:                 "only idle time is zero",
			maxOpenConns:         50,
			maxIdleConns:         10,
			connMaxLifetimeMins:  60,
			connMaxIdleTimeMins:  0,
			expectedOpenConns:    50,
			expectedIdleConns:    10,
			expectedLifetimeMins: 60,
			expectedIdleTimeMins: 5,
		},
		{
			name:                 "all zeros",
			maxOpenConns:         0,
			maxIdleConns:         0,
			connMaxLifetimeMins:  0,
			connMaxIdleTimeMins:  0,
			expectedOpenConns:    25,
			expectedIdleConns:    5,
			expectedLifetimeMins: 30,
			expectedIdleTimeMins: 5,
		},
		{
			name:                 "all custom values",
			maxOpenConns:         100,
			maxIdleConns:         20,
			connMaxLifetimeMins:  120,
			connMaxIdleTimeMins:  15,
			expectedOpenConns:    100,
			expectedIdleConns:    20,
			expectedLifetimeMins: 120,
			expectedIdleTimeMins: 15,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
			manager, err := NewTenantConnectionManager(
				mockConfig,
				tc.maxOpenConns,
				tc.maxIdleConns,
				tc.connMaxLifetimeMins,
				tc.connMaxIdleTimeMins,
			)

			require.NoError(t, err)
			require.NotNil(t, manager)
			assert.Equal(t, tc.expectedOpenConns, manager.maxOpenConns)
			assert.Equal(t, tc.expectedIdleConns, manager.maxIdleConns)
			assert.Equal(t, tc.expectedLifetimeMins, manager.connMaxLifetimeMins)
			assert.Equal(t, tc.expectedIdleTimeMins, manager.connMaxIdleTimeMins)
		})
	}
}

func TestManagerCaches_InitialState(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	assert.NotNil(t, manager.postgresCache)
	assert.NotNil(t, manager.redisCache)
	assert.Empty(t, manager.postgresCache)
	assert.Empty(t, manager.redisCache)
	assert.False(t, manager.closed)
}

func TestPostgresInfraKey_SpecialCharacters(t *testing.T) {
	t.Parallel()

	cfg := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://user:p@ss%20word@host:5432/db?sslmode=verify-full",
		PostgresReplicaDSN: "postgres://user:p@ss%20word@replica:5432/db?sslmode=verify-full",
		PostgresPrimaryDB:  "test-db_with_special",
		PostgresReplicaDB:  "test-db_with_special",
	}

	key := postgresInfraKey(cfg)
	assert.NotEmpty(t, key)
	assert.Len(t, key, 64)
}

func TestRedisInfraKey_SpecialCharactersInPassword(t *testing.T) {
	t.Parallel()

	cfg := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisPassword:  "p@ss:word/with!special#chars$%^&*()",
		RedisDB:        0,
	}

	key := redisInfraKey(cfg)
	assert.NotEmpty(t, key)
	assert.Len(t, key, 64)
}

func TestClose_AfterPartialCachePopulation(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	manager.postgresCache["test-key"] = &cachedPostgresConnection{conn: nil}
	manager.redisCache["test-key"] = &cachedRedisConnection{conn: nil}

	err = manager.Close()
	require.NoError(t, err)

	assert.True(t, manager.closed)
	assert.Empty(t, manager.postgresCache)
	assert.Empty(t, manager.redisCache)
}

func TestGetPostgresConnection_CacheHitAfterPopulation(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	cfg := defaultTestConfig()
	infraKey := postgresInfraKey(cfg)

	postgresClient := &libPostgres.Client{}
	manager.postgresCache[infraKey] = &cachedPostgresConnection{conn: postgresClient}

	var created atomic.Int32
	manager.postgresClientFactory = func(_ libPostgres.Config) (*libPostgres.Client, error) {
		created.Add(1)
		return &libPostgres.Client{}, nil
	}

	conn, getErr := manager.GetPostgresConnection(context.Background())

	require.NoError(t, getErr)
	require.NotNil(t, conn)
	assert.Same(t, postgresClient, conn.Connection())
	assert.Equal(t, int32(0), created.Load(), "cache hit should skip connection creation")
}

func TestGetRedisConnection_CacheHitAfterPopulation(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: defaultTestConfig()}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	cfg := defaultTestConfig()
	infraKey := redisInfraKey(cfg)

	redisClient := testutil.NewRedisClientWithMock(nil)
	manager.redisCache[infraKey] = &cachedRedisConnection{conn: redisClient}

	var created atomic.Int32
	manager.redisClientFactory = func(_ context.Context, _ libRedis.Config) (*libRedis.Client, error) {
		created.Add(1)
		return testutil.NewRedisClientWithMock(nil), nil
	}

	conn, getErr := manager.GetRedisConnection(context.Background())

	require.NoError(t, getErr)
	require.NotNil(t, conn)
	assert.Same(t, redisClient, conn.Connection())
	assert.Equal(t, int32(0), created.Load(), "cache hit should skip connection creation")
}

func TestRedisInfraKey_EmptyPassword(t *testing.T) {
	t.Parallel()

	cfg := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisPassword:  "",
		RedisDB:        0,
	}

	key := redisInfraKey(cfg)
	assert.NotEmpty(t, key)
	assert.Len(t, key, 64)
}

func TestRedisDefaults(t *testing.T) {
	t.Parallel()

	t.Run("default redis read timeout", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 3*time.Second, defaultRedisReadTimeout)
	})

	t.Run("default redis write timeout", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 3*time.Second, defaultRedisWriteTimeout)
	})

	t.Run("default redis dial timeout", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 5*time.Second, defaultRedisDialTimeout)
	})

	t.Run("default redis pool size", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 10, defaultRedisPoolSize)
	})

	t.Run("default redis min idle conns", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 2, defaultRedisMinIdleConns)
	})
}

func TestRedisInfraKey_IdenticalExceptProtocol(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisPassword:  "secret",
		RedisDB:        0,
		RedisProtocol:  2,
	}

	cfg2 := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisPassword:  "secret",
		RedisDB:        0,
		RedisProtocol:  3,
	}

	key1 := redisInfraKey(cfg1)
	key2 := redisInfraKey(cfg2)

	assert.NotEqual(t, key1, key2, "different protocols should produce different keys")
}

func TestRedisInfraKey_IdenticalExceptDB(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisPassword:  "secret",
		RedisDB:        0,
	}

	cfg2 := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisPassword:  "secret",
		RedisDB:        1,
	}

	key1 := redisInfraKey(cfg1)
	key2 := redisInfraKey(cfg2)

	assert.NotEqual(t, key1, key2, "different DB numbers should produce different keys")
}

func TestPostgresInfraKey_IdenticalExceptReplicaDSN(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://user:pass@host:5432/db",
		PostgresReplicaDSN: "",
		PostgresPrimaryDB:  "db1",
		PostgresReplicaDB:  "db1",
	}

	cfg2 := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://user:pass@host:5432/db",
		PostgresReplicaDSN: "postgres://user:pass@replica:5432/db",
		PostgresPrimaryDB:  "db1",
		PostgresReplicaDB:  "db1",
	}

	key1 := postgresInfraKey(cfg1)
	key2 := postgresInfraKey(cfg2)

	assert.NotEqual(t, key1, key2, "different replica DSNs should produce different keys")
}

func TestRedisInfraKey_SingleAddress(t *testing.T) {
	t.Parallel()

	cfg := &ports.TenantConfig{
		RedisAddresses: []string{"redis:6379"},
		RedisPassword:  "secret",
		RedisDB:        0,
	}

	key := redisInfraKey(cfg)
	assert.NotEmpty(t, key)
	assert.Len(t, key, 64)
}

func TestRedisInfraKey_MultipleAddressesSorted(t *testing.T) {
	t.Parallel()

	cfg1 := &ports.TenantConfig{
		RedisAddresses: []string{"a:6379", "b:6379", "c:6379"},
	}
	cfg2 := &ports.TenantConfig{
		RedisAddresses: []string{"c:6379", "a:6379", "b:6379"},
	}
	cfg3 := &ports.TenantConfig{
		RedisAddresses: []string{"b:6379", "c:6379", "a:6379"},
	}

	key1 := redisInfraKey(cfg1)
	key2 := redisInfraKey(cfg2)
	key3 := redisInfraKey(cfg3)

	assert.Equal(t, key1, key2)
	assert.Equal(t, key2, key3)
}

type closeErrorResolver struct {
	dbresolver.DB
	closeErr error
}

func (r *closeErrorResolver) Close() error {
	return r.closeErr
}

type errorClosingUniversalClient struct {
	*redis.Client
	closeErr error
}

func (c *errorClosingUniversalClient) Close() error {
	return c.closeErr
}

func TestPostgresInfraKey_ReplicaFallbackNormalization(t *testing.T) {
	t.Parallel()

	cfgWithoutReplica := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://user:pass@primary:5432/db",
		PostgresReplicaDSN: "",
		PostgresPrimaryDB:  "db",
		PostgresReplicaDB:  "",
	}
	cfgWithReplicaEqualPrimary := &ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://user:pass@primary:5432/db",
		PostgresReplicaDSN: "postgres://user:pass@primary:5432/db",
		PostgresPrimaryDB:  "db",
		PostgresReplicaDB:  "db",
	}

	assert.Equal(t, postgresInfraKey(cfgWithoutReplica), postgresInfraKey(cfgWithReplicaEqualPrimary))
}

func TestRedisInfraKey_DefaultOptionNormalization(t *testing.T) {
	t.Parallel()

	cfgWithZeros := &ports.TenantConfig{
		RedisAddresses:    []string{"redis:6379"},
		RedisPassword:     "secret",
		RedisDB:           0,
		RedisProtocol:     3,
		RedisReadTimeout:  0,
		RedisWriteTimeout: 0,
		RedisDialTimeout:  0,
		RedisPoolSize:     0,
		RedisMinIdleConns: 0,
	}
	cfgWithDefaults := &ports.TenantConfig{
		RedisAddresses:    []string{"redis:6379"},
		RedisPassword:     "secret",
		RedisDB:           0,
		RedisProtocol:     3,
		RedisReadTimeout:  defaultRedisReadTimeout,
		RedisWriteTimeout: defaultRedisWriteTimeout,
		RedisDialTimeout:  defaultRedisDialTimeout,
		RedisPoolSize:     defaultRedisPoolSize,
		RedisMinIdleConns: defaultRedisMinIdleConns,
	}

	assert.Equal(t, redisInfraKey(cfgWithZeros), redisInfraKey(cfgWithDefaults))
}

func TestRedisTopology(t *testing.T) {
	t.Parallel()

	t.Run("sentinel topology", func(t *testing.T) {
		t.Parallel()

		cfg := &ports.TenantConfig{
			RedisAddresses:  []string{"sentinel-a:26379", "sentinel-b:26379"},
			RedisMasterName: "mymaster",
		}

		topology, err := redisTopology(cfg)
		require.NoError(t, err)
		require.NotNil(t, topology.Sentinel)
		assert.Equal(t, "mymaster", topology.Sentinel.MasterName)
		assert.Equal(t, []string{"sentinel-a:26379", "sentinel-b:26379"}, topology.Sentinel.Addresses)
		assert.Nil(t, topology.Standalone)
		assert.Nil(t, topology.Cluster)
	})

	t.Run("cluster topology", func(t *testing.T) {
		t.Parallel()

		cfg := &ports.TenantConfig{RedisAddresses: []string{"node-a:6379", "node-b:6379"}}

		topology, err := redisTopology(cfg)
		require.NoError(t, err)
		require.NotNil(t, topology.Cluster)
		assert.Equal(t, []string{"node-a:6379", "node-b:6379"}, topology.Cluster.Addresses)
		assert.Nil(t, topology.Standalone)
		assert.Nil(t, topology.Sentinel)
	})

	t.Run("standalone topology with trimming", func(t *testing.T) {
		t.Parallel()

		cfg := &ports.TenantConfig{RedisAddresses: []string{"  redis:6379  ", ""}}

		topology, err := redisTopology(cfg)
		require.NoError(t, err)
		require.NotNil(t, topology.Standalone)
		assert.Equal(t, "redis:6379", topology.Standalone.Address)
		assert.Nil(t, topology.Sentinel)
		assert.Nil(t, topology.Cluster)
	})

	t.Run("empty addresses fails closed", func(t *testing.T) {
		t.Parallel()

		cfg := &ports.TenantConfig{RedisAddresses: []string{"", "   "}}

		topology, err := redisTopology(cfg)
		require.Error(t, err)
		assert.Equal(t, libRedis.Topology{}, topology)
		assert.ErrorIs(t, err, ErrRedisAddressesRequired)
	})

	t.Run("sentinel topology requires non-empty addresses", func(t *testing.T) {
		t.Parallel()

		cfg := &ports.TenantConfig{
			RedisAddresses:  []string{"", "   "},
			RedisMasterName: "mymaster",
		}

		topology, err := redisTopology(cfg)
		require.Error(t, err)
		assert.Equal(t, libRedis.Topology{}, topology)
		assert.ErrorIs(t, err, ErrRedisAddressesRequired)
	})
}

func TestGetRedisConnection_EmptyAddressesReturnsError(t *testing.T) {
	t.Parallel()

	mockConfig := &mockConfigurationPort{config: &ports.TenantConfig{RedisAddresses: []string{"", " "}}}
	manager, err := NewTenantConnectionManager(mockConfig, 25, 5, 30, 5)
	require.NoError(t, err)

	conn, err := manager.GetRedisConnection(context.Background())

	require.Error(t, err)
	assert.Nil(t, conn)
	assert.ErrorIs(t, err, ErrRedisAddressesRequired)
	assert.Contains(t, err.Error(), "invalid redis tenant config")
}

func TestBeginTx_ResolverError(t *testing.T) {
	t.Parallel()

	cfg := defaultTestConfig()
	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: cfg}, 25, 5, 30, 5)
	require.NoError(t, err)

	manager.postgresCache[postgresInfraKey(cfg)] = &cachedPostgresConnection{conn: &libPostgres.Client{}}

	tx, err := manager.BeginTx(context.Background())

	require.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "failed to get database connection")
}

func TestGetReplicaDB_ResolverError(t *testing.T) {
	t.Parallel()

	cfg := defaultTestConfig()
	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: cfg}, 25, 5, 30, 5)
	require.NoError(t, err)

	manager.postgresCache[postgresInfraKey(cfg)] = &cachedPostgresConnection{conn: &libPostgres.Client{}}

	db, err := manager.GetReplicaDB(context.Background())

	require.Error(t, err)
	assert.Nil(t, db)
	assert.Contains(t, err.Error(), "failed to get database connection")
}

func TestGetPostgresConnection_SuccessCachesConnection(t *testing.T) {
	t.Parallel()

	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: defaultTestConfig()}, 25, 5, 30, 5)
	require.NoError(t, err)

	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = primaryDB.Close()
	})

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(primaryDB))
	expectedClient := testutil.NewClientWithResolver(resolver)

	var factoryCalls atomic.Int32
	manager.postgresClientFactory = func(_ libPostgres.Config) (*libPostgres.Client, error) {
		factoryCalls.Add(1)
		return expectedClient, nil
	}
	manager.postgresConnector = func(_ context.Context, _ *libPostgres.Client) error {
		return nil
	}

	conn1, err1 := manager.GetPostgresConnection(context.Background())
	conn2, err2 := manager.GetPostgresConnection(context.Background())

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NotNil(t, conn1)
	require.NotNil(t, conn2)
	assert.Same(t, expectedClient, conn1.Connection())
	assert.Same(t, conn1.Connection(), conn2.Connection())
	assert.Equal(t, int32(1), factoryCalls.Load())
}

func TestGetRedisConnection_SuccessCachesConnection(t *testing.T) {
	t.Parallel()

	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: defaultTestConfig()}, 25, 5, 30, 5)
	require.NoError(t, err)

	redisInner := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() {
		_ = redisInner.Close()
	})

	expectedClient := testutil.NewRedisClientWithMock(redisInner)

	var factoryCalls atomic.Int32
	manager.redisClientFactory = func(_ context.Context, _ libRedis.Config) (*libRedis.Client, error) {
		factoryCalls.Add(1)
		return expectedClient, nil
	}

	conn1, err1 := manager.GetRedisConnection(context.Background())
	conn2, err2 := manager.GetRedisConnection(context.Background())

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NotNil(t, conn1)
	require.NotNil(t, conn2)
	assert.Same(t, expectedClient, conn1.Connection())
	assert.Same(t, conn1.Connection(), conn2.Connection())
	assert.Equal(t, int32(1), factoryCalls.Load())
}

func TestGetPostgresConnection_CanceledRequestContext_DoesNotCancelConnectionSetup(t *testing.T) {
	t.Parallel()

	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: defaultTestConfig()}, 25, 5, 30, 5)
	require.NoError(t, err)

	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = primaryDB.Close()
	})

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(primaryDB))
	expectedClient := testutil.NewClientWithResolver(resolver)

	manager.postgresClientFactory = func(_ libPostgres.Config) (*libPostgres.Client, error) {
		return expectedClient, nil
	}
	manager.postgresConnector = func(ctx context.Context, _ *libPostgres.Client) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		return nil
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()

	conn, err := manager.GetPostgresConnection(requestCtx)

	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Same(t, expectedClient, conn.Connection())
}

func TestGetPostgresConnection_ConcurrentRequests_SingleCreation(t *testing.T) {
	t.Parallel()

	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: defaultTestConfig()}, 25, 5, 30, 5)
	require.NoError(t, err)

	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = primaryDB.Close()
	})

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(primaryDB))
	expectedClient := testutil.NewClientWithResolver(resolver)

	var factoryCalls atomic.Int32
	manager.postgresClientFactory = func(_ libPostgres.Config) (*libPostgres.Client, error) {
		factoryCalls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return expectedClient, nil
	}
	manager.postgresConnector = func(_ context.Context, _ *libPostgres.Client) error {
		return nil
	}

	var (
		wg       sync.WaitGroup
		errCount atomic.Int32
		mismatch atomic.Int32
	)

	for i := 0; i < 20; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			conn, getErr := manager.GetPostgresConnection(context.Background())
			if getErr != nil {
				errCount.Add(1)
				return
			}
			defer conn.Release()

			if conn.Connection() != expectedClient {
				mismatch.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(0), errCount.Load())
	assert.Equal(t, int32(0), mismatch.Load())
	assert.Equal(t, int32(1), factoryCalls.Load())
}

func TestGetRedisConnection_ConcurrentRequests_SingleCreation(t *testing.T) {
	t.Parallel()

	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: defaultTestConfig()}, 25, 5, 30, 5)
	require.NoError(t, err)

	redisInner := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() {
		_ = redisInner.Close()
	})

	expectedClient := testutil.NewRedisClientWithMock(redisInner)

	var factoryCalls atomic.Int32
	manager.redisClientFactory = func(_ context.Context, _ libRedis.Config) (*libRedis.Client, error) {
		factoryCalls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return expectedClient, nil
	}

	var (
		wg       sync.WaitGroup
		errCount atomic.Int32
		mismatch atomic.Int32
	)

	for i := 0; i < 20; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			conn, getErr := manager.GetRedisConnection(context.Background())
			if getErr != nil {
				errCount.Add(1)
				return
			}
			defer conn.Release()

			if conn.Connection() != expectedClient {
				mismatch.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(0), errCount.Load())
	assert.Equal(t, int32(0), mismatch.Load())
	assert.Equal(t, int32(1), factoryCalls.Load())
}

func TestClose_AggregatesConnectionCloseErrors(t *testing.T) {
	t.Parallel()

	errPostgresClose := errors.New("postgres resolver close failed")
	errRedisClose := errors.New("redis close failed")

	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	resolver := &closeErrorResolver{
		DB:       dbresolver.New(dbresolver.WithPrimaryDBs(db)),
		closeErr: errPostgresClose,
	}
	postgresClient := testutil.NewClientWithResolver(resolver)

	redisInner := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() {
		_ = redisInner.Close()
	})

	redisClient := testutil.NewRedisClientWithMock(&errorClosingUniversalClient{
		Client:   redisInner,
		closeErr: errRedisClose,
	})

	manager, err := NewTenantConnectionManager(&mockConfigurationPort{config: defaultTestConfig()}, 25, 5, 30, 5)
	require.NoError(t, err)

	manager.postgresCache["pg"] = &cachedPostgresConnection{conn: postgresClient}
	manager.redisCache["redis"] = &cachedRedisConnection{conn: redisClient}

	err = manager.Close()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCloseConnections)
	assert.ErrorIs(t, err, errPostgresClose)
	assert.ErrorIs(t, err, errRedisClose)
	assert.Contains(t, err.Error(), "close postgres")
	assert.Contains(t, err.Error(), "close redis")
}

func TestIntOrDefault(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 10, intOrDefault(10, 1))
	assert.Equal(t, 1, intOrDefault(0, 1))
	assert.Equal(t, 1, intOrDefault(-2, 1))
}

func TestDurationOrDefault(t *testing.T) {
	t.Parallel()

	defaultValue := 2 * time.Second

	assert.Equal(t, 10*time.Second, durationOrDefault(10*time.Second, defaultValue))
	assert.Equal(t, defaultValue, durationOrDefault(0, defaultValue))
	assert.Equal(t, defaultValue, durationOrDefault(-1*time.Second, defaultValue))
}

func TestNewConnectionSetupContext_PreservesValuesWithoutCancellation(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancel(context.WithValue(context.Background(), setupContextKey{}, "trace-123"))
	cancelParent()

	setupCtx, cancelSetup := newConnectionSetupContext(parent)
	defer cancelSetup()

	assert.Equal(t, "trace-123", setupCtx.Value(setupContextKey{}))
	assert.NoError(t, setupCtx.Err(), "setup context must not inherit caller cancellation")
}

func TestApplySQLPoolSettings_AppliesOnlyNonNilEntries(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	applySQLPoolSettings([]*sql.DB{db, nil}, time.Minute, 30*time.Second)
}
