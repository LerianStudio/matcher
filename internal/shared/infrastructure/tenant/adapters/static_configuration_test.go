//go:build unit

package adapters

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestNewStaticConfigurationAdapter(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		PostgresReplicaDSN: "postgres://replica:5432/db",
		RedisAddresses:     []string{"redis:6379"},
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	require.NotNil(t, adapter)
	assert.Equal(t, cfg, adapter.config)
}

func TestGetTenantConfig_ReturnsCopy(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		PostgresReplicaDSN: "postgres://replica:5432/db",
		PostgresPrimaryDB:  "testdb",
		RedisAddresses:     []string{"redis:6379"},
		RedisDB:            1,
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	result1, err := adapter.GetTenantConfig(context.Background(), "tenant-1")
	require.NoError(t, err)
	require.NotNil(t, result1)

	result2, err := adapter.GetTenantConfig(context.Background(), "tenant-2")
	require.NoError(t, err)
	require.NotNil(t, result2)

	assert.NotSame(t, result1, result2, "each call should return a new pointer")

	result1.PostgresPrimaryDSN = "modified"
	result1.PostgresPrimaryDB = "modified-db"
	result1.RedisDB = 99

	result3, err := adapter.GetTenantConfig(context.Background(), "tenant-1")
	require.NoError(t, err)

	assert.Equal(t, "postgres://primary:5432/db", result3.PostgresPrimaryDSN)
	assert.Equal(t, "testdb", result3.PostgresPrimaryDB)
	assert.Equal(t, 1, result3.RedisDB)
}

func TestGetTenantConfig_IgnoresTenantID(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		RedisAddresses:     []string{"redis:6379"},
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	result1, err := adapter.GetTenantConfig(context.Background(), "tenant-1")
	require.NoError(t, err)

	result2, err := adapter.GetTenantConfig(context.Background(), "tenant-2")
	require.NoError(t, err)

	result3, err := adapter.GetTenantConfig(context.Background(), "")
	require.NoError(t, err)

	result4, err := adapter.GetTenantConfig(context.Background(), "completely-different-tenant")
	require.NoError(t, err)

	assert.Equal(t, result1.PostgresPrimaryDSN, result2.PostgresPrimaryDSN)
	assert.Equal(t, result2.PostgresPrimaryDSN, result3.PostgresPrimaryDSN)
	assert.Equal(t, result3.PostgresPrimaryDSN, result4.PostgresPrimaryDSN)
}

func TestGetTenantConfig_NilReceiver(t *testing.T) {
	t.Parallel()

	var adapter *StaticConfigurationAdapter

	result, err := adapter.GetTenantConfig(context.Background(), "tenant-1")

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, errNilStaticConfigAdapter)
}

func TestNewStaticConfigurationAdapter_ZeroValue(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{}

	adapter := NewStaticConfigurationAdapter(cfg)

	require.NotNil(t, adapter)
	assert.Equal(t, "", adapter.config.PostgresPrimaryDSN)
	assert.Equal(t, "", adapter.config.PostgresReplicaDSN)
	assert.Nil(t, adapter.config.RedisAddresses)
	assert.Equal(t, 0, adapter.config.RedisDB)
}

func TestGetTenantConfig_WithCanceledContext(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := adapter.GetTenantConfig(ctx, "tenant-1")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "postgres://primary:5432/db", result.PostgresPrimaryDSN)
}

func TestGetTenantConfig_EmptyTenantID(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		RedisAddresses:     []string{"redis:6379"},
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	result, err := adapter.GetTenantConfig(context.Background(), "")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "postgres://primary:5432/db", result.PostgresPrimaryDSN)
}

func TestGetTenantConfig_AllFieldsPreserved(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		PostgresReplicaDSN: "postgres://replica:5432/db",
		PostgresPrimaryDB:  "primary_db",
		PostgresReplicaDB:  "replica_db",
		RedisAddresses:     []string{"redis1:6379", "redis2:6379"},
		RedisPassword:      "secret",
		RedisDB:            5,
		RedisMasterName:    "mymaster",
		RedisProtocol:      3,
		RedisUseTLS:        true,
		RedisCACert:        "cert-content",
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	result, err := adapter.GetTenantConfig(context.Background(), "tenant-1")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "postgres://primary:5432/db", result.PostgresPrimaryDSN)
	assert.Equal(t, "postgres://replica:5432/db", result.PostgresReplicaDSN)
	assert.Equal(t, "primary_db", result.PostgresPrimaryDB)
	assert.Equal(t, "replica_db", result.PostgresReplicaDB)
	assert.Equal(t, []string{"redis1:6379", "redis2:6379"}, result.RedisAddresses)
	assert.Equal(t, "secret", result.RedisPassword)
	assert.Equal(t, 5, result.RedisDB)
	assert.Equal(t, "mymaster", result.RedisMasterName)
	assert.Equal(t, 3, result.RedisProtocol)
	assert.True(t, result.RedisUseTLS)
	assert.Equal(t, "cert-content", result.RedisCACert)
}

func TestStaticConfigurationAdapter_ImplementsConfigurationPort(t *testing.T) {
	t.Parallel()

	var _ ports.ConfigurationPort = (*StaticConfigurationAdapter)(nil)
}

func TestGetTenantConfig_TimeoutFieldsPreserved(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		RedisReadTimeout:  1000,
		RedisWriteTimeout: 2000,
		RedisDialTimeout:  3000,
		RedisPoolSize:     50,
		RedisMinIdleConns: 10,
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	result, err := adapter.GetTenantConfig(context.Background(), "tenant-1")

	require.NoError(t, err)
	assert.Equal(t, cfg.RedisReadTimeout, result.RedisReadTimeout)
	assert.Equal(t, cfg.RedisWriteTimeout, result.RedisWriteTimeout)
	assert.Equal(t, cfg.RedisDialTimeout, result.RedisDialTimeout)
	assert.Equal(t, cfg.RedisPoolSize, result.RedisPoolSize)
	assert.Equal(t, cfg.RedisMinIdleConns, result.RedisMinIdleConns)
}

func TestNewStaticConfigurationAdapter_PreservesAllFields(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		PostgresReplicaDSN: "postgres://replica:5432/db",
		PostgresPrimaryDB:  "primary_db",
		PostgresReplicaDB:  "replica_db",
		RedisAddresses:     []string{"redis1:6379", "redis2:6379"},
		RedisPassword:      "secret",
		RedisDB:            5,
		RedisMasterName:    "mymaster",
		RedisProtocol:      3,
		RedisUseTLS:        true,
		RedisCACert:        "cert-content",
		RedisReadTimeout:   1000,
		RedisWriteTimeout:  2000,
		RedisDialTimeout:   3000,
		RedisPoolSize:      50,
		RedisMinIdleConns:  10,
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	assert.Equal(t, cfg, adapter.config)
}

func TestGetTenantConfig_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	cfg := ports.TenantConfig{
		PostgresPrimaryDSN: "postgres://primary:5432/db",
		RedisAddresses:     []string{"redis:6379"},
	}

	adapter := NewStaticConfigurationAdapter(cfg)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(tenantID string) {
			defer wg.Done()

			result, err := adapter.GetTenantConfig(context.Background(), tenantID)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, cfg.PostgresPrimaryDSN, result.PostgresPrimaryDSN)
		}(string(rune('A' + i%26)))
	}

	wg.Wait()
}
