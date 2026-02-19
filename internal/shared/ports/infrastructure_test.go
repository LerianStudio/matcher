//go:build unit

// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"
	libRedis "github.com/LerianStudio/lib-uncommons/v2/uncommons/redis"
)

var (
	_ ConfigurationPort      = (*mockConfigurationPort)(nil)
	_ InfrastructureProvider = (*mockInfrastructureProvider)(nil)
)

type mockConfigurationPort struct{}

func (m *mockConfigurationPort) GetTenantConfig(
	_ context.Context,
	_ string,
) (*TenantConfig, error) {
	return nil, nil
}

type mockInfrastructureProvider struct{}

func (m *mockInfrastructureProvider) GetPostgresConnection(
	_ context.Context,
) (*libPostgres.Client, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetRedisConnection(
	_ context.Context,
) (*libRedis.Client, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) BeginTx(_ context.Context) (*sql.Tx, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

func TestTenantConfig_FieldAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		config         TenantConfig
		expectedConfig TenantConfig
	}{
		{
			name: "all fields populated",
			config: TenantConfig{
				PostgresPrimaryDSN: "postgres://user:pass@primary:5432/db",
				PostgresReplicaDSN: "postgres://user:pass@replica:5432/db",
				PostgresPrimaryDB:  "matcher_primary",
				PostgresReplicaDB:  "matcher_replica",
				RedisAddresses:     []string{"redis1:6379", "redis2:6379"},
				RedisPassword:      "secret",
				RedisDB:            0,
				RedisMasterName:    "mymaster",
				RedisProtocol:      3,
				RedisUseTLS:        true,
				RedisCACert:        "/path/to/ca.crt",
			},
			expectedConfig: TenantConfig{
				PostgresPrimaryDSN: "postgres://user:pass@primary:5432/db",
				PostgresReplicaDSN: "postgres://user:pass@replica:5432/db",
				PostgresPrimaryDB:  "matcher_primary",
				PostgresReplicaDB:  "matcher_replica",
				RedisAddresses:     []string{"redis1:6379", "redis2:6379"},
				RedisPassword:      "secret",
				RedisDB:            0,
				RedisMasterName:    "mymaster",
				RedisProtocol:      3,
				RedisUseTLS:        true,
				RedisCACert:        "/path/to/ca.crt",
			},
		},
		{
			name: "minimal configuration",
			config: TenantConfig{
				PostgresPrimaryDSN: "postgres://localhost:5432/db",
			},
			expectedConfig: TenantConfig{
				PostgresPrimaryDSN: "postgres://localhost:5432/db",
			},
		},
		{
			name:           "zero value configuration",
			config:         TenantConfig{},
			expectedConfig: TenantConfig{},
		},
		{
			name: "redis sentinel configuration",
			config: TenantConfig{
				RedisAddresses:  []string{"sentinel1:26379", "sentinel2:26379", "sentinel3:26379"},
				RedisMasterName: "mymaster",
				RedisPassword:   "sentinel-pass",
				RedisDB:         1,
				RedisProtocol:   3,
				RedisUseTLS:     false,
			},
			expectedConfig: TenantConfig{
				RedisAddresses:  []string{"sentinel1:26379", "sentinel2:26379", "sentinel3:26379"},
				RedisMasterName: "mymaster",
				RedisPassword:   "sentinel-pass",
				RedisDB:         1,
				RedisProtocol:   3,
				RedisUseTLS:     false,
			},
		},
		{
			name: "redis cluster configuration",
			config: TenantConfig{
				RedisAddresses: []string{"node1:6379", "node2:6379", "node3:6379"},
				RedisPassword:  "cluster-pass",
				RedisDB:        0,
				RedisProtocol:  3,
				RedisUseTLS:    true,
				RedisCACert:    "/etc/redis/ca.pem",
			},
			expectedConfig: TenantConfig{
				RedisAddresses: []string{"node1:6379", "node2:6379", "node3:6379"},
				RedisPassword:  "cluster-pass",
				RedisDB:        0,
				RedisProtocol:  3,
				RedisUseTLS:    true,
				RedisCACert:    "/etc/redis/ca.pem",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expectedConfig, tt.config)
		})
	}
}

func TestTenantConfig_PostgresFields(t *testing.T) {
	t.Parallel()

	config := TenantConfig{
		PostgresPrimaryDSN: "postgres://admin:password@primary.db.example.com:5432/matcher?sslmode=require",
		PostgresReplicaDSN: "postgres://readonly:password@replica.db.example.com:5432/matcher?sslmode=require",
		PostgresPrimaryDB:  "matcher",
		PostgresReplicaDB:  "matcher",
	}

	assert.Contains(t, config.PostgresPrimaryDSN, "primary.db.example.com")
	assert.Contains(t, config.PostgresReplicaDSN, "replica.db.example.com")
	assert.Equal(t, "matcher", config.PostgresPrimaryDB)
	assert.Equal(t, "matcher", config.PostgresReplicaDB)
}

func TestTenantConfig_RedisFields(t *testing.T) {
	t.Parallel()

	config := TenantConfig{
		RedisAddresses:  []string{"redis-0:6379", "redis-1:6379", "redis-2:6379"},
		RedisPassword:   "redis-secret",
		RedisDB:         2,
		RedisMasterName: "redis-master",
		RedisProtocol:   3,
		RedisUseTLS:     true,
		RedisCACert:     "/etc/ssl/redis-ca.crt",
	}

	assert.Len(t, config.RedisAddresses, 3)
	assert.Equal(t, "redis-secret", config.RedisPassword)
	assert.Equal(t, 2, config.RedisDB)
	assert.Equal(t, "redis-master", config.RedisMasterName)
	assert.Equal(t, 3, config.RedisProtocol)
	assert.True(t, config.RedisUseTLS)
	assert.Equal(t, "/etc/ssl/redis-ca.crt", config.RedisCACert)
}

func TestConfigurationPortInterface(t *testing.T) {
	t.Parallel()

	var port ConfigurationPort = &mockConfigurationPort{}

	config, err := port.GetTenantConfig(context.Background(), "test-tenant")
	require.NoError(t, err)
	assert.Nil(t, config)
}

func TestInfrastructureProviderInterface(t *testing.T) {
	t.Parallel()

	var provider InfrastructureProvider = &mockInfrastructureProvider{}

	pgConn, err := provider.GetPostgresConnection(context.Background())
	require.NoError(t, err)
	assert.Nil(t, pgConn)

	redisConn, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	assert.Nil(t, redisConn)

	tx, err := provider.BeginTx(context.Background())
	require.NoError(t, err)
	assert.Nil(t, tx)

	replicaDB, err := provider.GetReplicaDB(context.Background())
	require.NoError(t, err)
	assert.Nil(t, replicaDB)
}
