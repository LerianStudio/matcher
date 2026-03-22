//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

// --- buildPostgresDSN ---

func TestBuildPostgresDSN_BasicFields(t *testing.T) {
	t.Parallel()

	dsn := buildPostgresDSN("localhost", "5432", "user", "pass", "mydb", "disable", 10)

	assert.Contains(t, dsn, "host=localhost")
	assert.Contains(t, dsn, "port=5432")
	assert.Contains(t, dsn, "user=user")
	assert.Contains(t, dsn, "dbname=mydb")
	assert.Contains(t, dsn, "sslmode=disable")
	assert.Contains(t, dsn, "connect_timeout=10")
}

func TestBuildPostgresDSN_PasswordQuoted(t *testing.T) {
	t.Parallel()

	dsn := buildPostgresDSN("localhost", "5432", "user", "p@ss", "mydb", "disable", 5)

	assert.Contains(t, dsn, "password='p@ss'")
}

// --- escapePGValue ---

func TestEscapePGValue_NoSpecialChars(t *testing.T) {
	t.Parallel()

	result := escapePGValue("simple")

	assert.Equal(t, "simple", result)
}

func TestEscapePGValue_SingleQuotes(t *testing.T) {
	t.Parallel()

	result := escapePGValue("it's")

	assert.Equal(t, `it\'s`, result)
}

func TestEscapePGValue_Backslashes(t *testing.T) {
	t.Parallel()

	result := escapePGValue(`path\to\file`)

	assert.Equal(t, `path\\to\\file`, result)
}

func TestEscapePGValue_Mixed(t *testing.T) {
	t.Parallel()

	result := escapePGValue(`it's a\path`)

	assert.Equal(t, `it\'s a\\path`, result)
}

// --- coalesce ---

func TestCoalesce_ReturnsFirstNonEmpty(t *testing.T) {
	t.Parallel()

	result := coalesce("", "", "hello", "world")

	assert.Equal(t, "hello", result)
}

func TestCoalesce_AllEmpty(t *testing.T) {
	t.Parallel()

	result := coalesce("", "", "")

	assert.Equal(t, "", result)
}

func TestCoalesce_FirstIsNonEmpty(t *testing.T) {
	t.Parallel()

	result := coalesce("first", "second")

	assert.Equal(t, "first", result)
}

func TestCoalesce_NoArgs(t *testing.T) {
	t.Parallel()

	result := coalesce()

	assert.Equal(t, "", result)
}

// --- snapString ---

func TestSnapString_KeyExists(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": "val"})

	result := snapString(snap, "my.key", "fallback")

	assert.Equal(t, "val", result)
}

func TestSnapString_KeyMissing(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{})

	result := snapString(snap, "missing", "fallback")

	assert.Equal(t, "fallback", result)
}

func TestSnapString_NonStringValue(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": 42})

	result := snapString(snap, "my.key", "fallback")

	assert.Equal(t, "42", result)
}

// --- snapInt ---

func TestSnapInt_IntValue(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": 42})

	result := snapInt(snap, "my.key", 0)

	assert.Equal(t, 42, result)
}

func TestSnapInt_Int64Value(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": int64(99)})

	result := snapInt(snap, "my.key", 0)

	assert.Equal(t, 99, result)
}

func TestSnapInt_Float64Value(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": float64(77)})

	result := snapInt(snap, "my.key", 0)

	assert.Equal(t, 77, result)
}

func TestSnapInt_StringValue(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": "123"})

	result := snapInt(snap, "my.key", 0)

	assert.Equal(t, 123, result)
}

func TestSnapInt_InvalidStringFallback(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": "notanumber"})

	result := snapInt(snap, "my.key", 99)

	assert.Equal(t, 99, result)
}

func TestSnapInt_MissingKeyFallback(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{})

	result := snapInt(snap, "missing", 42)

	assert.Equal(t, 42, result)
}

func TestSnapInt_UnsupportedTypeFallback(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": []int{1, 2}})

	result := snapInt(snap, "my.key", 10)

	assert.Equal(t, 10, result)
}

// --- snapBool ---

func TestSnapBool_BoolValue(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": true})

	result := snapBool(snap, "my.key", false)

	assert.True(t, result)
}

func TestSnapBool_StringTrue(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": "true"})

	result := snapBool(snap, "my.key", false)

	assert.True(t, result)
}

func TestSnapBool_String1(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": "1"})

	result := snapBool(snap, "my.key", false)

	assert.True(t, result)
}

func TestSnapBool_StringFalse(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": "false"})

	result := snapBool(snap, "my.key", true)

	assert.False(t, result)
}

func TestSnapBool_MissingKeyFallback(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{})

	result := snapBool(snap, "missing", true)

	assert.True(t, result)
}

func TestSnapBool_UnsupportedTypeFallback(t *testing.T) {
	t.Parallel()

	snap := testSnapshot(map[string]any{"my.key": 42})

	result := snapBool(snap, "my.key", true)

	assert.True(t, result)
}

// --- extractPostgresConfig ---

func TestExtractPostgresConfig_Defaults(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{})

	cfg := factory.extractPostgresConfig(snap)

	assert.Equal(t, "localhost", cfg.PrimaryHost)
	assert.Equal(t, "5432", cfg.PrimaryPort)
	assert.Equal(t, "matcher", cfg.PrimaryUser)
	assert.Equal(t, "matcher", cfg.PrimaryDB)
	assert.Equal(t, "disable", cfg.PrimarySSLMode)
	assert.Equal(t, "", cfg.ReplicaHost)
}

func TestExtractPostgresConfig_OverrideValues(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{
		"postgres.primary_host": "dbhost",
		"postgres.primary_port": "5433",
	})

	cfg := factory.extractPostgresConfig(snap)

	assert.Equal(t, "dbhost", cfg.PrimaryHost)
	assert.Equal(t, "5433", cfg.PrimaryPort)
}

// --- extractRedisConfig ---

func TestExtractRedisConfig_Defaults(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{})

	cfg := factory.extractRedisConfig(snap)

	assert.Equal(t, "localhost:6379", cfg.Host)
	assert.Equal(t, "", cfg.MasterName)
	assert.Equal(t, "", cfg.Password)
	assert.False(t, cfg.TLS)
}

func TestExtractRedisConfig_OverrideValues(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{
		"redis.host":        "redis-host:6380",
		"redis.master_name": "mymaster",
		"redis.tls":         true,
	})

	cfg := factory.extractRedisConfig(snap)

	assert.Equal(t, "redis-host:6380", cfg.Host)
	assert.Equal(t, "mymaster", cfg.MasterName)
	assert.True(t, cfg.TLS)
}

// --- extractRabbitMQConfig ---

func TestExtractRabbitMQConfig_Defaults(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{})

	cfg := factory.extractRabbitMQConfig(snap)

	assert.Equal(t, "amqp", cfg.URI)
	assert.Equal(t, "localhost", cfg.Host)
	assert.Equal(t, "5672", cfg.Port)
	assert.Equal(t, "guest", cfg.User)
	assert.Equal(t, "guest", cfg.Password)
	assert.Equal(t, "/", cfg.VHost)
}

func TestExtractRabbitMQConfig_OverrideValues(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{
		"rabbitmq.host": "rabbitmq-server",
		"rabbitmq.port": "5673",
		"rabbitmq.user": "admin",
	})

	cfg := factory.extractRabbitMQConfig(snap)

	assert.Equal(t, "rabbitmq-server", cfg.Host)
	assert.Equal(t, "5673", cfg.Port)
	assert.Equal(t, "admin", cfg.User)
}

// --- buildRabbitMQDSN ---

func TestBuildRabbitMQDSN_BasicConnection(t *testing.T) {
	t.Parallel()

	cfg := rabbitMQConfigSnapshot{
		URI:      "amqp",
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
		VHost:    "/",
	}

	dsn := buildRabbitMQDSN(cfg)

	assert.Contains(t, dsn, "amqp://")
	assert.Contains(t, dsn, "guest")
	assert.Contains(t, dsn, "localhost:5672")
}

func TestBuildRabbitMQDSN_NoPassword(t *testing.T) {
	t.Parallel()

	cfg := rabbitMQConfigSnapshot{
		URI:      "amqp",
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "",
		VHost:    "/",
	}

	dsn := buildRabbitMQDSN(cfg)

	assert.Contains(t, dsn, "amqp://guest@")
}

func TestBuildRabbitMQDSN_CustomVHost(t *testing.T) {
	t.Parallel()

	cfg := rabbitMQConfigSnapshot{
		URI:      "amqp",
		Host:     "localhost",
		Port:     "5672",
		User:     "guest",
		Password: "guest",
		VHost:    "myvhost",
	}

	dsn := buildRabbitMQDSN(cfg)

	assert.Contains(t, dsn, "myvhost")
}

// --- buildLibRedisConfig ---

func TestBuildLibRedisConfig_StandaloneMode(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host:           "localhost:6379",
		Password:       "secret",
		DB:             1,
		Protocol:       3,
		PoolSize:       10,
		MinIdleConn:    2,
		ReadTimeoutMs:  500,
		WriteTimeoutMs: 500,
		DialTimeoutMs:  1000,
	}

	redisCfg := buildLibRedisConfig(cfg, "development", &libLog.NopLogger{})

	require.NotNil(t, redisCfg.Topology.Standalone)
	assert.Equal(t, "localhost:6379", redisCfg.Topology.Standalone.Address)
	assert.Nil(t, redisCfg.Topology.Sentinel)
	assert.Nil(t, redisCfg.Topology.Cluster)
}

func TestBuildLibRedisConfig_SentinelMode(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host:       "sentinel1:26379,sentinel2:26379",
		MasterName: "mymaster",
	}

	redisCfg := buildLibRedisConfig(cfg, "development", &libLog.NopLogger{})

	require.NotNil(t, redisCfg.Topology.Sentinel)
	assert.Equal(t, "mymaster", redisCfg.Topology.Sentinel.MasterName)
	assert.Len(t, redisCfg.Topology.Sentinel.Addresses, 2)
}

func TestBuildLibRedisConfig_ClusterMode(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host: "node1:6379,node2:6379,node3:6379",
	}

	redisCfg := buildLibRedisConfig(cfg, "development", &libLog.NopLogger{})

	require.NotNil(t, redisCfg.Topology.Cluster)
	assert.Len(t, redisCfg.Topology.Cluster.Addresses, 3)
}

func TestBuildLibRedisConfig_TLSEnabled(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host:   "localhost:6379",
		TLS:    true,
		CACert: "base64cert",
	}

	redisCfg := buildLibRedisConfig(cfg, "development", &libLog.NopLogger{})

	require.NotNil(t, redisCfg.TLS)
	assert.Equal(t, "base64cert", redisCfg.TLS.CACertBase64)
}

func TestBuildLibRedisConfig_TLSDisabled(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host: "localhost:6379",
		TLS:  false,
	}

	redisCfg := buildLibRedisConfig(cfg, "development", &libLog.NopLogger{})

	assert.Nil(t, redisCfg.TLS)
}

// --- objectStorageCloser ---

func TestObjectStorageCloser_CloseIsNoop(t *testing.T) {
	t.Parallel()

	closer := &objectStorageCloser{}

	err := closer.Close()

	assert.NoError(t, err)
}

// --- buildObjectStorageClient ---

func TestBuildObjectStorageClient_EmptyEndpoint_ReturnsNilNil(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{
		"object_storage.endpoint": "",
		"object_storage.bucket":   "mybucket",
	})

	client, err := factory.buildObjectStorageClient(context.Background(), snap)

	assert.Nil(t, client)
	assert.NoError(t, err)
}

func TestBuildObjectStorageClient_EmptyBucket_ReturnsNilNil(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{
		"object_storage.endpoint": "https://s3.example.com",
		"object_storage.bucket":   "",
	})

	client, err := factory.buildObjectStorageClient(context.Background(), snap)

	assert.Nil(t, client)
	assert.NoError(t, err)
}

// --- buildRabbitMQConnection ---

func TestBuildRabbitMQConnection_ReturnsConnection(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{
		"rabbitmq.host":     "localhost",
		"rabbitmq.port":     "5672",
		"rabbitmq.user":     "guest",
		"rabbitmq.password": "guest",
	})

	conn := factory.buildRabbitMQConnection(context.Background(), snap, &libLog.NopLogger{})

	require.NotNil(t, conn)
	assert.Equal(t, "localhost", conn.Host)
	assert.Equal(t, "5672", conn.Port)
	assert.Equal(t, "guest", conn.User)
	assert.Equal(t, "guest", conn.Pass)
}

// --- applySQLPoolSettings ---

func TestApplySQLPoolSettings_NilDBs(t *testing.T) {
	t.Parallel()

	// Must not panic with nil slice.
	assert.NotPanics(t, func() {
		applySQLPoolSettings(nil, 0, 0)
	})
}

// helper from systemplane_factory_test.go:
// testSnapshot and testBootstrapConfig are defined there and available in
// this package's test files since they share package bootstrap.
