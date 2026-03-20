// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface satisfaction check.
var _ ports.BundleFactory = (*MatcherBundleFactory)(nil)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// testSnapshot creates a domain.Snapshot with the given key-value pairs as
// effective configuration entries.
func testSnapshot(configs map[string]any) domain.Snapshot {
	effective := make(map[string]domain.EffectiveValue, len(configs))
	for key, val := range configs {
		effective[key] = domain.EffectiveValue{
			Key:   key,
			Value: val,
		}
	}

	return domain.Snapshot{Configs: effective}
}

// testBootstrapConfig returns a minimal BootstrapOnlyConfig suitable for unit
// tests.
func testBootstrapConfig() *BootstrapOnlyConfig {
	return &BootstrapOnlyConfig{
		EnvName:                    "development",
		ServerAddress:              ":4018",
		AuthEnabled:                false,
		TelemetryEnabled:           false,
		TelemetryServiceName:       "matcher",
		TelemetryLibraryName:       "github.com/LerianStudio/matcher",
		TelemetryServiceVersion:    "1.0.0",
		TelemetryDeploymentEnv:     "development",
		TelemetryCollectorEndpoint: "localhost:4317",
		TelemetryDBMetricsInterval: 15,
	}
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestMatcherBundleFactory_ImplementsBundleFactory(t *testing.T) {
	t.Parallel()

	var factory ports.BundleFactory = &MatcherBundleFactory{}
	assert.NotNil(t, factory)
}

func TestNewMatcherBundleFactory_NilConfig(t *testing.T) {
	t.Parallel()

	factory, err := NewMatcherBundleFactory(nil)

	require.Error(t, err)
	assert.Nil(t, factory)
	assert.ErrorIs(t, err, ErrBootstrapConfigNil)
}

func TestNewMatcherBundleFactory_Success(t *testing.T) {
	t.Parallel()

	cfg := testBootstrapConfig()
	factory, err := NewMatcherBundleFactory(cfg)

	require.NoError(t, err)
	require.NotNil(t, factory)
	assert.Equal(t, cfg, factory.bootstrapCfg)
}

// ---------------------------------------------------------------------------
// HTTP Policy extraction tests
// ---------------------------------------------------------------------------

func TestMatcherBundleFactory_BuildHTTPPolicy(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}

	snap := testSnapshot(map[string]any{
		"server.body_limit_bytes":     52428800,
		"server.cors_allowed_origins": "https://app.example.com",
		"server.cors_allowed_methods": "GET,POST",
		"server.cors_allowed_headers": "Authorization",
		"swagger.enabled":             true,
		"swagger.host":                "api.example.com",
		"swagger.schemes":             "http,https",
	})

	policy := factory.buildHTTPPolicy(snap)

	require.NotNil(t, policy)
	assert.Equal(t, 52428800, policy.BodyLimitBytes)
	assert.Equal(t, "https://app.example.com", policy.CORSAllowedOrigins)
	assert.Equal(t, "GET,POST", policy.CORSAllowedMethods)
	assert.Equal(t, "Authorization", policy.CORSAllowedHeaders)
	assert.True(t, policy.SwaggerEnabled)
	assert.Equal(t, "api.example.com", policy.SwaggerHost)
	assert.Equal(t, "http,https", policy.SwaggerSchemes)
}

func TestMatcherBundleFactory_BuildHTTPPolicy_Defaults(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}

	// Empty snapshot should use all defaults.
	snap := testSnapshot(nil)

	policy := factory.buildHTTPPolicy(snap)

	require.NotNil(t, policy)
	assert.Equal(t, defaultHTTPBodyLimitBytes, policy.BodyLimitBytes)
	assert.Equal(t, "http://localhost:3000", policy.CORSAllowedOrigins)
	assert.Equal(t, "GET,POST,PUT,PATCH,DELETE,OPTIONS", policy.CORSAllowedMethods)
	assert.Equal(t, "Origin,Content-Type,Accept,Authorization,X-Request-ID", policy.CORSAllowedHeaders)
	assert.False(t, policy.SwaggerEnabled)
	assert.Equal(t, "", policy.SwaggerHost)
	assert.Equal(t, "https", policy.SwaggerSchemes)
}

// ---------------------------------------------------------------------------
// Postgres config extraction tests
// ---------------------------------------------------------------------------

func TestMatcherBundleFactory_ExtractPostgresConfig(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}

	snap := testSnapshot(map[string]any{
		"postgres.primary_host":           "db.example.com",
		"postgres.primary_port":           "5433",
		"postgres.primary_user":           "admin",
		"postgres.primary_password":       "secret",
		"postgres.primary_db":             "mydb",
		"postgres.primary_ssl_mode":       "require",
		"postgres.replica_host":           "replica.example.com",
		"postgres.replica_port":           "5434",
		"postgres.max_open_connections":   50,
		"postgres.max_idle_connections":   10,
		"postgres.conn_max_lifetime_mins": 60,
		"postgres.connect_timeout_sec":    20,
	})

	cfg := factory.extractPostgresConfig(snap)

	assert.Equal(t, "db.example.com", cfg.PrimaryHost)
	assert.Equal(t, "5433", cfg.PrimaryPort)
	assert.Equal(t, "admin", cfg.PrimaryUser)
	assert.Equal(t, "secret", cfg.PrimaryPassword)
	assert.Equal(t, "mydb", cfg.PrimaryDB)
	assert.Equal(t, "require", cfg.PrimarySSLMode)
	assert.Equal(t, "replica.example.com", cfg.ReplicaHost)
	assert.Equal(t, "5434", cfg.ReplicaPort)
	assert.Equal(t, 50, cfg.MaxOpenConnections)
	assert.Equal(t, 10, cfg.MaxIdleConnections)
	assert.Equal(t, 60, cfg.ConnMaxLifetimeMins)
	assert.Equal(t, 20, cfg.ConnectTimeoutSec)
}

func TestMatcherBundleFactory_ExtractPostgresConfig_Defaults(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(nil)

	cfg := factory.extractPostgresConfig(snap)

	assert.Equal(t, "localhost", cfg.PrimaryHost)
	assert.Equal(t, "5432", cfg.PrimaryPort)
	assert.Equal(t, "matcher", cfg.PrimaryUser)
	assert.Equal(t, "", cfg.PrimaryPassword)
	assert.Equal(t, "matcher", cfg.PrimaryDB)
	assert.Equal(t, "disable", cfg.PrimarySSLMode)
	assert.Equal(t, "", cfg.ReplicaHost)
	assert.Equal(t, 25, cfg.MaxOpenConnections)
	assert.Equal(t, 5, cfg.MaxIdleConnections)
	assert.Equal(t, 30, cfg.ConnMaxLifetimeMins)
	assert.Equal(t, 10, cfg.ConnectTimeoutSec)
	assert.Equal(t, 30, cfg.QueryTimeoutSec)
	assert.Equal(t, "migrations", cfg.MigrationsPath)
}

// ---------------------------------------------------------------------------
// Redis config extraction tests
// ---------------------------------------------------------------------------

func TestMatcherBundleFactory_ExtractRedisConfig(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}

	snap := testSnapshot(map[string]any{
		"redis.host":            "redis.example.com:6380",
		"redis.master_name":     "mymaster",
		"redis.password":        "redis-secret",
		"redis.db":              2,
		"redis.protocol":        2,
		"redis.tls":             true,
		"redis.ca_cert":         "base64cert",
		"redis.pool_size":       20,
		"redis.min_idle_conn":   5,
		"redis.read_timeout_ms": 5000,
	})

	cfg := factory.extractRedisConfig(snap)

	assert.Equal(t, "redis.example.com:6380", cfg.Host)
	assert.Equal(t, "mymaster", cfg.MasterName)
	assert.Equal(t, "redis-secret", cfg.Password)
	assert.Equal(t, 2, cfg.DB)
	assert.Equal(t, 2, cfg.Protocol)
	assert.True(t, cfg.TLS)
	assert.Equal(t, "base64cert", cfg.CACert)
	assert.Equal(t, 20, cfg.PoolSize)
	assert.Equal(t, 5, cfg.MinIdleConn)
	assert.Equal(t, 5000, cfg.ReadTimeoutMs)
}

func TestMatcherBundleFactory_ExtractRedisConfig_Defaults(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(nil)

	cfg := factory.extractRedisConfig(snap)

	assert.Equal(t, "localhost:6379", cfg.Host)
	assert.Equal(t, "", cfg.MasterName)
	assert.Equal(t, "", cfg.Password)
	assert.Equal(t, 0, cfg.DB)
	assert.Equal(t, 3, cfg.Protocol)
	assert.False(t, cfg.TLS)
	assert.Equal(t, 10, cfg.PoolSize)
	assert.Equal(t, 2, cfg.MinIdleConn)
	assert.Equal(t, 3000, cfg.ReadTimeoutMs)
	assert.Equal(t, 3000, cfg.WriteTimeoutMs)
	assert.Equal(t, 5000, cfg.DialTimeoutMs)
}

// ---------------------------------------------------------------------------
// RabbitMQ config extraction tests
// ---------------------------------------------------------------------------

func TestMatcherBundleFactory_ExtractRabbitMQConfig(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}

	snap := testSnapshot(map[string]any{
		"rabbitmq.uri":                         "amqps",
		"rabbitmq.host":                        "mq.example.com",
		"rabbitmq.port":                        "5671",
		"rabbitmq.user":                        "producer",
		"rabbitmq.password":                    "mq-secret",
		"rabbitmq.vhost":                       "/production",
		"rabbitmq.health_url":                  "https://mq.example.com:15672",
		"rabbitmq.allow_insecure_health_check": true,
	})

	cfg := factory.extractRabbitMQConfig(snap)

	assert.Equal(t, "amqps", cfg.URI)
	assert.Equal(t, "mq.example.com", cfg.Host)
	assert.Equal(t, "5671", cfg.Port)
	assert.Equal(t, "producer", cfg.User)
	assert.Equal(t, "mq-secret", cfg.Password)
	assert.Equal(t, "/production", cfg.VHost)
	assert.Equal(t, "https://mq.example.com:15672", cfg.HealthURL)
	assert.True(t, cfg.AllowInsecureHealthCheck)
}

func TestMatcherBundleFactory_ExtractRabbitMQConfig_Defaults(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(nil)

	cfg := factory.extractRabbitMQConfig(snap)

	assert.Equal(t, "amqp", cfg.URI)
	assert.Equal(t, "localhost", cfg.Host)
	assert.Equal(t, "5672", cfg.Port)
	assert.Equal(t, "guest", cfg.User)
	assert.Equal(t, "guest", cfg.Password)
	assert.Equal(t, "/", cfg.VHost)
	assert.Equal(t, "http://localhost:15672", cfg.HealthURL)
	assert.False(t, cfg.AllowInsecureHealthCheck)
}

// ---------------------------------------------------------------------------
// Logger building tests
// ---------------------------------------------------------------------------

func TestMatcherBundleFactory_BuildLogger(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}

	snap := testSnapshot(map[string]any{
		"app.log_level": "debug",
	})

	bundle, err := factory.buildLogger(snap)

	require.NoError(t, err)
	require.NotNil(t, bundle)
	assert.Equal(t, "debug", bundle.Level)
	assert.NotNil(t, bundle.Logger)
}

func TestMatcherBundleFactory_BuildLogger_DefaultLevel(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(nil)

	bundle, err := factory.buildLogger(snap)

	require.NoError(t, err)
	require.NotNil(t, bundle)
	assert.Equal(t, "info", bundle.Level)
}

func TestMatcherBundleFactory_BuildLogger_InvalidLevel(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	snap := testSnapshot(map[string]any{
		"app.log_level": "banana",
	})

	bundle, err := factory.buildLogger(snap)

	require.NoError(t, err)
	require.NotNil(t, bundle)
	// Invalid level should fall back to "info".
	assert.Equal(t, "info", bundle.Level)
}

func TestMatcherBundleFactory_BuildLogger_ProductionEnv(t *testing.T) {
	t.Parallel()

	cfg := testBootstrapConfig()
	cfg.EnvName = "production"

	factory := &MatcherBundleFactory{bootstrapCfg: cfg}
	snap := testSnapshot(map[string]any{
		"app.log_level": "warn",
	})

	bundle, err := factory.buildLogger(snap)

	require.NoError(t, err)
	require.NotNil(t, bundle)
	assert.Equal(t, "warn", bundle.Level)
	assert.NotNil(t, bundle.Logger)
}

// ---------------------------------------------------------------------------
// DSN construction tests
// ---------------------------------------------------------------------------

func TestBuildPostgresDSN(t *testing.T) {
	t.Parallel()

	dsn := buildPostgresDSN("myhost", "5433", "admin", "pass", "mydb", "require", 15)

	assert.Equal(t, "host=myhost port=5433 user=admin password='pass' dbname=mydb sslmode=require connect_timeout=15", dsn)
}

func TestBuildPostgresDSN_SpecialCharsInPassword(t *testing.T) {
	t.Parallel()

	dsn := buildPostgresDSN("myhost", "5432", "admin", "p@ss'w\\rd", "mydb", "disable", 10)

	assert.Equal(t, `host=myhost port=5432 user=admin password='p@ss\'w\\rd' dbname=mydb sslmode=disable connect_timeout=10`, dsn)
}

func TestEscapePGValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "no special chars", input: "simple", expected: "simple"},
		{name: "single quote", input: "it's", expected: `it\'s`},
		{name: "backslash", input: `path\to`, expected: `path\\to`},
		{name: "both", input: `it's\here`, expected: `it\'s\\here`},
		{name: "empty", input: "", expected: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, escapePGValue(tt.input))
		})
	}
}

func TestBuildRabbitMQDSN_DefaultVhost(t *testing.T) {
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

	assert.Contains(t, dsn, "amqp://guest:guest@localhost:5672")
	assert.Contains(t, dsn, "%2F")
}

func TestBuildRabbitMQDSN_CustomVhost(t *testing.T) {
	t.Parallel()

	cfg := rabbitMQConfigSnapshot{
		URI:      "amqps",
		Host:     "mq.example.com",
		Port:     "5671",
		User:     "app",
		Password: "secret",
		VHost:    "/production",
	}

	dsn := buildRabbitMQDSN(cfg)

	assert.Contains(t, dsn, "amqps://app:secret@mq.example.com:5671")
	assert.Contains(t, dsn, "production")
}

func TestBuildRabbitMQDSN_EmptyPassword(t *testing.T) {
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

	assert.Contains(t, dsn, "amqp://guest@localhost:5672")
}

// ---------------------------------------------------------------------------
// Redis config builder tests
// ---------------------------------------------------------------------------

func TestBuildLibRedisConfig_Standalone(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host:           "localhost:6379",
		Password:       "test-pass",
		DB:             1,
		Protocol:       3,
		PoolSize:       15,
		MinIdleConn:    3,
		ReadTimeoutMs:  2000,
		WriteTimeoutMs: 2000,
		DialTimeoutMs:  3000,
	}

	redisCfg := buildLibRedisConfig(cfg, "development", nil)

	require.NotNil(t, redisCfg.Topology.Standalone)
	assert.Equal(t, "localhost:6379", redisCfg.Topology.Standalone.Address)
	assert.Nil(t, redisCfg.Topology.Sentinel)
	assert.Nil(t, redisCfg.Topology.Cluster)
	assert.Equal(t, 1, redisCfg.Options.DB)
	assert.Equal(t, 3, redisCfg.Options.Protocol)
	assert.Equal(t, 15, redisCfg.Options.PoolSize)
	assert.Nil(t, redisCfg.TLS)
}

func TestBuildLibRedisConfig_Sentinel(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host:       "sentinel1:26379,sentinel2:26379",
		MasterName: "mymaster",
		Password:   "pass",
	}

	redisCfg := buildLibRedisConfig(cfg, "production", nil)

	require.NotNil(t, redisCfg.Topology.Sentinel)
	assert.Equal(t, "mymaster", redisCfg.Topology.Sentinel.MasterName)
	assert.Equal(t, []string{"sentinel1:26379", "sentinel2:26379"}, redisCfg.Topology.Sentinel.Addresses)
	assert.Nil(t, redisCfg.Topology.Standalone)
	assert.Nil(t, redisCfg.Topology.Cluster)
}

func TestBuildLibRedisConfig_Cluster(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host: "node1:6379,node2:6379,node3:6379",
	}

	redisCfg := buildLibRedisConfig(cfg, "production", nil)

	require.NotNil(t, redisCfg.Topology.Cluster)
	assert.Equal(t, []string{"node1:6379", "node2:6379", "node3:6379"}, redisCfg.Topology.Cluster.Addresses)
	assert.Nil(t, redisCfg.Topology.Standalone)
	assert.Nil(t, redisCfg.Topology.Sentinel)
}

func TestBuildLibRedisConfig_WithTLS(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host:   "redis.example.com:6380",
		TLS:    true,
		CACert: "base64encodedcert",
	}

	redisCfg := buildLibRedisConfig(cfg, "production", nil)

	require.NotNil(t, redisCfg.TLS)
	assert.Equal(t, "base64encodedcert", redisCfg.TLS.CACertBase64)
}

func TestBuildLibRedisConfig_EmptyHost_Development(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host: "",
	}

	redisCfg := buildLibRedisConfig(cfg, "development", nil)

	require.NotNil(t, redisCfg.Topology.Standalone)
	assert.Equal(t, "localhost:6379", redisCfg.Topology.Standalone.Address)
}

func TestBuildLibRedisConfig_EmptyHost_Production(t *testing.T) {
	t.Parallel()

	cfg := redisConfigSnapshot{
		Host: "",
	}

	redisCfg := buildLibRedisConfig(cfg, "production", nil)

	require.NotNil(t, redisCfg.Topology.Standalone)
	assert.Equal(t, "", redisCfg.Topology.Standalone.Address)
}

// ---------------------------------------------------------------------------
// Snapshot value extraction helper tests
// ---------------------------------------------------------------------------

func TestSnapInt_Various(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		configs  map[string]any
		key      string
		fallback int
		want     int
	}{
		{
			name:     "int value",
			configs:  map[string]any{"k": 42},
			key:      "k",
			fallback: 0,
			want:     42,
		},
		{
			name:     "int64 value",
			configs:  map[string]any{"k": int64(100)},
			key:      "k",
			fallback: 0,
			want:     100,
		},
		{
			name:     "float64 value",
			configs:  map[string]any{"k": float64(3.14)},
			key:      "k",
			fallback: 0,
			want:     3,
		},
		{
			name:     "string numeric value",
			configs:  map[string]any{"k": "256"},
			key:      "k",
			fallback: 0,
			want:     256,
		},
		{
			name:     "string non-numeric value",
			configs:  map[string]any{"k": "not-a-number"},
			key:      "k",
			fallback: 99,
			want:     99,
		},
		{
			name:     "missing key uses fallback",
			configs:  nil,
			key:      "missing",
			fallback: 77,
			want:     77,
		},
		{
			name:     "unsupported type uses fallback",
			configs:  map[string]any{"k": []int{1, 2, 3}},
			key:      "k",
			fallback: 55,
			want:     55,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := testSnapshot(tt.configs)
			got := snapInt(snap, tt.key, tt.fallback)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSnapString_Various(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		configs  map[string]any
		key      string
		fallback string
		want     string
	}{
		{
			name:     "string value",
			configs:  map[string]any{"k": "hello"},
			key:      "k",
			fallback: "",
			want:     "hello",
		},
		{
			name:     "non-string value formatted",
			configs:  map[string]any{"k": 42},
			key:      "k",
			fallback: "",
			want:     "42",
		},
		{
			name:     "missing key uses fallback",
			configs:  nil,
			key:      "missing",
			fallback: "default",
			want:     "default",
		},
		{
			name:     "bool value formatted",
			configs:  map[string]any{"k": true},
			key:      "k",
			fallback: "",
			want:     "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := testSnapshot(tt.configs)
			got := snapString(snap, tt.key, tt.fallback)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSnapBool_Various(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		configs  map[string]any
		key      string
		fallback bool
		want     bool
	}{
		{
			name:     "bool true",
			configs:  map[string]any{"k": true},
			key:      "k",
			fallback: false,
			want:     true,
		},
		{
			name:     "bool false",
			configs:  map[string]any{"k": false},
			key:      "k",
			fallback: true,
			want:     false,
		},
		{
			name:     "string true",
			configs:  map[string]any{"k": "true"},
			key:      "k",
			fallback: false,
			want:     true,
		},
		{
			name:     "string TRUE case insensitive",
			configs:  map[string]any{"k": "TRUE"},
			key:      "k",
			fallback: false,
			want:     true,
		},
		{
			name:     "string 1",
			configs:  map[string]any{"k": "1"},
			key:      "k",
			fallback: false,
			want:     true,
		},
		{
			name:     "string false",
			configs:  map[string]any{"k": "false"},
			key:      "k",
			fallback: true,
			want:     false,
		},
		{
			name:     "missing key uses fallback true",
			configs:  nil,
			key:      "missing",
			fallback: true,
			want:     true,
		},
		{
			name:     "missing key uses fallback false",
			configs:  nil,
			key:      "missing",
			fallback: false,
			want:     false,
		},
		{
			name:     "unsupported type uses fallback",
			configs:  map[string]any{"k": 42},
			key:      "k",
			fallback: true,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := testSnapshot(tt.configs)
			got := snapBool(snap, tt.key, tt.fallback)

			assert.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// Coalesce helper tests
// ---------------------------------------------------------------------------

func TestCoalesce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{"first non-empty", []string{"", "second", "third"}, "second"},
		{"all empty", []string{"", "", ""}, ""},
		{"first is non-empty", []string{"first", "second"}, "first"},
		{"single value", []string{"only"}, "only"},
		{"no values", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := coalesce(tt.values...)
			assert.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// BootstrapOnlyConfig field accessibility tests
// ---------------------------------------------------------------------------

func TestBootstrapOnlyConfig_Fields(t *testing.T) {
	t.Parallel()

	cfg := &BootstrapOnlyConfig{
		EnvName:                    "production",
		ServerAddress:              ":8080",
		TLSCertFile:                "/etc/tls/cert.pem",
		TLSKeyFile:                 "/etc/tls/key.pem",
		TLSTerminatedUpstream:      true,
		TrustedProxies:             "10.0.0.0/8",
		AuthEnabled:                true,
		AuthHost:                   "auth.example.com",
		AuthTokenSecret:            "supersecret",
		TelemetryEnabled:           true,
		TelemetryServiceName:       "matcher",
		TelemetryLibraryName:       "github.com/LerianStudio/matcher",
		TelemetryServiceVersion:    "2.0.0",
		TelemetryDeploymentEnv:     "production",
		TelemetryCollectorEndpoint: "otel-collector:4317",
		TelemetryDBMetricsInterval: 30,
	}

	assert.Equal(t, "production", cfg.EnvName)
	assert.Equal(t, ":8080", cfg.ServerAddress)
	assert.Equal(t, "/etc/tls/cert.pem", cfg.TLSCertFile)
	assert.Equal(t, "/etc/tls/key.pem", cfg.TLSKeyFile)
	assert.True(t, cfg.TLSTerminatedUpstream)
	assert.Equal(t, "10.0.0.0/8", cfg.TrustedProxies)
	assert.True(t, cfg.AuthEnabled)
	assert.Equal(t, "auth.example.com", cfg.AuthHost)
	assert.Equal(t, "supersecret", cfg.AuthTokenSecret)
	assert.True(t, cfg.TelemetryEnabled)
	assert.Equal(t, "matcher", cfg.TelemetryServiceName)
	assert.Equal(t, "github.com/LerianStudio/matcher", cfg.TelemetryLibraryName)
	assert.Equal(t, "2.0.0", cfg.TelemetryServiceVersion)
	assert.Equal(t, "production", cfg.TelemetryDeploymentEnv)
	assert.Equal(t, "otel-collector:4317", cfg.TelemetryCollectorEndpoint)
	assert.Equal(t, 30, cfg.TelemetryDBMetricsInterval)
}

// ---------------------------------------------------------------------------
// Object storage config extraction tests
// ---------------------------------------------------------------------------

func TestMatcherBundleFactory_BuildObjectStorageClient_NotConfigured(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}

	// Empty endpoint -> nil client, nil error.
	snap := testSnapshot(map[string]any{
		"object_storage.endpoint": "",
		"object_storage.bucket":   "test-bucket",
	})

	client, err := factory.buildObjectStorageClient(t.Context(), snap)

	require.NoError(t, err)
	assert.Nil(t, client)
}

func TestMatcherBundleFactory_BuildObjectStorageClient_EmptyBucket(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}

	snap := testSnapshot(map[string]any{
		"object_storage.endpoint": "http://localhost:8333",
		"object_storage.bucket":   "",
	})

	client, err := factory.buildObjectStorageClient(t.Context(), snap)

	require.NoError(t, err)
	assert.Nil(t, client)
}

// ---------------------------------------------------------------------------
// diffAffectedComponents tests
// ---------------------------------------------------------------------------

// testSnapshotWithOverride creates a Snapshot where each EffectiveValue also
// carries an Override (used to test effectiveValuesEqual with overrides).
func testSnapshotWithOverride(configs map[string]any, overrides map[string]any) domain.Snapshot {
	effective := make(map[string]domain.EffectiveValue, len(configs))
	for key, val := range configs {
		ev := domain.EffectiveValue{
			Key:    key,
			Value:  val,
			Source: "test",
		}

		if ov, ok := overrides[key]; ok {
			ev.Override = ov
		}

		effective[key] = ev
	}

	return domain.Snapshot{Configs: effective}
}

func TestDiffAffectedComponents_SingleComponentChange(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{}

	prevSnap := testSnapshot(map[string]any{
		"redis.host": "localhost:6379",
	})

	newSnap := testSnapshot(map[string]any{
		"redis.host": "redis.example.com:6380",
	})

	affected := factory.diffAffectedComponents(newSnap, prevSnap)

	assert.Contains(t, affected, "redis", "redis component should be affected when redis.host changes")
	// Ensure we did NOT trigger a full rebuild: no postgres, rabbitmq, etc.
	assert.NotContains(t, affected, "postgres")
	assert.NotContains(t, affected, "rabbitmq")
}

func TestDiffAffectedComponents_NoChanges(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{}

	snap := testSnapshot(map[string]any{
		"redis.host":            "localhost:6379",
		"postgres.primary_host": "localhost",
	})

	affected := factory.diffAffectedComponents(snap, snap)

	assert.Empty(t, affected, "identical snapshots should produce empty affected set")
}

func TestDiffAffectedComponents_CrossCuttingKeyForcesFullRebuild(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{}

	// Use a key that is NOT in keyComponentMap at all → forces full rebuild.
	prevSnap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"totally.unknown.key": {Key: "totally.unknown.key", Value: "old"},
		},
	}

	newSnap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"totally.unknown.key": {Key: "totally.unknown.key", Value: "new"},
		},
	}

	affected := factory.diffAffectedComponents(newSnap, prevSnap)

	// Should return all components (full rebuild).
	assert.GreaterOrEqual(t, len(affected), managedComponentCount(),
		"unknown key change should force full rebuild")
}

func TestDiffAffectedComponents_RemovedKeyAffectsComponent(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{}

	prevSnap := testSnapshot(map[string]any{
		"redis.host":     "localhost:6379",
		"redis.password": "secret",
	})

	// New snapshot is missing redis.password.
	newSnap := testSnapshot(map[string]any{
		"redis.host": "localhost:6379",
	})

	affected := factory.diffAffectedComponents(newSnap, prevSnap)

	assert.Contains(t, affected, "redis",
		"removed redis key should affect the redis component")
}

func TestDiffAffectedComponents_ComponentNoneSkipped(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{}

	// "rate_limit.max" has Component = domain.ComponentNone.
	prevSnap := testSnapshot(map[string]any{
		"rate_limit.max": 100,
	})

	newSnap := testSnapshot(map[string]any{
		"rate_limit.max": 200,
	})

	affected := factory.diffAffectedComponents(newSnap, prevSnap)

	assert.NotContains(t, affected, domain.ComponentNone,
		"ComponentNone keys should not appear in affected set")
	assert.Empty(t, affected,
		"changing a ComponentNone key should not trigger any component rebuild")
}

func TestDiffAffectedComponents_AllComponentsChanged(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{}

	prevSnap := testSnapshot(map[string]any{
		"postgres.primary_host":   "old-pg",
		"redis.host":              "old-redis",
		"rabbitmq.host":           "old-rmq",
		"object_storage.bucket":   "old-bucket",
		"server.body_limit_bytes": 100,
		"app.log_level":           "info",
	})

	newSnap := testSnapshot(map[string]any{
		"postgres.primary_host":   "new-pg",
		"redis.host":              "new-redis",
		"rabbitmq.host":           "new-rmq",
		"object_storage.bucket":   "new-bucket",
		"server.body_limit_bytes": 200,
		"app.log_level":           "debug",
	})

	affected := factory.diffAffectedComponents(newSnap, prevSnap)

	assert.Contains(t, affected, "postgres")
	assert.Contains(t, affected, "redis")
	assert.Contains(t, affected, "rabbitmq")
	assert.Contains(t, affected, "s3")
	assert.Contains(t, affected, "http")
	assert.Contains(t, affected, "logger")
}

func TestDiffAffectedComponents_NilSnapshots(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{}

	// Nil Configs in either snapshot → full rebuild.
	affected := factory.diffAffectedComponents(
		domain.Snapshot{Configs: nil},
		domain.Snapshot{Configs: map[string]domain.EffectiveValue{}},
	)

	assert.GreaterOrEqual(t, len(affected), managedComponentCount(),
		"nil Configs should force full rebuild")
}

// ---------------------------------------------------------------------------
// effectiveValuesEqual tests
// ---------------------------------------------------------------------------

func TestEffectiveValuesEqual_SameValueAndOverride(t *testing.T) {
	t.Parallel()

	a := domain.EffectiveValue{Key: "k", Value: "hello", Override: "world", Source: "env"}
	b := domain.EffectiveValue{Key: "k", Value: "hello", Override: "world", Source: "env"}

	assert.True(t, effectiveValuesEqual(a, b))
}

func TestEffectiveValuesEqual_DifferentValue(t *testing.T) {
	t.Parallel()

	a := domain.EffectiveValue{Key: "k", Value: "hello"}
	b := domain.EffectiveValue{Key: "k", Value: "world"}

	assert.False(t, effectiveValuesEqual(a, b))
}

func TestEffectiveValuesEqual_DifferentOverride(t *testing.T) {
	t.Parallel()

	a := domain.EffectiveValue{Key: "k", Value: "same", Override: "override-a"}
	b := domain.EffectiveValue{Key: "k", Value: "same", Override: "override-b"}

	assert.False(t, effectiveValuesEqual(a, b))
}

func TestEffectiveValuesEqual_NumericCoercion(t *testing.T) {
	t.Parallel()

	// int 100 vs float64(100) should be treated as equal (JSON deserialization).
	a := domain.EffectiveValue{Key: "k", Value: 100}
	b := domain.EffectiveValue{Key: "k", Value: float64(100)}

	assert.True(t, effectiveValuesEqual(a, b),
		"int 100 and float64(100) should be considered equal via numeric coercion")
}

func TestEffectiveValuesEqual_DifferentSourceSameValue(t *testing.T) {
	t.Parallel()

	// Source differs but values same → still equal (Source is metadata-only).
	a := domain.EffectiveValue{Key: "k", Value: "val", Override: nil, Source: "env"}
	b := domain.EffectiveValue{Key: "k", Value: "val", Override: nil, Source: "store"}

	assert.True(t, effectiveValuesEqual(a, b),
		"different Source with same Value and Override should be equal")
}

func TestEffectiveValuesEqual_NilOverrides(t *testing.T) {
	t.Parallel()

	a := domain.EffectiveValue{Key: "k", Value: "val", Override: nil}
	b := domain.EffectiveValue{Key: "k", Value: "val", Override: nil}

	assert.True(t, effectiveValuesEqual(a, b),
		"nil overrides should be considered equal")
}

func TestEffectiveValuesEqual_NilVsNonNilOverride(t *testing.T) {
	t.Parallel()

	a := domain.EffectiveValue{Key: "k", Value: "val", Override: nil}
	b := domain.EffectiveValue{Key: "k", Value: "val", Override: "something"}

	assert.False(t, effectiveValuesEqual(a, b),
		"nil override vs non-nil override should not be equal")
}

// ---------------------------------------------------------------------------
// Component validation tests
// ---------------------------------------------------------------------------

func TestMatcherKeyDefs_AllComponentsAreValid(t *testing.T) {
	t.Parallel()

	for _, def := range matcherKeyDefs() {
		if def.Component != "" {
			_, ok := allComponents[def.Component]
			assert.True(t, ok, "key %s has unknown component %q", def.Key, def.Component)
		}
	}
}

func TestMatcherKeyDefs_NoEmptyComponent(t *testing.T) {
	t.Parallel()

	for _, def := range matcherKeyDefs() {
		assert.NotEmpty(t, def.Component,
			"key %s has empty Component; use domain.ComponentNone for business-logic keys", def.Key)
	}
}

func TestBuildIncremental_NoChangesKeepsPreviousBundleIntactUntilAdopt(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	previous := &MatcherBundle{
		ownershipTracked: true,
		Infra:            &InfraBundle{},
		Logger:           &LoggerBundle{Logger: &libLog.NopLogger{}, Level: "info"},
		HTTP:             &HTTPPolicyBundle{BodyLimitBytes: 1024},
		ownsLogger:       true,
	}
	prevSnapshot := testSnapshot(map[string]any{"app.log_level": "info"})
	newSnapshot := testSnapshot(map[string]any{"app.log_level": "info"})

	candidateBundle, err := factory.BuildIncremental(context.Background(), newSnapshot, previous, prevSnapshot)
	require.NoError(t, err)

	candidate, ok := candidateBundle.(*MatcherBundle)
	require.True(t, ok)
	assert.Same(t, previous.Logger, candidate.Logger)
	assert.Same(t, previous.HTTP, candidate.HTTP)
	assert.NotNil(t, previous.Logger)
	assert.NotNil(t, previous.HTTP)

	candidate.AdoptResourcesFrom(previous)
	assert.Nil(t, previous.Logger)
	assert.Nil(t, previous.HTTP)
	assert.True(t, candidate.ownsLogger)
}

func TestBuildIncremental_RebuildsChangedPostgresAndReusesUnchangedRedis(t *testing.T) {
	t.Parallel()

	factory := &MatcherBundleFactory{bootstrapCfg: testBootstrapConfig()}
	prevDB := &libPostgres.Client{}
	prevRedis := &libRedis.Client{}
	previous := &MatcherBundle{
		ownershipTracked: true,
		Infra: &InfraBundle{
			Postgres: prevDB,
			Redis:    prevRedis,
		},
		Logger:       &LoggerBundle{Logger: &libLog.NopLogger{}, Level: "info"},
		HTTP:         &HTTPPolicyBundle{BodyLimitBytes: 1024},
		ownsLogger:   true,
		ownsPostgres: true,
		ownsRedis:    true,
	}
	prevSnapshot := testSnapshot(map[string]any{
		"postgres.primary_host": "db-a",
		"redis.host":            "redis-a:6379",
	})
	newSnapshot := testSnapshot(map[string]any{
		"postgres.primary_host": "db-b",
		"redis.host":            "redis-a:6379",
	})

	candidateBundle, err := factory.BuildIncremental(context.Background(), newSnapshot, previous, prevSnapshot)
	require.NoError(t, err)

	candidate := candidateBundle.(*MatcherBundle)
	assert.NotSame(t, prevDB, candidate.DB())
	assert.Same(t, prevRedis, candidate.RedisClient())
	assert.NotNil(t, previous.DB())
	assert.NotNil(t, previous.RedisClient())
}
