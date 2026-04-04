// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateFromSystemplane_RejectNotInSeedMode(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	// Sanity: the manager is NOT in seed mode by default.
	require.False(t, cm.InSeedMode())

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level": {Value: "debug"},
		},
	}

	err = cm.UpdateFromSystemplane(snap)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in seed mode")
}

func TestUpdateFromSystemplane_Success(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level": {Value: "debug"},
		},
		GlobalSettings: map[string]domain.EffectiveValue{
			"rate_limit.max": {Value: 999},
		},
	}

	err = cm.UpdateFromSystemplane(snap)
	require.NoError(t, err)

	// The atomic config pointer was updated.
	updated := cm.Get()
	require.NotNil(t, updated)
	assert.Equal(t, "debug", updated.App.LogLevel)
	assert.Equal(t, 999, updated.RateLimit.Max)
}

func TestUpdateFromSystemplane_PreservesBootstrapFields(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// Set distinctive bootstrap-only field values so we can verify they survive.
	cfg.App.EnvName = "staging-test"
	cfg.Server.Address = ":9999"
	cfg.Server.BodyLimitBytes = 2048
	cfg.Server.TLSCertFile = "/etc/tls/cert.pem"
	cfg.Server.TLSKeyFile = "/etc/tls/key.pem"
	cfg.Postgres.MigrationsPath = "db/migrations"
	cfg.Infrastructure.ConnectTimeoutSec = 77
	cfg.Tenancy.MultiTenantRedisPassword = "tenant-redis-secret"
	cfg.Tenancy.MultiTenantServiceAPIKey = "tenant-manager-key"
	cfg.Postgres.PrimaryPassword = "primary-secret"
	cfg.Postgres.ReplicaPassword = "replica-secret"
	cfg.Redis.Password = "redis-secret"
	cfg.RabbitMQ.User = "rabbit-user"
	cfg.RabbitMQ.Password = "rabbit-secret"
	cfg.ObjectStorage.AccessKeyID = "access-key"
	cfg.ObjectStorage.SecretAccessKey = "secret-key"
	cfg.M2M.M2MTargetService = "ledger"
	cfg.M2M.AWSRegion = "us-east-2"
	cfg.Auth = AuthConfig{
		Enabled:     true,
		Host:        "https://auth.example.com",
		TokenSecret: "super-secret-jwt-key",
	}
	cfg.Telemetry = TelemetryConfig{
		Enabled:              true,
		ServiceName:          "matcher-staging",
		LibraryName:          "custom-lib",
		ServiceVersion:       "2.0.0",
		DeploymentEnv:        "staging",
		CollectorEndpoint:    "otel.example.com:4317",
		DBMetricsIntervalSec: 42,
	}
	cfg.Logger = &testLogger{}
	cfg.ShutdownGracePeriod = 15 * time.Second

	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	// An empty snapshot — all runtime fields will get defaults from
	// snapshotToFullConfig, but bootstrap fields must come from oldCfg.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{},
	}

	err = cm.UpdateFromSystemplane(snap)
	require.NoError(t, err)

	updated := cm.Get()
	require.NotNil(t, updated)

	// Bootstrap-only fields preserved.
	assert.Equal(t, "staging-test", updated.App.EnvName)
	assert.Equal(t, ":9999", updated.Server.Address)
	assert.Equal(t, defaultKeyBodyLimitBytes, updated.Server.BodyLimitBytes)
	assert.Equal(t, "/etc/tls/cert.pem", updated.Server.TLSCertFile)
	assert.Equal(t, "/etc/tls/key.pem", updated.Server.TLSKeyFile)
	assert.Equal(t, "db/migrations", updated.Postgres.MigrationsPath)
	assert.Equal(t, 77, updated.Infrastructure.ConnectTimeoutSec)
	assert.Equal(t, cfg.Auth, updated.Auth)
	assert.Equal(t, cfg.Telemetry, updated.Telemetry)
	assert.Equal(t, "ledger", updated.M2M.M2MTargetService)
	assert.Equal(t, "us-east-2", updated.M2M.AWSRegion)
	assert.Equal(t, cfg.Logger, updated.Logger)
	assert.Equal(t, 15*time.Second, updated.ShutdownGracePeriod)

	// Runtime-mutable config values should come from the snapshot/defaults, not
	// from the previous process config.
	assert.Equal(t, defaultPGPassword, updated.Postgres.PrimaryPassword)
	assert.Equal(t, "", updated.Postgres.ReplicaPassword)
	assert.Equal(t, "", updated.Redis.Password)
	assert.Equal(t, defaultRabbitUser, updated.RabbitMQ.User)
	assert.Equal(t, defaultRabbitPassword, updated.RabbitMQ.Password)
	assert.Equal(t, "", updated.ObjectStorage.AccessKeyID)
	assert.Equal(t, "", updated.ObjectStorage.SecretAccessKey)
	assert.Equal(t, "", updated.Tenancy.MultiTenantRedisPassword)
	assert.Equal(t, "", updated.Tenancy.MultiTenantServiceAPIKey)
}

func TestSnapshotToFullConfig_RuntimeFields(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level":           {Value: "debug"},
			"fetcher.enabled":         {Value: true},
			"archival.storage_bucket": {Value: "test-bucket"},
			"scheduler.interval_sec":  {Value: 120},
		},
		GlobalSettings: map[string]domain.EffectiveValue{
			"rate_limit.max":      {Value: 200},
			"webhook.timeout_sec": {Value: 45},
		},
	}

	result := snapshotToFullConfig(snap, oldCfg)

	assert.Equal(t, "debug", result.App.LogLevel)
	assert.Equal(t, 200, result.RateLimit.Max)
	assert.True(t, result.Fetcher.Enabled)
	assert.Equal(t, "test-bucket", result.Archival.StorageBucket)
	assert.Equal(t, 45, result.Webhook.TimeoutSec)
	assert.Equal(t, 120, result.Scheduler.IntervalSec)
}

func TestSnapshotToFullConfig_PrefersSettingsForMovedRuntimeKeys(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"rate_limit.max": {Value: 999},
		},
		GlobalSettings: map[string]domain.EffectiveValue{
			"rate_limit.max": {Value: 200},
		},
	}

	result := snapshotToFullConfig(snap, oldCfg)

	require.NotNil(t, result)
	assert.Equal(t, 200, result.RateLimit.Max)
}

func TestSnapshotToFullConfig_PreservesExplicitEnvOverrides(t *testing.T) {
	t.Setenv("RATE_LIMIT_MAX", "999")
	t.Setenv("WEBHOOK_TIMEOUT_SEC", "44")

	oldCfg := defaultConfig()
	oldCfg.RateLimit.Max = 999
	oldCfg.Webhook.TimeoutSec = 44

	snap := domain.Snapshot{
		GlobalSettings: map[string]domain.EffectiveValue{
			"rate_limit.max":      {Value: 100},
			"webhook.timeout_sec": {Value: 30},
		},
	}

	result := snapshotToFullConfig(snap, oldCfg)

	require.NotNil(t, result)
	assert.Equal(t, 999, result.RateLimit.Max)
	assert.Equal(t, 44, result.Webhook.TimeoutSec)
}

func TestSnapshotToFullConfig_RuntimeMutableSecretsComeFromSnapshot(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	oldCfg.Postgres.PrimaryPassword = "old-primary-secret"
	oldCfg.Postgres.ReplicaPassword = "old-replica-secret"
	oldCfg.Redis.Password = "old-redis-secret"
	oldCfg.RabbitMQ.User = "old-rabbit-user"
	oldCfg.RabbitMQ.Password = "old-rabbit-secret"
	oldCfg.ObjectStorage.AccessKeyID = "old-access-key"
	oldCfg.ObjectStorage.SecretAccessKey = "old-secret-key"
	oldCfg.Tenancy.MultiTenantRedisPassword = "old-tenant-redis-secret"
	oldCfg.Tenancy.MultiTenantServiceAPIKey = "old-tenant-service-key"

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"postgres.primary_password":            {Value: "new-primary-secret"},
			"postgres.replica_password":            {Value: "new-replica-secret"},
			"redis.password":                       {Value: "new-redis-secret"},
			"rabbitmq.user":                        {Value: "new-rabbit-user"},
			"rabbitmq.password":                    {Value: "new-rabbit-secret"},
			"object_storage.access_key_id":         {Value: "new-access-key"},
			"object_storage.secret_access_key":     {Value: "new-secret-key"},
			"tenancy.multi_tenant_redis_password":  {Value: "new-tenant-redis-secret"},
			"tenancy.multi_tenant_service_api_key": {Value: "new-tenant-service-key"},
		},
	}

	result := snapshotToFullConfig(snap, oldCfg)

	require.NotNil(t, result)
	assert.Equal(t, "new-primary-secret", result.Postgres.PrimaryPassword)
	assert.Equal(t, "new-replica-secret", result.Postgres.ReplicaPassword)
	assert.Equal(t, "new-redis-secret", result.Redis.Password)
	assert.Equal(t, "new-rabbit-user", result.RabbitMQ.User)
	assert.Equal(t, "new-rabbit-secret", result.RabbitMQ.Password)
	assert.Equal(t, "new-access-key", result.ObjectStorage.AccessKeyID)
	assert.Equal(t, "new-secret-key", result.ObjectStorage.SecretAccessKey)
	assert.Equal(t, "new-tenant-redis-secret", result.Tenancy.MultiTenantRedisPassword)
	assert.Equal(t, "new-tenant-service-key", result.Tenancy.MultiTenantServiceAPIKey)
}

func TestConfigFromSnapshot_RenamedKeysHydrate(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"cors.allowed_origins":    {Key: "cors.allowed_origins", Value: "https://app.example.com"},
			"cors.allowed_methods":    {Key: "cors.allowed_methods", Value: "GET,POST"},
			"cors.allowed_headers":    {Key: "cors.allowed_headers", Value: "Authorization"},
			"postgres.max_open_conns": {Key: "postgres.max_open_conns", Value: 41},
			"postgres.max_idle_conns": {Key: "postgres.max_idle_conns", Value: 9},
			"redis.min_idle_conns":    {Key: "redis.min_idle_conns", Value: 4},
			"rabbitmq.url":            {Key: "rabbitmq.url", Value: "amqps"},
		},
	}

	result := configFromSnapshot(snap)

	require.NotNil(t, result)
	assert.Equal(t, "https://app.example.com", result.Server.CORSAllowedOrigins)
	assert.Equal(t, "GET,POST", result.Server.CORSAllowedMethods)
	assert.Equal(t, "Authorization", result.Server.CORSAllowedHeaders)
	assert.Equal(t, 41, result.Postgres.MaxOpenConnections)
	assert.Equal(t, 9, result.Postgres.MaxIdleConnections)
	assert.Equal(t, 4, result.Redis.MinIdleConn)
	assert.Equal(t, "amqps", result.RabbitMQ.URI)
}

func TestConfigFromSnapshot_LegacyRenamedKeysHydrate(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"server.cors_allowed_origins":   {Key: "server.cors_allowed_origins", Value: "https://legacy.example.com"},
			"server.cors_allowed_methods":   {Key: "server.cors_allowed_methods", Value: "GET,POST"},
			"server.cors_allowed_headers":   {Key: "server.cors_allowed_headers", Value: "Authorization"},
			"postgres.max_open_connections": {Key: "postgres.max_open_connections", Value: 41},
			"postgres.max_idle_connections": {Key: "postgres.max_idle_connections", Value: 9},
			"redis.min_idle_conn":           {Key: "redis.min_idle_conn", Value: 4},
			"rabbitmq.uri":                  {Key: "rabbitmq.uri", Value: "amqps"},
		},
	}

	result := configFromSnapshot(snap)

	require.NotNil(t, result)
	assert.Equal(t, "https://legacy.example.com", result.Server.CORSAllowedOrigins)
	assert.Equal(t, "GET,POST", result.Server.CORSAllowedMethods)
	assert.Equal(t, "Authorization", result.Server.CORSAllowedHeaders)
	assert.Equal(t, 41, result.Postgres.MaxOpenConnections)
	assert.Equal(t, 9, result.Postgres.MaxIdleConnections)
	assert.Equal(t, 4, result.Redis.MinIdleConn)
	assert.Equal(t, "amqps", result.RabbitMQ.URI)
}

func TestConfigFromSnapshot_RenamedKeysPreferCanonicalWhenBothPresent(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"cors.allowed_origins":          {Key: "cors.allowed_origins", Value: "https://canonical.example.com"},
			"server.cors_allowed_origins":   {Key: "server.cors_allowed_origins", Value: "https://legacy.example.com"},
			"postgres.max_open_conns":       {Key: "postgres.max_open_conns", Value: 41},
			"postgres.max_open_connections": {Key: "postgres.max_open_connections", Value: 12},
			"redis.min_idle_conns":          {Key: "redis.min_idle_conns", Value: 4},
			"redis.min_idle_conn":           {Key: "redis.min_idle_conn", Value: 1},
			"rabbitmq.url":                  {Key: "rabbitmq.url", Value: "amqps://canonical"},
			"rabbitmq.uri":                  {Key: "rabbitmq.uri", Value: "amqp://legacy"},
		},
	}

	result := configFromSnapshot(snap)

	require.NotNil(t, result)
	assert.Equal(t, "https://canonical.example.com", result.Server.CORSAllowedOrigins)
	assert.Equal(t, 41, result.Postgres.MaxOpenConnections)
	assert.Equal(t, 4, result.Redis.MinIdleConn)
	assert.Equal(t, "amqps://canonical", result.RabbitMQ.URI)
}

func TestUpdateFromSystemplane_ValidationFailure_PreservesOldConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	initialLogLevel := cm.Get().App.LogLevel

	// Craft a snapshot with an invalid log level to trigger Validate() failure.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level": {Value: "louder"},
		},
	}

	err = cm.UpdateFromSystemplane(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validation")

	// Config should NOT have changed — the failed update was discarded.
	assert.Equal(t, initialLogLevel, cm.Get().App.LogLevel)
}

func TestUpdateFromSystemplane_CanChangeFormerlyImmutableKeys(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Fetcher.Enabled = false
	cfg.ExportWorker.Enabled = false

	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	// Keys like fetcher.enabled and export_worker.enabled were formerly immutable
	// during file-based reload. Via the systemplane path they must be changeable.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"fetcher.enabled":       {Value: true},
			"export_worker.enabled": {Value: true},
		},
	}

	require.NoError(t, cm.UpdateFromSystemplane(snap))
	assert.True(t, cm.Get().Fetcher.Enabled)
	assert.True(t, cm.Get().ExportWorker.Enabled)
}

func TestUpdateFromSystemplane_NilOldConfig_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	cm.enterSeedMode()

	// Force a nil config pointer to exercise the nil guard in UpdateFromSystemplane.
	cm.config.Store(nil)

	snap := domain.Snapshot{}
	err = cm.UpdateFromSystemplane(snap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpectedly nil")
}

func TestSnapshotToFullConfig_BootstrapFieldsPreserved(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	oldCfg.App.EnvName = "production"
	oldCfg.Server.Address = ":8080"
	oldCfg.Server.BodyLimitBytes = 4096
	oldCfg.Server.TLSCertFile = "/tls/cert.pem"
	oldCfg.Server.TLSKeyFile = "/tls/key.pem"
	oldCfg.Server.TLSTerminatedUpstream = true
	oldCfg.Server.TrustedProxies = "10.0.0.0/8"
	oldCfg.Auth = AuthConfig{
		Enabled:     true,
		Host:        "https://auth.prod.example.com",
		TokenSecret: "prod-jwt-secret",
	}
	oldCfg.Telemetry = TelemetryConfig{
		Enabled:              true,
		ServiceName:          "matcher-prod",
		LibraryName:          "prod-lib",
		ServiceVersion:       "3.0.0",
		DeploymentEnv:        "production",
		CollectorEndpoint:    "otel-prod.example.com:4317",
		DBMetricsIntervalSec: 30,
	}
	oldCfg.Logger = &testLogger{}
	oldCfg.ShutdownGracePeriod = 30 * time.Second
	oldCfg.Postgres.MigrationsPath = "db/migrations"
	oldCfg.Tenancy.DefaultTenantID = "22222222-2222-2222-2222-222222222222"
	oldCfg.Tenancy.DefaultTenantSlug = "prod-default"
	oldCfg.Idempotency.HMACSecret = "seed-secret"

	// Snapshot has no keys — all runtime fields get defaults, but bootstrap
	// fields must be copied from oldCfg regardless.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{},
	}

	result := snapshotToFullConfig(snap, oldCfg)

	// Bootstrap-only fields come from oldCfg, not from snapshot defaults.
	assert.Equal(t, "production", result.App.EnvName)
	assert.Equal(t, ":8080", result.Server.Address)
	assert.Equal(t, defaultKeyBodyLimitBytes, result.Server.BodyLimitBytes)
	assert.Equal(t, "/tls/cert.pem", result.Server.TLSCertFile)
	assert.Equal(t, "/tls/key.pem", result.Server.TLSKeyFile)
	assert.True(t, result.Server.TLSTerminatedUpstream)
	assert.Equal(t, "10.0.0.0/8", result.Server.TrustedProxies)
	assert.Equal(t, oldCfg.Auth, result.Auth)
	assert.Equal(t, "db/migrations", result.Postgres.MigrationsPath)
	assert.Equal(t, oldCfg.Tenancy.DefaultTenantID, result.Tenancy.DefaultTenantID)
	assert.Equal(t, oldCfg.Tenancy.DefaultTenantSlug, result.Tenancy.DefaultTenantSlug)
	assert.Equal(t, oldCfg.Telemetry, result.Telemetry)
	assert.Equal(t, oldCfg.Idempotency.HMACSecret, result.Idempotency.HMACSecret)
	assert.Equal(t, oldCfg.Logger, result.Logger)
	assert.Equal(t, 30*time.Second, result.ShutdownGracePeriod)

	// Verify runtime fields got defaults (not zeros) since snapshot is empty.
	assert.Equal(t, "info", result.App.LogLevel, "runtime field should get default, not zero")
	assert.Equal(t, 100, result.RateLimit.Max, "runtime field should get default, not zero")
	assert.True(t, result.RateLimit.Enabled, "runtime field should get default")
}

func TestUpdateFromSystemplane_TypeMismatchValue_HandledGracefully(t *testing.T) {
	t.Parallel()

	t.Run("string_where_int_expected_uses_default", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		cfg.RateLimit.Enabled = true
		cm, err := NewConfigManager(cfg, nil)
		require.NoError(t, err)

		cm.enterSeedMode()

		// Pre-condition: capture the default rate_limit.max so we can verify
		// it survives the type mismatch.
		initialMax := cm.Get().RateLimit.Max
		require.Equal(t, defaultRateLimitMax, initialMax, "sanity: initial max should be the default")

		// Provide a string where snapInt expects a numeric type.
		// snapInt's strconv.Atoi branch fails on "not-a-number" → falls back
		// to the default (100), so the resulting Config is valid and the update
		// succeeds without crashing.
		snap := domain.Snapshot{
			GlobalSettings: map[string]domain.EffectiveValue{
				"rate_limit.enabled": {Value: true},
				"rate_limit.max":     {Value: "not-a-number"},
			},
		}

		err = cm.UpdateFromSystemplane(snap)
		require.NoError(t, err, "type mismatch must not crash — snapInt falls back to default")

		updated := cm.Get()
		require.NotNil(t, updated)
		assert.Equal(t, defaultRateLimitMax, updated.RateLimit.Max,
			"snapInt should fall back to default when value is an unparseable string")
	})

	t.Run("negative_where_positive_expected_rejected_by_validation", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		cm, err := NewConfigManager(cfg, nil)
		require.NoError(t, err)

		cm.enterSeedMode()

		initialMax := cm.Get().RateLimit.Max

		// snapInt happily coerces -1 into an int, but Validate() enforces
		// "RATE_LIMIT_MAX must be positive" → the update is rejected and
		// the old config is preserved.
		snap := domain.Snapshot{
			GlobalSettings: map[string]domain.EffectiveValue{
				"rate_limit.enabled": {Value: true},
				"rate_limit.max":     {Value: -1},
			},
		}

		err = cm.UpdateFromSystemplane(snap)
		require.Error(t, err, "negative rate_limit.max should fail validation")
		assert.Contains(t, err.Error(), "validation")

		// Old config must be preserved — the failed update was discarded.
		assert.Equal(t, initialMax, cm.Get().RateLimit.Max,
			"config must not change when validation rejects the snapshot")
	})
}
