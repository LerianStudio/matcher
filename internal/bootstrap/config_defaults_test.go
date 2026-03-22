// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
)

func TestDefaultConfig_NotNil(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	require.NotNil(t, cfg)
}

func TestDefaultConfig_App(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, "development", cfg.App.EnvName)
	assert.Equal(t, "info", cfg.App.LogLevel)
}

func TestDefaultConfig_Server(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, ":4018", cfg.Server.Address)
	assert.Equal(t, 33554432, cfg.Server.BodyLimitBytes)
	assert.Equal(t, "http://localhost:3000", cfg.Server.CORSAllowedOrigins)
	assert.Contains(t, cfg.Server.CORSAllowedMethods, "GET")
	assert.Contains(t, cfg.Server.CORSAllowedHeaders, "Authorization")
	assert.False(t, cfg.Server.TLSTerminatedUpstream)
	assert.Empty(t, cfg.Server.TLSCertFile)
	assert.Empty(t, cfg.Server.TLSKeyFile)
}

func TestDefaultConfig_Tenancy(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, "11111111-1111-1111-1111-111111111111", cfg.Tenancy.DefaultTenantID)
	assert.Equal(t, "default", cfg.Tenancy.DefaultTenantSlug)
	assert.False(t, cfg.Tenancy.MultiTenantEnabled)
	assert.Empty(t, cfg.Tenancy.MultiTenantEnvironment)
	assert.Equal(t, 100, cfg.Tenancy.MultiTenantMaxTenantPools)
	assert.Equal(t, 300, cfg.Tenancy.MultiTenantIdleTimeoutSec)
	assert.Equal(t, 5, cfg.Tenancy.MultiTenantCircuitBreakerThreshold)
	assert.Equal(t, 30, cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec)
	assert.False(t, cfg.Tenancy.MultiTenantInfraEnabled)
}

func TestDefaultConfig_Postgres(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, "localhost", cfg.Postgres.PrimaryHost)
	assert.Equal(t, "5432", cfg.Postgres.PrimaryPort)
	assert.Equal(t, "matcher", cfg.Postgres.PrimaryUser)
	assert.Equal(t, "matcher", cfg.Postgres.PrimaryDB)
	assert.Equal(t, "disable", cfg.Postgres.PrimarySSLMode)
	assert.Equal(t, "matcher_dev_password", cfg.Postgres.PrimaryPassword, "dev default password for zero-config startup")
	assert.Equal(t, 25, cfg.Postgres.MaxOpenConnections)
	assert.Equal(t, 5, cfg.Postgres.MaxIdleConnections)
	assert.Equal(t, 30, cfg.Postgres.ConnMaxLifetimeMins)
	assert.Equal(t, 5, cfg.Postgres.ConnMaxIdleTimeMins)
	assert.Equal(t, 10, cfg.Postgres.ConnectTimeoutSec)
	assert.Equal(t, 30, cfg.Postgres.QueryTimeoutSec)
	assert.Equal(t, "migrations", cfg.Postgres.MigrationsPath)

	// Replica fields must be zero — only set via env vars
	assert.Empty(t, cfg.Postgres.ReplicaHost)
	assert.Empty(t, cfg.Postgres.ReplicaPort)
}

func TestDefaultConfig_Redis(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, "localhost:6379", cfg.Redis.Host)
	assert.Equal(t, 0, cfg.Redis.DB)
	assert.Equal(t, 3, cfg.Redis.Protocol)
	assert.False(t, cfg.Redis.TLS)
	assert.Equal(t, 10, cfg.Redis.PoolSize)
	assert.Equal(t, 2, cfg.Redis.MinIdleConn)
	assert.Equal(t, 3000, cfg.Redis.ReadTimeoutMs)
	assert.Equal(t, 3000, cfg.Redis.WriteTimeoutMs)
	assert.Equal(t, 5000, cfg.Redis.DialTimeoutMs)
	assert.Empty(t, cfg.Redis.Password, "password must not have a default")
	assert.Empty(t, cfg.Redis.MasterName)
}

func TestDefaultConfig_RabbitMQ(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, "amqp", cfg.RabbitMQ.URI)
	assert.Equal(t, "localhost", cfg.RabbitMQ.Host)
	assert.Equal(t, "5672", cfg.RabbitMQ.Port)
	assert.Equal(t, "matcher_admin", cfg.RabbitMQ.User)
	assert.Equal(t, "matcher_dev_password", cfg.RabbitMQ.Password)
	assert.Equal(t, "/", cfg.RabbitMQ.VHost)
	assert.Equal(t, "http://localhost:15672", cfg.RabbitMQ.HealthURL)
	assert.False(t, cfg.RabbitMQ.AllowInsecureHealthCheck)
}

func TestDefaultConfig_AuthDisabled(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.False(t, cfg.Auth.Enabled)
	assert.Empty(t, cfg.Auth.Host)
	assert.Empty(t, cfg.Auth.TokenSecret)
}

func TestDefaultConfig_TelemetryDisabled(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.False(t, cfg.Telemetry.Enabled)
	assert.Equal(t, "matcher", cfg.Telemetry.ServiceName)
	assert.Equal(t, "github.com/LerianStudio/matcher", cfg.Telemetry.LibraryName)
	assert.Equal(t, "1.1.0", cfg.Telemetry.ServiceVersion)
	assert.Equal(t, "development", cfg.Telemetry.DeploymentEnv)
	assert.Equal(t, "localhost:4317", cfg.Telemetry.CollectorEndpoint)
	assert.Equal(t, 15, cfg.Telemetry.DBMetricsIntervalSec)
}

func TestDefaultConfig_RateLimitEnabled(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.True(t, cfg.RateLimit.Enabled)
	assert.Equal(t, 100, cfg.RateLimit.Max)
	assert.Equal(t, 60, cfg.RateLimit.ExpirySec)
	assert.Equal(t, 10, cfg.RateLimit.ExportMax)
	assert.Equal(t, 60, cfg.RateLimit.ExportExpirySec)
	assert.Equal(t, 50, cfg.RateLimit.DispatchMax)
	assert.Equal(t, 60, cfg.RateLimit.DispatchExpirySec)
}

func TestDefaultConfig_ExportWorkerEnabled(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.True(t, cfg.ExportWorker.Enabled)
	assert.Equal(t, 5, cfg.ExportWorker.PollIntervalSec)
	assert.Equal(t, 1000, cfg.ExportWorker.PageSize)
	assert.Equal(t, 3600, cfg.ExportWorker.PresignExpirySec)
}

func TestDefaultConfig_FetcherDisabled(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.False(t, cfg.Fetcher.Enabled)
	assert.Equal(t, "http://localhost:4006", cfg.Fetcher.URL)
	assert.False(t, cfg.Fetcher.AllowPrivateIPs)
	assert.Equal(t, 5, cfg.Fetcher.HealthTimeoutSec)
	assert.Equal(t, 30, cfg.Fetcher.RequestTimeoutSec)
	assert.Equal(t, 60, cfg.Fetcher.DiscoveryIntervalSec)
	assert.Equal(t, 300, cfg.Fetcher.SchemaCacheTTLSec)
	assert.Equal(t, 5, cfg.Fetcher.ExtractionPollSec)
	assert.Equal(t, 600, cfg.Fetcher.ExtractionTimeoutSec)
}

func TestDefaultConfig_ArchivalDisabled(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.False(t, cfg.Archival.Enabled)
	assert.Equal(t, 24, cfg.Archival.IntervalHours)
	assert.Equal(t, 90, cfg.Archival.HotRetentionDays)
	assert.Equal(t, 24, cfg.Archival.WarmRetentionMonths)
	assert.Equal(t, 84, cfg.Archival.ColdRetentionMonths)
	assert.Equal(t, 5000, cfg.Archival.BatchSize)
	assert.Equal(t, "archives/audit-logs", cfg.Archival.StoragePrefix)
	assert.Equal(t, "GLACIER", cfg.Archival.StorageClass)
	assert.Equal(t, 3, cfg.Archival.PartitionLookahead)
	assert.Equal(t, 3600, cfg.Archival.PresignExpirySec)
}

func TestDefaultConfig_SwaggerDisabled(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.False(t, cfg.Swagger.Enabled)
	assert.Equal(t, "https", cfg.Swagger.Schemes)
}

func TestDefaultConfig_Infrastructure(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, 30, cfg.Infrastructure.ConnectTimeoutSec)
}

func TestDefaultConfig_Idempotency(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, 300, cfg.Idempotency.RetryWindowSec)
	assert.Equal(t, 168, cfg.Idempotency.SuccessTTLHours)
	assert.Empty(t, cfg.Idempotency.HMACSecret, "HMAC secret must not have a default")
}

func TestDefaultConfig_Dedupe(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, 3600, cfg.Dedupe.TTLSec)
}

func TestDefaultConfig_ObjectStorage(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, "http://localhost:8333", cfg.ObjectStorage.Endpoint)
	assert.Equal(t, "us-east-1", cfg.ObjectStorage.Region)
	assert.Equal(t, "matcher-exports", cfg.ObjectStorage.Bucket)
	assert.True(t, cfg.ObjectStorage.UsePathStyle)
	assert.Empty(t, cfg.ObjectStorage.AccessKeyID, "credentials must not have defaults")
	assert.Empty(t, cfg.ObjectStorage.SecretAccessKey, "credentials must not have defaults")
}

func TestDefaultConfig_CleanupWorker(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.True(t, cfg.CleanupWorker.Enabled)
	assert.Equal(t, 3600, cfg.CleanupWorker.IntervalSec)
	assert.Equal(t, 100, cfg.CleanupWorker.BatchSize)
	assert.Equal(t, 3600, cfg.CleanupWorker.GracePeriodSec)
}

func TestDefaultConfig_Scheduler(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, 60, cfg.Scheduler.IntervalSec)
}

func TestDefaultConfig_Webhook(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, 30, cfg.Webhook.TimeoutSec)
}

func TestDefaultConfig_CallbackRateLimit(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	assert.Equal(t, 60, cfg.CallbackRateLimit.PerMinute)
}

func TestDefaultConfig_SecretsHaveDevDefaults(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// Dev credentials have defaults for zero-config docker-compose startup.
	// Production deployments MUST override via env vars or systemplane.
	assert.Equal(t, "matcher_dev_password", cfg.Postgres.PrimaryPassword, "Postgres dev password")
	assert.Equal(t, "matcher_dev_password", cfg.RabbitMQ.Password, "RabbitMQ dev password")

	// Security-sensitive fields that must NEVER have defaults.
	assert.Empty(t, cfg.Redis.Password, "Redis password")
	assert.Empty(t, cfg.Auth.TokenSecret, "Auth JWT secret")
	assert.Empty(t, cfg.Idempotency.HMACSecret, "Idempotency HMAC secret")
	assert.Empty(t, cfg.ObjectStorage.AccessKeyID, "Object storage access key")
	assert.Empty(t, cfg.ObjectStorage.SecretAccessKey, "Object storage secret key")
	assert.Empty(t, cfg.Server.TLSCertFile, "TLS cert file")
	assert.Empty(t, cfg.Server.TLSKeyFile, "TLS key file")
}

func TestDefaultConfig_ValidatesSuccessfully(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	err := cfg.Validate()

	assert.NoError(t, err, "default config must pass validation without env vars")
}

// TestDefaultConfig_SyncWithEnvDefaultTags verifies that defaultConfig() values match
// the envDefault struct tags on Config fields. This catches drift between the defaults
// path (defaultConfig) and the env-var-only path (envDefault tags) — a common
// bug when a default is updated in one place but not the other.
//
// The test uses reflection to walk all Config fields recursively, extracting envDefault
// tag values and comparing them (after type conversion) to the corresponding field
// value from defaultConfig(). Fields without envDefault tags are skipped (they are
// secrets or infrastructure fields that intentionally have no default).
func TestDefaultConfig_SyncWithEnvDefaultTags(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	walkAndCompare(t, "", reflect.TypeOf(*cfg), reflect.ValueOf(*cfg))
}

// ---------------------------------------------------------------------------
// defaultSnapshotFromKeyDefs tests
// ---------------------------------------------------------------------------

func TestDefaultSnapshotFromKeyDefs_AllKeysPresent(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefs()
	snap := defaultSnapshotFromKeyDefs(defs)

	assert.Equal(t, len(defs), len(snap.Configs),
		"snapshot should contain one config entry per key definition")

	for _, def := range defs {
		ev, exists := snap.Configs[def.Key]
		require.True(t, exists, "snapshot missing key %q", def.Key)
		assert.Equal(t, def.DefaultValue, ev.Default,
			"key %q: Default should match KeyDef.DefaultValue", def.Key)
		assert.Equal(t, def.DefaultValue, ev.Value,
			"key %q: Value should match KeyDef.DefaultValue", def.Key)
	}
}

func TestDefaultSnapshotFromKeyDefs_SourceIsRegistryDefault(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefs()
	snap := defaultSnapshotFromKeyDefs(defs)

	for key, ev := range snap.Configs {
		assert.Equal(t, "registry-default", ev.Source,
			"key %q: Source should be 'registry-default', got %q", key, ev.Source)
	}
}

// ---------------------------------------------------------------------------
// configFromSnapshot tests
// ---------------------------------------------------------------------------

func TestConfigFromSnapshot_EmptySnapshot(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{Configs: map[string]domain.EffectiveValue{}}

	cfg := configFromSnapshot(snap)

	require.NotNil(t, cfg, "configFromSnapshot should return non-nil Config even for empty snapshot")

	// Empty snapshot → bootstrap fields should be zero-value (no snapshot entries).
	assert.Empty(t, cfg.App.EnvName, "EnvName should be empty with empty snapshot")
	assert.Empty(t, cfg.Server.Address, "Server.Address should be empty with empty snapshot")

	// Runtime fields with hardcoded fallbacks should have those fallbacks.
	assert.Equal(t, defaultCORSAllowedOrigins, cfg.Server.CORSAllowedOrigins,
		"CORSAllowedOrigins should fall back to default")
	assert.Equal(t, defaultPGHost, cfg.Postgres.PrimaryHost,
		"Postgres.PrimaryHost should fall back to default")
}

func TestConfigFromSnapshot_PopulatedSnapshot(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.env_name":            {Key: "app.env_name", Value: "staging"},
			"app.log_level":           {Key: "app.log_level", Value: "warn"},
			"postgres.primary_host":   {Key: "postgres.primary_host", Value: "db.staging.example.com"},
			"redis.host":              {Key: "redis.host", Value: "redis.staging:6380"},
			"rate_limit.max":          {Key: "rate_limit.max", Value: 500},
			"server.body_limit_bytes": {Key: "server.body_limit_bytes", Value: 2048},
		},
	}

	cfg := configFromSnapshot(snap)

	require.NotNil(t, cfg)
	assert.Equal(t, "staging", cfg.App.EnvName)
	assert.Equal(t, "warn", cfg.App.LogLevel)
	assert.Equal(t, "db.staging.example.com", cfg.Postgres.PrimaryHost)
	assert.Equal(t, "redis.staging:6380", cfg.Redis.Host)
	assert.Equal(t, 500, cfg.RateLimit.Max)
	assert.Equal(t, 2048, cfg.Server.BodyLimitBytes)
}

// walkAndCompare recursively walks a struct type, and for each field with an envDefault
// tag, asserts that the parsed tag value matches the corresponding field value from
// defaultConfig(). Nested structs are recursed into with a dotted path prefix.
func walkAndCompare(t *testing.T, prefix string, typ reflect.Type, val reflect.Value) {
	t.Helper()

	for i := range typ.NumField() {
		field := typ.Field(i)
		fieldVal := val.Field(i)

		// Build a human-readable path for subtest naming.
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}

		// Recurse into nested structs that don't have their own env tag.
		if field.Type.Kind() == reflect.Struct &&
			field.Tag.Get("env") == "" &&
			field.Tag.Get("mapstructure") != "-" {
			walkAndCompare(t, path, field.Type, fieldVal)

			continue
		}

		// Skip fields without envDefault tags — they are secrets or
		// infrastructure fields intentionally left as zero values.
		envDefault := field.Tag.Get("envDefault")
		if envDefault == "" {
			continue
		}

		t.Run(path, func(t *testing.T) {
			t.Parallel()

			switch field.Type.Kind() { //nolint:exhaustive // Only config field types need handling.
			case reflect.String:
				assert.Equal(t, envDefault, fieldVal.String(),
					"defaultConfig().%s = %q but envDefault tag = %q — sources of truth have drifted",
					path, fieldVal.String(), envDefault)

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				parsed, err := strconv.ParseInt(envDefault, 10, 64)
				require.NoError(t, err, "envDefault tag %q for %s is not a valid int", envDefault, path)

				assert.Equal(t, parsed, fieldVal.Int(),
					"defaultConfig().%s = %d but envDefault tag = %q — sources of truth have drifted",
					path, fieldVal.Int(), envDefault)

			case reflect.Bool:
				parsed, err := strconv.ParseBool(envDefault)
				require.NoError(t, err, "envDefault tag %q for %s is not a valid bool", envDefault, path)

				assert.Equal(t, parsed, fieldVal.Bool(),
					"defaultConfig().%s = %v but envDefault tag = %q — sources of truth have drifted",
					path, fieldVal.Bool(), envDefault)

			default:
				t.Errorf("unsupported field type %s for %s — extend walkAndCompare", field.Type.Kind(), path)
			}
		})
	}
}
