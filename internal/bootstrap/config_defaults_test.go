// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.Equal(t, 104857600, cfg.Server.BodyLimitBytes)
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
	assert.Empty(t, cfg.Postgres.PrimaryPassword, "password must not have a default")
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
	assert.Equal(t, "guest", cfg.RabbitMQ.User)
	assert.Equal(t, "guest", cfg.RabbitMQ.Password)
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
	assert.Equal(t, "1.0.0", cfg.Telemetry.ServiceVersion)
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

func TestDefaultConfig_SecretsAreZero(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	// Security invariant: no secret/credential field should have a non-empty default.
	assert.Empty(t, cfg.Postgres.PrimaryPassword, "Postgres password")
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

// TestDefaultConfig_SyncWithBindDefaults verifies that defaultConfig() values match
// bindDefaults() viper values for key fields. This catches drift between the two
// sources of truth — a common bug when a default is updated in one place but not the
// other (M1/M2).
func TestDefaultConfig_SyncWithBindDefaults(t *testing.T) {
	t.Parallel()

	v := viper.New()
	bindDefaults(v)

	cfg := defaultConfig()

	// Map of viper key → expected value from defaultConfig(). Cover all sections
	// to maximize drift detection surface area. Skip fields that are intentionally
	// zero in defaultConfig (secrets, replica configs) since those have "" defaults
	// in both sources and test nothing useful.
	checks := []struct {
		key     string
		want    any
		viperFn func(string) any
	}{
		// App
		{"app.env_name", cfg.App.EnvName, func(k string) any { return v.GetString(k) }},
		{"app.log_level", cfg.App.LogLevel, func(k string) any { return v.GetString(k) }},
		// Server
		{"server.address", cfg.Server.Address, func(k string) any { return v.GetString(k) }},
		{"server.body_limit_bytes", cfg.Server.BodyLimitBytes, func(k string) any { return v.GetInt(k) }},
		// Tenancy
		{"tenancy.default_tenant_id", cfg.Tenancy.DefaultTenantID, func(k string) any { return v.GetString(k) }},
		{"tenancy.default_tenant_slug", cfg.Tenancy.DefaultTenantSlug, func(k string) any { return v.GetString(k) }},
		// Postgres
		{"postgres.primary_host", cfg.Postgres.PrimaryHost, func(k string) any { return v.GetString(k) }},
		{"postgres.primary_port", cfg.Postgres.PrimaryPort, func(k string) any { return v.GetString(k) }},
		{"postgres.primary_user", cfg.Postgres.PrimaryUser, func(k string) any { return v.GetString(k) }},
		{"postgres.primary_db", cfg.Postgres.PrimaryDB, func(k string) any { return v.GetString(k) }},
		{"postgres.primary_ssl_mode", cfg.Postgres.PrimarySSLMode, func(k string) any { return v.GetString(k) }},
		{"postgres.max_open_connections", cfg.Postgres.MaxOpenConnections, func(k string) any { return v.GetInt(k) }},
		{"postgres.max_idle_connections", cfg.Postgres.MaxIdleConnections, func(k string) any { return v.GetInt(k) }},
		{"postgres.conn_max_lifetime_mins", cfg.Postgres.ConnMaxLifetimeMins, func(k string) any { return v.GetInt(k) }},
		{"postgres.connect_timeout_sec", cfg.Postgres.ConnectTimeoutSec, func(k string) any { return v.GetInt(k) }},
		{"postgres.query_timeout_sec", cfg.Postgres.QueryTimeoutSec, func(k string) any { return v.GetInt(k) }},
		{"postgres.migrations_path", cfg.Postgres.MigrationsPath, func(k string) any { return v.GetString(k) }},
		// Redis
		{"redis.host", cfg.Redis.Host, func(k string) any { return v.GetString(k) }},
		{"redis.protocol", cfg.Redis.Protocol, func(k string) any { return v.GetInt(k) }},
		{"redis.pool_size", cfg.Redis.PoolSize, func(k string) any { return v.GetInt(k) }},
		{"redis.min_idle_conn", cfg.Redis.MinIdleConn, func(k string) any { return v.GetInt(k) }},
		{"redis.read_timeout_ms", cfg.Redis.ReadTimeoutMs, func(k string) any { return v.GetInt(k) }},
		{"redis.write_timeout_ms", cfg.Redis.WriteTimeoutMs, func(k string) any { return v.GetInt(k) }},
		{"redis.dial_timeout_ms", cfg.Redis.DialTimeoutMs, func(k string) any { return v.GetInt(k) }},
		// RabbitMQ
		{"rabbitmq.uri", cfg.RabbitMQ.URI, func(k string) any { return v.GetString(k) }},
		{"rabbitmq.host", cfg.RabbitMQ.Host, func(k string) any { return v.GetString(k) }},
		{"rabbitmq.port", cfg.RabbitMQ.Port, func(k string) any { return v.GetString(k) }},
		{"rabbitmq.user", cfg.RabbitMQ.User, func(k string) any { return v.GetString(k) }},
		{"rabbitmq.password", cfg.RabbitMQ.Password, func(k string) any { return v.GetString(k) }},
		{"rabbitmq.vhost", cfg.RabbitMQ.VHost, func(k string) any { return v.GetString(k) }},
		{"rabbitmq.health_url", cfg.RabbitMQ.HealthURL, func(k string) any { return v.GetString(k) }},
		// Telemetry
		{"telemetry.service_name", cfg.Telemetry.ServiceName, func(k string) any { return v.GetString(k) }},
		{"telemetry.library_name", cfg.Telemetry.LibraryName, func(k string) any { return v.GetString(k) }},
		{"telemetry.service_version", cfg.Telemetry.ServiceVersion, func(k string) any { return v.GetString(k) }},
		{"telemetry.deployment_env", cfg.Telemetry.DeploymentEnv, func(k string) any { return v.GetString(k) }},
		{"telemetry.collector_endpoint", cfg.Telemetry.CollectorEndpoint, func(k string) any { return v.GetString(k) }},
		{"telemetry.db_metrics_interval_sec", cfg.Telemetry.DBMetricsIntervalSec, func(k string) any { return v.GetInt(k) }},
		// RateLimit
		{"rate_limit.max", cfg.RateLimit.Max, func(k string) any { return v.GetInt(k) }},
		{"rate_limit.expiry_sec", cfg.RateLimit.ExpirySec, func(k string) any { return v.GetInt(k) }},
		{"rate_limit.export_max", cfg.RateLimit.ExportMax, func(k string) any { return v.GetInt(k) }},
		{"rate_limit.export_expiry_sec", cfg.RateLimit.ExportExpirySec, func(k string) any { return v.GetInt(k) }},
		{"rate_limit.dispatch_max", cfg.RateLimit.DispatchMax, func(k string) any { return v.GetInt(k) }},
		{"rate_limit.dispatch_expiry_sec", cfg.RateLimit.DispatchExpirySec, func(k string) any { return v.GetInt(k) }},
		// Infrastructure
		{"infrastructure.connect_timeout_sec", cfg.Infrastructure.ConnectTimeoutSec, func(k string) any { return v.GetInt(k) }},
		// Idempotency
		{"idempotency.retry_window_sec", cfg.Idempotency.RetryWindowSec, func(k string) any { return v.GetInt(k) }},
		{"idempotency.success_ttl_hours", cfg.Idempotency.SuccessTTLHours, func(k string) any { return v.GetInt(k) }},
		// Deduplication
		{"deduplication.ttl_sec", cfg.Dedupe.TTLSec, func(k string) any { return v.GetInt(k) }},
		// ObjectStorage
		{"object_storage.endpoint", cfg.ObjectStorage.Endpoint, func(k string) any { return v.GetString(k) }},
		{"object_storage.region", cfg.ObjectStorage.Region, func(k string) any { return v.GetString(k) }},
		{"object_storage.bucket", cfg.ObjectStorage.Bucket, func(k string) any { return v.GetString(k) }},
		// ExportWorker
		{"export_worker.poll_interval_sec", cfg.ExportWorker.PollIntervalSec, func(k string) any { return v.GetInt(k) }},
		{"export_worker.page_size", cfg.ExportWorker.PageSize, func(k string) any { return v.GetInt(k) }},
		{"export_worker.presign_expiry_sec", cfg.ExportWorker.PresignExpirySec, func(k string) any { return v.GetInt(k) }},
		// CleanupWorker
		{"cleanup_worker.interval_sec", cfg.CleanupWorker.IntervalSec, func(k string) any { return v.GetInt(k) }},
		{"cleanup_worker.batch_size", cfg.CleanupWorker.BatchSize, func(k string) any { return v.GetInt(k) }},
		{"cleanup_worker.grace_period_sec", cfg.CleanupWorker.GracePeriodSec, func(k string) any { return v.GetInt(k) }},
		// Scheduler
		{"scheduler.interval_sec", cfg.Scheduler.IntervalSec, func(k string) any { return v.GetInt(k) }},
		// Archival
		{"archival.interval_hours", cfg.Archival.IntervalHours, func(k string) any { return v.GetInt(k) }},
		{"archival.hot_retention_days", cfg.Archival.HotRetentionDays, func(k string) any { return v.GetInt(k) }},
		{"archival.warm_retention_months", cfg.Archival.WarmRetentionMonths, func(k string) any { return v.GetInt(k) }},
		{"archival.cold_retention_months", cfg.Archival.ColdRetentionMonths, func(k string) any { return v.GetInt(k) }},
		{"archival.batch_size", cfg.Archival.BatchSize, func(k string) any { return v.GetInt(k) }},
		{"archival.storage_prefix", cfg.Archival.StoragePrefix, func(k string) any { return v.GetString(k) }},
		{"archival.storage_class", cfg.Archival.StorageClass, func(k string) any { return v.GetString(k) }},
		{"archival.partition_lookahead", cfg.Archival.PartitionLookahead, func(k string) any { return v.GetInt(k) }},
		{"archival.presign_expiry_sec", cfg.Archival.PresignExpirySec, func(k string) any { return v.GetInt(k) }},
		// Webhook
		{"webhook.timeout_sec", cfg.Webhook.TimeoutSec, func(k string) any { return v.GetInt(k) }},
		// CallbackRateLimit
		{"callback_rate_limit.per_minute", cfg.CallbackRateLimit.PerMinute, func(k string) any { return v.GetInt(k) }},
		// Fetcher
		{"fetcher.allow_private_ips", cfg.Fetcher.AllowPrivateIPs, func(k string) any { return v.GetBool(k) }},
		{"fetcher.health_timeout_sec", cfg.Fetcher.HealthTimeoutSec, func(k string) any { return v.GetInt(k) }},
		{"fetcher.request_timeout_sec", cfg.Fetcher.RequestTimeoutSec, func(k string) any { return v.GetInt(k) }},
		{"fetcher.discovery_interval_sec", cfg.Fetcher.DiscoveryIntervalSec, func(k string) any { return v.GetInt(k) }},
		{"fetcher.schema_cache_ttl_sec", cfg.Fetcher.SchemaCacheTTLSec, func(k string) any { return v.GetInt(k) }},
		{"fetcher.extraction_poll_sec", cfg.Fetcher.ExtractionPollSec, func(k string) any { return v.GetInt(k) }},
		{"fetcher.extraction_timeout_sec", cfg.Fetcher.ExtractionTimeoutSec, func(k string) any { return v.GetInt(k) }},
	}

	for _, c := range checks {
		t.Run(c.key, func(t *testing.T) {
			t.Parallel()

			got := c.viperFn(c.key)
			assert.Equal(t, c.want, got,
				"defaultConfig().%s = %v but bindDefaults() sets %v — sources of truth have drifted",
				c.key, c.want, got)
		})
	}
}

// TestDefaultConfig_SyncWithEnvDefaultTags verifies that defaultConfig() values match
// the envDefault struct tags on Config fields. This catches drift between the YAML
// defaults path (defaultConfig) and the env-var-only path (envDefault tags) — a common
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
