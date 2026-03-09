//go:build unit

package bootstrap

import (
	"testing"

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

// func TestDefaultConfig_FetcherDisabled(t *testing.T) {
// 	t.Parallel()
// 
// 	cfg := defaultConfig()
// 
// 	assert.False(t, cfg.Fetcher.Enabled)
// 	assert.Equal(t, "http://localhost:4006", cfg.Fetcher.URL)
// 	assert.False(t, cfg.Fetcher.AllowPrivateIPs)
// 	assert.Equal(t, 5, cfg.Fetcher.HealthTimeoutSec)
// 	assert.Equal(t, 30, cfg.Fetcher.RequestTimeoutSec)
// 	assert.Equal(t, 60, cfg.Fetcher.DiscoveryIntervalSec)
// 	assert.Equal(t, 300, cfg.Fetcher.SchemaCacheTTLSec)
// 	assert.Equal(t, 5, cfg.Fetcher.ExtractionPollSec)
// 	assert.Equal(t, 600, cfg.Fetcher.ExtractionTimeoutSec)
// }

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

// func TestDefaultConfig_FetcherValidationSkippedWhenDisabled(t *testing.T) {
// 	t.Parallel()
// 
// 	cfg := defaultConfig()
// 	// Fetcher is disabled by default, so even an empty URL should not cause errors.
// 	cfg.Fetcher.URL = ""
// 
// 	err := cfg.validateFetcherConfig()
// 
// 	assert.NoError(t, err, "fetcher validation must be skipped when disabled")
// }
