//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_DefaultConfig_Passes(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidate_InvalidRateLimit(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.ExportMax = 0 // must be positive

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EXPORT_RATE_LIMIT_MAX")
}

func TestValidate_ProductionWildcardCORS(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Postgres.PrimaryPassword = "secure-password"
	cfg.Server.CORSAllowedOrigins = "*"
	cfg.RabbitMQ.User = "produser"
	cfg.RabbitMQ.Password = "prodpass"
	cfg.RabbitMQ.AllowInsecureHealthCheck = false
	cfg.Redis.Password = "redis-pass"
	cfg.RateLimit.Enabled = true

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CORS")
}

func TestValidate_ProductionEmptyCORS(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Postgres.PrimaryPassword = "secure-password"
	cfg.Server.CORSAllowedOrigins = ""
	cfg.RabbitMQ.User = "produser"
	cfg.RabbitMQ.Password = "prodpass"
	cfg.RabbitMQ.AllowInsecureHealthCheck = false
	cfg.Redis.Password = "redis-pass"
	cfg.RateLimit.Enabled = true

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CORS")
}

func TestValidateAuthConfig_EnabledButNoHost(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Host = ""
	cfg.Auth.TokenSecret = "some-secret"

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AUTH_SERVICE_ADDRESS")
}

func TestValidateAuthConfig_EnabledButNoSecret(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Host = "http://auth:8080"
	cfg.Auth.TokenSecret = ""

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AUTH_JWT_SECRET")
}

func TestValidateAuthConfig_DisabledAllowsMissing(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Auth.Enabled = false
	cfg.Auth.Host = ""
	cfg.Auth.TokenSecret = ""

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidateFetcherConfig_EnabledButNoURL(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Fetcher.Enabled = true
	cfg.Fetcher.URL = ""

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FETCHER_URL")
}

func TestValidateFetcherConfig_DisabledAllowsMissing(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Fetcher.Enabled = false
	cfg.Fetcher.URL = ""

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidateArchivalConfig_EnabledButNoStorageBucket(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Archival.Enabled = true
	cfg.Archival.StorageBucket = ""
	cfg.Archival.HotRetentionDays = 90
	cfg.Archival.WarmRetentionMonths = 24
	cfg.Archival.ColdRetentionMonths = 84
	cfg.Archival.BatchSize = 5000
	cfg.Archival.PartitionLookahead = 3

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ARCHIVAL_STORAGE_BUCKET")
}

func TestValidateArchivalConfig_DisabledAllowsMissing(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Archival.Enabled = false
	cfg.Archival.StorageBucket = ""

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidate_InvalidBodyLimit(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Server.BodyLimitBytes = 0

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_BODY_LIMIT_BYTES")
}

func TestValidate_InvalidLogLevel(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "invalid_level"

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LOG_LEVEL")
}

func TestValidate_TLSPartialConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Server.TLSCertFile = "/path/to/cert.pem"
	cfg.Server.TLSKeyFile = "" // missing — must be set together

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS")
}

func TestValidateRateLimitConfig_DisabledSkipsDetailValidation(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.Enabled = false
	cfg.RateLimit.Max = 0         // would fail if enabled
	cfg.RateLimit.ExpirySec = 0   // would fail if enabled
	cfg.RateLimit.DispatchMax = 0 // would fail if enabled

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidateProductionConfig_GuestRabbitMQ(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Postgres.PrimaryPassword = "secure-password"
	cfg.Server.CORSAllowedOrigins = "https://app.example.com"
	cfg.RabbitMQ.User = "guest"
	cfg.RabbitMQ.Password = "guest"
	cfg.Redis.Password = "redis-pass"
	cfg.RateLimit.Enabled = true

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RABBITMQ")
}

func TestValidateProductionConfig_MissingPostgresPassword(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Postgres.PrimaryPassword = ""

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POSTGRES_PASSWORD")
}
