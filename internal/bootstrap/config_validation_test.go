// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

func TestValidate_TrustedProxiesRejectsUniversalTrust(t *testing.T) {
	t.Parallel()

	for _, trustedProxies := range []string{"*", "0.0.0.0/0", "::/0", "127.0.0.1,0.0.0.0/0"} {
		cfg := defaultConfig()
		cfg.Server.TrustedProxies = trustedProxies

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "TRUSTED_PROXIES")
	}
}

func TestValidate_TrustedProxiesAllowsSpecificRanges(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Server.TrustedProxies = "127.0.0.1,10.0.0.0/8,192.168.1.10"

	assert.NoError(t, cfg.Validate())
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

func TestValidateRateLimitConfig_RejectsExcessiveMax(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.Max = maxRateLimitRequestsPerWindow + 1

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RATE_LIMIT_MAX")
}

func TestValidateRateLimitConfig_RejectsExcessiveExpiry(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.ExpirySec = maxRateLimitWindowSeconds + 1

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RATE_LIMIT_EXPIRY_SEC")
}

func TestValidateRateLimitConfig_RejectsExcessiveExportLimits(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.ExportMax = maxRateLimitRequestsPerWindow + 1

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EXPORT_RATE_LIMIT_MAX")

	cfg = defaultConfig()
	cfg.RateLimit.ExportExpirySec = maxRateLimitWindowSeconds + 1

	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EXPORT_RATE_LIMIT_EXPIRY_SEC")
}

func TestIsWellKnownDevCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"exact match", "matcher_dev_password", true},
		{"case insensitive", "Matcher_Dev_Password", true},
		{"with whitespace", "  password  ", true},
		{"changeme", "changeme", true},
		{"secret", "secret", true},
		{"strong password", "xK9!mPq2@vR7wL4z", false},
		{"empty string", "", false},
		{"partial match", "matcher_dev", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isWellKnownDevCredential(tt.value))
		})
	}
}

func TestValidateProductionConfig_RejectsDevPostgresPassword(t *testing.T) {
	t.Parallel()

	for _, devPwd := range []string{"matcher_dev_password", "password", "changeme", "secret"} {
		t.Run(devPwd, func(t *testing.T) {
			t.Parallel()

			cfg := defaultConfig()
			cfg.App.EnvName = "production"
			cfg.Postgres.PrimaryPassword = devPwd
			cfg.Server.CORSAllowedOrigins = "https://app.example.com"
			cfg.RabbitMQ.User = "produser"
			cfg.RabbitMQ.Password = "prodpass"
			cfg.RabbitMQ.AllowInsecureHealthCheck = false
			cfg.Redis.Password = "redis-pass"

			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "POSTGRES_PASSWORD must not use a well-known development default")
		})
	}
}

func TestValidateProductionConfig_RejectsDevRabbitMQPassword(t *testing.T) {
	t.Parallel()

	for _, devPwd := range []string{"matcher_dev_password", "password", "changeme", "secret"} {
		t.Run(devPwd, func(t *testing.T) {
			t.Parallel()

			cfg := defaultConfig()
			cfg.App.EnvName = "production"
			cfg.Postgres.PrimaryPassword = "secure-pg-password"
			cfg.Server.CORSAllowedOrigins = "https://app.example.com"
			cfg.RabbitMQ.User = "produser"
			cfg.RabbitMQ.Password = devPwd
			cfg.RabbitMQ.AllowInsecureHealthCheck = false
			cfg.Redis.Password = "redis-pass"

			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "RABBITMQ_PASSWORD must not use a well-known development default")
		})
	}
}

func TestValidateProductionConfig_AcceptsStrongCredentials(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Postgres.PrimaryPassword = "xK9!mPq2@vR7wL4z"
	cfg.Server.CORSAllowedOrigins = "https://app.example.com"
	cfg.RabbitMQ.User = "matcher_prod"
	cfg.RabbitMQ.Password = "rQ3$nT8&jF5yB2mX"
	cfg.RabbitMQ.AllowInsecureHealthCheck = false
	cfg.Redis.Password = "redis-prod-pass"

	// Production endpoint validation requires HTTPS for configured service URLs.
	// The default config uses http://localhost:* which is rejected in production.
	cfg.ObjectStorage.Endpoint = "https://s3.example.com"
	cfg.Fetcher.URL = ""

	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidateRateLimitConfig_RejectsExcessiveDispatchLimits(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.DispatchMax = maxRateLimitRequestsPerWindow + 1

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DISPATCH_RATE_LIMIT_MAX")

	cfg = defaultConfig()
	cfg.RateLimit.DispatchExpirySec = maxRateLimitWindowSeconds + 1

	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DISPATCH_RATE_LIMIT_EXPIRY_SEC")
}
