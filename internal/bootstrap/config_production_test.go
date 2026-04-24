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

func TestConfig_ProductionRedisPasswordValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("requires redis password in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                "production",
			DefaultTenantID:        validTenantID,
			PrimaryDBPassword:      "dbsecret",
			RedisHost:              "redis.example.com",
			RedisPassword:          "",
			RabbitMQUser:           "matcher",
			RabbitMQPassword:       "rabbitsecret",
			CORSAllowedOrigins:     "https://example.com",
			BodyLimitBytes:         1024,
			LogLevel:               "info",
			InfraConnectTimeoutSec: 30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"REDIS_PASSWORD is required in production",
		)
	})

	t.Run("requires redis password even when host is empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "production",
			DefaultTenantID:          validTenantID,
			PrimaryDBPassword:        "dbsecret",
			RedisHost:                "",
			RabbitMQUser:             "matcher",
			RabbitMQPassword:         "rabbitsecret",
			CORSAllowedOrigins:       "https://example.com",
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "REDIS_PASSWORD is required in production")
	})

	t.Run("passes when redis password provided", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "production",
			DefaultTenantID:          validTenantID,
			PrimaryDBPassword:        "dbsecret",
			RedisHost:                "redis.example.com",
			RedisPassword:            "redissecret",
			RabbitMQUser:             "matcher",
			RabbitMQPassword:         "rabbitsecret",
			CORSAllowedOrigins:       "https://example.com",
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestConfig_ProductionRateLimitEnforcement(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("enforces rate limiting when disabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:          envProduction,
			DefaultTenantID:  validTenantID,
			RateLimitEnabled: false, // Attempting to disable rate limiting
		})

		// Call enforcement directly
		cfg.enforceProductionSecurityDefaults(nil)

		// Rate limiting should be forced to enabled
		assert.True(
			t,
			cfg.RateLimit.Enabled,
			"rate limiting should be forced to enabled in production",
		)
	})

	t.Run("does not modify rate limiting when already enabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:          envProduction,
			DefaultTenantID:  validTenantID,
			RateLimitEnabled: true,
		})

		cfg.enforceProductionSecurityDefaults(nil)

		assert.True(t, cfg.RateLimit.Enabled)
	})

	t.Run("allows disabling rate limiting in non-production environments", func(t *testing.T) {
		t.Parallel()

		environments := []string{"development", "staging", "test", ""}

		for _, env := range environments {
			cfg := buildConfig(flatConfig{
				EnvName:          env,
				DefaultTenantID:  validTenantID,
				RateLimitEnabled: false,
			})

			cfg.enforceProductionSecurityDefaults(nil)

			assert.False(
				t,
				cfg.RateLimit.Enabled,
				"rate limiting should remain disabled in %s environment",
				env,
			)
		}
	})
}

func TestConfig_ValidateServerConfig_TLSValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("fails when only cert file is set", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ServerTLSCertFile:        "/path/to/cert.pem",
			ServerTLSKeyFile:         "",
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE must be set together",
		)
	})

	t.Run("fails when only key file is set", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ServerTLSCertFile:        "",
			ServerTLSKeyFile:         "/path/to/key.pem",
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE must be set together",
		)
	})

	t.Run("passes when both TLS files are set", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ServerTLSCertFile:        "/path/to/cert.pem",
			ServerTLSKeyFile:         "/path/to/key.pem",
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestConfig_ValidateInvalidLogLevel(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           1024,
		LogLevel:                 "invalid_level",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LOG_LEVEL must be one of")
}

func TestConfig_ValidateInvalidOtelDeploymentEnv(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           1024,
		LogLevel:                 "info",
		EnableTelemetry:          true,
		OtelDeploymentEnv:        "invalid_env",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT must be one of")
}

