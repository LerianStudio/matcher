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

func TestConfig_ProductionTLSValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("passes with TLS terminated upstream", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "production",
			DefaultTenantID:          validTenantID,
			PrimaryDBPassword:        "pr0d-s3cure-p@ss!",
			PrimaryDBSSLMode:         "require",
			RedisTLS:                 true,
			RedisHost:                "redis.example.com",
			RedisPassword:            "redissecret",
			RabbitMQURI:              "amqps",
			RabbitMQUser:             "matcher",
			RabbitMQPassword:         "rabbitsecret",
			AuthEnabled:              true,
			AuthHost:                 "http://auth:8080",
			AuthTokenSecret:          "jwtsecret",
			CORSAllowedOrigins:       "https://example.com",
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			TLSTerminatedUpstream:    true,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("passes with server TLS configured", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "production",
			DefaultTenantID:          validTenantID,
			PrimaryDBPassword:        "pr0d-s3cure-p@ss!",
			PrimaryDBSSLMode:         "require",
			RedisTLS:                 true,
			RedisHost:                "redis.example.com",
			RedisPassword:            "redissecret",
			RabbitMQURI:              "amqps",
			RabbitMQUser:             "matcher",
			RabbitMQPassword:         "rabbitsecret",
			AuthEnabled:              true,
			AuthHost:                 "http://auth:8080",
			AuthTokenSecret:          "jwtsecret",
			CORSAllowedOrigins:       "https://example.com",
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			TLSTerminatedUpstream:    false,
			ServerTLSCertFile:        "/path/to/cert.pem",
			ServerTLSKeyFile:         "/path/to/key.pem",
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestConfig_AuthHostAndSecretValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("fails when auth enabled but host missing", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			AuthEnabled:              true,
			AuthHost:                 "",
			AuthTokenSecret:          "jwt-pr0d-t0ken-s3cret!",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PLUGIN_AUTH_ADDRESS is required when PLUGIN_AUTH_ENABLED=true")
	})

	t.Run("fails when auth enabled but secret missing", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			AuthEnabled:              true,
			AuthHost:                 "http://auth:8080",
			AuthTokenSecret:          "",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "AUTH_JWT_SECRET is required when PLUGIN_AUTH_ENABLED=true")
	})

	t.Run("fails when auth enabled but host is whitespace only", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			AuthEnabled:              true,
			AuthHost:                 "   ",
			AuthTokenSecret:          "jwt-pr0d-t0ken-s3cret!",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PLUGIN_AUTH_ADDRESS is required when PLUGIN_AUTH_ENABLED=true")
	})
}

func TestConfig_NegativeBodyLimit(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           -100,
		LogLevel:                 "info",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_BODY_LIMIT_BYTES must be positive")
}

func TestConfig_ProductionCORSWildcard(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("fails with wildcard CORS in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:               "production",
			DefaultTenantID:       validTenantID,
			PrimaryDBPassword:     "pr0d-s3cure-p@ss!",
			PrimaryDBSSLMode:      "require",
			RedisTLS:              true,
			RabbitMQURI:           "amqps",
			RabbitMQUser:          "matcher",
			RabbitMQPassword:      "rabbitsecret",
			AuthEnabled:           true,
			AuthHost:              "http://auth:8080",
			AuthTokenSecret:       "jwtsecret",
			CORSAllowedOrigins:    "*",
			BodyLimitBytes:        1024,
			LogLevel:              "info",
			TLSTerminatedUpstream: true,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CORS_ALLOWED_ORIGINS must be restricted in production")
	})
}

