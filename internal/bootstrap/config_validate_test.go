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

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	t.Run("valid configs", func(t *testing.T) {
		t.Parallel()
		testConfigValidCases(t)
	})

	t.Run("production validations", func(t *testing.T) {
		t.Parallel()
		testConfigProductionValidations(t)
	})

	t.Run("auth validations", func(t *testing.T) {
		t.Parallel()
		testConfigAuthValidations(t)
	})

	t.Run("other validations", func(t *testing.T) {
		t.Parallel()
		testConfigOtherValidations(t)
	})
}

func testConfigValidCases(t *testing.T) {
	t.Helper()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name   string
		config Config
	}{
		{
			name: "valid development config",
			config: buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				PrimaryDBHost:            "localhost",
				PrimaryDBPort:            "5432",
				PrimaryDBUser:            "matcher",
				PrimaryDBName:            "matcher",
				PrimaryDBSSLMode:         "disable",
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			}),
		},
		{
			name: "valid production config with password",
			config: buildConfig(flatConfig{
				EnvName:                  "production",
				DefaultTenantID:          validTenantID,
				PrimaryDBPassword:        "pr0d-s3cure-p@ss!",
				RedisPassword:            "redis-secret",
				RabbitMQUser:             "matcher",
				RabbitMQPassword:         "secure",
				CORSAllowedOrigins:       "https://example.com",
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				EnableTelemetry:          true,
				OtelDeploymentEnv:        "production",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				TLSTerminatedUpstream:    true,
				InfraConnectTimeoutSec:   30,
			}),
		},
		{
			name: "valid config with auth",
			config: buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				AuthEnabled:              true,
				AuthHost:                 "http://auth:8080",
				AuthTokenSecret:          "jwt-pr0d-t0ken-s3cret!",
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			}),
		},
		{
			name: "valid otel deployment env when otel enabled",
			config: buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				EnableTelemetry:          true,
				OtelDeploymentEnv:        "staging",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			}),
		},
		{
			name: "otel deployment env not validated when otel disabled",
			config: buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				EnableTelemetry:          false,
				OtelDeploymentEnv:        "invalid",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			require.NoError(t, err)
		})
	}
}

func testConfigProductionValidations(t *testing.T) {
	t.Helper()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name   string
		config Config
		errMsg string
	}{
		{
			name: "production requires password",
			config: buildConfig(flatConfig{
				EnvName:            "production",
				DefaultTenantID:    validTenantID,
				PrimaryDBPassword:  "",
				RabbitMQUser:       "matcher",
				RabbitMQPassword:   "secure",
				CORSAllowedOrigins: "https://example.com",
				BodyLimitBytes:     1024,
				LogLevel:           "info",
			}),
			errMsg: "POSTGRES_PASSWORD is required in production",
		},
		{
			name: "production requires non-default rabbitmq credentials",
			config: buildConfig(flatConfig{
				EnvName:            "production",
				DefaultTenantID:    validTenantID,
				PrimaryDBPassword:  "pr0d-s3cure-p@ss!",
				RabbitMQUser:       "guest",
				RabbitMQPassword:   "guest",
				CORSAllowedOrigins: "https://example.com",
				BodyLimitBytes:     1024,
				LogLevel:           "info",
			}),
			errMsg: "RABBITMQ credentials must be set to non-default values in production",
		},
		{
			name: "production requires restricted CORS",
			config: buildConfig(flatConfig{
				EnvName:            "production",
				DefaultTenantID:    validTenantID,
				PrimaryDBPassword:  "pr0d-s3cure-p@ss!",
				RabbitMQUser:       "matcher",
				RabbitMQPassword:   "secure",
				CORSAllowedOrigins: "*",
				BodyLimitBytes:     1024,
				LogLevel:           "info",
			}),
			errMsg: "CORS_ALLOWED_ORIGINS must be restricted in production",
		},
		{
			name: "production requires insecure health check disabled",
			config: buildConfig(flatConfig{
				EnvName:                     "production",
				DefaultTenantID:             validTenantID,
				PrimaryDBPassword:           "pr0d-s3cure-p@ss!",
				PrimaryDBSSLMode:            "require",
				RedisTLS:                    true,
				RedisPassword:               "redis-secret",
				RabbitMQURI:                 "amqps",
				RabbitMQUser:                "matcher",
				RabbitMQPassword:            "secure",
				RabbitMQAllowInsecureHealth: true,
				AuthEnabled:                 true,
				AuthHost:                    "http://auth:8080",
				AuthTokenSecret:             "jwt-pr0d-t0ken-s3cret!",
				CORSAllowedOrigins:          "https://example.com",
				BodyLimitBytes:              1024,
				LogLevel:                    "info",
				TLSTerminatedUpstream:       true,
			}),
			errMsg: "RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK must be false in production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func testConfigAuthValidations(t *testing.T) {
	t.Helper()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name   string
		config Config
		errMsg string
	}{
		{
			name: "auth enabled requires host",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: validTenantID,
				AuthEnabled:     true,
				AuthHost:        "",
				AuthTokenSecret: "jwt-pr0d-t0ken-s3cret!",
				BodyLimitBytes:  1024,
				LogLevel:        "info",
			}),
			errMsg: "PLUGIN_AUTH_ADDRESS is required when PLUGIN_AUTH_ENABLED=true",
		},
		{
			name: "auth enabled requires token secret",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: validTenantID,
				AuthEnabled:     true,
				AuthHost:        "http://auth:8080",
				BodyLimitBytes:  1024,
				LogLevel:        "info",
			}),
			errMsg: "AUTH_JWT_SECRET is required when PLUGIN_AUTH_ENABLED=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func testConfigOtherValidations(t *testing.T) {
	t.Helper()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name   string
		config Config
		errMsg string
	}{
		{
			name: "invalid default tenant id",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: "default",
				BodyLimitBytes:  1024,
				LogLevel:        "info",
			}),
			errMsg: "DEFAULT_TENANT_ID must be a valid UUID",
		},
		{
			name: "tls cert and key must be paired",
			config: buildConfig(flatConfig{
				EnvName:           "development",
				DefaultTenantID:   validTenantID,
				ServerTLSCertFile: "/etc/tls/cert.pem",
				BodyLimitBytes:    1024,
				LogLevel:          "info",
			}),
			errMsg: "SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE must be set together",
		},
		{
			name: "body limit must be positive - zero",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: validTenantID,
				BodyLimitBytes:  0,
				LogLevel:        "info",
			}),
			errMsg: "HTTP_BODY_LIMIT_BYTES must be positive",
		},
		{
			name: "body limit must be positive - negative",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: validTenantID,
				BodyLimitBytes:  -100,
				LogLevel:        "info",
			}),
			errMsg: "HTTP_BODY_LIMIT_BYTES must be positive",
		},
		{
			name: "invalid log level",
			config: buildConfig(flatConfig{
				EnvName:                "development",
				DefaultTenantID:        validTenantID,
				BodyLimitBytes:         1024,
				LogLevel:               "invalid",
				InfraConnectTimeoutSec: 30,
			}),
			errMsg: "LOG_LEVEL must be one of: debug, info, warn, error, fatal",
		},
		{
			name: "invalid otel deployment env when otel enabled",
			config: buildConfig(flatConfig{
				EnvName:                "development",
				DefaultTenantID:        validTenantID,
				BodyLimitBytes:         1024,
				LogLevel:               "info",
				EnableTelemetry:        true,
				OtelDeploymentEnv:      "invalid",
				InfraConnectTimeoutSec: 30,
			}),
			errMsg: "OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT must be one of: development, staging, production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}
