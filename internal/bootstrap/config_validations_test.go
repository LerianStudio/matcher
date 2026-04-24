// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_ValidateZeroInfraConnectTimeout(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           1024,
		LogLevel:                 "info",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   0,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InfraConnectTimeoutSec must be positive")
}

func TestConfig_ValidateInvalidTenantID(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          "not-a-uuid",
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
	assert.Contains(t, err.Error(), "DEFAULT_TENANT_ID must be a valid UUID")
}

func TestConfig_ValidateRateLimitConfig(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("skips rate limit validation when disabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitEnabled:         false,
			RateLimitMax:             0,
			RateLimitExpirySec:       0,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("fails when export rate limit max is zero", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       0,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "EXPORT_RATE_LIMIT_MAX must be positive")
	})

	t.Run("fails when export rate limit expiry is zero", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 0,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "EXPORT_RATE_LIMIT_EXPIRY_SEC must be positive")
	})

	t.Run("fails when rate limit max is zero and enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitEnabled:         true,
			RateLimitMax:             0,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RATE_LIMIT_MAX must be positive")
	})
}

func TestConfig_ValidateBodyLimitBytes(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           0,
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

func TestConfig_EnforceProductionSecurityDefaults_SwaggerWarning(t *testing.T) {
	t.Parallel()

	t.Run("disables swagger when enabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:          envProduction,
			SwaggerEnabled:   true,
			RateLimitEnabled: true,
		})

		cfg.enforceProductionSecurityDefaults(&libLog.NopLogger{})

		assert.False(t, cfg.Swagger.Enabled, "swagger should be disabled in production")
	})

	t.Run("no warning when swagger disabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:          envProduction,
			SwaggerEnabled:   false,
			RateLimitEnabled: true,
		})

		cfg.enforceProductionSecurityDefaults(&libLog.NopLogger{})

		assert.False(t, cfg.Swagger.Enabled)
	})
}

func TestConfig_ValidateRateLimitConfig_ExpiryValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("fails when rate limit expiry is zero and enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitEnabled:         true,
			RateLimitMax:             100,
			RateLimitExpirySec:       0,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RATE_LIMIT_EXPIRY_SEC must be positive")
	})

	t.Run("fails when dispatch rate limit max is zero and enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                    "development",
			DefaultTenantID:            validTenantID,
			BodyLimitBytes:             1024,
			LogLevel:                   "info",
			RateLimitEnabled:           true,
			RateLimitMax:               100,
			RateLimitExpirySec:         60,
			ExportRateLimitMax:         10,
			ExportRateLimitExpirySec:   60,
			DispatchRateLimitMax:       0,
			DispatchRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:     30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DISPATCH_RATE_LIMIT_MAX must be positive")
	})

	t.Run("fails when dispatch rate limit expiry is zero and enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                    "development",
			DefaultTenantID:            validTenantID,
			BodyLimitBytes:             1024,
			LogLevel:                   "info",
			RateLimitEnabled:           true,
			RateLimitMax:               100,
			RateLimitExpirySec:         60,
			ExportRateLimitMax:         10,
			ExportRateLimitExpirySec:   60,
			DispatchRateLimitMax:       50,
			DispatchRateLimitExpirySec: 0,
			InfraConnectTimeoutSec:     30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DISPATCH_RATE_LIMIT_EXPIRY_SEC must be positive")
	})

	t.Run("passes when all rate limit values are positive", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                    "development",
			DefaultTenantID:            validTenantID,
			BodyLimitBytes:             1024,
			LogLevel:                   "info",
			RateLimitEnabled:           true,
			RateLimitMax:               100,
			RateLimitExpirySec:         60,
			ExportRateLimitMax:         10,
			ExportRateLimitExpirySec:   60,
			DispatchRateLimitMax:       50,
			DispatchRateLimitExpirySec: 60,
			AdminRateLimitMax:          30,
			AdminRateLimitExpirySec:    60,
			InfraConnectTimeoutSec:     30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestConfig_ValidateAllLogLevels(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"
	validLevels := []string{
		"debug",
		"info",
		"warn",
		"error",
		"fatal",
		"DEBUG",
		"INFO",
		"WARN",
		"ERROR",
		"FATAL",
	}

	for _, level := range validLevels {
		t.Run("accepts log level "+level, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				BodyLimitBytes:           1024,
				LogLevel:                 level,
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
}

func TestConfig_ValidateAllOtelEnvs(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"
	validEnvs := []string{
		"development",
		"staging",
		"production",
		"DEVELOPMENT",
		"STAGING",
		"PRODUCTION",
	}

	for _, env := range validEnvs {
		t.Run("accepts otel env "+env, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				EnableTelemetry:          true,
				OtelDeploymentEnv:        env,
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
}

