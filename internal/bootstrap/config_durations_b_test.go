// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_RedisTimeouts(t *testing.T) {
	t.Parallel()

	t.Run("RedisReadTimeout returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{RedisReadTimeoutMs: 3000})
		assert.Equal(t, 3000*time.Millisecond, cfg.RedisReadTimeout())
	})

	t.Run("RedisWriteTimeout returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{RedisWriteTimeoutMs: 2000})
		assert.Equal(t, 2000*time.Millisecond, cfg.RedisWriteTimeout())
	})

	t.Run("RedisDialTimeout returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{RedisDialTimeoutMs: 5000})
		assert.Equal(t, 5000*time.Millisecond, cfg.RedisDialTimeout())
	})
}

func TestConfig_ConnMaxLifetimeAndIdleTime(t *testing.T) {
	t.Parallel()

	t.Run("ConnMaxLifetime returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{ConnMaxLifetimeMins: 30})
		assert.Equal(t, 30*time.Minute, cfg.ConnMaxLifetime())
	})

	t.Run("ConnMaxIdleTime returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{ConnMaxIdleTimeMins: 5})
		assert.Equal(t, 5*time.Minute, cfg.ConnMaxIdleTime())
	})
}

func TestConfig_ExportWorkerPollInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			interval: 10,
			expected: 10 * time.Second,
		},
		{
			name:     "zero returns default 5 seconds",
			interval: 0,
			expected: 5 * time.Second,
		},
		{
			name:     "negative returns default 5 seconds",
			interval: -5,
			expected: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{ExportWorkerPollIntervalSec: tt.interval})
			assert.Equal(t, tt.expected, cfg.ExportWorkerPollInterval())
		})
	}
}

func TestConfig_WebhookTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			timeout:  15,
			expected: 15 * time.Second,
		},
		{
			name:     "default value 30 seconds",
			timeout:  30,
			expected: 30 * time.Second,
		},
		{
			name:     "zero returns default 30 seconds",
			timeout:  0,
			expected: 30 * time.Second,
		},
		{
			name:     "negative returns default 30 seconds",
			timeout:  -10,
			expected: 30 * time.Second,
		},
		{
			name:     "caps at 300 seconds maximum",
			timeout:  600,
			expected: 300 * time.Second,
		},
		{
			name:     "exactly at maximum 300 seconds",
			timeout:  300,
			expected: 300 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{WebhookTimeoutSec: tt.timeout})
			assert.Equal(t, tt.expected, cfg.WebhookTimeout())
		})
	}
}

func TestConfig_WebhookTimeout_LogsWarningOnCap(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{WebhookTimeoutSec: 600})
	cfg.Logger = &libLog.NopLogger{}

	result := cfg.WebhookTimeout()
	assert.Equal(t, 300*time.Second, result)
}

func TestConfig_WebhookTimeout_NilConfigUsesDefault(t *testing.T) {
	t.Parallel()

	var cfg *Config

	assert.Equal(t, 30*time.Second, cfg.WebhookTimeout())
}
func TestConfig_QueryTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			timeout:  45,
			expected: 45 * time.Second,
		},
		{
			name:     "default value 30 seconds",
			timeout:  30,
			expected: 30 * time.Second,
		},
		{
			name:     "zero returns default 30 seconds",
			timeout:  0,
			expected: 30 * time.Second,
		},
		{
			name:     "negative returns default 30 seconds",
			timeout:  -10,
			expected: 30 * time.Second,
		},
		{
			name:     "large value returns configured duration",
			timeout:  120,
			expected: 120 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{PostgresQueryTimeoutSec: tt.timeout})
			assert.Equal(t, tt.expected, cfg.QueryTimeout())
		})
	}
}

func TestConfig_ValidateNegativePostgresQueryTimeout(t *testing.T) {
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
		InfraConnectTimeoutSec:   30,
		PostgresQueryTimeoutSec:  -1,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PostgresQueryTimeoutSec must be non-negative")
}

func TestConfig_ValidateNegativePostgresConnectTimeout(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                   "development",
		DefaultTenantID:           validTenantID,
		BodyLimitBytes:            1024,
		LogLevel:                  "info",
		RateLimitMax:              100,
		RateLimitExpirySec:        60,
		ExportRateLimitMax:        10,
		ExportRateLimitExpirySec:  60,
		InfraConnectTimeoutSec:    30,
		PostgresConnectTimeoutSec: -1,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PostgresConnectTimeoutSec must be non-negative")
}

