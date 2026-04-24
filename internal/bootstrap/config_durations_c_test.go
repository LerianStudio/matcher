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

func TestConfig_ArchivalInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			interval: 12,
			expected: 12 * time.Hour,
		},
		{
			name:     "default value 24 hours",
			interval: 24,
			expected: 24 * time.Hour,
		},
		{
			name:     "zero returns minimum 1 hour",
			interval: 0,
			expected: time.Hour,
		},
		{
			name:     "negative returns minimum 1 hour",
			interval: -5,
			expected: time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{ArchivalIntervalHours: tt.interval})
			assert.Equal(t, tt.expected, cfg.ArchivalInterval())
		})
	}
}

func TestConfig_ArchivalPresignExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expiry   int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			expiry:   1800,
			expected: 1800 * time.Second,
		},
		{
			name:     "default value 3600 seconds (1 hour)",
			expiry:   3600,
			expected: 3600 * time.Second,
		},
		{
			name:     "zero returns default 1 hour",
			expiry:   0,
			expected: 3600 * time.Second,
		},
		{
			name:     "negative returns default 1 hour",
			expiry:   -10,
			expected: 3600 * time.Second,
		},
		{
			name:     "caps at S3 maximum of 7 days",
			expiry:   700000,
			expected: 604800 * time.Second,
		},
		{
			name:     "exactly at S3 maximum",
			expiry:   604800,
			expected: 604800 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{ArchivalPresignExpirySec: tt.expiry})
			assert.Equal(t, tt.expected, cfg.ArchivalPresignExpiry())
		})
	}
}

func TestConfig_ArchivalPresignExpiry_NilConfigUsesDefault(t *testing.T) {
	t.Parallel()

	var cfg *Config

	assert.Equal(t, 3600*time.Second, cfg.ArchivalPresignExpiry())
}

func TestConfig_ArchivalPresignExpiry_LogsWarningOnCap(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{ArchivalPresignExpirySec: 700000})
	cfg.Logger = &libLog.NopLogger{}

	result := cfg.ArchivalPresignExpiry()
	assert.Equal(t, 604800*time.Second, result)
}

func TestConfig_SchedulerInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			interval: 120,
			expected: 120 * time.Second,
		},
		{
			name:     "default value 60 seconds (1 minute)",
			interval: 60,
			expected: 60 * time.Second,
		},
		{
			name:     "zero returns default 1 minute",
			interval: 0,
			expected: time.Minute,
		},
		{
			name:     "negative returns default 1 minute",
			interval: -10,
			expected: time.Minute,
		},
		{
			name:     "small positive value",
			interval: 5,
			expected: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{}
			cfg.Scheduler.IntervalSec = tt.interval
			assert.Equal(t, tt.expected, cfg.SchedulerInterval())
		})
	}
}

func TestConfig_ExportPresignExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expiry   int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			expiry:   1800,
			expected: 1800 * time.Second,
		},
		{
			name:     "default value 3600 seconds (1 hour)",
			expiry:   3600,
			expected: 3600 * time.Second,
		},
		{
			name:     "zero returns default 1 hour",
			expiry:   0,
			expected: 3600 * time.Second,
		},
		{
			name:     "negative returns default 1 hour",
			expiry:   -10,
			expected: 3600 * time.Second,
		},
		{
			name:     "caps at S3 maximum of 7 days",
			expiry:   700000,
			expected: 604800 * time.Second,
		},
		{
			name:     "exactly at S3 maximum",
			expiry:   604800,
			expected: 604800 * time.Second,
		},
		{
			name:     "just below S3 maximum",
			expiry:   604799,
			expected: 604799 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{}
			cfg.ExportWorker.PresignExpirySec = tt.expiry
			assert.Equal(t, tt.expected, cfg.ExportPresignExpiry())
		})
	}
}

func TestConfig_ExportPresignExpiry_NilConfigUsesDefault(t *testing.T) {
	t.Parallel()

	var cfg *Config

	assert.Equal(t, 3600*time.Second, cfg.ExportPresignExpiry())
}

func TestConfig_ExportPresignExpiry_LogsWarningOnCap(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.ExportWorker.PresignExpirySec = 700000
	cfg.Logger = &libLog.NopLogger{}

	result := cfg.ExportPresignExpiry()
	assert.Equal(t, 604800*time.Second, result)
}

func TestConfig_ValidateNegativeWebhookTimeout(t *testing.T) {
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
		WebhookTimeoutSec:        -1,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WEBHOOK_TIMEOUT_SEC must be non-negative")
}

