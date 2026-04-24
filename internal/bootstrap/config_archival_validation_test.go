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

func TestConfig_ValidateArchivalConfig(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("passes with valid archival config disabled", func(t *testing.T) {
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
			ArchivalEnabled:          false,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("passes with valid archival config enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                     "development",
			DefaultTenantID:             validTenantID,
			BodyLimitBytes:              1024,
			LogLevel:                    "info",
			RateLimitMax:                100,
			RateLimitExpirySec:          60,
			ExportRateLimitMax:          10,
			ExportRateLimitExpirySec:    60,
			InfraConnectTimeoutSec:      30,
			ArchivalEnabled:             true,
			ArchivalStorageBucket:       "my-bucket",
			ArchivalHotRetentionDays:    90,
			ArchivalWarmRetentionMonths: 24,
			ArchivalColdRetentionMonths: 84,
			ArchivalBatchSize:           5000,
			ArchivalPartitionLookahead:  3,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("fails when enabled but no bucket", func(t *testing.T) {
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
			ArchivalEnabled:          true,
			ArchivalStorageBucket:    "",
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_STORAGE_BUCKET is required when ARCHIVAL_WORKER_ENABLED=true")
	})

	t.Run("fails when enabled but bucket is whitespace", func(t *testing.T) {
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
			ArchivalEnabled:          true,
			ArchivalStorageBucket:    "   ",
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_STORAGE_BUCKET is required when ARCHIVAL_WORKER_ENABLED=true")
	})

	t.Run("fails when warm retention <= hot retention / 30", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                     "development",
			DefaultTenantID:             validTenantID,
			BodyLimitBytes:              1024,
			LogLevel:                    "info",
			RateLimitMax:                100,
			RateLimitExpirySec:          60,
			ExportRateLimitMax:          10,
			ExportRateLimitExpirySec:    60,
			InfraConnectTimeoutSec:      30,
			ArchivalEnabled:             true,
			ArchivalStorageBucket:       "my-bucket",
			ArchivalHotRetentionDays:    90, // 90 / 30 = 3
			ArchivalWarmRetentionMonths: 3,  // 3 is not > 3
			ArchivalColdRetentionMonths: 84,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_WARM_RETENTION_MONTHS must be greater than ARCHIVAL_HOT_RETENTION_DAYS / 30")
	})

	t.Run("fails when cold retention < warm retention", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                     "development",
			DefaultTenantID:             validTenantID,
			BodyLimitBytes:              1024,
			LogLevel:                    "info",
			RateLimitMax:                100,
			RateLimitExpirySec:          60,
			ExportRateLimitMax:          10,
			ExportRateLimitExpirySec:    60,
			InfraConnectTimeoutSec:      30,
			ArchivalEnabled:             true,
			ArchivalStorageBucket:       "my-bucket",
			ArchivalHotRetentionDays:    90,
			ArchivalWarmRetentionMonths: 24,
			ArchivalColdRetentionMonths: 12, // 12 < 24
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_COLD_RETENTION_MONTHS must be >= ARCHIVAL_WARM_RETENTION_MONTHS")
	})

	t.Run("fails when hot retention days is zero", func(t *testing.T) {
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
			ArchivalEnabled:          true,
			ArchivalStorageBucket:    "my-bucket",
			ArchivalHotRetentionDays: -1,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_HOT_RETENTION_DAYS must be positive")
	})

	t.Run("fails when batch size is zero", func(t *testing.T) {
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
			ArchivalEnabled:          true,
			ArchivalStorageBucket:    "my-bucket",
			ArchivalBatchSize:        -1,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_BATCH_SIZE must be positive")
	})

	t.Run("fails when partition lookahead is zero", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                    "development",
			DefaultTenantID:            validTenantID,
			BodyLimitBytes:             1024,
			LogLevel:                   "info",
			RateLimitMax:               100,
			RateLimitExpirySec:         60,
			ExportRateLimitMax:         10,
			ExportRateLimitExpirySec:   60,
			InfraConnectTimeoutSec:     30,
			ArchivalEnabled:            true,
			ArchivalStorageBucket:      "my-bucket",
			ArchivalPartitionLookahead: -1,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_PARTITION_LOOKAHEAD must be positive")
	})

	t.Run("does not require bucket when disabled", func(t *testing.T) {
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
			ArchivalEnabled:          false,
			ArchivalStorageBucket:    "",
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}
