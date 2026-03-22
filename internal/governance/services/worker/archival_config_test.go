//go:build unit

package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestArchivalWorkerConfig_DefaultValues(t *testing.T) {
	t.Parallel()

	cfg := ArchivalWorkerConfig{}

	assert.Zero(t, cfg.Interval)
	assert.Zero(t, cfg.HotRetentionDays)
	assert.Zero(t, cfg.WarmRetentionMonths)
	assert.Zero(t, cfg.ColdRetentionMonths)
	assert.Zero(t, cfg.BatchSize)
	assert.Empty(t, cfg.StorageBucket)
	assert.Empty(t, cfg.StoragePrefix)
	assert.Empty(t, cfg.StorageClass)
	assert.Zero(t, cfg.PartitionLookahead)
}

func TestArchivalWorkerConfig_FieldAssignment(t *testing.T) {
	t.Parallel()

	cfg := ArchivalWorkerConfig{
		Interval:            24 * time.Hour,
		HotRetentionDays:    90,
		WarmRetentionMonths: 24,
		ColdRetentionMonths: 84,
		BatchSize:           5000,
		StorageBucket:       "my-bucket",
		StoragePrefix:       "archives/audit-logs",
		StorageClass:        "GLACIER",
		PartitionLookahead:  3,
	}

	assert.Equal(t, 24*time.Hour, cfg.Interval)
	assert.Equal(t, 90, cfg.HotRetentionDays)
	assert.Equal(t, 24, cfg.WarmRetentionMonths)
	assert.Equal(t, 84, cfg.ColdRetentionMonths)
	assert.Equal(t, 5000, cfg.BatchSize)
	assert.Equal(t, "my-bucket", cfg.StorageBucket)
	assert.Equal(t, "archives/audit-logs", cfg.StoragePrefix)
	assert.Equal(t, "GLACIER", cfg.StorageClass)
	assert.Equal(t, 3, cfg.PartitionLookahead)
}
