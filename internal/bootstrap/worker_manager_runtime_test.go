// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"sync"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
)

// --- Test doubles for cleanup, archival, and scheduler workers ---

type runtimeAwareCleanupWorker struct {
	mockWorker
	mu      sync.Mutex
	updates []reportingWorker.CleanupWorkerConfig
}

func (w *runtimeAwareCleanupWorker) UpdateRuntimeConfig(cfg reportingWorker.CleanupWorkerConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.updates = append(w.updates, cfg)
}

func (w *runtimeAwareCleanupWorker) lastUpdate() *reportingWorker.CleanupWorkerConfig {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.updates) == 0 {
		return nil
	}

	u := w.updates[len(w.updates)-1]

	return &u
}

type runtimeAwareArchivalWorker struct {
	mockWorker
	mu      sync.Mutex
	updates []governanceWorker.ArchivalWorkerConfig
}

func (w *runtimeAwareArchivalWorker) UpdateRuntimeConfig(cfg governanceWorker.ArchivalWorkerConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.updates = append(w.updates, cfg)
}

func (w *runtimeAwareArchivalWorker) lastUpdate() *governanceWorker.ArchivalWorkerConfig {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.updates) == 0 {
		return nil
	}

	u := w.updates[len(w.updates)-1]

	return &u
}

type runtimeAwareSchedulerWorker struct {
	mockWorker
	mu      sync.Mutex
	updates []configWorker.SchedulerWorkerConfig
}

func (w *runtimeAwareSchedulerWorker) UpdateRuntimeConfig(cfg configWorker.SchedulerWorkerConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.updates = append(w.updates, cfg)
}

func (w *runtimeAwareSchedulerWorker) lastUpdate() *configWorker.SchedulerWorkerConfig {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.updates) == 0 {
		return nil
	}

	u := w.updates[len(w.updates)-1]

	return &u
}

// --- extractWorkerConfig tests ---

func TestExtractWorkerConfig_AllNames(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	tests := []struct {
		name     string
		wantNil  bool
		expected any
	}{
		{"export", false, cfg.ExportWorker},
		{"cleanup", false, cfg.CleanupWorker},
		{"archival", false, cfg.Archival},
		{"scheduler", false, cfg.Scheduler},
		{"discovery", false, cfg.Fetcher},
		{"unknown", true, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := extractWorkerConfig(tt.name, cfg)
			if tt.wantNil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestExtractWorkerConfig_NilConfig(t *testing.T) {
	t.Parallel()

	got := extractWorkerConfig("export", nil)
	assert.Nil(t, got)
}

// --- workerConfigChanged tests ---

func TestWorkerConfigChanged_SameConfig_ReturnsFalse(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	assert.False(t, workerConfigChanged("export", cfg, cfg))
}

func TestWorkerConfigChanged_DifferentConfig_ReturnsTrue(t *testing.T) {
	t.Parallel()

	old := defaultConfig()
	newCfg := defaultConfig()
	newCfg.ExportWorker.PageSize = 9999

	assert.True(t, workerConfigChanged("export", old, newCfg))
}

func TestWorkerConfigChanged_NilOldConfig_ReturnsTrue(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	assert.True(t, workerConfigChanged("export", nil, cfg))
}

func TestWorkerConfigChanged_UnknownWorker_ReturnsTrue(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	assert.True(t, workerConfigChanged("nonexistent", cfg, cfg))
}

// --- applyWorkerRuntimeConfig tests ---

func TestApplyWorkerRuntimeConfig_Export(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareExportWorker{}
	cfg := defaultConfig()
	cfg.ExportWorker.PollIntervalSec = 10
	cfg.ExportWorker.PageSize = 500

	applyWorkerRuntimeConfig(context.Background(), "export", worker, cfg)

	updates := worker.lastUpdates()
	require.Len(t, updates, 1)
	assert.Equal(t, cfg.ExportWorkerPollInterval(), updates[0].PollInterval)
	assert.Equal(t, 500, updates[0].PageSize)
}

func TestApplyWorkerRuntimeConfig_Cleanup(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareCleanupWorker{}
	cfg := defaultConfig()
	cfg.CleanupWorker.IntervalSec = 7200
	cfg.CleanupWorker.BatchSize = 50

	applyWorkerRuntimeConfig(context.Background(), "cleanup", worker, cfg)

	u := worker.lastUpdate()
	require.NotNil(t, u)
	assert.Equal(t, cfg.CleanupWorkerInterval(), u.Interval)
	assert.Equal(t, 50, u.BatchSize)
}

func TestApplyWorkerRuntimeConfig_Archival(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareArchivalWorker{}
	cfg := defaultConfig()
	cfg.Archival.HotRetentionDays = 30
	cfg.Archival.BatchSize = 2000

	applyWorkerRuntimeConfig(context.Background(), "archival", worker, cfg)

	u := worker.lastUpdate()
	require.NotNil(t, u)
	assert.Equal(t, 30, u.HotRetentionDays)
	assert.Equal(t, 2000, u.BatchSize)
}

func TestApplyWorkerRuntimeConfig_Scheduler(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareSchedulerWorker{}
	cfg := defaultConfig()
	cfg.Scheduler.IntervalSec = 120

	applyWorkerRuntimeConfig(context.Background(), "scheduler", worker, cfg)

	u := worker.lastUpdate()
	require.NotNil(t, u)
	assert.Equal(t, cfg.SchedulerInterval(), u.Interval)
}

func TestApplyWorkerRuntimeConfig_NilConfig_IsNoop(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareExportWorker{}
	assert.NotPanics(t, func() {
		applyWorkerRuntimeConfig(context.Background(), "export", worker, nil)
	})
	assert.Empty(t, worker.lastUpdates())
}

func TestApplyWorkerRuntimeConfig_NilWorker_IsNoop(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	assert.NotPanics(t, func() {
		applyWorkerRuntimeConfig(context.Background(), "export", nil, cfg)
	})
}

func TestApplyWorkerRuntimeConfig_WrongInterface_IsNoop(t *testing.T) {
	t.Parallel()

	// A plain mockWorker does not implement UpdateRuntimeConfig — should be a no-op.
	worker := &mockWorker{}
	cfg := defaultConfig()

	assert.NotPanics(t, func() {
		applyWorkerRuntimeConfig(context.Background(), "export", worker, cfg)
	})
}

// --- archivalPresignExpiryWithContext ---

func TestArchivalPresignExpiryWithContext_NilConfig(t *testing.T) {
	t.Parallel()

	got := archivalPresignExpiryWithContext(context.Background(), nil)
	assert.Equal(t, time.Hour, got)
}

func TestArchivalPresignExpiryWithContext_Zero_DefaultsToOneHour(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Archival.PresignExpirySec = 0

	got := archivalPresignExpiryWithContext(context.Background(), cfg)
	assert.Equal(t, 3600*time.Second, got)
}

func TestArchivalPresignExpiryWithContext_ExceedsMax_CapsToMax(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Archival.PresignExpirySec = 999999
	cfg.Logger = &libLog.NopLogger{}

	got := archivalPresignExpiryWithContext(context.Background(), cfg)
	assert.Equal(t, 604800*time.Second, got)
}

func TestArchivalPresignExpiryWithContext_ValidValue(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Archival.PresignExpirySec = 1800

	got := archivalPresignExpiryWithContext(context.Background(), cfg)
	assert.Equal(t, 1800*time.Second, got)
}
