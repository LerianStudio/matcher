// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
)

// --- Test doubles for cleanup, archival, and scheduler workers ---

type runtimeAwareCleanupWorker struct {
	mockWorker
	mu      sync.Mutex
	updates []reportingWorker.CleanupWorkerConfig
}

func (w *runtimeAwareCleanupWorker) UpdateRuntimeConfig(cfg reportingWorker.CleanupWorkerConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.updates = append(w.updates, cfg)

	return nil
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
	mu             sync.Mutex
	updates        []governanceWorker.ArchivalWorkerConfig
	storageUpdates []objectstorage.Backend
}

func (w *runtimeAwareArchivalWorker) UpdateRuntimeConfig(cfg governanceWorker.ArchivalWorkerConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.updates = append(w.updates, cfg)

	return nil
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

func (w *runtimeAwareArchivalWorker) UpdateRuntimeStorage(storage objectstorage.Backend) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.storageUpdates = append(w.storageUpdates, storage)

	return nil
}

func (w *runtimeAwareArchivalWorker) lastStorageUpdate() objectstorage.Backend {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.storageUpdates) == 0 {
		return nil
	}

	return w.storageUpdates[len(w.storageUpdates)-1]
}

type runtimeAwareSchedulerWorker struct {
	mockWorker
	mu      sync.Mutex
	updates []configWorker.SchedulerWorkerConfig
}

func (w *runtimeAwareSchedulerWorker) UpdateRuntimeConfig(cfg configWorker.SchedulerWorkerConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.updates = append(w.updates, cfg)

	return nil
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

type runtimeAwareDiscoveryWorker struct {
	mockWorker
	mu      sync.Mutex
	updates []discoveryWorker.DiscoveryWorkerConfig
}

func (w *runtimeAwareDiscoveryWorker) UpdateRuntimeConfig(cfg discoveryWorker.DiscoveryWorkerConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.updates = append(w.updates, cfg)
}

func (w *runtimeAwareDiscoveryWorker) lastUpdate() *discoveryWorker.DiscoveryWorkerConfig {
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
		{"export", false, exportWorkerComparableConfig{PollIntervalSec: cfg.ExportWorker.PollIntervalSec, PageSize: cfg.ExportWorker.PageSize}},
		{"cleanup", false, cleanupWorkerComparableConfig{IntervalSec: cfg.CleanupWorker.IntervalSec, BatchSize: cfg.CleanupWorker.BatchSize, GracePeriodSec: cfg.CleanupWorker.GracePeriodSec}},
		{"archival", false, archivalWorkerComparableConfig{IntervalHours: cfg.Archival.IntervalHours, HotRetentionDays: cfg.Archival.HotRetentionDays, WarmRetentionMonths: cfg.Archival.WarmRetentionMonths, ColdRetentionMonths: cfg.Archival.ColdRetentionMonths, BatchSize: cfg.Archival.BatchSize, StorageBucket: cfg.Archival.StorageBucket, StoragePrefix: cfg.Archival.StoragePrefix, StorageClass: cfg.Archival.StorageClass, PartitionLookahead: cfg.Archival.PartitionLookahead, StorageEndpoint: cfg.ObjectStorage.Endpoint, StorageRegion: cfg.ObjectStorage.Region, StorageAccessKeyID: cfg.ObjectStorage.AccessKeyID, StorageSecretKey: cfg.ObjectStorage.SecretAccessKey, StorageUsePathStyle: cfg.ObjectStorage.UsePathStyle}},
		{"scheduler", false, schedulerWorkerComparableConfig{IntervalSec: cfg.Scheduler.IntervalSec}},
		{"discovery", false, discoveryWorkerRuntimeConfig{Interval: cfg.FetcherDiscoveryInterval()}},
		{"fetcher_bridge", false, fetcherBridgeWorkerComparableConfig{
			IntervalSec:       cfg.Fetcher.BridgeIntervalSec,
			BatchSize:         cfg.Fetcher.BridgeBatchSize,
			TenantConcurrency: cfg.Fetcher.BridgeTenantConcurrency,
			RetryMaxAttempts:  cfg.Fetcher.BridgeRetryMaxAttempts,
		}},
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

func TestWorkerConfigChanged_ExportPresignExpiryOnly_ReturnsFalse(t *testing.T) {
	t.Parallel()

	old := defaultConfig()
	newCfg := defaultConfig()
	newCfg.ExportWorker.PresignExpirySec = old.ExportWorker.PresignExpirySec + 300

	assert.False(t, workerConfigChanged("export", old, newCfg))
}

func TestWorkerConfigChanged_ArchivalPresignExpiryOnly_ReturnsFalse(t *testing.T) {
	t.Parallel()

	old := defaultConfig()
	newCfg := defaultConfig()
	newCfg.Archival.PresignExpirySec = old.Archival.PresignExpirySec + 300

	assert.False(t, workerConfigChanged("archival", old, newCfg))
}

func TestWorkerConfigChanged_ArchivalObjectStorageChange_ReturnsTrue(t *testing.T) {
	t.Parallel()

	old := defaultConfig()
	newCfg := defaultConfig()
	newCfg.ObjectStorage.Endpoint = "http://updated-storage:9000"

	assert.True(t, workerConfigChanged("archival", old, newCfg))
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

	require.NoError(t, applyWorkerRuntimeConfig(context.Background(), "export", worker, cfg))

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

	require.NoError(t, applyWorkerRuntimeConfig(context.Background(), "cleanup", worker, cfg))

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
	cfg.Archival.StorageBucket = "archive-bucket"
	cfg.ObjectStorage.Endpoint = "http://localhost:8333"

	originalNewS3Client := newS3ClientFn
	t.Cleanup(func() { newS3ClientFn = originalNewS3Client })

	newS3ClientFn = func(context.Context, reportingStorage.S3Config) (*reportingStorage.S3Client, error) {
		return &reportingStorage.S3Client{}, nil
	}

	require.NoError(t, applyWorkerRuntimeConfig(context.Background(), "archival", worker, cfg))

	u := worker.lastUpdate()
	require.NotNil(t, u)
	assert.Equal(t, 30, u.HotRetentionDays)
	assert.Equal(t, 2000, u.BatchSize)
	assert.NotNil(t, worker.lastStorageUpdate())
}

func TestApplyWorkerRuntimeConfig_Scheduler(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareSchedulerWorker{}
	cfg := defaultConfig()
	cfg.Scheduler.IntervalSec = 120

	require.NoError(t, applyWorkerRuntimeConfig(context.Background(), "scheduler", worker, cfg))

	u := worker.lastUpdate()
	require.NotNil(t, u)
	assert.Equal(t, cfg.SchedulerInterval(), u.Interval)
}

func TestApplyWorkerRuntimeConfig_Discovery(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareDiscoveryWorker{}
	cfg := defaultConfig()
	cfg.Fetcher.DiscoveryIntervalSec = 45

	require.NoError(t, applyWorkerRuntimeConfig(context.Background(), "discovery", worker, cfg))

	u := worker.lastUpdate()
	require.NotNil(t, u)
	assert.Equal(t, cfg.FetcherDiscoveryInterval(), u.Interval)
}

func TestApplyWorkerRuntimeConfig_NilConfig_IsNoop(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareExportWorker{}
	assert.NotPanics(t, func() {
		require.NoError(t, applyWorkerRuntimeConfig(context.Background(), "export", worker, nil))
	})
	assert.Empty(t, worker.lastUpdates())
}

func TestApplyWorkerRuntimeConfig_NilWorker_IsNoop(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	assert.NotPanics(t, func() {
		require.NoError(t, applyWorkerRuntimeConfig(context.Background(), "export", nil, cfg))
	})
}

func TestApplyWorkerRuntimeConfig_WrongInterface_IsNoop(t *testing.T) {
	t.Parallel()

	// A plain mockWorker does not implement UpdateRuntimeConfig — should be a no-op.
	worker := &mockWorker{}
	cfg := defaultConfig()

	assert.NotPanics(t, func() {
		require.NoError(t, applyWorkerRuntimeConfig(context.Background(), "export", worker, cfg))
	})
}

// --- Fetcher bridge runtime reconciliation (Fix 4) ---

// runtimeAwareFetcherBridgeWorker is a test double satisfying the bridge
// worker's UpdateRuntimeConfig contract. Captures every call so the test
// can assert that applyFetcherBridgeRuntimeConfig forwards the cfg knobs
// faithfully.
type runtimeAwareFetcherBridgeWorker struct {
	mockWorker
	mu      sync.Mutex
	updates []discoveryWorker.BridgeWorkerConfig
	err     error
}

func (w *runtimeAwareFetcherBridgeWorker) UpdateRuntimeConfig(cfg discoveryWorker.BridgeWorkerConfig) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.updates = append(w.updates, cfg)

	return w.err
}

func (w *runtimeAwareFetcherBridgeWorker) lastUpdate() *discoveryWorker.BridgeWorkerConfig {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.updates) == 0 {
		return nil
	}

	u := w.updates[len(w.updates)-1]

	return &u
}

// TestApplyFetcherBridgeRuntimeConfig_UpdatesWorkerOnMutation exercises Fix 4:
// when the systemplane reconciler hands a Config carrying new bridge knobs
// to applyWorkerRuntimeConfig, the call must reach UpdateRuntimeConfig with
// the corresponding values. Pre-fix this code path was missing the
// "fetcher_bridge" case and operators changing the keys via systemplane saw
// audit logs but the worker silently kept its old values.
func TestApplyFetcherBridgeRuntimeConfig_UpdatesWorkerOnMutation(t *testing.T) {
	t.Parallel()

	worker := &runtimeAwareFetcherBridgeWorker{}
	cfg := defaultConfig()
	cfg.Fetcher.BridgeIntervalSec = 75
	cfg.Fetcher.BridgeBatchSize = 25
	cfg.Fetcher.BridgeTenantConcurrency = 8

	require.NoError(t, applyWorkerRuntimeConfig(context.Background(), workerNameFetcherBridge, worker, cfg))

	last := worker.lastUpdate()
	require.NotNil(t, last, "applyFetcherBridgeRuntimeConfig must forward to UpdateRuntimeConfig")
	assert.Equal(t, cfg.FetcherBridgeInterval(), last.Interval,
		"interval must be derived via FetcherBridgeInterval helper for default-coalescing")
	assert.Equal(t, cfg.FetcherBridgeBatchSize(), last.BatchSize,
		"batch size must be derived via FetcherBridgeBatchSize helper for default-coalescing")
	assert.Equal(t, cfg.FetcherBridgeTenantConcurrency(), last.TenantConcurrency,
		"tenant concurrency must be derived via FetcherBridgeTenantConcurrency helper for default-coalescing")
}
