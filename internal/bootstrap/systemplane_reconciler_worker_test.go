// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface satisfaction check.
var _ ports.BundleReconciler = (*WorkerReconciler)(nil)

func TestWorkerReconciler_ImplementsBundleReconciler(t *testing.T) {
	t.Parallel()

	var rec ports.BundleReconciler = &WorkerReconciler{}
	assert.NotNil(t, rec)
}

func TestNewWorkerReconciler_NilManager(t *testing.T) {
	t.Parallel()

	rec, err := NewWorkerReconciler(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errWorkerReconcilerManagerRequired)
	assert.Nil(t, rec)
}

func TestNewWorkerReconciler_Success(t *testing.T) {
	t.Parallel()

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)

	rec, err := NewWorkerReconciler(wm)

	require.NoError(t, err)
	assert.NotNil(t, rec)
}

func TestWorkerReconciler_Name(t *testing.T) {
	t.Parallel()

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)

	rec, err := NewWorkerReconciler(wm)
	require.NoError(t, err)

	assert.Equal(t, "worker-reconciler", rec.Name())
}

func TestWorkerReconciler_Reconcile_EmptySnapshot(t *testing.T) {
	t.Parallel()

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)

	rec, err := NewWorkerReconciler(wm)
	require.NoError(t, err)

	// Empty snapshot with no registered workers should be a no-op.
	snap := domain.Snapshot{}

	err = rec.Reconcile(context.Background(), nil, nil, snap)

	assert.NoError(t, err)
}

func TestWorkerReconciler_Reconcile_ApplyConfigError(t *testing.T) {
	t.Parallel()

	factoryErr := errors.New("factory: dependency unavailable")

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)

	// Register a slot that is initially disabled (returns false for the initial
	// config) but becomes enabled on the subsequent ApplyConfig call.
	// The factory always returns an error, and the slot is critical so the
	// error propagates through reconcileSlotLocked.
	initialConfig := &Config{
		ExportWorker: ExportWorkerConfig{Enabled: false},
	}

	wm.Register(
		"test-failing-worker",
		func(_ *Config) (WorkerLifecycle, error) {
			return nil, factoryErr
		},
		func(cfg *Config) bool { return cfg.ExportWorker.Enabled },
		func(_ *Config) bool { return true }, // critical
	)

	// Start the WM with the initial config where the slot is disabled.
	err := wm.Start(context.Background(), initialConfig)
	require.NoError(t, err)

	defer func() { _ = wm.Stop() }()

	rec, err := NewWorkerReconciler(wm)
	require.NoError(t, err)

	// Build a snapshot that enables the export_worker, triggering the slot's
	// enable transition which will invoke the failing factory.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"export_worker.enabled":           {Key: "export_worker.enabled", Value: true},
			"export_worker.poll_interval_sec": {Key: "export_worker.poll_interval_sec", Value: 5},
			"export_worker.page_size":         {Key: "export_worker.page_size", Value: 1000},
		},
	}

	err = rec.Reconcile(context.Background(), nil, nil, snap)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "worker reconciler: apply config")
	assert.ErrorIs(t, err, factoryErr)
}

// --- snapshotToWorkerConfig tests ---

func TestSnapshotToWorkerConfig_Defaults(t *testing.T) {
	t.Parallel()

	// An empty snapshot should produce a Config with all default values
	// matching the envDefault tags from the Config struct.
	snap := domain.Snapshot{}

	cfg := snapshotToWorkerConfig(snap)

	require.NotNil(t, cfg)

	// Fetcher defaults
	assert.False(t, cfg.Fetcher.Enabled)
	assert.Equal(t, 60, cfg.Fetcher.DiscoveryIntervalSec)
	assert.Equal(t, "http://localhost:4006", cfg.Fetcher.URL)
	assert.False(t, cfg.Fetcher.AllowPrivateIPs)
	assert.Equal(t, 5, cfg.Fetcher.HealthTimeoutSec)
	assert.Equal(t, 30, cfg.Fetcher.RequestTimeoutSec)
	assert.Equal(t, 300, cfg.Fetcher.SchemaCacheTTLSec)
	assert.Equal(t, 5, cfg.Fetcher.ExtractionPollSec)
	assert.Equal(t, 600, cfg.Fetcher.ExtractionTimeoutSec)

	// Export worker defaults
	assert.True(t, cfg.ExportWorker.Enabled)
	assert.Equal(t, 5, cfg.ExportWorker.PollIntervalSec)
	assert.Equal(t, 1000, cfg.ExportWorker.PageSize)

	// Cleanup worker defaults
	assert.True(t, cfg.CleanupWorker.Enabled)
	assert.Equal(t, 3600, cfg.CleanupWorker.IntervalSec)
	assert.Equal(t, 100, cfg.CleanupWorker.BatchSize)
	assert.Equal(t, 3600, cfg.CleanupWorker.GracePeriodSec)

	// Scheduler defaults
	assert.Equal(t, 60, cfg.Scheduler.IntervalSec)

	// Archival defaults
	assert.False(t, cfg.Archival.Enabled)
	assert.Equal(t, 24, cfg.Archival.IntervalHours)
	assert.Equal(t, 5000, cfg.Archival.BatchSize)
	assert.Equal(t, 90, cfg.Archival.HotRetentionDays)
	assert.Equal(t, 24, cfg.Archival.WarmRetentionMonths)
	assert.Equal(t, 84, cfg.Archival.ColdRetentionMonths)
	assert.Equal(t, "", cfg.Archival.StorageBucket)
	assert.Equal(t, "archives/audit-logs", cfg.Archival.StoragePrefix)
	assert.Equal(t, "GLACIER", cfg.Archival.StorageClass)
	assert.Equal(t, 3, cfg.Archival.PartitionLookahead)
}

func TestSnapshotToWorkerConfig_Overrides(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"export_worker.poll_interval_sec": {Key: "export_worker.poll_interval_sec", Value: 15},
			"export_worker.page_size":         {Key: "export_worker.page_size", Value: 500},
			"cleanup_worker.interval_sec":     {Key: "cleanup_worker.interval_sec", Value: 7200},
			"scheduler.interval_sec":          {Key: "scheduler.interval_sec", Value: 120},
			"archival.batch_size":             {Key: "archival.batch_size", Value: 10000},
			"archival.storage_bucket":         {Key: "archival.storage_bucket", Value: "my-bucket"},
		},
	}

	cfg := snapshotToWorkerConfig(snap)

	require.NotNil(t, cfg)

	// Overridden values
	assert.Equal(t, 15, cfg.ExportWorker.PollIntervalSec)
	assert.Equal(t, 500, cfg.ExportWorker.PageSize)
	assert.Equal(t, 7200, cfg.CleanupWorker.IntervalSec)
	assert.Equal(t, 120, cfg.Scheduler.IntervalSec)
	assert.Equal(t, 10000, cfg.Archival.BatchSize)
	assert.Equal(t, "my-bucket", cfg.Archival.StorageBucket)

	// Non-overridden values remain at defaults
	assert.True(t, cfg.ExportWorker.Enabled)
	assert.Equal(t, 100, cfg.CleanupWorker.BatchSize)
	assert.Equal(t, "GLACIER", cfg.Archival.StorageClass)
}

func TestSnapshotToWorkerConfig_FetcherEnabled(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"fetcher.enabled":                {Key: "fetcher.enabled", Value: true},
			"fetcher.discovery_interval_sec": {Key: "fetcher.discovery_interval_sec", Value: 30},
		},
	}

	cfg := snapshotToWorkerConfig(snap)

	require.NotNil(t, cfg)
	assert.True(t, cfg.Fetcher.Enabled)
	assert.Equal(t, 30, cfg.Fetcher.DiscoveryIntervalSec)
}

func TestSnapshotToWorkerConfig_AllWorkerSections(t *testing.T) {
	t.Parallel()

	// Provide at least one override for each of the 5 worker config sections
	// to verify that all sections are populated from the snapshot.
	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			// Fetcher section
			"fetcher.enabled":                {Key: "fetcher.enabled", Value: true},
			"fetcher.discovery_interval_sec": {Key: "fetcher.discovery_interval_sec", Value: 45},
			// Export section
			"export_worker.enabled":           {Key: "export_worker.enabled", Value: false},
			"export_worker.poll_interval_sec": {Key: "export_worker.poll_interval_sec", Value: 10},
			// Cleanup section
			"cleanup_worker.enabled":      {Key: "cleanup_worker.enabled", Value: false},
			"cleanup_worker.interval_sec": {Key: "cleanup_worker.interval_sec", Value: 1800},
			// Scheduler section
			"scheduler.interval_sec": {Key: "scheduler.interval_sec", Value: 90},
			// Archival section
			"archival.enabled":        {Key: "archival.enabled", Value: true},
			"archival.interval_hours": {Key: "archival.interval_hours", Value: 12},
		},
	}

	cfg := snapshotToWorkerConfig(snap)

	require.NotNil(t, cfg)

	// Fetcher
	assert.True(t, cfg.Fetcher.Enabled)
	assert.Equal(t, 45, cfg.Fetcher.DiscoveryIntervalSec)

	// Export
	assert.False(t, cfg.ExportWorker.Enabled)
	assert.Equal(t, 10, cfg.ExportWorker.PollIntervalSec)

	// Cleanup
	assert.False(t, cfg.CleanupWorker.Enabled)
	assert.Equal(t, 1800, cfg.CleanupWorker.IntervalSec)

	// Scheduler
	assert.Equal(t, 90, cfg.Scheduler.IntervalSec)

	// Archival
	assert.True(t, cfg.Archival.Enabled)
	assert.Equal(t, 12, cfg.Archival.IntervalHours)
}

// --- snapInt / snapBool / snapString helper tests ---
//
// These test the canonical helpers in systemplane_factory.go which are now
// shared by both the BundleFactory and the WorkerReconciler. The factory
// versions provide more robust type coercion (e.g., string→int, string→bool).

func TestSnapInt_TypeConversions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    any
		fallback int
		want     int
	}{
		{name: "int value", value: 42, fallback: 0, want: 42},
		{name: "int64 value", value: int64(99), fallback: 0, want: 99},
		{name: "float64 whole number", value: float64(100), fallback: 0, want: 100},
		{name: "float64 fractional", value: 3.14, fallback: 7, want: 3},
		{name: "string numeric", value: "123", fallback: 5, want: 123},
		{name: "string non-numeric uses fallback", value: "not-a-number", fallback: 5, want: 5},
		{name: "nil value uses fallback", value: nil, fallback: 10, want: 10},
		{name: "bool value uses fallback", value: true, fallback: 1, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := domain.Snapshot{}
			if tt.value != nil {
				snap.Configs = map[string]domain.EffectiveValue{
					"test.key": {Key: "test.key", Value: tt.value},
				}
			}

			got := snapInt(snap, "test.key", tt.fallback)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSnapBool_TypeConversions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    any
		fallback bool
		want     bool
	}{
		{name: "true value", value: true, fallback: false, want: true},
		{name: "false value", value: false, fallback: true, want: false},
		{name: "string true", value: "true", fallback: false, want: true},
		{name: "string TRUE", value: "TRUE", fallback: false, want: true},
		{name: "string 1", value: "1", fallback: false, want: true},
		{name: "string false", value: "false", fallback: true, want: false},
		{name: "int value uses fallback", value: 1, fallback: true, want: true},
		{name: "absent key uses fallback true", value: nil, fallback: true, want: true},
		{name: "absent key uses fallback false", value: nil, fallback: false, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := domain.Snapshot{}
			if tt.value != nil {
				snap.Configs = map[string]domain.EffectiveValue{
					"test.key": {Key: "test.key", Value: tt.value},
				}
			}

			got := snapBool(snap, "test.key", tt.fallback)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSnapString_TypeConversions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    any
		fallback string
		want     string
	}{
		{name: "string value", value: "hello", fallback: "default", want: "hello"},
		{name: "empty string", value: "", fallback: "default", want: ""},
		{name: "int value stringified", value: 42, fallback: "default", want: "42"},
		{name: "bool value stringified", value: true, fallback: "default", want: "true"},
		{name: "absent key uses fallback", value: nil, fallback: "default", want: "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap := domain.Snapshot{}
			if tt.value != nil {
				snap.Configs = map[string]domain.EffectiveValue{
					"test.key": {Key: "test.key", Value: tt.value},
				}
			}

			got := snapString(snap, "test.key", tt.fallback)
			assert.Equal(t, tt.want, got)
		})
	}
}
