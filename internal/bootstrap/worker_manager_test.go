// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
)

// mockWorker is a test double satisfying WorkerLifecycle.
type mockWorker struct {
	mu       sync.Mutex
	started  atomic.Int32
	stopped  atomic.Int32
	startErr error
	stopErr  error
}

type panicWorker struct{}

type blockingWorker struct {
	startCalled chan struct{}
	startGate   chan struct{}
	stopCalled  chan struct{}
	stopGate    chan struct{}
}

type contextTrackingWorker struct {
	canceled chan struct{}
	once     sync.Once
}

func (panicWorker) Start(_ context.Context) error {
	panic("boom")
}

func (panicWorker) Stop() error {
	return nil
}

func (worker *blockingWorker) Start(_ context.Context) error {
	if worker.startCalled != nil {
		close(worker.startCalled)
	}
	if worker.startGate != nil {
		<-worker.startGate
	}

	return nil
}

func (worker *blockingWorker) Stop() error {
	if worker.stopCalled != nil {
		close(worker.stopCalled)
	}
	if worker.stopGate != nil {
		<-worker.stopGate
	}

	return nil
}

func (worker *contextTrackingWorker) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		worker.once.Do(func() {
			close(worker.canceled)
		})
	}()

	return nil
}

func (worker *contextTrackingWorker) Stop() error {
	return nil
}

type runtimeAwareExportWorker struct {
	mockWorker

	seqMu         sync.Mutex
	sequence      []string
	updates       []reportingWorker.ExportWorkerConfig
	failNextStart atomic.Bool
}

func (worker *runtimeAwareExportWorker) UpdateRuntimeConfig(cfg reportingWorker.ExportWorkerConfig) error {
	worker.seqMu.Lock()
	defer worker.seqMu.Unlock()

	worker.sequence = append(worker.sequence, "update")
	worker.updates = append(worker.updates, cfg)

	return nil
}

func (worker *runtimeAwareExportWorker) Start(ctx context.Context) error {
	worker.seqMu.Lock()
	worker.sequence = append(worker.sequence, "start")
	fail := worker.failNextStart.Swap(false)
	worker.seqMu.Unlock()

	if fail {
		return errors.New("start failed")
	}

	return worker.mockWorker.Start(ctx)
}

func (worker *runtimeAwareExportWorker) Stop() error {
	worker.seqMu.Lock()
	worker.sequence = append(worker.sequence, "stop")
	worker.seqMu.Unlock()

	return worker.mockWorker.Stop()
}

func (worker *runtimeAwareExportWorker) events() []string {
	worker.seqMu.Lock()
	defer worker.seqMu.Unlock()

	cloned := make([]string, len(worker.sequence))
	copy(cloned, worker.sequence)

	return cloned
}

func (worker *runtimeAwareExportWorker) lastUpdates() []reportingWorker.ExportWorkerConfig {
	worker.seqMu.Lock()
	defer worker.seqMu.Unlock()

	cloned := make([]reportingWorker.ExportWorkerConfig, len(worker.updates))
	copy(cloned, worker.updates)

	return cloned
}

func (w *mockWorker) Start(_ context.Context) error {
	if w.startErr != nil {
		return w.startErr
	}

	w.started.Add(1)

	return nil
}

func (w *mockWorker) Stop() error {
	if w.stopErr != nil {
		return w.stopErr
	}

	w.stopped.Add(1)

	return nil
}

func (w *mockWorker) startCount() int { return int(w.started.Load()) }
func (w *mockWorker) stopCount() int  { return int(w.stopped.Load()) }

func runningWorkerNames(wm *WorkerManager) []string {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	names := make([]string, 0, len(wm.slots))
	for _, slot := range wm.slots {
		if !isNilWorkerLifecycle(slot.instance) {
			names = append(names, slot.name)
		}
	}

	return names
}

// alwaysEnabled returns a config predicate that is always true.
func alwaysEnabled(_ *Config) bool { return true }

// alwaysDisabled returns a config predicate that is always false.
func alwaysDisabled(_ *Config) bool { return false }

// neverCritical returns a config predicate that marks the worker non-critical.
func neverCritical(_ *Config) bool { return false }

// alwaysCritical returns a config predicate that marks the worker as critical.
func alwaysCritical(_ *Config) bool { return true }

// newTestConfig creates a minimal valid Config for worker manager tests.
func newTestConfig() *Config {
	return &Config{
		App:    AppConfig{EnvName: "test"},
		Server: ServerConfig{Address: ":4018"},
		ExportWorker: ExportWorkerConfig{
			Enabled:         true,
			PollIntervalSec: 5,
			PageSize:        1000,
		},
		CleanupWorker: CleanupWorkerConfig{
			Enabled:     true,
			IntervalSec: 3600,
			BatchSize:   100,
		},
	}
}

func TestWorkerManager_StartStop(t *testing.T) {
	t.Parallel()

	t.Run("starts enabled workers and stops them", func(t *testing.T) {
		t.Parallel()

		worker1 := &mockWorker{}
		worker2 := &mockWorker{}
		logger := &libLog.NopLogger{}

		wm := NewWorkerManager(logger, nil)
		wm.Register("w1", func(_ *Config) (WorkerLifecycle, error) {
			return worker1, nil
		}, alwaysEnabled, neverCritical)
		wm.Register("w2", func(_ *Config) (WorkerLifecycle, error) {
			return worker2, nil
		}, alwaysEnabled, neverCritical)

		cfg := newTestConfig()

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		assert.Equal(t, 1, worker1.startCount())
		assert.Equal(t, 1, worker2.startCount())

		running := runningWorkerNames(wm)
		assert.Len(t, running, 2)
		assert.Contains(t, running, "w1")
		assert.Contains(t, running, "w2")

		err = wm.Stop()
		require.NoError(t, err)

		assert.Equal(t, 1, worker1.stopCount())
		assert.Equal(t, 1, worker2.stopCount())
	})

	t.Run("applies runtime config before initial discovery start", func(t *testing.T) {
		t.Parallel()

		worker := &runtimeAwareDiscoveryWorker{}
		logger := &libLog.NopLogger{}
		wm := NewWorkerManager(logger, nil)
		wm.Register("discovery", func(_ *Config) (WorkerLifecycle, error) {
			return worker, nil
		}, alwaysEnabled, neverCritical)

		cfg := newTestConfig()
		cfg.Fetcher.DiscoveryIntervalSec = 123

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)
		require.NotNil(t, worker.lastUpdate())
		assert.Equal(t, 123*time.Second, worker.lastUpdate().Interval)
		assert.Equal(t, 1, worker.startCount())

		err = wm.Stop()
		require.NoError(t, err)
	})

	t.Run("does not start disabled workers", func(t *testing.T) {
		t.Parallel()

		worker1 := &mockWorker{}
		logger := &libLog.NopLogger{}

		wm := NewWorkerManager(logger, nil)
		wm.Register("disabled", func(_ *Config) (WorkerLifecycle, error) {
			return worker1, nil
		}, alwaysDisabled, neverCritical)

		cfg := newTestConfig()

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		assert.Equal(t, 0, worker1.startCount())
		assert.Empty(t, runningWorkerNames(wm))

		err = wm.Stop()
		require.NoError(t, err)
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		t.Parallel()

		worker1 := &mockWorker{}
		logger := &libLog.NopLogger{}

		wm := NewWorkerManager(logger, nil)
		wm.Register("w1", func(_ *Config) (WorkerLifecycle, error) {
			return worker1, nil
		}, alwaysEnabled, neverCritical)

		cfg := newTestConfig()

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		err = wm.Stop()
		require.NoError(t, err)

		// Second stop should be a no-op.
		err = wm.Stop()
		require.NoError(t, err)

		// Worker Stop() called exactly once (first Stop).
		assert.Equal(t, 1, worker1.stopCount())
	})

	t.Run("start is idempotent when already running", func(t *testing.T) {
		t.Parallel()

		callCount := atomic.Int32{}
		logger := &libLog.NopLogger{}

		wm := NewWorkerManager(logger, nil)
		wm.Register("w1", func(_ *Config) (WorkerLifecycle, error) {
			callCount.Add(1)
			return &mockWorker{}, nil
		}, alwaysEnabled, neverCritical)

		cfg := newTestConfig()

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		// Second start should be a no-op.
		err = wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		assert.Equal(t, int32(1), callCount.Load())

		require.NoError(t, wm.Stop())
	})

	t.Run("worker start panic returns error instead of escaping", func(t *testing.T) {
		t.Parallel()

		logger := &libLog.NopLogger{}

		wm := NewWorkerManager(logger, nil)
		wm.Register("panic-worker", func(_ *Config) (WorkerLifecycle, error) {
			return panicWorker{}, nil
		}, alwaysEnabled, alwaysCritical)

		err := wm.Start(context.Background(), newTestConfig())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "panic-worker")
		assert.Contains(t, err.Error(), "worker start panicked: boom")
		assert.False(t, wm.running)
	})
}

func TestStartWorkerWithTimeout_DoesNotCancelWorkerContextAfterStart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := &contextTrackingWorker{canceled: make(chan struct{})}

	require.NoError(t, startWorkerWithTimeout(ctx, worker))

	select {
	case <-worker.canceled:
		t.Fatal("worker context canceled immediately after successful start")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()

	require.Eventually(t, func() bool {
		select {
		case <-worker.canceled:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestWorkerManager_CriticalWorkerFailure(t *testing.T) {
	t.Parallel()

	t.Run("returns error when critical worker has nil factory", func(t *testing.T) {
		t.Parallel()

		wm := NewWorkerManager(&libLog.NopLogger{}, nil)
		wm.Register("critical-nil-factory", nil, alwaysEnabled, alwaysCritical)

		err := wm.Start(context.Background(), newTestConfig())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "worker factory is required")
	})

	t.Run("returns error when critical worker fails to start", func(t *testing.T) {
		t.Parallel()

		logger := &libLog.NopLogger{}

		wm := NewWorkerManager(logger, nil)
		wm.Register("critical-fail", func(_ *Config) (WorkerLifecycle, error) {
			return &mockWorker{startErr: errors.New("boom")}, nil
		}, alwaysEnabled, alwaysCritical)

		cfg := newTestConfig()

		err := wm.Start(context.Background(), cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "critical worker")
		assert.Contains(t, err.Error(), "boom")
	})

	t.Run("non-critical worker failure does not block startup", func(t *testing.T) {
		t.Parallel()

		goodWorker := &mockWorker{}
		logger := &libLog.NopLogger{}

		wm := NewWorkerManager(logger, nil)
		wm.Register("failing", func(_ *Config) (WorkerLifecycle, error) {
			return nil, errors.New("factory error")
		}, alwaysEnabled, neverCritical)
		wm.Register("good", func(_ *Config) (WorkerLifecycle, error) {
			return goodWorker, nil
		}, alwaysEnabled, neverCritical)

		cfg := newTestConfig()

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		assert.Equal(t, 1, goodWorker.startCount())

		require.NoError(t, wm.Stop())
	})
}

func TestWorkerManager_ConfigChange(t *testing.T) {
	t.Parallel()

	t.Run("restarts worker when config changes", func(t *testing.T) {
		t.Parallel()

		createCount := atomic.Int32{}
		workers := make([]*mockWorker, 0, 2)
		workerMu := sync.Mutex{}
		logger := &libLog.NopLogger{}

		cfg := newTestConfig()
		cm := newWorkerMgrTestConfigManager(t, cfg)

		wm := NewWorkerManager(logger, cm)
		wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
			createCount.Add(1)
			w := &mockWorker{}
			workerMu.Lock()
			workers = append(workers, w)
			workerMu.Unlock()

			return w, nil
		}, alwaysEnabled, neverCritical)

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		assert.Equal(t, int32(1), createCount.Load())

		// Simulate a config change via the subscriber callback.
		newCfg := newTestConfig()
		newCfg.ExportWorker.PollIntervalSec = 30 // changed!
		wm.onConfigChange(newCfg)

		assert.Equal(t, int32(2), createCount.Load())

		workerMu.Lock()
		assert.Len(t, workers, 2)
		assert.Equal(t, 1, workers[0].stopCount(), "first worker should be stopped")
		assert.Equal(t, 1, workers[1].startCount(), "second worker should be started")
		workerMu.Unlock()

		require.NoError(t, wm.Stop())
	})

	t.Run("does not restart worker when config is unchanged", func(t *testing.T) {
		t.Parallel()

		createCount := atomic.Int32{}
		logger := &libLog.NopLogger{}

		cfg := newTestConfig()

		wm := NewWorkerManager(logger, nil)
		wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
			createCount.Add(1)

			return &mockWorker{}, nil
		}, alwaysEnabled, neverCritical)

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		// Same config — no restart.
		sameCfg := newTestConfig()
		wm.onConfigChange(sameCfg)

		assert.Equal(t, int32(1), createCount.Load())

		require.NoError(t, wm.Stop())
	})

	t.Run("ignores startup-only enable toggle at runtime", func(t *testing.T) {
		t.Parallel()

		worker1 := &mockWorker{}
		logger := &testLogger{}

		cfg := newTestConfig()
		cfg.ExportWorker.Enabled = false

		wm := NewWorkerManager(logger, nil)
		wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
			return worker1, nil
		}, func(currentCfg *Config) bool {
			return currentCfg != nil && currentCfg.ExportWorker.Enabled
		}, neverCritical)

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		// Worker not started because disabled.
		assert.Equal(t, 0, worker1.startCount())

		// Enabling at runtime is startup-only and should be ignored.
		newCfg := newTestConfig()
		newCfg.ExportWorker.Enabled = true
		wm.onConfigChange(newCfg)

		assert.Equal(t, 0, worker1.startCount())
		assert.Empty(t, runningWorkerNames(wm))

		require.NoError(t, wm.Stop())
	})

	t.Run("ignores startup-only disable toggle at runtime", func(t *testing.T) {
		t.Parallel()

		worker1 := &mockWorker{}
		logger := &libLog.NopLogger{}

		enabledByConfig := func(cfg *Config) bool {
			return cfg.ExportWorker.Enabled
		}

		cfg := newTestConfig()
		cfg.ExportWorker.Enabled = true

		wm := NewWorkerManager(logger, nil)
		wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
			return worker1, nil
		}, enabledByConfig, neverCritical)

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		assert.Equal(t, 1, worker1.startCount())

		// Disabling at runtime is startup-only and should be ignored.
		newCfg := newTestConfig()
		newCfg.ExportWorker.Enabled = false
		wm.onConfigChange(newCfg)

		assert.Equal(t, 0, worker1.stopCount())
		assert.Equal(t, []string{"export"}, runningWorkerNames(wm))

		require.NoError(t, wm.Stop())
	})

	t.Run("config change while not running is a no-op", func(t *testing.T) {
		t.Parallel()

		logger := &libLog.NopLogger{}
		wm := NewWorkerManager(logger, nil)

		// Should not panic.
		wm.onConfigChange(newTestConfig())
	})

	t.Run("nil config change is a no-op", func(t *testing.T) {
		t.Parallel()

		logger := &libLog.NopLogger{}

		cfg := newTestConfig()
		wm := NewWorkerManager(logger, nil)
		wm.Register("w1", func(_ *Config) (WorkerLifecycle, error) {
			return &mockWorker{}, nil
		}, alwaysEnabled, neverCritical)

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		// nil config should be a no-op.
		wm.onConfigChange(nil)

		require.NoError(t, wm.Stop())
	})

	t.Run("restart failure does not crash manager", func(t *testing.T) {
		t.Parallel()

		callCount := atomic.Int32{}
		logger := &testLogger{}

		cfg := newTestConfig()

		wm := NewWorkerManager(logger, nil)
		wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
			n := callCount.Add(1)
			if n == 2 {
				// Second creation (restart) fails.
				return nil, errors.New("factory failed on restart")
			}

			return &mockWorker{}, nil
		}, alwaysEnabled, neverCritical)

		err := wm.Start(context.Background(), cfg)
		require.NoError(t, err)

		// Trigger restart — factory will fail.
		newCfg := newTestConfig()
		newCfg.ExportWorker.PollIntervalSec = 99
		wm.onConfigChange(newCfg)

		// Manager should still be running and keep the previous worker instance.
		assert.True(t, wm.running)
		assert.Equal(t, []string{"export"}, runningWorkerNames(wm))

		require.NoError(t, wm.Stop())
	})
}

func TestWorkerManager_NilLogger(t *testing.T) {
	t.Parallel()

	wm := NewWorkerManager(nil, nil)
	assert.NotNil(t, wm.logger)
}

func TestWorkerManager_FactoryReturnsNil(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}

	wm := NewWorkerManager(logger, nil)
	wm.Register("nilworker", func(_ *Config) (WorkerLifecycle, error) {
		return nil, nil // dependency unavailable
	}, alwaysEnabled, neverCritical)

	cfg := defaultConfig()

	err := wm.Start(context.Background(), cfg)
	require.NoError(t, err)

	assert.Empty(t, runningWorkerNames(wm))

	require.NoError(t, wm.Stop())
}

func TestWorkerManager_FactoryReturnsTypedNil(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}

	wm := NewWorkerManager(logger, nil)
	wm.Register("typednil", func(_ *Config) (WorkerLifecycle, error) {
		var worker *mockWorker
		return worker, nil
	}, alwaysEnabled, alwaysCritical)

	err := wm.Start(context.Background(), newTestConfig())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "worker dependency unavailable")
}

func TestWorkerConfigChanged(t *testing.T) {
	t.Parallel()

	t.Run("detects export worker config change", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.ExportWorker.PollIntervalSec = 99

		assert.True(t, workerConfigChanged("export", old, new_))
	})

	t.Run("ignores export presign expiry only change", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.ExportWorker.PresignExpirySec = old.ExportWorker.PresignExpirySec + 300

		assert.False(t, workerConfigChanged("export", old, new_))
	})

	t.Run("no change returns false", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()

		assert.False(t, workerConfigChanged("export", old, new_))
	})

	t.Run("nil old config returns true", func(t *testing.T) {
		t.Parallel()

		assert.True(t, workerConfigChanged("export", nil, newTestConfig()))
	})

	t.Run("unknown worker returns true", func(t *testing.T) {
		t.Parallel()

		assert.True(t, workerConfigChanged("unknown", newTestConfig(), newTestConfig()))
	})

	t.Run("detects cleanup worker config change", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.CleanupWorker.BatchSize = 500

		assert.True(t, workerConfigChanged("cleanup", old, new_))
	})

	t.Run("detects archival config change", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.Archival.BatchSize = 10000

		assert.True(t, workerConfigChanged("archival", old, new_))
	})

	t.Run("ignores archival presign expiry only change", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.Archival.PresignExpirySec = old.Archival.PresignExpirySec + 300

		assert.False(t, workerConfigChanged("archival", old, new_))
	})

	t.Run("detects scheduler config change", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.Scheduler.IntervalSec = 120

		assert.True(t, workerConfigChanged("scheduler", old, new_))
	})

	t.Run("detects discovery (fetcher) config change", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.Fetcher.DiscoveryIntervalSec = 120

		assert.True(t, workerConfigChanged("discovery", old, new_))
	})

	t.Run("ignores non-runtime discovery config changes", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.Fetcher.SchemaCacheTTLSec = old.Fetcher.SchemaCacheTTLSec + 10

		assert.False(t, workerConfigChanged("discovery", old, new_))
	})

	t.Run("ignores discovery enabled flips", func(t *testing.T) {
		t.Parallel()

		old := newTestConfig()
		new_ := newTestConfig()
		new_.Fetcher.Enabled = !old.Fetcher.Enabled

		assert.False(t, workerConfigChanged("discovery", old, new_))
	})
}

func TestExtractWorkerConfig(t *testing.T) {
	t.Parallel()

	cfg := newTestConfig()

	t.Run("returns ExportWorkerConfig for export", func(t *testing.T) {
		t.Parallel()

		result := extractWorkerConfig("export", cfg)
		_, ok := result.(exportWorkerComparableConfig)

		assert.True(t, ok)
	})

	t.Run("returns CleanupWorkerConfig for cleanup", func(t *testing.T) {
		t.Parallel()

		result := extractWorkerConfig("cleanup", cfg)
		_, ok := result.(cleanupWorkerComparableConfig)

		assert.True(t, ok)
	})

	t.Run("returns ArchivalConfig for archival", func(t *testing.T) {
		t.Parallel()

		result := extractWorkerConfig("archival", cfg)
		_, ok := result.(archivalWorkerComparableConfig)

		assert.True(t, ok)
	})

	t.Run("returns SchedulerConfig for scheduler", func(t *testing.T) {
		t.Parallel()

		result := extractWorkerConfig("scheduler", cfg)
		_, ok := result.(schedulerWorkerComparableConfig)

		assert.True(t, ok)
	})

	t.Run("returns discovery runtime config for discovery", func(t *testing.T) {
		t.Parallel()

		result := extractWorkerConfig("discovery", cfg)
		runtimeCfg, ok := result.(discoveryWorkerRuntimeConfig)

		assert.True(t, ok)
		assert.Equal(t, cfg.FetcherDiscoveryInterval(), runtimeCfg.Interval)
	})

	t.Run("returns nil for unknown worker", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, extractWorkerConfig("unknown", cfg))
	})
}

func TestWorkerManager_StopErrorHandledGracefully(t *testing.T) {
	t.Parallel()

	goodWorker := &mockWorker{}
	failingWorker := &mockWorker{stopErr: errors.New("stop failed")}
	logger := &libLog.NopLogger{}

	wm := NewWorkerManager(logger, nil)
	wm.Register("good", func(_ *Config) (WorkerLifecycle, error) {
		return goodWorker, nil
	}, alwaysEnabled, neverCritical)
	wm.Register("failing-stop", func(_ *Config) (WorkerLifecycle, error) {
		return failingWorker, nil
	}, alwaysEnabled, neverCritical)

	cfg := newTestConfig()

	err := wm.Start(context.Background(), cfg)
	require.NoError(t, err)

	assert.Equal(t, 1, goodWorker.startCount())
	assert.Equal(t, 1, failingWorker.startCount())

	running := runningWorkerNames(wm)
	assert.Len(t, running, 2)

	// Stop should report the failure while still attempting sibling shutdown.
	err = wm.Stop()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stop worker \"failing-stop\"")

	assert.Equal(t, 1, goodWorker.stopCount(), "good worker should be stopped despite sibling failure")
}

func TestWorkerManager_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}
	cfg := newTestConfig()
	createCount := atomic.Int32{}
	workers := make([]*mockWorker, 0, 8)
	var workersMu sync.Mutex

	wm := NewWorkerManager(logger, nil)
	wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
		worker := &mockWorker{}
		createCount.Add(1)
		workersMu.Lock()
		workers = append(workers, worker)
		workersMu.Unlock()
		return worker, nil
	}, alwaysEnabled, neverCritical)

	// Start first so there's state for goroutines to contend on.
	err := wm.Start(context.Background(), cfg)
	require.NoError(t, err)

	const goroutines = 20

	var wg sync.WaitGroup
	startErrs := make(chan error, goroutines/4)

	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()

			switch n % 4 {
			case 0:
				// Attempt Start (should be idempotent/no-op while running).
				startErrs <- wm.Start(context.Background(), cfg)
			case 1:
				// Read running workers.
				_ = runningWorkerNames(wm)
			case 2:
				// Trigger config change handler.
				newCfg := newTestConfig()
				newCfg.ExportWorker.PollIntervalSec = cfg.ExportWorker.PollIntervalSec + n + 1
				wm.onConfigChange(newCfg)
			case 3:
				// Read running workers again (different timing).
				_ = runningWorkerNames(wm)
			}
		}(i)
	}

	wg.Wait()
	close(startErrs)

	for startErr := range startErrs {
		require.NoError(t, startErr)
	}

	running := runningWorkerNames(wm)
	assert.Equal(t, []string{"export"}, running)
	assert.GreaterOrEqual(t, createCount.Load(), int32(1))

	workersMu.Lock()
	createdWorkers := append([]*mockWorker(nil), workers...)
	workersMu.Unlock()

	err = wm.Stop()
	require.NoError(t, err)

	for _, worker := range createdWorkers {
		assert.Equal(t, 1, worker.startCount())
		assert.Equal(t, 1, worker.stopCount())
	}
}

func TestWorkerManager_RestartWhenFactoryReturnsSameInstance(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}
	cfg := newTestConfig()
	worker := &mockWorker{}

	wm := NewWorkerManager(logger, nil)
	wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, alwaysEnabled, neverCritical)

	err := wm.Start(context.Background(), cfg)
	require.NoError(t, err)

	updatedCfg := newTestConfig()
	updatedCfg.ExportWorker.PollIntervalSec = cfg.ExportWorker.PollIntervalSec + 1

	wm.onConfigChange(updatedCfg)

	assert.Equal(t, 2, worker.startCount(), "worker should be restarted even when factory returns same instance")
	assert.Equal(t, 1, worker.stopCount(), "worker should be stopped before restart")

	err = wm.Stop()
	require.NoError(t, err)
}

func TestWorkerManager_DiscoveryIgnoresFetcherEnabledReloads(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}
	startCfg := newTestConfig()
	startCfg.Fetcher.Enabled = true
	worker := &mockWorker{}

	wm := NewWorkerManager(logger, nil)
	wm.Register("discovery", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, func(_ *Config) bool { return true }, neverCritical)

	require.NoError(t, wm.Start(context.Background(), startCfg))

	updatedCfg := newTestConfig()
	updatedCfg.Fetcher.Enabled = false

	wm.onConfigChange(updatedCfg)

	assert.Equal(t, 1, worker.startCount(), "discovery worker should keep running")
	assert.Equal(t, 0, worker.stopCount(), "discovery worker should not stop on fetcher.enabled reload")

	require.NoError(t, wm.Stop())
}

func TestWorkerManager_RestartSameInstance_AppliesRuntimeConfigAfterStop(t *testing.T) {
	t.Parallel()

	cfg := newTestConfig()
	worker := &runtimeAwareExportWorker{}

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)
	wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, alwaysEnabled, alwaysCritical)

	require.NoError(t, wm.Start(context.Background(), cfg))

	updatedCfg := newTestConfig()
	updatedCfg.ExportWorker.PollIntervalSec = cfg.ExportWorker.PollIntervalSec + 3
	updatedCfg.ExportWorker.PageSize = cfg.ExportWorker.PageSize + 200

	wm.onConfigChange(updatedCfg)

	events := worker.events()
	require.GreaterOrEqual(t, len(events), 4)
	assert.Equal(t, []string{"stop", "update", "start"}, events[len(events)-3:])

	updates := worker.lastUpdates()
	require.Len(t, updates, 2)
	assert.Equal(t, cfg.ExportWorkerPollInterval(), updates[0].PollInterval)
	assert.Equal(t, cfg.ExportWorker.PageSize, updates[0].PageSize)
	assert.Equal(t, updatedCfg.ExportWorkerPollInterval(), updates[1].PollInterval)
	assert.Equal(t, updatedCfg.ExportWorker.PageSize, updates[1].PageSize)

	require.NoError(t, wm.Stop())
}

func TestWorkerManager_RestartRollback_ReappliesPreviousRuntimeConfig(t *testing.T) {
	t.Parallel()

	cfg := newTestConfig()
	worker := &runtimeAwareExportWorker{}

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)
	wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, alwaysEnabled, alwaysCritical)

	require.NoError(t, wm.Start(context.Background(), cfg))

	worker.failNextStart.Store(true)

	updatedCfg := newTestConfig()
	updatedCfg.ExportWorker.PollIntervalSec = cfg.ExportWorker.PollIntervalSec + 5
	updatedCfg.ExportWorker.PageSize = cfg.ExportWorker.PageSize + 100

	wm.onConfigChange(updatedCfg)

	updates := worker.lastUpdates()
	require.Len(t, updates, 3)
	assert.Equal(t, cfg.ExportWorkerPollInterval(), updates[0].PollInterval)
	assert.Equal(t, cfg.ExportWorker.PageSize, updates[0].PageSize)
	assert.Equal(t, updatedCfg.ExportWorkerPollInterval(), updates[1].PollInterval)
	assert.Equal(t, updatedCfg.ExportWorker.PageSize, updates[1].PageSize)
	assert.Equal(t, cfg.ExportWorkerPollInterval(), updates[2].PollInterval)
	assert.Equal(t, cfg.ExportWorker.PageSize, updates[2].PageSize)

	assert.Equal(t, []string{"export"}, runningWorkerNames(wm))

	// The same updated config should still be treated as unapplied after rollback.
	wm.onConfigChange(updatedCfg)
	assert.GreaterOrEqual(t, worker.startCount(), 2)
	assert.Equal(t, []string{"export"}, runningWorkerNames(wm))

	require.NoError(t, wm.Stop())
}

func TestWorkerManager_RestartRollback_ReappliesPreviousRuntimeConfigToRebuiltWorker(t *testing.T) {
	t.Parallel()

	cfg := newTestConfig()
	initialWorker := &runtimeAwareExportWorker{}
	failingWorker := &runtimeAwareExportWorker{}
	failingWorker.failNextStart.Store(true)
	rollbackWorker := &runtimeAwareExportWorker{}

	callCount := atomic.Int32{}
	wm := NewWorkerManager(&libLog.NopLogger{}, nil)
	wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
		switch callCount.Add(1) {
		case 1:
			return initialWorker, nil
		case 2:
			return failingWorker, nil
		default:
			return rollbackWorker, nil
		}
	}, alwaysEnabled, alwaysCritical)

	require.NoError(t, wm.Start(context.Background(), cfg))

	updatedCfg := newTestConfig()
	updatedCfg.ExportWorker.PollIntervalSec = cfg.ExportWorker.PollIntervalSec + 5
	updatedCfg.ExportWorker.PageSize = cfg.ExportWorker.PageSize + 100

	wm.onConfigChange(updatedCfg)

	updates := rollbackWorker.lastUpdates()
	require.Len(t, updates, 1)
	assert.Equal(t, cfg.ExportWorkerPollInterval(), updates[0].PollInterval)
	assert.Equal(t, cfg.ExportWorker.PageSize, updates[0].PageSize)
	assert.Equal(t, []string{"export"}, runningWorkerNames(wm))

	require.NoError(t, wm.Stop())
}

func TestWorkerManager_Start_UsesLatestConfigManagerSnapshot(t *testing.T) {
	t.Parallel()

	initialCfg := newTestConfig()
	initialCfg.ExportWorker.Enabled = false

	latestCfg := newTestConfig()
	latestCfg.ExportWorker.Enabled = true

	cm := newWorkerMgrTestConfigManager(t, initialCfg)
	cm.config.Store(latestCfg)

	worker := &mockWorker{}
	wm := NewWorkerManager(&libLog.NopLogger{}, cm)
	wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, func(cfg *Config) bool {
		return cfg != nil && cfg.ExportWorker.Enabled
	}, alwaysCritical)

	require.NoError(t, wm.Start(context.Background(), initialCfg))
	assert.Equal(t, 1, worker.startCount())
	require.NoError(t, wm.Stop())
}

func TestWorkerManager_Stop_DoesNotBlockRunningWorkersRead(t *testing.T) {
	t.Parallel()

	worker := &blockingWorker{
		stopCalled: make(chan struct{}),
		stopGate:   make(chan struct{}),
	}

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)
	wm.Register("blocking", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, alwaysEnabled, neverCritical)

	require.NoError(t, wm.Start(context.Background(), newTestConfig()))

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- wm.Stop()
	}()

	<-worker.stopCalled

	done := make(chan []string, 1)
	go func() {
		done <- runningWorkerNames(wm)
	}()

	select {
	case names := <-done:
		assert.Equal(t, []string{"blocking"}, names)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("RunningWorkers blocked while worker stop was in progress")
	}

	close(worker.stopGate)
	require.NoError(t, <-stopDone)
}

func TestWorkerManager_Stop_ReturnsTimeoutWhenWorkerBlocks(t *testing.T) {
	// Not parallel: overrides package-level timeout.

	originalTimeout := workerStopTimeout
	workerStopTimeout = 20 * time.Millisecond
	t.Cleanup(func() { workerStopTimeout = originalTimeout })

	worker := &blockingWorker{stopGate: make(chan struct{})}

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)
	wm.Register("blocking", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, alwaysEnabled, neverCritical)

	require.NoError(t, wm.Start(context.Background(), newTestConfig()))

	err := wm.Stop()
	require.Error(t, err)
	assert.ErrorIs(t, err, errWorkerStopTimedOut)

	close(worker.stopGate)
}

func TestWorkerManager_Start_ReturnsTimeoutWhenWorkerBlocks(t *testing.T) {
	// Not parallel: overrides package-level timeout.

	originalTimeout := workerStartTimeout
	workerStartTimeout = 20 * time.Millisecond
	t.Cleanup(func() { workerStartTimeout = originalTimeout })

	worker := &blockingWorker{startGate: make(chan struct{})}

	wm := NewWorkerManager(&libLog.NopLogger{}, nil)
	wm.Register("blocking", func(_ *Config) (WorkerLifecycle, error) {
		return worker, nil
	}, alwaysEnabled, alwaysCritical)

	err := wm.Start(context.Background(), newTestConfig())
	require.Error(t, err)
	assert.ErrorIs(t, err, errWorkerStartTimedOut)

	close(worker.startGate)
}

func TestWorkerManager_RegisterNilEnabledPredicate_DoesNotPanicOnNilConfig(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}
	wm := NewWorkerManager(logger, nil)

	wm.Register("worker", func(_ *Config) (WorkerLifecycle, error) {
		return &mockWorker{}, nil
	}, nil, nil)

	assert.NotPanics(t, func() {
		require.NoError(t, wm.Start(context.Background(), nil))
		require.NoError(t, wm.Stop())
	})
}

func TestWorkerManager_ConfigManagerSubscriptionLifecycle(t *testing.T) {
	// Not parallel: uses t.Setenv for config manager validation prerequisites.

	cfg := defaultConfig()
	t.Setenv("DEFAULT_TENANT_ID", cfg.Tenancy.DefaultTenantID)
	t.Setenv("DEFAULT_TENANT_SLUG", cfg.Tenancy.DefaultTenantSlug)
	cm, err := NewConfigManager(cfg, "", &libLog.NopLogger{})
	require.NoError(t, err)
	t.Cleanup(cm.Stop)

	wm := NewWorkerManager(&libLog.NopLogger{}, cm)
	wm.Register("export", func(_ *Config) (WorkerLifecycle, error) {
		return &mockWorker{}, nil
	}, alwaysEnabled, neverCritical)

	beforeCount := len(cm.subscribers)
	require.NoError(t, wm.Start(context.Background(), cfg))
	assert.Len(t, cm.subscribers, beforeCount+1)

	require.NoError(t, wm.Stop())
	assert.Len(t, cm.subscribers, beforeCount)
}

// newWorkerMgrTestConfigManager creates a ConfigManager suitable for worker
// manager tests without file watcher or YAML I/O.
func newWorkerMgrTestConfigManager(t *testing.T, cfg *Config) *ConfigManager {
	t.Helper()

	// Provide a minimal valid config if nil.
	if cfg == nil {
		cfg = newTestConfig()
	}

	cm := &ConfigManager{
		logger: &libLog.NopLogger{},
		stopCh: make(chan struct{}),
	}
	cm.config.Store(cfg)
	cm.lastReload.Store(time.Now().UTC())

	return cm
}
