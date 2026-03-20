// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// WorkerLifecycle is the interface that all managed workers must satisfy.
// It matches the Start/Stop contract already used by ExportWorker, CleanupWorker,
// ArchivalWorker, SchedulerWorker, and DiscoveryWorker.
type WorkerLifecycle interface {
	Start(ctx context.Context) error
	Stop() error
}

// WorkerFactory is a function that creates a new worker instance from the current config.
// The factory captures heavy dependencies (repos, services) in its closure so that
// only the config needs to change on restart.
type WorkerFactory func(cfg *Config) (WorkerLifecycle, error)

var (
	errWorkerFactoryRequired       = errors.New("worker factory is required")
	errWorkerDependencyUnavailable = errors.New("worker dependency unavailable")
	errNoPreviousWorkerForRollback = errors.New("no previous worker instance available for rollback")
	errWorkerStartTimedOut         = errors.New("worker start timed out")
	errWorkerStopTimedOut          = errors.New("worker stop timed out")
	errWorkerStartPanicked         = errors.New("worker start panicked")
	errWorkerStopPanicked          = errors.New("worker stop panicked")
)

var (
	workerStartTimeout = 30 * time.Second
	workerStopTimeout  = 30 * time.Second
)

// workerSlot tracks a single managed worker: its current instance, its factory,
// the config sub-section relevant to it, and whether it should be considered
// critical on startup failure.
type workerSlot struct {
	name     string
	factory  WorkerFactory
	instance WorkerLifecycle
	lastCfg  *Config
	critical func(cfg *Config) bool // returns true if this worker is critical given the config
	enabled  func(cfg *Config) bool // returns true if this worker should be running given the config
}

// WorkerManager orchestrates background worker lifecycles and subscribes to
// ConfigManager for hot-reload. When config changes, it stops affected workers
// and restarts them with the new config.
//
// Thread-safety: all worker mutations are serialized via mu. The config subscriber
// callback runs under mu, so concurrent reloads are safe.
type WorkerManager struct {
	mu            sync.Mutex
	opsMu         sync.Mutex
	logger        libLog.Logger
	configManager *ConfigManager
	slots         []*workerSlot
	parentCtx     context.Context
	cancel        context.CancelFunc
	running       bool
}

// NewWorkerManager creates a WorkerManager. If configManager is non-nil,
// the manager subscribes to config changes for hot-reload.
func NewWorkerManager(logger libLog.Logger, configManager *ConfigManager) *WorkerManager {
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &WorkerManager{
		logger:        logger,
		configManager: configManager,
	}
}

// Register adds a worker slot. Must be called before Start().
// The enabled function determines whether the worker should run for a given config.
// The critical function determines whether startup failure is fatal.
// Both enabled and factory must be non-nil; critical may be nil (defaults to non-critical).
func (wm *WorkerManager) Register(
	name string,
	factory WorkerFactory,
	enabled func(cfg *Config) bool,
	critical func(cfg *Config) bool,
) {
	if factory == nil {
		factory = func(_ *Config) (WorkerLifecycle, error) {
			return nil, errWorkerFactoryRequired
		}
	}

	if enabled == nil {
		enabled = func(_ *Config) bool { return true }
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.slots = append(wm.slots, &workerSlot{
		name:     name,
		factory:  factory,
		enabled:  enabled,
		critical: critical,
	})
}

// Start launches all enabled workers and subscribes to config changes.
// Returns an error if a critical worker fails to start.
func (wm *WorkerManager) Start(ctx context.Context, cfg *Config) error {
	wm.opsMu.Lock()
	defer wm.opsMu.Unlock()

	wm.mu.Lock()

	if wm.running {
		wm.mu.Unlock()
		return nil
	}

	workerCtx, cancel := context.WithCancel(ctx)
	wm.parentCtx = workerCtx
	wm.cancel = cancel
	wm.running = true

	if wm.configManager != nil {
		if latestCfg := wm.configManager.Get(); latestCfg != nil {
			cfg = latestCfg
		}
	}

	wm.mu.Unlock()

	if err := wm.startEnabledWorkers(workerCtx, cfg); err != nil {
		// On critical failure, stop any workers that did start.
		_ = wm.stopAllWorkers()

		wm.mu.Lock()

		if wm.cancel != nil {
			wm.cancel()
		}

		wm.parentCtx = nil
		wm.cancel = nil
		wm.running = false
		wm.mu.Unlock()

		return err
	}

	return nil
}

// Stop gracefully shuts down all running workers. Idempotent.
func (wm *WorkerManager) Stop() error {
	wm.opsMu.Lock()
	defer wm.opsMu.Unlock()

	wm.mu.Lock()

	if !wm.running {
		wm.mu.Unlock()
		return nil
	}
	wm.mu.Unlock()

	stopErr := wm.stopAllWorkers()

	wm.mu.Lock()

	if wm.cancel != nil {
		wm.cancel()
		wm.cancel = nil
	}

	wm.running = false
	wm.parentCtx = nil
	wm.mu.Unlock()

	return stopErr
}

// ApplyConfig is the public entry point for applying a config change to all
// managed workers. It is called by the systemplane WorkerReconciler when the
// Supervisor detects a configuration change, replacing the ConfigManager
// subscription path. Delegates to onConfigChange for the actual reconciliation.
func (wm *WorkerManager) ApplyConfig(cfg *Config) error {
	return wm.onConfigChange(cfg)
}

// onConfigChange is the subscriber callback invoked by ConfigManager after
// every successful config reload. It compares the worker-relevant config
// sections and restarts workers whose config actually changed.
func (wm *WorkerManager) onConfigChange(newCfg *Config) error {
	defer runtime.RecoverAndLogWithContext(
		context.Background(),
		wm.logger,
		constants.ApplicationName,
		"worker_manager.on_config_change",
	)

	wm.opsMu.Lock()
	defer wm.opsMu.Unlock()

	wm.mu.Lock()

	if !wm.running || newCfg == nil {
		wm.mu.Unlock()
		return nil
	}

	// Use parentCtx for worker lifecycle operations (Start needs a cancellable ctx).
	ctx := wm.parentCtx
	if ctx == nil {
		ctx = context.TODO()
	}
	wm.mu.Unlock()

	var reconcileErr error

	for _, slot := range wm.slots {
		if err := wm.reconcileSlotLocked(ctx, slot, newCfg); err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
		}
	}

	return reconcileErr
}

// startEnabledWorkers starts all workers that are enabled in the given config.
// Returns an error if a critical worker fails to start.
func (wm *WorkerManager) startEnabledWorkers(ctx context.Context, cfg *Config) error {
	for _, slot := range wm.slots {
		if !isSlotEnabled(slot, cfg) {
			wm.logger.Log(ctx, libLog.LevelDebug,
				fmt.Sprintf("worker %q disabled, skipping", slot.name))

			continue
		}

		if err := wm.startSlotLocked(ctx, slot, cfg); err != nil {
			if isSlotCritical(slot, cfg) {
				return fmt.Errorf("critical worker %q failed to start: %w", slot.name, err)
			}

			wm.logger.Log(ctx, libLog.LevelWarn,
				fmt.Sprintf("worker %q failed to start (non-critical, continuing): %v", slot.name, err))
		}
	}

	return nil
}

// startSlotLocked creates a new worker via the factory and starts it.
// Caller must hold wm.mu.
func (wm *WorkerManager) startSlotLocked(ctx context.Context, slot *workerSlot, cfg *Config) (startErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			startErr = fmt.Errorf("start worker %q: %w: %v", slot.name, errWorkerStartPanicked, recovered)
		}
	}()

	if slot.factory == nil {
		return errWorkerFactoryRequired
	}

	worker, err := slot.factory(cfg)
	if err != nil {
		return fmt.Errorf("create worker %q: %w", slot.name, err)
	}

	if isNilWorkerLifecycle(worker) {
		return fmt.Errorf("worker %q: %w", slot.name, errWorkerDependencyUnavailable)
	}

	if err := applyWorkerRuntimeConfig(ctx, slot.name, worker, cfg); err != nil {
		return fmt.Errorf("apply worker %q runtime config: %w", slot.name, err)
	}

	if err := startWorkerWithTimeout(ctx, worker); err != nil {
		return fmt.Errorf("start worker %q: %w", slot.name, err)
	}

	wm.mu.Lock()
	slot.instance = worker
	slot.lastCfg = cfg
	wm.mu.Unlock()

	wm.logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("worker %q started", slot.name))

	return nil
}

// stopSlotLocked stops a single worker slot. Caller must hold wm.mu.
func (wm *WorkerManager) stopSlotLocked(slot *workerSlot) error {
	wm.mu.Lock()
	instance := slot.instance
	wm.mu.Unlock()

	if isNilWorkerLifecycle(instance) {
		wm.mu.Lock()
		slot.instance = nil
		wm.mu.Unlock()

		return nil
	}

	if err := stopWorkerWithTimeout(instance); err != nil {
		return fmt.Errorf("stop worker %q: %w", slot.name, err)
	}

	wm.mu.Lock()
	slot.instance = nil
	wm.mu.Unlock()

	return nil
}

// stopAllWorkersLocked stops all running workers in reverse registration order.
// Caller must hold wm.mu.

func (wm *WorkerManager) stopAllWorkers() error {
	var stopErr error

	for i := len(wm.slots) - 1; i >= 0; i-- {
		if err := wm.stopSlotLocked(wm.slots[i]); err != nil {
			stopErr = errors.Join(stopErr, err)
		}
	}

	return stopErr
}

func startWorkerWithTimeout(ctx context.Context, worker WorkerLifecycle) error {
	ctx = fallbackContext(ctx)

	timer := time.NewTimer(workerStartTimeout)
	defer timer.Stop()

	errCh := make(chan error, 1)

	runtime.SafeGo(nil, "worker_manager.start_worker_with_timeout", runtime.KeepRunning, func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				errCh <- fmt.Errorf("%w: %v", errWorkerStartPanicked, recovered)
			}
		}()

		errCh <- worker.Start(ctx)
	})

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		select {
		case err := <-errCh:
			return err
		default:
		}

		return fmt.Errorf("worker start canceled: %w", ctx.Err())
	case <-timer.C:
		select {
		case err := <-errCh:
			return err
		default:
		}

		return errWorkerStartTimedOut
	}
}

func stopWorkerWithTimeout(worker WorkerLifecycle) error {
	errCh := make(chan error, 1)

	runtime.SafeGo(nil, "worker_manager.stop_worker_with_timeout", runtime.KeepRunning, func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				errCh <- fmt.Errorf("%w: %v", errWorkerStopPanicked, recovered)
			}
		}()

		errCh <- worker.Stop()
	})

	select {
	case err := <-errCh:
		return err
	case <-time.After(workerStopTimeout):
		return errWorkerStopTimedOut
	}
}

func isSlotEnabled(slot *workerSlot, cfg *Config) bool {
	if slot == nil || cfg == nil {
		return false
	}

	if slot.enabled == nil {
		return true
	}

	return slot.enabled(cfg)
}

func isSlotCritical(slot *workerSlot, cfg *Config) bool {
	if slot == nil || cfg == nil || slot.critical == nil {
		return false
	}

	return slot.critical(cfg)
}

func sameWorkerInstance(a, other WorkerLifecycle) bool {
	if isNilWorkerLifecycle(a) || isNilWorkerLifecycle(other) {
		return false
	}

	av := reflect.ValueOf(a)
	otherVal := reflect.ValueOf(other)

	if av.Kind() != reflect.Pointer || otherVal.Kind() != reflect.Pointer {
		return false
	}

	return av.Pointer() == otherVal.Pointer()
}

func isNilWorkerLifecycle(worker WorkerLifecycle) bool {
	return isNilInterface(worker)
}
