package bootstrap

import (
	"context"
	"fmt"
	"reflect"
	"sync"

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

// workerSlot tracks a single managed worker: its current instance, its factory,
// the config sub-section relevant to it, and whether it should be considered
// critical on startup failure.
type workerSlot struct {
	name     string
	factory  WorkerFactory
	instance WorkerLifecycle
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
	logger        libLog.Logger
	configManager *ConfigManager
	slots         []*workerSlot
	lastCfg       *Config
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
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.running {
		return nil
	}

	workerCtx, cancel := context.WithCancel(ctx)
	wm.parentCtx = workerCtx
	wm.cancel = cancel
	wm.lastCfg = cfg
	wm.running = true

	if err := wm.startEnabledWorkersLocked(workerCtx, cfg); err != nil {
		// On critical failure, stop any workers that did start.
		wm.stopAllWorkersLocked(workerCtx)
		wm.running = false

		return err
	}

	// Subscribe to config changes for hot-reload.
	if wm.configManager != nil {
		wm.configManager.Subscribe(wm.onConfigChange)
	}

	return nil
}

// Stop gracefully shuts down all running workers. Idempotent.
func (wm *WorkerManager) Stop() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if !wm.running {
		return nil
	}

	wm.stopAllWorkersLocked(context.Background())

	if wm.cancel != nil {
		wm.cancel()
	}

	wm.running = false

	return nil
}

// RunningWorkers returns the names of currently running workers.
// Useful for health checks and diagnostics.
func (wm *WorkerManager) RunningWorkers() []string {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	var names []string

	for _, slot := range wm.slots {
		if slot.instance != nil {
			names = append(names, slot.name)
		}
	}

	return names
}

// onConfigChange is the subscriber callback invoked by ConfigManager after
// every successful config reload. It compares the worker-relevant config
// sections and restarts workers whose config actually changed.
func (wm *WorkerManager) onConfigChange(newCfg *Config) {
	defer runtime.RecoverAndLogWithContext(
		context.Background(),
		wm.logger,
		constants.ApplicationName,
		"worker_manager.on_config_change",
	)

	wm.mu.Lock()
	defer wm.mu.Unlock()

	if !wm.running || newCfg == nil {
		return
	}

	// Use parentCtx for worker lifecycle operations (Start needs a cancellable ctx).
	ctx := wm.parentCtx
	if ctx == nil {
		ctx = context.Background()
	}

	oldCfg := wm.lastCfg
	wm.lastCfg = newCfg

	for _, slot := range wm.slots {
		wm.reconcileSlotLocked(ctx, slot, oldCfg, newCfg)
	}
}

// reconcileSlotLocked handles a single worker slot: starts, stops, or restarts
// it based on the old and new configs. Caller must hold wm.mu.
func (wm *WorkerManager) reconcileSlotLocked(ctx context.Context, slot *workerSlot, oldCfg, newCfg *Config) {
	wasEnabled := isSlotEnabled(slot, oldCfg)
	nowEnabled := isSlotEnabled(slot, newCfg)

	switch {
	case !wasEnabled && nowEnabled:
		// Worker was disabled, now enabled — start it.
		wm.logger.Log(ctx, libLog.LevelInfo,
			fmt.Sprintf("worker %q enabled by config change, starting", slot.name))

		if err := wm.startSlotLocked(ctx, slot, newCfg); err != nil {
			wm.logger.Log(ctx, libLog.LevelWarn,
				fmt.Sprintf("worker %q failed to start after enable: %v", slot.name, err))
		}

	case wasEnabled && !nowEnabled:
		// Worker was enabled, now disabled — stop it.
		wm.logger.Log(ctx, libLog.LevelInfo,
			fmt.Sprintf("worker %q disabled by config change, stopping", slot.name))
		wm.stopSlotLocked(ctx, slot)

	case wasEnabled && nowEnabled:
		// Worker enabled in both — check if config changed.
		if workerConfigChanged(slot.name, oldCfg, newCfg) {
			wm.logger.Log(ctx, libLog.LevelInfo,
				fmt.Sprintf("worker %q config changed, restarting", slot.name))

			if err := wm.restartSlotLocked(ctx, slot, newCfg); err != nil {
				wm.logger.Log(ctx, libLog.LevelWarn,
					fmt.Sprintf("worker %q failed to restart after config change: %v", slot.name, err))
			}
		}

	default:
		// Was disabled, still disabled — nothing to do.
	}
}

// startEnabledWorkersLocked starts all workers that are enabled in the given config.
// Returns an error if a critical worker fails to start. Caller must hold wm.mu.
func (wm *WorkerManager) startEnabledWorkersLocked(ctx context.Context, cfg *Config) error {
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

// restartSlotLocked restarts a running worker with a newly built instance.
// If the factory returns the same instance, restart is skipped to avoid
// stopping a worker that cannot be safely re-started with the same object.
func (wm *WorkerManager) restartSlotLocked(ctx context.Context, slot *workerSlot, cfg *Config) error {
	if slot == nil {
		return nil
	}

	if slot.instance == nil {
		return wm.startSlotLocked(ctx, slot, cfg)
	}

	if slot.factory == nil {
		return nil
	}

	candidate, err := slot.factory(cfg)
	if err != nil {
		return fmt.Errorf("create worker %q for restart: %w", slot.name, err)
	}

	if candidate == nil {
		wm.logger.Log(ctx, libLog.LevelDebug,
			fmt.Sprintf("worker %q restart skipped: factory returned nil", slot.name))

		return nil
	}

	if sameWorkerInstance(slot.instance, candidate) {
		wm.logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("worker %q restart skipped: factory returned same instance", slot.name))

		return nil
	}

	wm.stopSlotLocked(ctx, slot)

	if err := candidate.Start(ctx); err != nil {
		return fmt.Errorf("start worker %q after restart: %w", slot.name, err)
	}

	slot.instance = candidate

	wm.logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("worker %q restarted", slot.name))

	return nil
}

// startSlotLocked creates a new worker via the factory and starts it.
// Caller must hold wm.mu.
func (wm *WorkerManager) startSlotLocked(ctx context.Context, slot *workerSlot, cfg *Config) error {
	if slot.factory == nil {
		wm.logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("worker %q has no factory, skipping", slot.name))

		return nil
	}

	worker, err := slot.factory(cfg)
	if err != nil {
		return fmt.Errorf("create worker %q: %w", slot.name, err)
	}

	if worker == nil {
		wm.logger.Log(ctx, libLog.LevelDebug,
			fmt.Sprintf("worker %q factory returned nil (dependency unavailable), skipping", slot.name))

		return nil
	}

	if err := worker.Start(ctx); err != nil {
		return fmt.Errorf("start worker %q: %w", slot.name, err)
	}

	slot.instance = worker

	wm.logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("worker %q started", slot.name))

	return nil
}

// stopSlotLocked stops a single worker slot. Caller must hold wm.mu.
func (wm *WorkerManager) stopSlotLocked(ctx context.Context, slot *workerSlot) {
	if slot.instance == nil {
		return
	}

	if err := slot.instance.Stop(); err != nil {
		wm.logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("worker %q stop error (non-fatal): %v", slot.name, err))
	}

	slot.instance = nil
}

// stopAllWorkersLocked stops all running workers in reverse registration order.
// Caller must hold wm.mu.
func (wm *WorkerManager) stopAllWorkersLocked(ctx context.Context) {
	for i := len(wm.slots) - 1; i >= 0; i-- {
		wm.stopSlotLocked(ctx, wm.slots[i])
	}
}

// workerConfigChanged checks whether the config section relevant to a worker
// has changed between two configs. Uses reflect.DeepEqual on the relevant
// sub-struct for simplicity — this runs at most once per config reload per worker,
// so the reflection cost is negligible.
func workerConfigChanged(name string, oldCfg, newCfg *Config) bool {
	old := extractWorkerConfig(name, oldCfg)
	new_ := extractWorkerConfig(name, newCfg)

	// If either config extraction returned nil (unknown worker name or nil config),
	// assume changed to trigger a reconciliation. This is safe — reconciliation
	// handles nil gracefully — but log-worthy if it happens repeatedly.
	if old == nil || new_ == nil {
		return true
	}

	return !reflect.DeepEqual(old, new_)
}

// extractWorkerConfig returns the config sub-struct relevant to a named worker.
// Returns nil if cfg is nil or the worker name is unrecognized.
func extractWorkerConfig(name string, cfg *Config) any {
	if cfg == nil {
		return nil
	}

	switch name {
	case "export":
		return cfg.ExportWorker
	case "cleanup":
		return cfg.CleanupWorker
	case "archival":
		return cfg.Archival
	case "scheduler":
		return cfg.Scheduler
	case "discovery":
		return cfg.Fetcher
	default:
		return nil
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
	if a == nil || other == nil {
		return false
	}

	av := reflect.ValueOf(a)
	otherVal := reflect.ValueOf(other)

	if av.Kind() != reflect.Pointer || otherVal.Kind() != reflect.Pointer {
		return false
	}

	return av.Pointer() == otherVal.Pointer()
}
