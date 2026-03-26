// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"reflect"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type exportWorkerComparableConfig struct {
	PollIntervalSec int
	PageSize        int
}

type cleanupWorkerComparableConfig struct {
	IntervalSec    int
	BatchSize      int
	GracePeriodSec int
}

type archivalWorkerComparableConfig struct {
	IntervalHours        int
	HotRetentionDays     int
	WarmRetentionMonths  int
	ColdRetentionMonths  int
	BatchSize            int
	StorageBucket        string
	StoragePrefix        string
	StorageClass         string
	PartitionLookahead   int
	StorageEndpoint      string
	StorageRegion        string
	StorageAccessKeyID   string
	StorageSecretKey     string
	StorageUsePathStyle  bool
	StorageAllowInsecure bool
}

type schedulerWorkerComparableConfig struct {
	IntervalSec int
}

const (
	workerNameExport    = "export"
	workerNameCleanup   = "cleanup"
	workerNameArchival  = "archival"
	workerNameScheduler = "scheduler"
	workerNameDiscovery = "discovery"
)

type discoveryWorkerRuntimeConfig struct {
	Interval time.Duration
}

// reconcileSlotLocked handles a single worker slot: starts, stops, or restarts
// it based on the old and new configs. Caller must hold wm.mu.
func (wm *WorkerManager) reconcileSlotLocked(ctx context.Context, slot *workerSlot, newCfg *Config) error {
	wm.mu.Lock()
	wasEnabled := !isNilWorkerLifecycle(slot.instance)
	wm.mu.Unlock()

	nowEnabled := isSlotEnabled(slot, newCfg)
	if !workerSupportsRuntimeToggle(slot.name) {
		nowEnabled = wasEnabled
	}

	switch {
	case !wasEnabled && nowEnabled:
		if err := wm.handleSlotEnableTransition(ctx, slot, newCfg); err != nil {
			return err
		}

	case wasEnabled && !nowEnabled:
		if err := wm.handleSlotDisableTransition(ctx, slot); err != nil {
			return err
		}

	case wasEnabled && nowEnabled:
		// Worker enabled in both — check if config changed.
		if err := wm.reconcileRunningSlotLocked(ctx, slot, newCfg); err != nil {
			return err
		}

	default:
		// Was disabled, still disabled — nothing to do.
	}

	if !nowEnabled {
		slot.lastCfg = newCfg
	}

	return nil
}

func (wm *WorkerManager) handleSlotEnableTransition(ctx context.Context, slot *workerSlot, newCfg *Config) error {
	wm.logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("worker %q enabled by config change, starting", slot.name))

	if err := wm.startSlotLocked(ctx, slot, newCfg); err != nil {
		if isSlotCritical(slot, newCfg) {
			wm.logger.Log(ctx, libLog.LevelError,
				fmt.Sprintf("critical worker %q failed to start after enable: %v", slot.name, err))

			return fmt.Errorf("critical worker %q failed to start after enable: %w", slot.name, err)
		}

		wm.logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("worker %q failed to start after enable: %v", slot.name, err))
	}

	return nil
}

func (wm *WorkerManager) handleSlotDisableTransition(ctx context.Context, slot *workerSlot) error {
	wm.logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("worker %q disabled by config change, stopping", slot.name))

	return wm.stopSlotLocked(slot)
}

// reconcileRunningSlotLocked handles the case where a worker is enabled in both
// old and new configs — it restarts the worker only if its config section changed.
// Caller must hold wm.mu.
func (wm *WorkerManager) reconcileRunningSlotLocked(ctx context.Context, slot *workerSlot, newCfg *Config) error {
	wm.mu.Lock()
	previousCfg := slot.lastCfg
	wm.mu.Unlock()

	if !workerConfigChanged(slot.name, previousCfg, newCfg) {
		return nil
	}

	wm.logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("worker %q config changed, restarting", slot.name))

	if err := wm.restartSlotLocked(ctx, slot, previousCfg, newCfg); err != nil {
		if isSlotCritical(slot, newCfg) {
			wm.logger.Log(ctx, libLog.LevelError,
				fmt.Sprintf("critical worker %q failed to restart after config change: %v", slot.name, err))

			return fmt.Errorf("critical worker %q failed to restart after config change: %w", slot.name, err)
		}

		wm.logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("worker %q failed to restart after config change: %v", slot.name, err))
	}

	return nil
}

// restartSlotLocked restarts a running worker with a newly built instance.
// If the factory returns the same instance, restart is skipped to avoid
// stopping a worker that cannot be safely re-started with the same object.
func (wm *WorkerManager) restartSlotLocked(ctx context.Context, slot *workerSlot, oldCfg, newCfg *Config) error {
	if slot == nil {
		return nil
	}

	wm.mu.Lock()
	currentInstance := slot.instance
	wm.mu.Unlock()

	if currentInstance == nil {
		return wm.startSlotLocked(ctx, slot, newCfg)
	}

	if slot.factory == nil {
		return errWorkerFactoryRequired
	}

	candidate, err := slot.factory(newCfg)
	if err != nil {
		return fmt.Errorf("create worker %q for restart: %w", slot.name, err)
	}

	if isNilWorkerLifecycle(candidate) {
		return fmt.Errorf("worker %q: %w", slot.name, errWorkerDependencyUnavailable)
	}

	previous := currentInstance
	sameInstance := sameWorkerInstance(previous, candidate)

	if err := wm.stopSlotLocked(slot); err != nil {
		return err
	}

	// Always apply runtime config to the candidate before starting, regardless of
	// whether it is the same instance. If factories are refactored to return new
	// instances in the future, this ensures they always receive the latest config.
	if err := applyWorkerRuntimeConfig(ctx, slot.name, candidate, newCfg); err != nil {
		return fmt.Errorf("apply worker %q runtime config before restart: %w", slot.name, err)
	}

	if err := startWorkerWithTimeout(ctx, candidate); err != nil {
		if rollbackErr := wm.rollbackAfterRestartFailureLocked(ctx, slot, previous, oldCfg, sameInstance); rollbackErr != nil {
			return fmt.Errorf("start worker %q after restart: %w (rollback failed: %w)", slot.name, err, rollbackErr)
		}

		return fmt.Errorf("start worker %q after restart: %w (rolled back to previous config)", slot.name, err)
	}

	wm.mu.Lock()
	slot.instance = candidate
	slot.lastCfg = newCfg
	wm.mu.Unlock()

	wm.logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("worker %q restarted", slot.name))

	return nil
}

func (wm *WorkerManager) rollbackAfterRestartFailureLocked(
	ctx context.Context,
	slot *workerSlot,
	previous WorkerLifecycle,
	oldCfg *Config,
	sameInstance bool,
) error {
	if slot == nil || previous == nil {
		return errNoPreviousWorkerForRollback
	}

	rollbackCandidate, err := prepareRollbackCandidate(ctx, slot, previous, oldCfg, sameInstance)
	if err != nil {
		return err
	}

	if err := startWorkerWithTimeout(ctx, rollbackCandidate); err != nil {
		return fmt.Errorf("restart previous worker %q: %w", slot.name, err)
	}

	wm.mu.Lock()
	slot.instance = rollbackCandidate
	wm.mu.Unlock()

	wm.logger.Log(ctx, libLog.LevelWarn,
		fmt.Sprintf("worker %q rollback succeeded after restart failure", slot.name))

	return nil
}

// prepareRollbackCandidate determines the best worker instance to use for
// rollback after a restart failure. It applies runtime config to the previous
// instance if same-instance, or rebuilds via factory otherwise.
func prepareRollbackCandidate(
	ctx context.Context,
	slot *workerSlot,
	previous WorkerLifecycle,
	oldCfg *Config,
	sameInstance bool,
) (WorkerLifecycle, error) {
	if oldCfg == nil {
		return previous, nil
	}

	if sameInstance {
		if err := applyWorkerRuntimeConfig(ctx, slot.name, previous, oldCfg); err != nil {
			return nil, fmt.Errorf("reapply worker %q runtime config for rollback: %w", slot.name, err)
		}

		return previous, nil
	}

	if slot.factory == nil {
		return previous, nil
	}

	rebuilt, err := slot.factory(oldCfg)
	if err != nil {
		return nil, fmt.Errorf("rebuild rollback worker %q: %w", slot.name, err)
	}

	if isNilWorkerLifecycle(rebuilt) {
		return previous, nil
	}

	if err := applyWorkerRuntimeConfig(ctx, slot.name, rebuilt, oldCfg); err != nil {
		return nil, fmt.Errorf("apply rebuilt worker %q runtime config for rollback: %w", slot.name, err)
	}

	return rebuilt, nil
}

func applyWorkerRuntimeConfig(ctx context.Context, name string, worker WorkerLifecycle, cfg *Config) error {
	if cfg == nil || worker == nil {
		return nil
	}

	switch name {
	case workerNameExport:
		return applyExportRuntimeConfig(worker, cfg)
	case workerNameCleanup:
		return applyCleanupRuntimeConfig(worker, cfg)
	case workerNameArchival:
		return applyArchivalRuntimeConfig(ctx, worker, cfg)
	case workerNameScheduler:
		return applySchedulerRuntimeConfig(worker, cfg)
	case workerNameDiscovery:
		return applyDiscoveryRuntimeConfig(worker, cfg)
	default:
		return nil
	}
}

func applyExportRuntimeConfig(worker WorkerLifecycle, cfg *Config) error {
	exportWorker, ok := worker.(interface {
		UpdateRuntimeConfig(reportingWorker.ExportWorkerConfig) error
	})
	if !ok {
		return nil
	}

	if err := exportWorker.UpdateRuntimeConfig(reportingWorker.ExportWorkerConfig{
		PollInterval: cfg.ExportWorkerPollInterval(),
		PageSize:     cfg.ExportWorker.PageSize,
	}); err != nil {
		return fmt.Errorf("update export runtime config: %w", err)
	}

	return nil
}

func applyCleanupRuntimeConfig(worker WorkerLifecycle, cfg *Config) error {
	cleanupWorker, ok := worker.(interface {
		UpdateRuntimeConfig(reportingWorker.CleanupWorkerConfig) error
	})
	if !ok {
		return nil
	}

	if err := cleanupWorker.UpdateRuntimeConfig(reportingWorker.CleanupWorkerConfig{
		Interval:              cfg.CleanupWorkerInterval(),
		BatchSize:             cfg.CleanupWorkerBatchSize(),
		FileDeleteGracePeriod: cfg.CleanupWorkerGracePeriod(),
	}); err != nil {
		return fmt.Errorf("update cleanup runtime config: %w", err)
	}

	return nil
}

func applyArchivalRuntimeConfig(ctx context.Context, worker WorkerLifecycle, cfg *Config) error {
	archivalWorker, ok := worker.(interface {
		UpdateRuntimeConfig(governanceWorker.ArchivalWorkerConfig) error
	})
	if !ok {
		return nil
	}

	if storageAwareWorker, storageAware := worker.(interface {
		UpdateRuntimeStorage(sharedPorts.ObjectStorageClient) error
	}); storageAware {
		storage, err := createArchivalStorage(ctx, cfg)
		if err != nil {
			return fmt.Errorf("update archival runtime storage: %w", err)
		}

		if storage == nil {
			return ErrArchivalStorageRequired
		}

		if err := storageAwareWorker.UpdateRuntimeStorage(storage); err != nil {
			return fmt.Errorf("update archival runtime storage client: %w", err)
		}
	}

	if err := archivalWorker.UpdateRuntimeConfig(governanceWorker.ArchivalWorkerConfig{
		Interval:            cfg.ArchivalInterval(),
		HotRetentionDays:    cfg.Archival.HotRetentionDays,
		WarmRetentionMonths: cfg.Archival.WarmRetentionMonths,
		ColdRetentionMonths: cfg.Archival.ColdRetentionMonths,
		BatchSize:           cfg.Archival.BatchSize,
		StorageBucket:       cfg.Archival.StorageBucket,
		StoragePrefix:       cfg.Archival.StoragePrefix,
		StorageClass:        cfg.Archival.StorageClass,
		PartitionLookahead:  cfg.Archival.PartitionLookahead,
	}); err != nil {
		return fmt.Errorf("update archival runtime config: %w", err)
	}

	return nil
}

func applySchedulerRuntimeConfig(worker WorkerLifecycle, cfg *Config) error {
	schedulerWorker, ok := worker.(interface {
		UpdateRuntimeConfig(configWorker.SchedulerWorkerConfig) error
	})
	if !ok {
		return nil
	}

	if err := schedulerWorker.UpdateRuntimeConfig(configWorker.SchedulerWorkerConfig{
		Interval: cfg.SchedulerInterval(),
	}); err != nil {
		return fmt.Errorf("update scheduler runtime config: %w", err)
	}

	return nil
}

func applyDiscoveryRuntimeConfig(worker WorkerLifecycle, cfg *Config) error {
	discovery, ok := worker.(interface {
		UpdateRuntimeConfig(discoveryWorker.DiscoveryWorkerConfig)
	})
	if !ok {
		return nil
	}

	discovery.UpdateRuntimeConfig(discoveryWorker.DiscoveryWorkerConfig{
		Interval: cfg.FetcherDiscoveryInterval(),
	})

	return nil
}

// workerConfigChanged checks whether the config section relevant to a worker
// has changed between two configs. Uses reflect.DeepEqual on the relevant
// sub-struct for simplicity — this runs at most once per config reload per worker,
// so the reflection cost is negligible.
func workerConfigChanged(name string, oldCfg, newCfg *Config) bool {
	old := extractWorkerConfig(name, oldCfg)
	updated := extractWorkerConfig(name, newCfg)

	// If either config extraction returned nil (unknown worker name or nil config),
	// assume changed to trigger a reconciliation. This is safe — reconciliation
	// handles nil gracefully — but log-worthy if it happens repeatedly.
	if old == nil || updated == nil {
		return true
	}

	return !reflect.DeepEqual(old, updated)
}

// extractWorkerConfig returns the config sub-struct relevant to a named worker.
// Returns nil if cfg is nil or the worker name is unrecognized.
func extractWorkerConfig(name string, cfg *Config) any {
	if cfg == nil {
		return nil
	}

	switch name {
	case workerNameExport:
		return exportWorkerComparableConfig{
			PollIntervalSec: cfg.ExportWorker.PollIntervalSec,
			PageSize:        cfg.ExportWorker.PageSize,
		}
	case workerNameCleanup:
		return cleanupWorkerComparableConfig{
			IntervalSec:    cfg.CleanupWorker.IntervalSec,
			BatchSize:      cfg.CleanupWorker.BatchSize,
			GracePeriodSec: cfg.CleanupWorker.GracePeriodSec,
		}
	case workerNameArchival:
		return archivalWorkerComparableConfig{
			IntervalHours:        cfg.Archival.IntervalHours,
			HotRetentionDays:     cfg.Archival.HotRetentionDays,
			WarmRetentionMonths:  cfg.Archival.WarmRetentionMonths,
			ColdRetentionMonths:  cfg.Archival.ColdRetentionMonths,
			BatchSize:            cfg.Archival.BatchSize,
			StorageBucket:        cfg.Archival.StorageBucket,
			StoragePrefix:        cfg.Archival.StoragePrefix,
			StorageClass:         cfg.Archival.StorageClass,
			PartitionLookahead:   cfg.Archival.PartitionLookahead,
			StorageEndpoint:      cfg.ObjectStorage.Endpoint,
			StorageRegion:        cfg.ObjectStorage.Region,
			StorageAccessKeyID:   cfg.ObjectStorage.AccessKeyID,
			StorageSecretKey:     cfg.ObjectStorage.SecretAccessKey,
			StorageUsePathStyle:  cfg.ObjectStorage.UsePathStyle,
			StorageAllowInsecure: cfg.ObjectStorage.AllowInsecure,
		}
	case workerNameScheduler:
		return schedulerWorkerComparableConfig{
			IntervalSec: cfg.Scheduler.IntervalSec,
		}
	case workerNameDiscovery:
		return discoveryWorkerRuntimeConfig{Interval: cfg.FetcherDiscoveryInterval()}
	default:
		return nil
	}
}

func workerSupportsRuntimeToggle(name string) bool {
	return true
}
