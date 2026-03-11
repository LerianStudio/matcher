package bootstrap

import (
	"context"
	"fmt"
	"reflect"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
)

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

			if err := wm.restartSlotLocked(ctx, slot, oldCfg, newCfg); err != nil {
				wm.logger.Log(ctx, libLog.LevelWarn,
					fmt.Sprintf("worker %q failed to restart after config change: %v", slot.name, err))
			}
		}

	default:
		// Was disabled, still disabled — nothing to do.
	}
}

// restartSlotLocked restarts a running worker with a newly built instance.
// If the factory returns the same instance, restart is skipped to avoid
// stopping a worker that cannot be safely re-started with the same object.
func (wm *WorkerManager) restartSlotLocked(ctx context.Context, slot *workerSlot, oldCfg, newCfg *Config) error {
	if slot == nil {
		return nil
	}

	if slot.instance == nil {
		return wm.startSlotLocked(ctx, slot, newCfg)
	}

	if slot.factory == nil {
		return errWorkerFactoryRequired
	}

	candidate, err := slot.factory(newCfg)
	if err != nil {
		return fmt.Errorf("create worker %q for restart: %w", slot.name, err)
	}

	if candidate == nil {
		return fmt.Errorf("worker %q: %w", slot.name, errWorkerDependencyUnavailable)
	}

	previous := slot.instance
	sameInstance := sameWorkerInstance(previous, candidate)

	wm.stopSlotLocked(ctx, slot)

	// Always apply runtime config to the candidate before starting, regardless of
	// whether it is the same instance. If factories are refactored to return new
	// instances in the future, this ensures they always receive the latest config.
	applyWorkerRuntimeConfig(ctx, slot.name, candidate, newCfg)

	if err := candidate.Start(ctx); err != nil {
		if rollbackErr := wm.rollbackAfterRestartFailureLocked(ctx, slot, previous, oldCfg, sameInstance); rollbackErr != nil {
			return fmt.Errorf("start worker %q after restart: %w (rollback failed: %w)", slot.name, err, rollbackErr)
		}

		return fmt.Errorf("start worker %q after restart: %w (rolled back to previous config)", slot.name, err)
	}

	slot.instance = candidate

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

	if err := rollbackCandidate.Start(ctx); err != nil {
		return fmt.Errorf("restart previous worker %q: %w", slot.name, err)
	}

	slot.instance = rollbackCandidate

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
		applyWorkerRuntimeConfig(ctx, slot.name, previous, oldCfg)
		return previous, nil
	}

	if slot.factory == nil {
		return previous, nil
	}

	rebuilt, err := slot.factory(oldCfg)
	if err != nil {
		return nil, fmt.Errorf("rebuild rollback worker %q: %w", slot.name, err)
	}

	if rebuilt == nil {
		return previous, nil
	}

	if sameWorkerInstance(rebuilt, previous) {
		applyWorkerRuntimeConfig(ctx, slot.name, rebuilt, oldCfg)
	}

	return rebuilt, nil
}

func applyWorkerRuntimeConfig(ctx context.Context, name string, worker WorkerLifecycle, cfg *Config) {
	if cfg == nil || worker == nil {
		return
	}

	switch name {
	case "export":
		exportWorker, ok := worker.(interface {
			UpdateRuntimeConfig(reportingWorker.ExportWorkerConfig)
		})
		if !ok {
			return
		}

		exportWorker.UpdateRuntimeConfig(reportingWorker.ExportWorkerConfig{
			PollInterval: cfg.ExportWorkerPollInterval(),
			PageSize:     cfg.ExportWorker.PageSize,
		})
	case "cleanup":
		cleanupWorker, ok := worker.(interface {
			UpdateRuntimeConfig(reportingWorker.CleanupWorkerConfig)
		})
		if !ok {
			return
		}

		cleanupWorker.UpdateRuntimeConfig(reportingWorker.CleanupWorkerConfig{
			Interval:              cfg.CleanupWorkerInterval(),
			BatchSize:             cfg.CleanupWorkerBatchSize(),
			FileDeleteGracePeriod: cfg.CleanupWorkerGracePeriod(),
		})
	case "archival":
		archivalWorker, ok := worker.(interface {
			UpdateRuntimeConfig(governanceWorker.ArchivalWorkerConfig)
		})
		if !ok {
			return
		}

		archivalWorker.UpdateRuntimeConfig(governanceWorker.ArchivalWorkerConfig{
			Interval:            cfg.ArchivalInterval(),
			HotRetentionDays:    cfg.Archival.HotRetentionDays,
			WarmRetentionMonths: cfg.Archival.WarmRetentionMonths,
			ColdRetentionMonths: cfg.Archival.ColdRetentionMonths,
			BatchSize:           cfg.Archival.BatchSize,
			StorageBucket:       cfg.Archival.StorageBucket,
			StoragePrefix:       cfg.Archival.StoragePrefix,
			StorageClass:        cfg.Archival.StorageClass,
			PartitionLookahead:  cfg.Archival.PartitionLookahead,
			PresignExpiry:       archivalPresignExpiryWithContext(ctx, cfg),
		})
	case "scheduler":
		schedulerWorker, ok := worker.(interface {
			UpdateRuntimeConfig(configWorker.SchedulerWorkerConfig)
		})
		if !ok {
			return
		}

		schedulerWorker.UpdateRuntimeConfig(configWorker.SchedulerWorkerConfig{
			Interval: cfg.SchedulerInterval(),
		})
	}
}

func archivalPresignExpiryWithContext(ctx context.Context, cfg *Config) time.Duration {
	if cfg == nil {
		return time.Hour
	}

	const (
		maxPresignExpirySeconds     = 604800
		defaultPresignExpirySeconds = 3600
	)

	if cfg.Archival.PresignExpirySec <= 0 {
		return time.Duration(defaultPresignExpirySeconds) * time.Second
	}

	if cfg.Archival.PresignExpirySec > maxPresignExpirySeconds {
		if cfg.Logger != nil {
			cfg.Logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("ARCHIVAL_PRESIGN_EXPIRY_SEC=%d exceeds S3 maximum of %d seconds, capping to maximum",
				cfg.Archival.PresignExpirySec, maxPresignExpirySeconds))
		}

		return time.Duration(maxPresignExpirySeconds) * time.Second
	}

	return time.Duration(cfg.Archival.PresignExpirySec) * time.Second
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
