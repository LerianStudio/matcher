// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// IMPORTANT: Worker re-entrancy contract
// Each factory closure returns the SAME worker instance (captured from modules).
// The WorkerManager calls Stop() -> UpdateRuntimeConfig() -> Start() on the same
// instance during restarts. All workers MUST support this lifecycle by implementing
// prepareRunState() to reinitialize channels and sync primitives. Workers that do
// NOT support Stop -> Start re-entrancy may hang or panic on restart because
// they can retain closed channels or stale synchronization state.
// registerCriticalWorkers registers workers that are critical when explicitly enabled
// via config (export, cleanup, archival).
func registerCriticalWorkers(wm *WorkerManager, modules *modulesResult) {
	if modules.exportWorker != nil {
		w := modules.exportWorker

		wm.Register("export",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.ExportWorker.Enabled },
			func(cfg *Config) bool { return cfg != nil && cfg.ExportWorker.Enabled },
		)
	}

	if modules.cleanupWorker != nil {
		w := modules.cleanupWorker

		wm.Register("cleanup",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.CleanupWorker.Enabled },
			func(cfg *Config) bool { return cfg != nil && cfg.CleanupWorker.Enabled },
		)
	}

	if modules.archivalWorker != nil {
		w := modules.archivalWorker

		wm.Register("archival",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.Archival.Enabled },
			func(cfg *Config) bool { return cfg != nil && cfg.Archival.Enabled },
		)
	}
}

// buildWorkerManager creates a WorkerManager and registers all workers from the
// init-time module results. Each worker is wrapped in a factory closure that
// returns the pre-built instance. The factory's cfg parameter is available for
// future hot-reload support where workers can be reconstructed from new config.
func buildWorkerManager(modules *modulesResult, existing *WorkerManager, configManager *ConfigManager, logger libLog.Logger) *WorkerManager {
	wm := existing
	if wm == nil {
		wm = NewWorkerManager(logger, configManager)
	}

	if modules == nil {
		return wm
	}

	registerCriticalWorkers(wm, modules)

	// Scheduler worker — always non-critical.
	if modules.schedulerWorker != nil {
		w := modules.schedulerWorker

		wm.Register("scheduler",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(_ *Config) bool { return true }, // always enabled when present
			nil,                                  // never critical
		)
	}

	// Discovery worker — always non-critical.
	if modules.discoveryWorker != nil {
		w := modules.discoveryWorker

		wm.Register("discovery",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.Fetcher.Enabled },
			nil, // never critical
		)
	}

	// Fetcher bridge worker (T-003) — runs only when Fetcher is enabled
	// AND the verified-artifact pipeline is operational. Non-critical:
	// startup failure to Start does NOT abort matcher boot because the
	// bridge worker's absence only affects Fetcher-sourced data; other
	// reconciliation flows continue.
	if modules.bridgeWorker != nil {
		w := modules.bridgeWorker

		wm.Register("fetcher_bridge",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.Fetcher.Enabled },
			nil, // never critical
		)
	}

	// Custody retention sweep worker (T-006) — runs only when Fetcher is
	// enabled AND the verified-artifact pipeline is operational (which
	// means the custody store is available to delete from). Non-critical
	// because retention is a background housekeeping task: orphan
	// accumulation rates are bounded by happy-path bridge throughput so a
	// short sweep outage is operationally tolerable.
	if modules.custodyRetentionWorker != nil {
		w := modules.custodyRetentionWorker

		wm.Register("custody_retention",
			func(_ *Config) (WorkerLifecycle, error) { return w, nil },
			func(cfg *Config) bool { return cfg != nil && cfg.Fetcher.Enabled },
			nil, // never critical
		)
	}

	return wm
}
