// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.BundleReconciler = (*WorkerReconciler)(nil)

var errWorkerReconcilerManagerRequired = errors.New("new worker reconciler: worker manager is required")

// WorkerReconciler translates snapshot config changes into WorkerManager
// operations. When the systemplane supervisor detects a configuration change,
// it invokes this reconciler which:
//
//  1. Extracts worker-relevant values from the snapshot into a *Config.
//  2. Delegates to WorkerManager.ApplyConfig, which uses the same slot
//     reconciliation logic as the ConfigManager subscriber (detect
//     enabled/disabled transitions, detect changed intervals/batch sizes,
//     restart affected workers).
//
// This reconciler replaces the ConfigManager -> WorkerManager subscription
// path when the systemplane is active.
type WorkerReconciler struct {
	workerManager *WorkerManager
}

// NewWorkerReconciler creates a new WorkerReconciler.
// The WorkerManager must be non-nil; it is the target of all reconciled
// configuration changes.
func NewWorkerReconciler(wm *WorkerManager) (*WorkerReconciler, error) {
	if wm == nil {
		return nil, errWorkerReconcilerManagerRequired
	}

	return &WorkerReconciler{workerManager: wm}, nil
}

// Name returns the reconciler's identifier for logging and metrics.
func (r *WorkerReconciler) Name() string {
	return "worker-reconciler"
}

// Reconcile extracts a worker-oriented Config from the snapshot and applies
// it to the WorkerManager. The previous and candidate RuntimeBundle parameters
// are unused because worker reconciliation depends only on the snapshot's
// effective configuration values, not on the bundle's infrastructure clients.
func (r *WorkerReconciler) Reconcile(_ context.Context, _, _ domain.RuntimeBundle, snap domain.Snapshot) error {
	cfg := snapshotToWorkerConfig(snap)

	if err := r.workerManager.ApplyConfig(cfg); err != nil { //nolint:contextcheck // ApplyConfig is context-free by design; config reconciliation is a synchronous state transition.
		return fmt.Errorf("worker reconciler: apply config: %w", err)
	}

	return nil
}

// snapshotToWorkerConfig builds a *Config containing ONLY worker-relevant
// fields populated from the given snapshot. All other Config fields remain
// at their zero values — downstream consumers (WorkerManager.onConfigChange,
// workerConfigChanged) must ONLY read worker-specific fields from this config.
//
// Each ConfigValue call specifies the key exactly as registered in
// RegisterMatcherKeys (dotted mapstructure path) and a fallback that matches
// the envDefault from the corresponding Config struct tag.
//
// Uses snapInt/snapBool/snapString from systemplane_factory.go which provide
// more robust type coercion (e.g., string→bool, string→int from JSON/YAML).
func snapshotToWorkerConfig(snap domain.Snapshot) *Config {
	return &Config{
		Fetcher: FetcherConfig{
			Enabled:              snapBool(snap, "fetcher.enabled", defaultFetcherEnabled),
			URL:                  snapString(snap, "fetcher.url", defaultFetcherURL),
			AllowPrivateIPs:      snapBool(snap, "fetcher.allow_private_ips", defaultFetcherAllowPrivateIPs),
			HealthTimeoutSec:     snapInt(snap, "fetcher.health_timeout_sec", defaultKeyFetcherHealthTimeout),
			RequestTimeoutSec:    snapInt(snap, "fetcher.request_timeout_sec", defaultKeyFetcherRequestTimeout),
			DiscoveryIntervalSec: snapInt(snap, "fetcher.discovery_interval_sec", defaultFetcherDiscoveryInt),
			SchemaCacheTTLSec:    snapInt(snap, "fetcher.schema_cache_ttl_sec", defaultKeyFetcherSchemaCacheTTL),
			ExtractionPollSec:    snapInt(snap, "fetcher.extraction_poll_sec", defaultFetcherExtractionPoll),
			ExtractionTimeoutSec: snapInt(snap, "fetcher.extraction_timeout_sec", defaultFetcherExtractionTO),
		},
		ExportWorker: ExportWorkerConfig{
			Enabled:         snapBool(snap, "export_worker.enabled", defaultExportEnabled),
			PollIntervalSec: snapInt(snap, "export_worker.poll_interval_sec", defaultExportPollInt),
			PageSize:        snapInt(snap, "export_worker.page_size", defaultExportPageSize),
		},
		CleanupWorker: CleanupWorkerConfig{
			Enabled:        snapBool(snap, "cleanup_worker.enabled", defaultCleanupEnabled),
			IntervalSec:    snapInt(snap, "cleanup_worker.interval_sec", defaultCleanupInterval),
			BatchSize:      snapInt(snap, "cleanup_worker.batch_size", defaultCleanupBatchSize),
			GracePeriodSec: snapInt(snap, "cleanup_worker.grace_period_sec", defaultCleanupGracePeriod),
		},
		Scheduler: SchedulerConfig{
			IntervalSec: snapInt(snap, "scheduler.interval_sec", defaultSchedulerInterval),
		},
		Archival: ArchivalConfig{
			Enabled:             snapBool(snap, "archival.enabled", defaultArchivalEnabled),
			IntervalHours:       snapInt(snap, "archival.interval_hours", defaultArchivalInterval),
			BatchSize:           snapInt(snap, "archival.batch_size", defaultArchivalBatchSize),
			HotRetentionDays:    snapInt(snap, "archival.hot_retention_days", defaultArchivalHotDays),
			WarmRetentionMonths: snapInt(snap, "archival.warm_retention_months", defaultArchivalWarmMonths),
			ColdRetentionMonths: snapInt(snap, "archival.cold_retention_months", defaultArchivalColdMonths),
			StorageBucket:       snapString(snap, "archival.storage_bucket", ""),
			StoragePrefix:       snapString(snap, "archival.storage_prefix", defaultArchivalStoragePrefix),
			StorageClass:        snapString(snap, "archival.storage_class", defaultArchivalStorageClass),
			PartitionLookahead:  snapInt(snap, "archival.partition_lookahead", defaultArchivalPartitionLA),
		},
	}
}
