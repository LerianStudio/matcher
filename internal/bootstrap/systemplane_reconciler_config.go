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
var _ ports.BundleReconciler = (*ConfigBridgeReconciler)(nil)

var errConfigBridgeManagerRequired = errors.New("config bridge reconciler: config manager is required")

// ConfigBridgeReconciler synchronizes the ConfigManager's atomic pointer with
// the systemplane supervisor's snapshot. When the supervisor detects a
// configuration change, it invokes this reconciler which converts the snapshot
// into a full *Config and atomically stores it. This ensures all existing
// per-request consumers (rate limiters, health checks, etc.) see
// systemplane-backed values through the familiar configManager.Get() path.
//
// This reconciler MUST run before downstream reconcilers (e.g., WorkerReconciler)
// that may read from ConfigManager.Get() during their own reconciliation.
type ConfigBridgeReconciler struct {
	configManager *ConfigManager
}

// NewConfigBridgeReconciler creates a new ConfigBridgeReconciler.
// The ConfigManager must be non-nil; it is the target of all reconciled
// configuration changes.
func NewConfigBridgeReconciler(cm *ConfigManager) (*ConfigBridgeReconciler, error) {
	if cm == nil {
		return nil, errConfigBridgeManagerRequired
	}

	return &ConfigBridgeReconciler{configManager: cm}, nil
}

// Name returns the reconciler's identifier for logging and metrics.
func (r *ConfigBridgeReconciler) Name() string {
	return "config-bridge-reconciler"
}

// Phase returns PhaseStateSync because the config bridge updates shared
// in-process state (ConfigManager's atomic pointer) that downstream
// reconcilers depend on via configManager.Get().
func (r *ConfigBridgeReconciler) Phase() domain.ReconcilerPhase {
	return domain.PhaseStateSync
}

// Reconcile converts the snapshot into a full *Config and atomically updates
// the ConfigManager's config pointer. The previous and candidate RuntimeBundle
// parameters are unused because the config bridge depends only on the
// snapshot's effective configuration values.
func (r *ConfigBridgeReconciler) Reconcile(_ context.Context, _, _ domain.RuntimeBundle, snap domain.Snapshot) error {
	if err := r.configManager.UpdateFromSystemplane(snap); err != nil { //nolint:contextcheck // Validate() creates its own context.Background(); standalone validation does not need request-scoped context.
		return fmt.Errorf("config bridge reconciler: %w", err)
	}

	return nil
}
