// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time interface satisfaction check.
var _ ports.BundleReconciler = (*ConfigBridgeReconciler)(nil)

func TestConfigBridgeReconciler_Name(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	rec, err := NewConfigBridgeReconciler(cm)
	require.NoError(t, err)

	assert.Equal(t, "config-bridge-reconciler", rec.Name())
}

func TestConfigBridgeReconciler_Phase(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	rec, err := NewConfigBridgeReconciler(cm)
	require.NoError(t, err)

	assert.Equal(t, domain.PhaseStateSync, rec.Phase(), "config bridge must run in state-sync phase")
}

func TestNewConfigBridgeReconciler_NilManager(t *testing.T) {
	t.Parallel()

	rec, err := NewConfigBridgeReconciler(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errConfigBridgeManagerRequired)
	assert.Nil(t, rec)
}

func TestConfigBridgeReconciler_Reconcile_Success(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	// The reconciler calls UpdateFromSystemplane which requires seed mode.
	cm.enterSeedMode()

	rec, err := NewConfigBridgeReconciler(cm)
	require.NoError(t, err)

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level":  {Value: "warn"},
			"rate_limit.max": {Value: 500},
		},
	}

	err = rec.Reconcile(context.Background(), nil, nil, snap)
	require.NoError(t, err)

	// Verify the ConfigManager's config was updated through the reconciler.
	updated := cm.Get()
	require.NotNil(t, updated)
	assert.Equal(t, "warn", updated.App.LogLevel)
	assert.Equal(t, 500, updated.RateLimit.Max)
	assert.Equal(t, uint64(1), cm.Version())
}

func TestConfigBridgeReconciler_Reconcile_NotSeedMode(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	// Do NOT enter seed mode — the reconciler should propagate the error.
	rec, err := NewConfigBridgeReconciler(cm)
	require.NoError(t, err)

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level": {Value: "debug"},
		},
	}

	err = rec.Reconcile(context.Background(), nil, nil, snap)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in seed mode")
}
