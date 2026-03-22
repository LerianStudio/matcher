//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

func TestMatcherKeyDefsArchival_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsArchival()

	require.NotEmpty(t, defs, "matcherKeyDefsArchival must return at least one key definition")
}

func TestMatcherKeyDefsArchival_CombinesAllSubGroups(t *testing.T) {
	t.Parallel()

	scheduler := matcherKeyDefsScheduler()
	lifecycle := matcherKeyDefsArchivalLifecycle()
	storage := matcherKeyDefsArchivalStorage()
	runtime := matcherKeyDefsArchivalRuntime()

	combined := matcherKeyDefsArchival()

	expected := len(scheduler) + len(lifecycle) + len(storage) + len(runtime)
	assert.Len(t, combined, expected,
		"matcherKeyDefsArchival must combine scheduler + lifecycle + storage + runtime")
}

func TestMatcherKeyDefsScheduler_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsScheduler()

	require.Len(t, defs, 1)

	def := defs[0]
	assert.Equal(t, "scheduler.interval_sec", def.Key)
	assert.Equal(t, domain.KindConfig, def.Kind)
	assert.Equal(t, "scheduler", def.Group)
	assert.Equal(t, domain.ValueTypeInt, def.ValueType)
	assert.NotNil(t, def.Validator)
	assert.Equal(t, domain.ApplyWorkerReconcile, def.ApplyBehavior)
	assert.True(t, def.MutableAtRuntime)
	assert.NotEmpty(t, def.Description)
}

func TestMatcherKeyDefsArchivalLifecycle_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsArchivalLifecycle()

	require.Len(t, defs, 6)

	expectedKeys := []string{
		"archival.enabled",
		"archival.interval_hours",
		"archival.hot_retention_days",
		"archival.warm_retention_months",
		"archival.cold_retention_months",
		"archival.batch_size",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "archival", def.Group)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsArchivalLifecycle_EnabledIsBundleRebuildAndReconcile(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsArchivalLifecycle()
	require.NotEmpty(t, defs)

	enabledDef := defs[0]
	assert.Equal(t, "archival.enabled", enabledDef.Key)
	assert.Equal(t, domain.ApplyBundleRebuildAndReconcile, enabledDef.ApplyBehavior)
	assert.Equal(t, domain.ValueTypeBool, enabledDef.ValueType)
}

func TestMatcherKeyDefsArchivalLifecycle_IntKeysAreWorkerReconcile(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsArchivalLifecycle()

	for _, def := range defs {
		if def.ValueType == domain.ValueTypeInt {
			t.Run(def.Key, func(t *testing.T) {
				t.Parallel()

				assert.Equal(t, domain.ApplyWorkerReconcile, def.ApplyBehavior,
					"archival integer key %q must use ApplyWorkerReconcile", def.Key)
				assert.NotNil(t, def.Validator,
					"archival integer key %q must have a validator", def.Key)
			})
		}
	}
}

func TestMatcherKeyDefsArchivalStorage_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsArchivalStorage()

	require.Len(t, defs, 3)

	expectedKeys := []string{
		"archival.storage_bucket",
		"archival.storage_prefix",
		"archival.storage_class",
	}

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "archival", def.Group)
			assert.Equal(t, domain.ValueTypeString, def.ValueType)
			assert.Equal(t, domain.ApplyBundleRebuildAndReconcile, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsArchivalRuntime_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsArchivalRuntime()

	require.Len(t, defs, 2)

	tests := []struct {
		key      string
		behavior domain.ApplyBehavior
	}{
		{
			key:      "archival.partition_lookahead",
			behavior: domain.ApplyWorkerReconcile,
		},
		{
			key:      "archival.presign_expiry_sec",
			behavior: domain.ApplyLiveRead,
		},
	}

	for i, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, tt.key, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "archival", def.Group)
			assert.Equal(t, domain.ValueTypeInt, def.ValueType)
			assert.NotNil(t, def.Validator)
			assert.Equal(t, tt.behavior, def.ApplyBehavior)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
		})
	}
}

func TestMatcherKeyDefsArchival_NoSecretKeys(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsArchival()

	for _, def := range defs {
		assert.False(t, def.Secret,
			"archival key %q must not be a secret", def.Key)
		assert.Equal(t, domain.RedactNone, def.RedactPolicy,
			"archival key %q must have RedactNone policy", def.Key)
	}
}

func TestMatcherKeyDefsArchival_AllKeysHaveGlobalScope(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsArchival()

	for _, def := range defs {
		require.Len(t, def.AllowedScopes, 1,
			"archival key %q must have exactly one allowed scope", def.Key)
		assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0],
			"archival key %q must have ScopeGlobal", def.Key)
	}
}
