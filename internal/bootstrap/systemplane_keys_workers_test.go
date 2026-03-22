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

// --- matcherKeyDefsWorkers ---

func TestMatcherKeyDefsWorkers_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsWorkers()

	require.NotEmpty(t, defs, "matcherKeyDefsWorkers must return at least one key definition")
}

func TestMatcherKeyDefsWorkers_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsWorkers()

	expectedKeys := []string{
		"webhook.timeout_sec",
		"cleanup_worker.enabled",
		"cleanup_worker.interval_sec",
		"cleanup_worker.batch_size",
		"cleanup_worker.grace_period_sec",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsWorkers_WebhookTimeout(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsWorkers()
	require.NotEmpty(t, defs)

	webhookDef := defs[0]
	assert.Equal(t, "webhook.timeout_sec", webhookDef.Key)
	assert.Equal(t, domain.ValueTypeInt, webhookDef.ValueType)
	assert.Equal(t, domain.ApplyLiveRead, webhookDef.ApplyBehavior)
	assert.Equal(t, "webhook", webhookDef.Group)
	assert.NotNil(t, webhookDef.Validator, "webhook.timeout_sec must have a validator")
}

func TestMatcherKeyDefsWorkers_CleanupEnabledIsBool(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsWorkers()
	require.True(t, len(defs) >= 2)

	cleanupEnabled := defs[1]
	assert.Equal(t, "cleanup_worker.enabled", cleanupEnabled.Key)
	assert.Equal(t, domain.ValueTypeBool, cleanupEnabled.ValueType)
	assert.Equal(t, domain.ApplyBundleRebuildAndReconcile, cleanupEnabled.ApplyBehavior)
}

func TestMatcherKeyDefsWorkers_CleanupIntFieldsHaveValidators(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsWorkers()

	for _, def := range defs {
		if def.ValueType == domain.ValueTypeInt {
			t.Run(def.Key+"_has_validator", func(t *testing.T) {
				t.Parallel()

				assert.NotNil(t, def.Validator, "%s must have a validator", def.Key)
			})
		}
	}
}

func TestMatcherKeyDefsWorkers_CleanupFieldsAreWorkerReconcile(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsWorkers()

	cleanupIntKeys := map[string]bool{
		"cleanup_worker.interval_sec":     true,
		"cleanup_worker.batch_size":       true,
		"cleanup_worker.grace_period_sec": true,
	}

	for _, def := range defs {
		if cleanupIntKeys[def.Key] {
			t.Run(def.Key+"_is_worker_reconcile", func(t *testing.T) {
				t.Parallel()

				assert.Equal(t, domain.ApplyWorkerReconcile, def.ApplyBehavior,
					"%s must use worker reconcile apply behavior", def.Key)
			})
		}
	}
}

func TestMatcherKeyDefsWorkers_AllGlobalScope(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsWorkers()

	for _, def := range defs {
		t.Run(def.Key+"_global_scope", func(t *testing.T) {
			t.Parallel()

			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}
