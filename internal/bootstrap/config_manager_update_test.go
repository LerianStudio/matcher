// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValueTypeCompatible_ExtendedEdgeCases(t *testing.T) {
	t.Parallel()

	// Complements TestIsValueTypeCompatible in config_manager_helpers_test.go
	// with additional edge cases not covered there.
	tests := []struct {
		name         string
		value        any
		expectedType string
		want         bool
	}{
		{"bool_does_not_match_string", true, "string", false},
		{"string_does_not_match_bool", "true", "bool", false},
		{"nil_is_never_compatible_for_int", nil, "int", false},
		{"nil_is_never_compatible_for_bool", nil, "bool", false},
		{"unknown_type_rejects_nil", nil, "complex", false},
		{"int_does_not_match_string", 42, "string", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isValueTypeCompatible(tt.value, tt.expectedType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestClassifyApplicableChanges_MutableKeysAccepted(t *testing.T) {
	t.Parallel()

	changes := map[string]any{
		"rate_limit.max": 200,
	}
	result := &UpdateResult{}

	applicable := classifyApplicableChanges(changes, result)

	assert.Len(t, applicable, 1)
	assert.Equal(t, 200, applicable["rate_limit.max"])
	assert.Empty(t, result.Rejected)
}

func TestClassifyApplicableChanges_ImmutableKeysRejected(t *testing.T) {
	t.Parallel()

	changes := map[string]any{
		"postgres.primary_host": "evil-host",
	}
	result := &UpdateResult{}

	applicable := classifyApplicableChanges(changes, result)

	assert.Empty(t, applicable)
	require.Len(t, result.Rejected, 1)
	assert.Equal(t, "postgres.primary_host", result.Rejected[0].Key)
	assert.Contains(t, result.Rejected[0].Reason, "not mutable")
}

func TestClassifyApplicableChanges_MixedKeys(t *testing.T) {
	t.Parallel()

	changes := map[string]any{
		"rate_limit.max":        200,
		"rate_limit.expiry_sec": 30,
		"postgres.primary_host": "evil-host",
	}
	result := &UpdateResult{}

	applicable := classifyApplicableChanges(changes, result)

	assert.Len(t, applicable, 2)
	assert.Contains(t, applicable, "rate_limit.max")
	assert.Contains(t, applicable, "rate_limit.expiry_sec")
	require.Len(t, result.Rejected, 1)
}

func TestRollbackViperKeysLocked_RestoresOldValues(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	// Record old value.
	oldValue := cm.viper.Get("rate_limit.max")

	// Set to a new value.
	newValue := 9999
	cm.viper.Set("rate_limit.max", newValue)
	assert.Equal(t, newValue, cm.viper.GetInt("rate_limit.max"))

	// Rollback.
	keys := map[string]any{"rate_limit.max": newValue}
	oldValues := map[string]any{"rate_limit.max": oldValue}

	cm.mu.Lock()
	cm.rollbackViperKeysLocked(keys, oldValues)
	cm.mu.Unlock()

	// Should be restored.
	assert.Equal(t, oldValue, cm.viper.Get("rate_limit.max"))
}

func TestRollbackViperKeysLocked_MultipleKeys(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	oldMax := cm.viper.Get("rate_limit.max")
	oldExpiry := cm.viper.Get("rate_limit.expiry_sec")

	cm.viper.Set("rate_limit.max", 1)
	cm.viper.Set("rate_limit.expiry_sec", 1)

	keys := map[string]any{
		"rate_limit.max":        1,
		"rate_limit.expiry_sec": 1,
	}
	oldValues := map[string]any{
		"rate_limit.max":        oldMax,
		"rate_limit.expiry_sec": oldExpiry,
	}

	cm.mu.Lock()
	cm.rollbackViperKeysLocked(keys, oldValues)
	cm.mu.Unlock()

	assert.Equal(t, oldMax, cm.viper.Get("rate_limit.max"))
	assert.Equal(t, oldExpiry, cm.viper.Get("rate_limit.expiry_sec"))
}

func TestSortedChangeKeys_ReturnsStableOrder(t *testing.T) {
	t.Parallel()

	changes := map[string]any{
		"z_key": 1,
		"a_key": 2,
		"m_key": 3,
	}

	keys := sortedChangeKeys(changes)

	assert.Equal(t, []string{"a_key", "m_key", "z_key"}, keys)
}
