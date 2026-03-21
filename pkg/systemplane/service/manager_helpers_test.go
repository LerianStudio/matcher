//go:build unit

// Copyright 2025 Lerian Studio.

package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

func TestCloneSnapshot_DeepClonesNestedRuntimeValues(t *testing.T) {
	t.Parallel()

	snapshot := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"feature.flags": {
				Key: "feature.flags",
				Value: map[string]any{
					"enabled": true,
					"nested":  []any{"alpha", map[string]any{"beta": true}},
				},
				Default: map[string]any{"enabled": false},
				Override: []any{
					map[string]any{"kind": "manual"},
				},
			},
		},
	}

	cloned := cloneSnapshot(snapshot)

	valueMap, ok := cloned.Configs["feature.flags"].Value.(map[string]any)
	require.True(t, ok)
	nestedSlice, ok := valueMap["nested"].([]any)
	require.True(t, ok)
	nestedMap, ok := nestedSlice[1].(map[string]any)
	require.True(t, ok)

	defaultMap, ok := cloned.Configs["feature.flags"].Default.(map[string]any)
	require.True(t, ok)
	overrideSlice, ok := cloned.Configs["feature.flags"].Override.([]any)
	require.True(t, ok)
	overrideMap, ok := overrideSlice[0].(map[string]any)
	require.True(t, ok)

	nestedMap["beta"] = false
	defaultMap["enabled"] = true
	overrideMap["kind"] = "automatic"

	originalValueMap := snapshot.Configs["feature.flags"].Value.(map[string]any)
	originalNestedSlice := originalValueMap["nested"].([]any)
	originalNestedMap := originalNestedSlice[1].(map[string]any)
	originalDefaultMap := snapshot.Configs["feature.flags"].Default.(map[string]any)
	originalOverrideSlice := snapshot.Configs["feature.flags"].Override.([]any)
	originalOverrideMap := originalOverrideSlice[0].(map[string]any)

	assert.Equal(t, true, originalNestedMap["beta"])
	assert.Equal(t, false, originalDefaultMap["enabled"])
	assert.Equal(t, "manual", originalOverrideMap["kind"])
}

func TestRedactValue_RedactMaskMasksStringTail(t *testing.T) {
	t.Parallel()

	masked := redactValue(domain.KeyDef{RedactPolicy: domain.RedactMask}, "sk_live_12345678")

	assert.Equal(t, "************5678", masked)
}

func TestRedactValue_RedactMaskFallsBackForNonString(t *testing.T) {
	t.Parallel()

	masked := redactValue(domain.KeyDef{RedactPolicy: domain.RedactMask}, map[string]any{"token": "abc"})

	assert.Equal(t, "****", masked)
}
