//go:build unit

// Copyright 2025 Lerian Studio.

package service

import (
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRegistry creates a registry pre-loaded with keys spanning all
// mutable apply behaviors, plus a bootstrap-only and an immutable key.
func newTestRegistry() registry.Registry {
	reg := registry.New()

	reg.MustRegister(domain.KeyDef{
		Key:              "live.key",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "default",
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
	})

	reg.MustRegister(domain.KeyDef{
		Key:              "worker.key",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeInt,
		DefaultValue:     60,
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyWorkerReconcile,
		MutableAtRuntime: true,
	})

	reg.MustRegister(domain.KeyDef{
		Key:              "bundle.key",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeBool,
		DefaultValue:     false,
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyBundleRebuild,
		MutableAtRuntime: true,
	})

	reg.MustRegister(domain.KeyDef{
		Key:              "combo.key",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "combo",
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
		MutableAtRuntime: true,
	})

	reg.MustRegister(domain.KeyDef{
		Key:              "bootstrap.key",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "boot",
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyBootstrapOnly,
		MutableAtRuntime: true,
	})

	reg.MustRegister(domain.KeyDef{
		Key:              "immutable.key",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "frozen",
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: false,
	})

	return reg
}

func TestEscalate_EmptyBatch_ReturnsLiveRead(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()

	behavior, keys, err := Escalate(reg, nil)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyLiveRead, behavior)
	assert.Nil(t, keys)
}

func TestEscalate_EmptySlice_ReturnsLiveRead(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()

	behavior, keys, err := Escalate(reg, []ports.WriteOp{})

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyLiveRead, behavior)
	assert.Nil(t, keys)
}

func TestEscalate_OnlyLiveReadKeys_ReturnsLiveRead(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "live.key", Value: "new-value"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyLiveRead, behavior)
	assert.Equal(t, []string{"live.key"}, keys)
}

func TestEscalate_WorkerReconcileKey_EscalatesToWorkerReconcile(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "live.key", Value: "v"},
		{Key: "worker.key", Value: 30},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyWorkerReconcile, behavior)
	assert.Equal(t, []string{"worker.key"}, keys)
}

func TestEscalate_BundleRebuildKey_EscalatesToBundleRebuild(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "live.key", Value: "v"},
		{Key: "bundle.key", Value: true},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyBundleRebuild, behavior)
	assert.Equal(t, []string{"bundle.key"}, keys)
}

func TestEscalate_MixOfRebuildAndReconcile_EscalatesToCombo(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "worker.key", Value: 10},
		{Key: "bundle.key", Value: true},
	}

	// bundle-rebuild (strength 3) > worker-reconcile (strength 2),
	// but neither is the combo. The strongest single key wins.
	behavior, keys, err := Escalate(reg, ops)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyBundleRebuild, behavior)
	assert.Equal(t, []string{"bundle.key"}, keys)
}

func TestEscalate_ComboKey_EscalatesToBundleRebuildAndReconcile(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "live.key", Value: "v"},
		{Key: "combo.key", Value: "updated"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyBundleRebuildAndReconcile, behavior)
	assert.Equal(t, []string{"combo.key"}, keys)
}

func TestEscalate_MultipleKeysAtSameStrength_AllCollected(t *testing.T) {
	t.Parallel()

	// Register a second live-read key.
	reg := newTestRegistry()
	reg.MustRegister(domain.KeyDef{
		Key:              "live.key2",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "default2",
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
	})

	ops := []ports.WriteOp{
		{Key: "live.key", Value: "a"},
		{Key: "live.key2", Value: "b"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyLiveRead, behavior)
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "live.key")
	assert.Contains(t, keys, "live.key2")
}

func TestEscalate_BootstrapOnlyKey_RejectsWithErrKeyNotMutable(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "live.key", Value: "v"},
		{Key: "bootstrap.key", Value: "nope"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyNotMutable)
	assert.Contains(t, err.Error(), "bootstrap.key")
	assert.Empty(t, string(behavior))
	assert.Nil(t, keys)
}

func TestEscalate_ImmutableKey_RejectsWithErrKeyNotMutable(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "immutable.key", Value: "attempt"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyNotMutable)
	assert.Contains(t, err.Error(), "immutable.key")
	assert.Empty(t, string(behavior))
	assert.Nil(t, keys)
}

func TestEscalate_UnknownKey_RejectsWithErrKeyUnknown(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "no.such.key", Value: "v"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
	assert.Contains(t, err.Error(), "no.such.key")
	assert.Empty(t, string(behavior))
	assert.Nil(t, keys)
}

func TestEscalate_BootstrapBeforeMutableKeys_RejectsImmediately(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	// bootstrap.key appears first, so it should short-circuit.
	ops := []ports.WriteOp{
		{Key: "bootstrap.key", Value: "blocked"},
		{Key: "live.key", Value: "ok"},
		{Key: "worker.key", Value: 42},
	}

	_, _, err := Escalate(reg, ops)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyNotMutable)
}

func TestEscalate_SingleComboKey_ReturnsComboWithThatKey(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "combo.key", Value: "val"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyBundleRebuildAndReconcile, behavior)
	assert.Equal(t, []string{"combo.key"}, keys)
}

func TestEscalate_AllMutableBehaviors_HighestWins(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "live.key", Value: "a"},
		{Key: "worker.key", Value: 1},
		{Key: "bundle.key", Value: true},
		{Key: "combo.key", Value: "x"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyBundleRebuildAndReconcile, behavior)
	assert.Equal(t, []string{"combo.key"}, keys)
}

func TestEscalate_DuplicateKeys_RejectsBatch(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry()
	ops := []ports.WriteOp{
		{Key: "live.key", Value: "first"},
		{Key: "live.key", Value: "second"},
	}

	behavior, keys, err := Escalate(reg, ops)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
	assert.Contains(t, err.Error(), "duplicate key \"live.key\" in batch")
	assert.Empty(t, string(behavior))
	assert.Nil(t, keys)
}
