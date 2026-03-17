//go:build unit

// Copyright 2025 Lerian Studio.

package bootstrap

import (
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
)

func TestIsBootstrapOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		def  domain.KeyDef
		want bool
	}{
		{name: "bootstrap-only apply behavior", def: domain.KeyDef{ApplyBehavior: domain.ApplyBootstrapOnly, MutableAtRuntime: true}, want: true},
		{name: "non-mutable key is bootstrap-only", def: domain.KeyDef{ApplyBehavior: domain.ApplyLiveRead, MutableAtRuntime: false}, want: true},
		{name: "mutable runtime key is runtime-managed", def: domain.KeyDef{ApplyBehavior: domain.ApplyLiveRead, MutableAtRuntime: true}, want: false},
		{name: "worker reconcile key is runtime-managed", def: domain.KeyDef{ApplyBehavior: domain.ApplyWorkerReconcile, MutableAtRuntime: true}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsBootstrapOnly(tt.def))
		})
	}
}

func TestIsBootstrapOnly_ReliesOnMetadataNotStrings(t *testing.T) {
	t.Parallel()

	def := domain.KeyDef{Key: "SERVER.ADDRESS", ApplyBehavior: domain.ApplyLiveRead, MutableAtRuntime: true}
	assert.False(t, IsBootstrapOnly(def))
}

func TestIsRuntimeManaged_IsComplement(t *testing.T) {
	t.Parallel()

	defs := []domain.KeyDef{
		{ApplyBehavior: domain.ApplyBootstrapOnly, MutableAtRuntime: true},
		{ApplyBehavior: domain.ApplyLiveRead, MutableAtRuntime: false},
		{ApplyBehavior: domain.ApplyLiveRead, MutableAtRuntime: true},
		{ApplyBehavior: domain.ApplyWorkerReconcile, MutableAtRuntime: true},
	}

	for i, def := range defs {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			t.Parallel()
			assert.NotEqual(t, IsBootstrapOnly(def), IsRuntimeManaged(def))
		})
	}
}
