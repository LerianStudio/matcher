//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyBehavior_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		behavior ApplyBehavior
		want     bool
	}{
		{name: "live-read is valid", behavior: ApplyLiveRead, want: true},
		{name: "worker-reconcile is valid", behavior: ApplyWorkerReconcile, want: true},
		{name: "bundle-rebuild is valid", behavior: ApplyBundleRebuild, want: true},
		{name: "bundle-rebuild+worker-reconcile is valid", behavior: ApplyBundleRebuildAndReconcile, want: true},
		{name: "bootstrap-only is valid", behavior: ApplyBootstrapOnly, want: true},
		{name: "empty is invalid", behavior: ApplyBehavior(""), want: false},
		{name: "unknown is invalid", behavior: ApplyBehavior("unknown"), want: false},
		{name: "uppercase is invalid", behavior: ApplyBehavior("LIVE-READ"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.behavior.IsValid())
		})
	}
}

func TestApplyBehavior_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "live-read", ApplyLiveRead.String())
	assert.Equal(t, "worker-reconcile", ApplyWorkerReconcile.String())
	assert.Equal(t, "bundle-rebuild", ApplyBundleRebuild.String())
	assert.Equal(t, "bundle-rebuild+worker-reconcile", ApplyBundleRebuildAndReconcile.String())
	assert.Equal(t, "bootstrap-only", ApplyBootstrapOnly.String())
}

func TestApplyBehavior_Strength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		behavior ApplyBehavior
		want     int
	}{
		{name: "bootstrap-only is 0", behavior: ApplyBootstrapOnly, want: 0},
		{name: "live-read is 1", behavior: ApplyLiveRead, want: 1},
		{name: "worker-reconcile is 2", behavior: ApplyWorkerReconcile, want: 2},
		{name: "bundle-rebuild is 3", behavior: ApplyBundleRebuild, want: 3},
		{name: "bundle-rebuild+worker-reconcile is 4", behavior: ApplyBundleRebuildAndReconcile, want: 4},
		{name: "unknown returns -1", behavior: ApplyBehavior("unknown"), want: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.behavior.Strength())
		})
	}
}

func TestApplyBehavior_StrengthOrdering(t *testing.T) {
	t.Parallel()

	// Verify the ordering invariant: each behavior is strictly stronger than the previous.
	ordered := []ApplyBehavior{
		ApplyBootstrapOnly,
		ApplyLiveRead,
		ApplyWorkerReconcile,
		ApplyBundleRebuild,
		ApplyBundleRebuildAndReconcile,
	}

	for i := 1; i < len(ordered); i++ {
		assert.Greater(t, ordered[i].Strength(), ordered[i-1].Strength(),
			"%s should be stronger than %s", ordered[i], ordered[i-1])
	}
}

func TestParseApplyBehavior(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    ApplyBehavior
		wantErr bool
	}{
		{name: "lowercase live-read", input: "live-read", want: ApplyLiveRead},
		{name: "lowercase worker-reconcile", input: "worker-reconcile", want: ApplyWorkerReconcile},
		{name: "lowercase bundle-rebuild", input: "bundle-rebuild", want: ApplyBundleRebuild},
		{name: "lowercase bundle-rebuild+worker-reconcile", input: "bundle-rebuild+worker-reconcile", want: ApplyBundleRebuildAndReconcile},
		{name: "lowercase bootstrap-only", input: "bootstrap-only", want: ApplyBootstrapOnly},
		{name: "uppercase LIVE-READ", input: "LIVE-READ", want: ApplyLiveRead},
		{name: "mixed case Bundle-Rebuild", input: "Bundle-Rebuild", want: ApplyBundleRebuild},
		{name: "with whitespace", input: "  live-read  ", want: ApplyLiveRead},
		{name: "invalid value", input: "invalid", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseApplyBehavior(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidApplyBehavior)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
