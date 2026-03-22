//go:build unit

// Copyright 2025 Lerian Studio.

package ports

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubReconciler is a minimal test double for BundleReconciler.
type stubReconciler struct {
	name         string
	phase        domain.ReconcilerPhase
	reconcileErr error
	calls        int
}

func (r *stubReconciler) Name() string {
	return r.name
}

func (r *stubReconciler) Phase() domain.ReconcilerPhase {
	return r.phase
}

func (r *stubReconciler) Reconcile(_ context.Context, _ domain.RuntimeBundle,
	_ domain.RuntimeBundle, _ domain.Snapshot,
) error {
	r.calls++
	return r.reconcileErr
}

// Compile-time interface check.
var _ BundleReconciler = (*stubReconciler)(nil)

func TestBundleReconciler_CompileCheck(t *testing.T) {
	t.Parallel()

	var rec BundleReconciler = &stubReconciler{}
	require.NotNil(t, rec)
}

func TestBundleReconciler_Name(t *testing.T) {
	t.Parallel()

	rec := &stubReconciler{name: "my-reconciler"}

	assert.Equal(t, "my-reconciler", rec.Name())
}

func TestBundleReconciler_Phase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		phase domain.ReconcilerPhase
	}{
		{name: "state-sync", phase: domain.PhaseStateSync},
		{name: "validation", phase: domain.PhaseValidation},
		{name: "side-effect", phase: domain.PhaseSideEffect},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &stubReconciler{phase: tt.phase}
			assert.Equal(t, tt.phase, rec.Phase())
		})
	}
}

func TestBundleReconciler_Reconcile_Success(t *testing.T) {
	t.Parallel()

	rec := &stubReconciler{name: "ok-rec"}

	err := rec.Reconcile(context.Background(), nil, nil, domain.Snapshot{})

	require.NoError(t, err)
	assert.Equal(t, 1, rec.calls)
}

func TestBundleReconciler_Reconcile_ReturnsError(t *testing.T) {
	t.Parallel()

	rec := &stubReconciler{reconcileErr: assert.AnError}

	err := rec.Reconcile(context.Background(), nil, nil, domain.Snapshot{})

	require.ErrorIs(t, err, assert.AnError)
}

func TestBundleReconciler_Reconcile_MultipleCalls(t *testing.T) {
	t.Parallel()

	rec := &stubReconciler{}

	for range 5 {
		_ = rec.Reconcile(context.Background(), nil, nil, domain.Snapshot{})
	}

	assert.Equal(t, 5, rec.calls)
}
