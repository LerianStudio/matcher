//go:build unit

// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFakeReconciler_DefaultPhase(t *testing.T) {
	t.Parallel()

	rec := NewFakeReconciler("test-rec")

	require.NotNil(t, rec)
	assert.Equal(t, "test-rec", rec.Name())
	assert.Equal(t, domain.PhaseSideEffect, rec.Phase())
	assert.Equal(t, 0, rec.CallCount())
}

func TestNewFakeReconcilerWithPhase_CustomPhase(t *testing.T) {
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

			rec := NewFakeReconcilerWithPhase("rec-"+tt.name, tt.phase)
			assert.Equal(t, "rec-"+tt.name, rec.Name())
			assert.Equal(t, tt.phase, rec.Phase())
		})
	}
}

func TestFakeReconciler_ImplementsBundleReconciler(t *testing.T) {
	t.Parallel()

	rec := NewFakeReconciler("iface-check")

	var _ ports.BundleReconciler = rec
}

func TestFakeReconciler_Reconcile_RecordsCalls(t *testing.T) {
	t.Parallel()

	rec := NewFakeReconciler("recorder")
	ctx := context.Background()
	prev := &FakeBundle{}
	candidate := &FakeBundle{}
	snap := domain.Snapshot{Revision: domain.Revision(7)}

	err := rec.Reconcile(ctx, prev, candidate, snap)

	require.NoError(t, err)
	assert.Equal(t, 1, rec.CallCount())

	require.Len(t, rec.Calls, 1)
	assert.Same(t, prev, rec.Calls[0].Previous.(*FakeBundle))
	assert.Same(t, candidate, rec.Calls[0].Candidate.(*FakeBundle))
	assert.Equal(t, domain.Revision(7), rec.Calls[0].Snapshot.Revision)
}

func TestFakeReconciler_Reconcile_ReturnsConfiguredError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("reconcile failed")
	rec := NewFakeReconciler("failing")
	rec.ReconcileErr = wantErr

	err := rec.Reconcile(context.Background(), nil, &FakeBundle{}, domain.Snapshot{})

	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, rec.CallCount())
}

func TestFakeReconciler_Reconcile_NilError(t *testing.T) {
	t.Parallel()

	rec := NewFakeReconciler("success")

	err := rec.Reconcile(context.Background(), nil, &FakeBundle{}, domain.Snapshot{})

	require.NoError(t, err)
}

func TestFakeReconciler_CallCount_MultipleCalls(t *testing.T) {
	t.Parallel()

	rec := NewFakeReconciler("counter")
	ctx := context.Background()
	snap := domain.Snapshot{}

	for range 5 {
		_ = rec.Reconcile(ctx, nil, &FakeBundle{}, snap)
	}

	assert.Equal(t, 5, rec.CallCount())
	assert.Len(t, rec.Calls, 5)
}

func TestFakeReconciler_ConcurrentReconcileCalls(t *testing.T) {
	t.Parallel()

	rec := NewFakeReconciler("concurrent")
	ctx := context.Background()
	snap := domain.Snapshot{}

	const goroutines = 50

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			err := rec.Reconcile(ctx, nil, &FakeBundle{}, snap)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	assert.Equal(t, goroutines, rec.CallCount())
}
