// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"sync"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.BundleReconciler = (*FakeReconciler)(nil)

// ReconcileCall records the arguments passed to a single Reconcile invocation.
type ReconcileCall struct {
	Previous  domain.RuntimeBundle
	Candidate domain.RuntimeBundle
	Snapshot  domain.Snapshot
}

// FakeReconciler records all Reconcile calls for test assertions. It returns
// ReconcileErr (default nil) from every call. Set ReconcileErr to simulate
// reconciliation failures.
type FakeReconciler struct {
	mu           sync.Mutex
	name         string
	phase        domain.ReconcilerPhase
	Calls        []ReconcileCall
	ReconcileErr error // configurable error to return
}

// NewFakeReconciler creates a reconciler with the given name, default
// PhaseSideEffect phase, and no error.
func NewFakeReconciler(name string) *FakeReconciler {
	return &FakeReconciler{
		name: name,
		// Default to PhaseSideEffect — the most common phase for test reconcilers.
		// Tests that need a specific phase should set PhaseValue explicitly.
		phase: domain.PhaseSideEffect,
	}
}

// NewFakeReconcilerWithPhase creates a reconciler with the given name and phase.
func NewFakeReconcilerWithPhase(name string, phase domain.ReconcilerPhase) *FakeReconciler {
	return &FakeReconciler{
		name:  name,
		phase: phase,
	}
}

// Name returns the human-readable identifier for this reconciler.
func (reconciler *FakeReconciler) Name() string {
	return reconciler.name
}

// Phase returns the reconciler's execution phase.
func (reconciler *FakeReconciler) Phase() domain.ReconcilerPhase {
	return reconciler.phase
}

// Reconcile records the call and returns ReconcileErr.
func (reconciler *FakeReconciler) Reconcile(_ context.Context, previous domain.RuntimeBundle,
	candidate domain.RuntimeBundle, snap domain.Snapshot,
) error {
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()

	reconciler.Calls = append(reconciler.Calls, ReconcileCall{
		Previous:  previous,
		Candidate: candidate,
		Snapshot:  snap,
	})

	return reconciler.ReconcileErr
}

// CallCount returns the number of Reconcile invocations observed so far.
func (reconciler *FakeReconciler) CallCount() int {
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()

	return len(reconciler.Calls)
}

// LastCall returns the most recent ReconcileCall, or the zero value if no
// calls have been recorded.
func (reconciler *FakeReconciler) LastCall() ReconcileCall {
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()

	if len(reconciler.Calls) == 0 {
		return ReconcileCall{}
	}

	return reconciler.Calls[len(reconciler.Calls)-1]
}
