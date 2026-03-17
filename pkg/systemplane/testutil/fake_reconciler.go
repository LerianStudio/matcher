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
	Calls        []ReconcileCall
	ReconcileErr error // configurable error to return
}

// NewFakeReconciler creates a reconciler with the given name and no error.
func NewFakeReconciler(name string) *FakeReconciler {
	return &FakeReconciler{
		name: name,
	}
}

// Name returns the human-readable identifier for this reconciler.
func (r *FakeReconciler) Name() string {
	return r.name
}

// Reconcile records the call and returns ReconcileErr.
func (r *FakeReconciler) Reconcile(_ context.Context, previous domain.RuntimeBundle,
	candidate domain.RuntimeBundle, snap domain.Snapshot,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Calls = append(r.Calls, ReconcileCall{
		Previous:  previous,
		Candidate: candidate,
		Snapshot:  snap,
	})

	return r.ReconcileErr
}

// CallCount returns the number of Reconcile invocations observed so far.
func (r *FakeReconciler) CallCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.Calls)
}

// LastCall returns the most recent ReconcileCall, or the zero value if no
// calls have been recorded.
func (r *FakeReconciler) LastCall() ReconcileCall {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.Calls) == 0 {
		return ReconcileCall{}
	}

	return r.Calls[len(r.Calls)-1]
}
