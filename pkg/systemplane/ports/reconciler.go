// Copyright 2025 Lerian Studio.

package ports

import (
	"context"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// BundleReconciler applies side effects when the runtime bundle or snapshot
// changes. Reconcilers are sorted by Phase before execution, ensuring
// deterministic ordering:
//
//   - PhaseStateSync  → update shared in-process state
//   - PhaseValidation → gates that can reject the change
//   - PhaseSideEffect → external side effects (worker restarts, etc.)
type BundleReconciler interface {
	// Name returns a human-readable identifier for logging and metrics.
	Name() string

	// Phase returns the reconciler's execution phase. The supervisor sorts
	// all reconcilers by phase before running them, so implementations do
	// not need to worry about registration order.
	Phase() domain.ReconcilerPhase

	// Reconcile applies the snapshot to the runtime bundle.
	// previous may be nil on first load; candidate is the newly built bundle.
	Reconcile(ctx context.Context, previous domain.RuntimeBundle,
		candidate domain.RuntimeBundle, snap domain.Snapshot) error
}
