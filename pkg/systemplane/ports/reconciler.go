// Copyright 2025 Lerian Studio.

package ports

import (
	"context"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// BundleReconciler applies side effects when the runtime bundle or snapshot
// changes.
type BundleReconciler interface {
	// Name returns a human-readable identifier for logging and metrics.
	Name() string

	// Reconcile applies the snapshot to the runtime bundle.
	// previous may be nil on first load; candidate is the newly built bundle.
	Reconcile(ctx context.Context, previous domain.RuntimeBundle,
		candidate domain.RuntimeBundle, snap domain.Snapshot) error
}
