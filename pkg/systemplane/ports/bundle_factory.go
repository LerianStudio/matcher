// Copyright 2025 Lerian Studio.

package ports

import (
	"context"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// BundleFactory builds application-specific runtime bundles from a snapshot.
// The factory is provided by the host application (e.g., Matcher) and
// typically creates DB clients, rate limiters, feature flags, or other
// runtime dependencies derived from the current configuration state.
//
// Build must be safe to call concurrently. If the snapshot is unchanged from
// the previous call, implementations may return a cached bundle.
type BundleFactory interface {
	// Build creates a new RuntimeBundle from the given snapshot.
	// Returns domain.ErrBundleBuildFailed if the bundle cannot be constructed
	// from the provided snapshot.
	Build(ctx context.Context, snap domain.Snapshot) (domain.RuntimeBundle, error)
}
