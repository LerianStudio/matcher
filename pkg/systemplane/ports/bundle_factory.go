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

// IncrementalBundleFactory extends BundleFactory with component-granular
// rebuilds. When the Supervisor detects that only a subset of infrastructure
// components changed between snapshots, it calls BuildIncremental instead of
// Build, allowing the factory to reuse unchanged components from the previous
// bundle.
//
// The implementation is responsible for:
//  1. Diffing prevSnap vs snap to determine which components changed.
//  2. Building fresh instances only for changed components.
//  3. Transferring unchanged components from previous (pointer move).
//  4. Nil-ing transferred pointers in previous so that previous.Close()
//     does NOT close borrowed components.
//
// The Supervisor calls Close on previous AFTER the swap, so only replaced
// components are torn down.
type IncrementalBundleFactory interface {
	BundleFactory

	// BuildIncremental creates a new bundle, reusing unchanged components
	// from previous. prevSnap is the snapshot the previous bundle was built
	// from — the factory diffs it against snap to identify changed components.
	//
	// The factory MUST nil-out transferred component pointers in previous
	// so that previous.Close() only closes replaced components.
	BuildIncremental(ctx context.Context, snap domain.Snapshot,
		previous domain.RuntimeBundle, prevSnap domain.Snapshot) (domain.RuntimeBundle, error)
}
