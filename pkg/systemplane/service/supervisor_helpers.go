// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

type resourceAdopter interface {
	AdoptResourcesFrom(previous domain.RuntimeBundle)
}

type rollbackDiscarder interface {
	Discard(context.Context) error
}

func discardFailedCandidate(ctx context.Context, candidate domain.RuntimeBundle, strategy BuildStrategy) {
	if isNilRuntimeBundle(candidate) {
		return
	}

	if strategy == BuildStrategyIncremental {
		discarder, ok := candidate.(rollbackDiscarder)
		if !ok {
			// Incremental candidates may already share adopted resources with the
			// previous bundle. Without an explicit discard contract, avoid closing
			// them here and let the factory-specific cleanup path decide.
			return
		}

		_ = discarder.Discard(ctx)

		return
	}

	if discarder, ok := candidate.(rollbackDiscarder); ok {
		_ = discarder.Discard(ctx)

		return
	}

	_ = candidate.Close(ctx)
}

func startSupervisorSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled
	ctx, span := tracer.Start(ctx, "systemplane.supervisor."+operation)

	return ctx, span
}

func (supervisor *defaultSupervisor) isStopped() bool {
	select {
	case <-supervisor.stopCh:
		return true
	default:
		return false
	}
}

func isNilRuntimeBundle(bundle domain.RuntimeBundle) bool {
	return domain.IsNilValue(bundle)
}

func isNilReconciler(reconciler ports.BundleReconciler) bool {
	return domain.IsNilValue(reconciler)
}

// buildBundle attempts an incremental build first (if the factory supports it
// and a previous bundle exists), falling back to a full build. Returns the
// build strategy used for observability.
func (supervisor *defaultSupervisor) buildBundle(
	ctx context.Context,
	snap domain.Snapshot,
	previousBundle domain.RuntimeBundle,
	prevSnap *domain.Snapshot,
) (domain.RuntimeBundle, BuildStrategy, error) {
	// Incremental path: reuse unchanged components from the previous bundle.
	if incFactory, ok := supervisor.factory.(ports.IncrementalBundleFactory); ok &&
		prevSnap != nil && !isNilRuntimeBundle(previousBundle) {
		candidate, err := incFactory.BuildIncremental(ctx, snap, previousBundle, *prevSnap)
		if err == nil && !isNilRuntimeBundle(candidate) {
			return candidate, BuildStrategyIncremental, nil
		}

		// Discard partially-built candidate to prevent resource leaks.
		if err != nil && !isNilRuntimeBundle(candidate) {
			// RuntimeBundle.Close(ctx) is the contract for releasing held resources.
			_ = candidate.Close(ctx)
		}
		// Incremental build failed — fall through to full build.
	}

	// Full build: construct everything from scratch.
	bundle, err := supervisor.factory.Build(ctx, snap)
	if err != nil {
		return nil, BuildStrategyFull, fmt.Errorf("build full bundle: %w", err)
	}

	return bundle, BuildStrategyFull, nil
}

// sortReconcilersByPhase returns a copy of the reconciler slice sorted by
// phase in ascending order (StateSync → Validation → SideEffect). Reconcilers
// within the same phase retain their original relative order (stable sort).
func sortReconcilersByPhase(reconcilers []ports.BundleReconciler) []ports.BundleReconciler {
	sorted := make([]ports.BundleReconciler, len(reconcilers))
	copy(sorted, reconcilers)

	slices.SortStableFunc(sorted, func(a, b ports.BundleReconciler) int {
		return int(a.Phase()) - int(b.Phase())
	})

	return sorted
}

// mergeUniqueTenantIDs merges extra tenant IDs into the base list, deduplicating
// and sorting the result. This ensures first-seen tenants (not yet in any
// snapshot) are included in bundle rebuilds.
func mergeUniqueTenantIDs(base, extra []string) []string {
	if len(extra) == 0 {
		return base
	}

	seen := make(map[string]struct{}, len(base)+len(extra))

	for _, id := range base {
		seen[id] = struct{}{}
	}

	for _, id := range extra {
		if id == "" {
			continue
		}

		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			base = append(base, id)
		}
	}

	sort.Strings(base)

	return base
}

func cachedTenantIDs(snapshot *domain.Snapshot) []string {
	if snapshot == nil || len(snapshot.TenantSettings) == 0 {
		return nil
	}

	tenantIDs := make([]string, 0, len(snapshot.TenantSettings))

	for tenantID := range snapshot.TenantSettings {
		tenantIDs = append(tenantIDs, tenantID)
	}

	sort.Strings(tenantIDs)

	return tenantIDs
}
