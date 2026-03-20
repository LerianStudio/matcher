// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

var (
	errSupervisorBuilderRequired = errors.New("new supervisor: snapshot builder is required")
	errSupervisorFactoryRequired = errors.New("new supervisor: bundle factory is required")
	errSupervisorNilReconciler   = errors.New("new supervisor: reconciler is nil")
)

// Supervisor manages the runtime bundle lifecycle with atomic snapshot/bundle swaps.
type Supervisor interface {
	Current() domain.RuntimeBundle
	Snapshot() domain.Snapshot
	PublishSnapshot(ctx context.Context, snap domain.Snapshot, reason string) error
	ReconcileCurrent(ctx context.Context, snap domain.Snapshot, reason string) error
	Reload(ctx context.Context, reason string) error
	Stop(ctx context.Context) error
}

// BuildStrategy describes which build path the Supervisor took during a reload.
type BuildStrategy string

const (
	// BuildStrategyFull indicates all bundle components were built from scratch.
	BuildStrategyFull BuildStrategy = "full"

	// BuildStrategyIncremental indicates only changed components were rebuilt;
	// unchanged components were reused from the previous bundle.
	BuildStrategyIncremental BuildStrategy = "incremental"
)

// ReloadEvent carries structured information about a completed reload cycle.
// Passed to the optional Observer callback on SupervisorConfig.
type ReloadEvent struct {
	Strategy BuildStrategy // which build path was taken
	Reason   string        // caller-supplied reason (e.g., "changefeed-signal")
	Snapshot domain.Snapshot
	Bundle   domain.RuntimeBundle
}

// SupervisorConfig holds the dependencies needed to construct a supervisor.
type SupervisorConfig struct {
	Builder     *SnapshotBuilder
	Factory     ports.BundleFactory
	Reconcilers []ports.BundleReconciler

	// Observer is an optional callback invoked after each successful reload
	// with structured information about the build strategy used. This
	// provides observability without coupling the Supervisor to a logger.
	// Nil means no observation.
	Observer func(ReloadEvent)
}

// NewSupervisor creates a new supervisor.
func NewSupervisor(cfg SupervisorConfig) (Supervisor, error) {
	if cfg.Builder == nil {
		return nil, fmt.Errorf("%w", errSupervisorBuilderRequired)
	}

	if domain.IsNilValue(cfg.Factory) {
		return nil, fmt.Errorf("%w", errSupervisorFactoryRequired)
	}

	for i, reconciler := range cfg.Reconcilers {
		if isNilReconciler(reconciler) {
			return nil, fmt.Errorf("%w: index %d", errSupervisorNilReconciler, i)
		}
	}

	sorted := sortReconcilersByPhase(cfg.Reconcilers)

	return &defaultSupervisor{
		builder:     cfg.Builder,
		factory:     cfg.Factory,
		reconcilers: sorted,
		observer:    cfg.Observer,
		stopCh:      make(chan struct{}),
	}, nil
}

type bundleHolder struct {
	bundle domain.RuntimeBundle
}

type defaultSupervisor struct {
	snapshot    atomic.Pointer[domain.Snapshot]
	bundle      atomic.Pointer[bundleHolder]
	mu          sync.Mutex
	builder     *SnapshotBuilder
	factory     ports.BundleFactory
	reconcilers []ports.BundleReconciler
	observer    func(ReloadEvent)
	stopCh      chan struct{}
	stopOnce    sync.Once
}

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

// Current returns the currently active runtime bundle.
func (supervisor *defaultSupervisor) Current() domain.RuntimeBundle {
	holder := supervisor.bundle.Load()
	if holder == nil || isNilRuntimeBundle(holder.bundle) {
		return nil
	}

	return holder.bundle
}

// Snapshot returns the latest published snapshot.
func (supervisor *defaultSupervisor) Snapshot() domain.Snapshot {
	snap := supervisor.snapshot.Load()
	if snap == nil {
		return domain.Snapshot{}
	}

	return *snap
}

// PublishSnapshot publishes a snapshot without rebuilding bundles.
func (supervisor *defaultSupervisor) PublishSnapshot(_ context.Context, snap domain.Snapshot, _ string) error {
	if supervisor.isStopped() {
		return domain.ErrSupervisorStopped
	}

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	supervisor.snapshot.Store(&snap)

	return nil
}

// ReconcileCurrent reconciles the current bundle against a provided snapshot.
func (supervisor *defaultSupervisor) ReconcileCurrent(ctx context.Context, snap domain.Snapshot, _ string) error {
	if supervisor.isStopped() {
		return domain.ErrSupervisorStopped
	}

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	holder := supervisor.bundle.Load()
	if holder == nil || isNilRuntimeBundle(holder.bundle) {
		return domain.ErrNoCurrentBundle
	}

	previous := supervisor.snapshot.Load()
	supervisor.snapshot.Store(&snap)

	currentBundle := holder.bundle
	for _, reconciler := range supervisor.reconcilers {
		if err := reconciler.Reconcile(ctx, currentBundle, currentBundle, snap); err != nil {
			if previous != nil {
				supervisor.snapshot.Store(previous)
			}

			return fmt.Errorf("%s: %w: %w", reconciler.Name(), domain.ErrReconcileFailed, err)
		}
	}

	return nil
}

// Reload rebuilds the snapshot and runtime bundle, then reconciles consumers.
func (supervisor *defaultSupervisor) Reload(ctx context.Context, reason string) error {
	if supervisor.isStopped() {
		return domain.ErrSupervisorStopped
	}

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	tenantIDs := cachedTenantIDs(supervisor.snapshot.Load())

	snap, err := supervisor.builder.BuildFull(ctx, tenantIDs...)
	if err != nil {
		return fmt.Errorf("reload: %w: %w", domain.ErrSnapshotBuildFailed, err)
	}

	prevSnap := supervisor.snapshot.Load()
	prevHolder := supervisor.bundle.Load()

	var previousBundle domain.RuntimeBundle
	if prevHolder != nil {
		previousBundle = prevHolder.bundle
	}

	// Try incremental build first if the factory supports it and we have a
	// previous snapshot+bundle. This reuses unchanged infrastructure components
	// (Postgres, Redis, etc.) instead of rebuilding everything.
	// Falls back to full build on failure or when incremental is not available.
	candidate, strategy, err := supervisor.buildBundle(ctx, snap, previousBundle, prevSnap)
	if err != nil {
		return fmt.Errorf("reload: %w: %w", domain.ErrBundleBuildFailed, err)
	}

	if isNilRuntimeBundle(candidate) {
		return fmt.Errorf("reload: %w: nil runtime bundle", domain.ErrBundleBuildFailed)
	}

	// Reconcile BEFORE committing: run all reconcilers against the candidate
	// bundle while the previous bundle is still the active one. This prevents
	// state corruption when incremental builds nil-out transferred pointers in
	// the previous bundle — if we stored the candidate first and a reconciler
	// failed, the "rollback" would restore a gutted previous bundle.
	for _, reconciler := range supervisor.reconcilers {
		if err := reconciler.Reconcile(ctx, previousBundle, candidate, snap); err != nil {
			discardFailedCandidate(ctx, candidate, strategy)

			return fmt.Errorf("reload: %s: %w: %w", reconciler.Name(), domain.ErrReconcileFailed, err)
		}
	}

	// All reconcilers passed — commit atomically.
	supervisor.snapshot.Store(&snap)
	supervisor.bundle.Store(&bundleHolder{bundle: candidate})

	if adopter, ok := candidate.(resourceAdopter); ok && !isNilRuntimeBundle(previousBundle) {
		adopter.AdoptResourcesFrom(previousBundle)
	}

	if supervisor.observer != nil {
		supervisor.observer(ReloadEvent{Strategy: strategy, Reason: reason, Snapshot: snap, Bundle: candidate})
	}

	// Close previous AFTER commit so transferred components are not torn down
	// while still referenced by the now-active candidate bundle or by external
	// runtime delegates that are repointed by the observer.
	if !isNilRuntimeBundle(previousBundle) {
		_ = previousBundle.Close(ctx)
	}

	return nil
}

// Stop terminates supervisor operations and closes the active bundle.
func (supervisor *defaultSupervisor) Stop(ctx context.Context) error {
	supervisor.stopOnce.Do(func() {
		close(supervisor.stopCh)
	})

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	holder := supervisor.bundle.Load()
	if holder != nil && !isNilRuntimeBundle(holder.bundle) {
		if err := holder.bundle.Close(ctx); err != nil {
			return fmt.Errorf("stop close current bundle: %w", err)
		}
	}

	return nil
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
