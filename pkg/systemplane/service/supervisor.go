// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

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
	Reload(ctx context.Context, reason string, extraTenantIDs ...string) error
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
// The context and reason parameters are part of the Supervisor contract and are
// reserved for future tracing/audit hooks even though the current implementation
// only needs the snapshot payload.
func (supervisor *defaultSupervisor) PublishSnapshot(ctx context.Context, snap domain.Snapshot, _ string) error {
	_, span := startSupervisorSpan(ctx, "publish_snapshot")
	defer span.End()

	if supervisor.isStopped() {
		libOpentelemetry.HandleSpanError(span, "supervisor stopped", domain.ErrSupervisorStopped)
		return domain.ErrSupervisorStopped
	}

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	supervisor.snapshot.Store(&snap)

	return nil
}

// ReconcileCurrent reconciles the current bundle against a provided snapshot.
// The reason parameter is reserved for future tracing/audit hooks while the
// current implementation only needs the context and snapshot.
func (supervisor *defaultSupervisor) ReconcileCurrent(ctx context.Context, snap domain.Snapshot, _ string) error {
	ctx, span := startSupervisorSpan(ctx, "reconcile_current")
	defer span.End()

	if supervisor.isStopped() {
		libOpentelemetry.HandleSpanError(span, "supervisor stopped", domain.ErrSupervisorStopped)
		return domain.ErrSupervisorStopped
	}

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	holder := supervisor.bundle.Load()
	if holder == nil || isNilRuntimeBundle(holder.bundle) {
		libOpentelemetry.HandleSpanError(span, "missing current bundle", domain.ErrNoCurrentBundle)
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

			libOpentelemetry.HandleSpanError(span, "reconcile current bundle", err)

			return fmt.Errorf("%s: %w: %w", reconciler.Name(), domain.ErrReconcileFailed, err)
		}
	}

	return nil
}

// Reload rebuilds the snapshot and runtime bundle, then reconciles consumers.
// Optional extraTenantIDs are merged into the cached tenant list to ensure
// first-seen tenants (not yet in any snapshot) are included in the rebuild.
func (supervisor *defaultSupervisor) Reload(ctx context.Context, reason string, extraTenantIDs ...string) error {
	ctx, span := startSupervisorSpan(ctx, "reload")
	defer span.End()

	if supervisor.isStopped() {
		libOpentelemetry.HandleSpanError(span, "supervisor stopped", domain.ErrSupervisorStopped)
		return domain.ErrSupervisorStopped
	}

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	tenantIDs := mergeUniqueTenantIDs(cachedTenantIDs(supervisor.snapshot.Load()), extraTenantIDs)

	snap, err := supervisor.builder.BuildFull(ctx, tenantIDs...)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "build full snapshot", err)
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
		libOpentelemetry.HandleSpanError(span, "build runtime bundle", err)
		return fmt.Errorf("reload: %w: %w", domain.ErrBundleBuildFailed, err)
	}

	if isNilRuntimeBundle(candidate) {
		libOpentelemetry.HandleSpanError(span, "nil runtime bundle", domain.ErrBundleBuildFailed)
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

			libOpentelemetry.HandleSpanError(span, "reconcile candidate bundle", err)

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
	ctx, span := startSupervisorSpan(ctx, "stop")
	defer span.End()

	supervisor.stopOnce.Do(func() {
		close(supervisor.stopCh)
	})

	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()

	holder := supervisor.bundle.Load()
	if holder != nil && !isNilRuntimeBundle(holder.bundle) {
		if err := holder.bundle.Close(ctx); err != nil {
			libOpentelemetry.HandleSpanError(span, "close current bundle", err)
			return fmt.Errorf("stop close current bundle: %w", err)
		}
	}

	return nil
}
