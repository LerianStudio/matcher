// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"fmt"
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

// SupervisorConfig holds the dependencies needed to construct a supervisor.
type SupervisorConfig struct {
	Builder     *SnapshotBuilder
	Factory     ports.BundleFactory
	Reconcilers []ports.BundleReconciler
}

// NewSupervisor creates a new supervisor.
func NewSupervisor(cfg SupervisorConfig) (Supervisor, error) {
	if cfg.Builder == nil {
		return nil, fmt.Errorf("%w", errSupervisorBuilderRequired)
	}

	if cfg.Factory == nil {
		return nil, fmt.Errorf("%w", errSupervisorFactoryRequired)
	}

	for i, reconciler := range cfg.Reconcilers {
		if isNilReconciler(reconciler) {
			return nil, fmt.Errorf("%w: index %d", errSupervisorNilReconciler, i)
		}
	}

	return &defaultSupervisor{
		builder:     cfg.Builder,
		factory:     cfg.Factory,
		reconcilers: cfg.Reconcilers,
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
func (supervisor *defaultSupervisor) Reload(ctx context.Context, _ string) error {
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

	candidate, err := supervisor.factory.Build(ctx, snap)
	if err != nil {
		return fmt.Errorf("reload: %w: %w", domain.ErrBundleBuildFailed, err)
	}

	if isNilRuntimeBundle(candidate) {
		return fmt.Errorf("reload: %w: nil runtime bundle", domain.ErrBundleBuildFailed)
	}

	prevSnap := supervisor.snapshot.Load()
	prevHolder := supervisor.bundle.Load()

	var previousBundle domain.RuntimeBundle
	if prevHolder != nil {
		previousBundle = prevHolder.bundle
	}

	supervisor.snapshot.Store(&snap)
	supervisor.bundle.Store(&bundleHolder{bundle: candidate})

	for _, reconciler := range supervisor.reconcilers {
		if err := reconciler.Reconcile(ctx, previousBundle, candidate, snap); err != nil {
			if prevSnap != nil {
				supervisor.snapshot.Store(prevSnap)
			}

			if prevHolder != nil {
				supervisor.bundle.Store(prevHolder)
			} else {
				supervisor.bundle.Store(nil)
			}

			_ = candidate.Close(ctx)

			return fmt.Errorf("reload: %s: %w: %w", reconciler.Name(), domain.ErrReconcileFailed, err)
		}
	}

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
