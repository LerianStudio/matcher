// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
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
		return nil, fmt.Errorf("new supervisor: snapshot builder is required")
	}
	if cfg.Factory == nil {
		return nil, fmt.Errorf("new supervisor: bundle factory is required")
	}
	for i, reconciler := range cfg.Reconcilers {
		if isNilReconciler(reconciler) {
			return nil, fmt.Errorf("new supervisor: reconciler %d is nil", i)
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

func (s *defaultSupervisor) Current() domain.RuntimeBundle {
	holder := s.bundle.Load()
	if holder == nil || isNilRuntimeBundle(holder.bundle) {
		return nil
	}

	return holder.bundle
}

func (s *defaultSupervisor) Snapshot() domain.Snapshot {
	snap := s.snapshot.Load()
	if snap == nil {
		return domain.Snapshot{}
	}

	return *snap
}

func (s *defaultSupervisor) PublishSnapshot(_ context.Context, snap domain.Snapshot, _ string) error {
	if s.isStopped() {
		return domain.ErrSupervisorStopped
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot.Store(&snap)

	return nil
}

func (s *defaultSupervisor) ReconcileCurrent(ctx context.Context, snap domain.Snapshot, _ string) error {
	if s.isStopped() {
		return domain.ErrSupervisorStopped
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	holder := s.bundle.Load()
	if holder == nil || isNilRuntimeBundle(holder.bundle) {
		return domain.ErrNoCurrentBundle
	}

	previous := s.snapshot.Load()
	s.snapshot.Store(&snap)

	currentBundle := holder.bundle
	for _, reconciler := range s.reconcilers {
		if err := reconciler.Reconcile(ctx, currentBundle, currentBundle, snap); err != nil {
			if previous != nil {
				s.snapshot.Store(previous)
			}

			return fmt.Errorf("%s: %w: %w", reconciler.Name(), domain.ErrReconcileFailed, err)
		}
	}

	return nil
}

func (s *defaultSupervisor) Reload(ctx context.Context, _ string) error {
	if s.isStopped() {
		return domain.ErrSupervisorStopped
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tenantIDs := cachedTenantIDs(s.snapshot.Load())
	snap, err := s.builder.BuildFull(ctx, tenantIDs...)
	if err != nil {
		return fmt.Errorf("reload: %w: %w", domain.ErrSnapshotBuildFailed, err)
	}

	candidate, err := s.factory.Build(ctx, snap)
	if err != nil {
		return fmt.Errorf("reload: %w: %w", domain.ErrBundleBuildFailed, err)
	}
	if isNilRuntimeBundle(candidate) {
		return fmt.Errorf("reload: %w: nil runtime bundle", domain.ErrBundleBuildFailed)
	}

	prevSnap := s.snapshot.Load()
	prevHolder := s.bundle.Load()

	var previousBundle domain.RuntimeBundle
	if prevHolder != nil {
		previousBundle = prevHolder.bundle
	}

	s.snapshot.Store(&snap)
	s.bundle.Store(&bundleHolder{bundle: candidate})

	for _, reconciler := range s.reconcilers {
		if err := reconciler.Reconcile(ctx, previousBundle, candidate, snap); err != nil {
			if prevSnap != nil {
				s.snapshot.Store(prevSnap)
			}
			if prevHolder != nil {
				s.bundle.Store(prevHolder)
			} else {
				s.bundle.Store(nil)
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

func (s *defaultSupervisor) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	s.mu.Lock()
	defer s.mu.Unlock()

	holder := s.bundle.Load()
	if holder != nil && !isNilRuntimeBundle(holder.bundle) {
		return holder.bundle.Close(ctx)
	}

	return nil
}

func (s *defaultSupervisor) isStopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

func isNilRuntimeBundle(bundle domain.RuntimeBundle) bool {
	if bundle == nil {
		return true
	}

	value := reflect.ValueOf(bundle)
	if !value.IsValid() {
		return true
	}

	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilReconciler(reconciler ports.BundleReconciler) bool {
	if reconciler == nil {
		return true
	}

	value := reflect.ValueOf(reconciler)
	if !value.IsValid() {
		return true
	}

	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
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
