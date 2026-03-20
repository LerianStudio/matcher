//go:build unit

// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
	"github.com/LerianStudio/matcher/pkg/systemplane/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSupervisorDeps(t *testing.T) (*SnapshotBuilder, *testutil.FakeStore, *testutil.FakeBundleFactory) {
	t.Helper()

	reg := registry.New()
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "app.workers",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeInt,
		DefaultValue:     4,
		ApplyBehavior:    domain.ApplyBundleRebuild,
		MutableAtRuntime: true,
		Description:      "Number of workers",
		Group:            "app",
	}))

	reg.MustRegister(domain.KeyDef{
		Key:              "ui.theme",
		Kind:             domain.KindSetting,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "light",
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "Theme",
		Group:            "ui",
	})

	store := testutil.NewFakeStore()
	factory := testutil.NewFakeBundleFactory()
	builder, err := NewSnapshotBuilder(reg, store)
	require.NoError(t, err)

	return builder, store, factory
}

func TestNewSupervisor_RejectsNilDependencies(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)

	_, err := NewSupervisor(SupervisorConfig{Factory: factory})
	require.Error(t, err)

	_, err = NewSupervisor(SupervisorConfig{Builder: builder})
	require.Error(t, err)

	_, err = NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory, Reconcilers: []ports.BundleReconciler{nil}})
	require.Error(t, err)
}

func TestSupervisor_Reload_Success(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	rec := testutil.NewFakeReconciler("test-rec")

	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory, Reconcilers: []ports.BundleReconciler{rec}})
	require.NoError(t, err)

	err = sv.Reload(context.Background(), "initial")
	require.NoError(t, err)
	assert.NotNil(t, sv.Current())
	assert.NotEmpty(t, sv.Snapshot().Configs)
	assert.Equal(t, 1, factory.CallCount())
	assert.Equal(t, 1, rec.CallCount())
}

func TestSupervisor_Reload_BundleBuildFailed_KeepsPrevious(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory})
	require.NoError(t, err)
	require.NoError(t, sv.Reload(context.Background(), "initial"))

	previousBundle := sv.Current()
	previousSnapshot := sv.Snapshot()
	factory.SetError(errors.New("out of memory"))

	err = sv.Reload(context.Background(), "broken")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrBundleBuildFailed)
	assert.Equal(t, previousBundle, sv.Current())
	assert.Equal(t, previousSnapshot, sv.Snapshot())
}

func TestSupervisor_Reload_ReconcileFailed_RollsBackAndClosesCandidate(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	goodRec := testutil.NewFakeReconciler("good-rec")
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory, Reconcilers: []ports.BundleReconciler{goodRec}})
	require.NoError(t, err)
	require.NoError(t, sv.Reload(context.Background(), "initial"))

	goodRec.ReconcileErr = errors.New("boom")
	var candidate *testutil.FakeBundle
	factory.BuildFn = func(_ context.Context, _ domain.Snapshot) (domain.RuntimeBundle, error) {
		candidate = &testutil.FakeBundle{}
		return candidate, nil
	}

	previous := sv.Current()
	err = sv.Reload(context.Background(), "second")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrReconcileFailed)
	assert.Equal(t, previous, sv.Current())
	require.NotNil(t, candidate)
	assert.True(t, candidate.Closed)
}

func TestSupervisor_Reload_FailedReconcileDoesNotNotifyObserver(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	reconciler := testutil.NewFakeReconciler("failing-rec")
	reconciler.ReconcileErr = errors.New("boom")
	observerCalls := 0

	sv, err := NewSupervisor(SupervisorConfig{
		Builder:     builder,
		Factory:     factory,
		Reconcilers: []ports.BundleReconciler{reconciler},
		Observer: func(ReloadEvent) {
			observerCalls++
		},
	})
	require.NoError(t, err)

	err = sv.Reload(context.Background(), "initial")
	require.Error(t, err)
	assert.Zero(t, observerCalls)
}

func TestSupervisor_Reload_IncrementalReconcileFailureDoesNotCloseCandidateWithoutDiscarder(t *testing.T) {
	t.Parallel()

	builder, _, _ := testSupervisorDeps(t)
	factory := testutil.NewFakeIncrementalBundleFactory()
	validation := testutil.NewFakeReconcilerWithPhase("validation", domain.PhaseValidation)

	sv, err := NewSupervisor(SupervisorConfig{
		Builder:     builder,
		Factory:     factory,
		Reconcilers: []ports.BundleReconciler{validation},
	})
	require.NoError(t, err)
	require.NoError(t, sv.Reload(context.Background(), "initial"))

	var candidate *testutil.FakeBundle
	factory.IncrementalBuildFunc = func(_ context.Context, _ domain.Snapshot, _ domain.RuntimeBundle, _ domain.Snapshot) (domain.RuntimeBundle, error) {
		candidate = &testutil.FakeBundle{}
		return candidate, nil
	}
	validation.ReconcileErr = errors.New("boom")

	err = sv.Reload(context.Background(), "incremental-failure")
	require.Error(t, err)
	require.NotNil(t, candidate)
	assert.False(t, candidate.Closed)
}

func TestSupervisor_ReconcileCurrent_RequiresCurrentBundle(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	rec := testutil.NewFakeReconciler("worker-rec")
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory, Reconcilers: []ports.BundleReconciler{rec}})
	require.NoError(t, err)

	err = sv.ReconcileCurrent(context.Background(), domain.Snapshot{BuiltAt: time.Now().UTC()}, "worker-reconcile")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrNoCurrentBundle)
}

func TestSupervisor_ReconcileCurrent_RevertsSnapshotOnFailure(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	rec := testutil.NewFakeReconciler("worker-rec")
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory, Reconcilers: []ports.BundleReconciler{rec}})
	require.NoError(t, err)
	require.NoError(t, sv.Reload(context.Background(), "initial"))

	previous := sv.Snapshot()
	rec.ReconcileErr = errors.New("cannot reconcile")
	err = sv.ReconcileCurrent(context.Background(), domain.Snapshot{Configs: map[string]domain.EffectiveValue{"app.workers": {Key: "app.workers", Value: 10, Revision: 1}}, BuiltAt: time.Now().UTC()}, "worker-reconcile")
	require.Error(t, err)
	assert.Equal(t, previous, sv.Snapshot())
}

func TestSupervisor_Stop_PreventsFurtherOperations(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory})
	require.NoError(t, err)
	require.NoError(t, sv.Reload(context.Background(), "initial"))
	require.NoError(t, sv.Stop(context.Background()))

	assert.ErrorIs(t, sv.Reload(context.Background(), "after-stop"), domain.ErrSupervisorStopped)
	assert.ErrorIs(t, sv.PublishSnapshot(context.Background(), domain.Snapshot{}, "after-stop"), domain.ErrSupervisorStopped)
	assert.ErrorIs(t, sv.ReconcileCurrent(context.Background(), domain.Snapshot{}, "after-stop"), domain.ErrSupervisorStopped)
}

func TestSupervisor_RejectsTypedNilBundle(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	factory.BuildFn = func(_ context.Context, _ domain.Snapshot) (domain.RuntimeBundle, error) {
		var bundle *testutil.FakeBundle
		return bundle, nil
	}

	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory})
	require.NoError(t, err)

	err = sv.Reload(context.Background(), "typed-nil")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrBundleBuildFailed)
}

func TestSupervisor_Reload_ReconcilersRunInPhaseOrder(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)

	// Register reconcilers in REVERSE phase order to prove sorting works.
	var callOrder []string
	var mu sync.Mutex

	sideEffect := testutil.NewFakeReconcilerWithPhase("side-effect", domain.PhaseSideEffect)
	sideEffect.ReconcileErr = nil

	validation := testutil.NewFakeReconcilerWithPhase("validation", domain.PhaseValidation)
	validation.ReconcileErr = nil

	stateSync := testutil.NewFakeReconcilerWithPhase("state-sync", domain.PhaseStateSync)
	stateSync.ReconcileErr = nil

	// Wrap each reconciler to record call order.
	wrappedSideEffect := &orderTrackingReconciler{inner: sideEffect, mu: &mu, order: &callOrder}
	wrappedValidation := &orderTrackingReconciler{inner: validation, mu: &mu, order: &callOrder}
	wrappedStateSync := &orderTrackingReconciler{inner: stateSync, mu: &mu, order: &callOrder}

	// Pass in REVERSE order — supervisor must sort by phase.
	sv, err := NewSupervisor(SupervisorConfig{
		Builder: builder,
		Factory: factory,
		Reconcilers: []ports.BundleReconciler{
			wrappedSideEffect, // PhaseSideEffect (2) — passed first
			wrappedValidation, // PhaseValidation (1)
			wrappedStateSync,  // PhaseStateSync (0) — passed last
		},
	})
	require.NoError(t, err)

	err = sv.Reload(context.Background(), "phase-order-test")
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, callOrder, 3)
	assert.Equal(t, "state-sync", callOrder[0], "state-sync must run first")
	assert.Equal(t, "validation", callOrder[1], "validation must run second")
	assert.Equal(t, "side-effect", callOrder[2], "side-effect must run last")
}

// orderTrackingReconciler wraps a FakeReconciler to record call order.
type orderTrackingReconciler struct {
	inner *testutil.FakeReconciler
	mu    *sync.Mutex
	order *[]string
}

func (r *orderTrackingReconciler) Name() string                  { return r.inner.Name() }
func (r *orderTrackingReconciler) Phase() domain.ReconcilerPhase { return r.inner.Phase() }
func (r *orderTrackingReconciler) Reconcile(ctx context.Context, prev domain.RuntimeBundle, cand domain.RuntimeBundle, snap domain.Snapshot) error {
	r.mu.Lock()
	*r.order = append(*r.order, r.inner.Name())
	r.mu.Unlock()

	return r.inner.Reconcile(ctx, prev, cand, snap)
}

func TestSupervisor_Reload_ObserverReceivesEvent(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)

	var events []ReloadEvent
	var mu sync.Mutex

	sv, err := NewSupervisor(SupervisorConfig{
		Builder: builder,
		Factory: factory,
		Observer: func(event ReloadEvent) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	})
	require.NoError(t, err)

	err = sv.Reload(context.Background(), "initial-bootstrap")
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, events, 1)
	assert.Equal(t, BuildStrategyFull, events[0].Strategy, "first reload must use full build")
	assert.Equal(t, "initial-bootstrap", events[0].Reason)
}

func TestSupervisor_Reload_NilObserverDoesNotPanic(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)

	sv, err := NewSupervisor(SupervisorConfig{
		Builder:  builder,
		Factory:  factory,
		Observer: nil, // explicitly nil
	})
	require.NoError(t, err)

	err = sv.Reload(context.Background(), "no-observer")
	require.NoError(t, err)
	assert.NotNil(t, sv.Current())
}

// ---------------------------------------------------------------------------
// Incremental build path tests
// ---------------------------------------------------------------------------

func TestSupervisor_Reload_UsesIncrementalWhenAvailable(t *testing.T) {
	t.Parallel()

	builder, _, _ := testSupervisorDeps(t)
	factory := testutil.NewFakeIncrementalBundleFactory()

	var events []ReloadEvent
	var mu sync.Mutex

	sv, err := NewSupervisor(SupervisorConfig{
		Builder: builder,
		Factory: factory,
		Observer: func(event ReloadEvent) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	})
	require.NoError(t, err)

	// First reload: no previous bundle → full build.
	err = sv.Reload(context.Background(), "initial")
	require.NoError(t, err)

	// Second reload: previous bundle exists → incremental build should be attempted.
	err = sv.Reload(context.Background(), "config-change")
	require.NoError(t, err)

	assert.GreaterOrEqual(t, factory.IncrementalCallCount(), 1,
		"BuildIncremental should be called when previous bundle exists")

	mu.Lock()
	defer mu.Unlock()

	require.GreaterOrEqual(t, len(events), 2)
	assert.Equal(t, BuildStrategyFull, events[0].Strategy, "first reload must use full build")
	assert.Equal(t, BuildStrategyIncremental, events[1].Strategy, "second reload should use incremental build")
}

func TestSupervisor_Reload_FallsBackToFullOnIncrementalError(t *testing.T) {
	t.Parallel()

	builder, _, _ := testSupervisorDeps(t)
	factory := testutil.NewFakeIncrementalBundleFactory()

	// Make incremental build fail — supervisor should fall back to full Build.
	factory.IncrementalBuildFunc = func(_ context.Context, _ domain.Snapshot,
		_ domain.RuntimeBundle, _ domain.Snapshot,
	) (domain.RuntimeBundle, error) {
		return nil, errors.New("incremental build exploded")
	}

	var events []ReloadEvent
	var eventMu sync.Mutex

	sv, err := NewSupervisor(SupervisorConfig{
		Builder: builder,
		Factory: factory,
		Observer: func(event ReloadEvent) {
			eventMu.Lock()
			events = append(events, event)
			eventMu.Unlock()
		},
	})
	require.NoError(t, err)

	// First reload: full build (no previous).
	err = sv.Reload(context.Background(), "initial")
	require.NoError(t, err)

	// Second reload: incremental fails → fallback to full.
	err = sv.Reload(context.Background(), "config-change")
	require.NoError(t, err)

	// Verify full Build was called as fallback (at least 2 full builds total).
	assert.GreaterOrEqual(t, factory.CallCount(), 2,
		"full Build should be called as fallback when incremental fails")

	eventMu.Lock()
	defer eventMu.Unlock()

	require.GreaterOrEqual(t, len(events), 2)
	assert.Equal(t, BuildStrategyFull, events[0].Strategy, "first reload: full")
	assert.Equal(t, BuildStrategyFull, events[1].Strategy, "second reload: full (fallback from incremental failure)")
}

// ---------------------------------------------------------------------------
// sortReconcilersByPhase stability test
// ---------------------------------------------------------------------------

func TestSortReconcilersByPhase_StableWithinPhase(t *testing.T) {
	t.Parallel()

	// Create 2 reconcilers with same phase but different names.
	first := testutil.NewFakeReconcilerWithPhase("alpha", domain.PhaseSideEffect)
	second := testutil.NewFakeReconcilerWithPhase("beta", domain.PhaseSideEffect)

	sorted := sortReconcilersByPhase([]ports.BundleReconciler{first, second})

	require.Len(t, sorted, 2)
	assert.Equal(t, "alpha", sorted[0].Name(),
		"stable sort should preserve insertion order within same phase")
	assert.Equal(t, "beta", sorted[1].Name())
}

func TestSortReconcilersByPhase_AcrossPhases(t *testing.T) {
	t.Parallel()

	sideEffect := testutil.NewFakeReconcilerWithPhase("side-effect", domain.PhaseSideEffect)
	stateSync := testutil.NewFakeReconcilerWithPhase("state-sync", domain.PhaseStateSync)
	validation := testutil.NewFakeReconcilerWithPhase("validation", domain.PhaseValidation)

	// Input in reverse phase order.
	sorted := sortReconcilersByPhase([]ports.BundleReconciler{sideEffect, validation, stateSync})

	require.Len(t, sorted, 3)
	assert.Equal(t, "state-sync", sorted[0].Name(), "StateSync(0) should be first")
	assert.Equal(t, "validation", sorted[1].Name(), "Validation(1) should be second")
	assert.Equal(t, "side-effect", sorted[2].Name(), "SideEffect(2) should be last")
}

func TestSortReconcilersByPhase_EmptySlice(t *testing.T) {
	t.Parallel()

	sorted := sortReconcilersByPhase(nil)

	assert.Empty(t, sorted)
}

func TestSupervisor_ConcurrentReloads_Serialized(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory})
	require.NoError(t, err)
	require.NoError(t, sv.Reload(context.Background(), "initial"))

	const goroutines = 10
	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			errCh <- sv.Reload(context.Background(), "concurrent")
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err)
	}
	assert.NotNil(t, sv.Current())
	assert.NotEmpty(t, sv.Snapshot().Configs)
	assert.GreaterOrEqual(t, factory.CallCount(), 2)
}
