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
	builder := NewSnapshotBuilder(reg, store)

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
