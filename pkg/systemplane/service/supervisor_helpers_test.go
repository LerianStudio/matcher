//go:build unit

// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// discardFailedCandidate
// ---------------------------------------------------------------------------

func TestDiscardFailedCandidate_NilCandidate_DoesNotPanic(t *testing.T) {
	t.Parallel()

	// Should be a no-op for nil.
	assert.NotPanics(t, func() {
		discardFailedCandidate(context.Background(), nil, BuildStrategyFull)
	})
}

func TestDiscardFailedCandidate_FullStrategy_ClosesCandidate(t *testing.T) {
	t.Parallel()

	candidate := &testutil.FakeBundle{}
	discardFailedCandidate(context.Background(), candidate, BuildStrategyFull)

	assert.True(t, candidate.Closed, "full strategy should close the candidate via Close()")
}

// fakeDiscarder implements both RuntimeBundle and rollbackDiscarder.
type fakeDiscarder struct {
	discardCalled bool
	closeCalled   bool
	discardErr    error
}

func (d *fakeDiscarder) Close(_ context.Context) error {
	d.closeCalled = true
	return nil
}

func (d *fakeDiscarder) Discard(_ context.Context) error {
	d.discardCalled = true
	return d.discardErr
}

func TestDiscardFailedCandidate_FullStrategy_PrefersDiscard(t *testing.T) {
	t.Parallel()

	candidate := &fakeDiscarder{}
	discardFailedCandidate(context.Background(), candidate, BuildStrategyFull)

	assert.True(t, candidate.discardCalled, "full strategy with Discard should prefer Discard()")
	assert.False(t, candidate.closeCalled, "should not call Close when Discard is available")
}

func TestDiscardFailedCandidate_IncrementalStrategy_WithDiscarder_CallsDiscard(t *testing.T) {
	t.Parallel()

	candidate := &fakeDiscarder{}
	discardFailedCandidate(context.Background(), candidate, BuildStrategyIncremental)

	assert.True(t, candidate.discardCalled)
	assert.False(t, candidate.closeCalled)
}

func TestDiscardFailedCandidate_IncrementalStrategy_WithoutDiscarder_SkipsClose(t *testing.T) {
	t.Parallel()

	// FakeBundle does NOT implement rollbackDiscarder, so incremental
	// strategy should skip cleanup entirely (no Close either, because
	// incremental candidates may share adopted resources).
	candidate := &testutil.FakeBundle{}
	discardFailedCandidate(context.Background(), candidate, BuildStrategyIncremental)

	assert.False(t, candidate.Closed, "incremental without discarder should NOT call Close")
}

// ---------------------------------------------------------------------------
// isNilRuntimeBundle
// ---------------------------------------------------------------------------

func TestIsNilRuntimeBundle_UntypedNil_ReturnsTrue(t *testing.T) {
	t.Parallel()

	assert.True(t, isNilRuntimeBundle(nil))
}

func TestIsNilRuntimeBundle_TypedNil_ReturnsTrue(t *testing.T) {
	t.Parallel()

	var bundle *testutil.FakeBundle
	assert.True(t, isNilRuntimeBundle(bundle))
}

func TestIsNilRuntimeBundle_ValidBundle_ReturnsFalse(t *testing.T) {
	t.Parallel()

	bundle := &testutil.FakeBundle{}
	assert.False(t, isNilRuntimeBundle(bundle))
}

// ---------------------------------------------------------------------------
// isNilReconciler
// ---------------------------------------------------------------------------

func TestIsNilReconciler_UntypedNil_ReturnsTrue(t *testing.T) {
	t.Parallel()

	assert.True(t, isNilReconciler(nil))
}

func TestIsNilReconciler_TypedNil_ReturnsTrue(t *testing.T) {
	t.Parallel()

	var rec *testutil.FakeReconciler
	assert.True(t, isNilReconciler(rec))
}

func TestIsNilReconciler_ValidReconciler_ReturnsFalse(t *testing.T) {
	t.Parallel()

	rec := testutil.NewFakeReconciler("test")
	assert.False(t, isNilReconciler(rec))
}

// ---------------------------------------------------------------------------
// mergeUniqueTenantIDs
// ---------------------------------------------------------------------------

func TestMergeUniqueTenantIDs_EmptyExtra_ReturnsBase(t *testing.T) {
	t.Parallel()

	base := []string{"t1", "t2"}
	result := mergeUniqueTenantIDs(base, nil)

	assert.Equal(t, []string{"t1", "t2"}, result)
}

func TestMergeUniqueTenantIDs_EmptyBase_ReturnsExtraSorted(t *testing.T) {
	t.Parallel()

	result := mergeUniqueTenantIDs(nil, []string{"t3", "t1"})

	assert.Equal(t, []string{"t1", "t3"}, result)
}

func TestMergeUniqueTenantIDs_DeduplicatesAndSorts(t *testing.T) {
	t.Parallel()

	base := []string{"t2", "t1"}
	extra := []string{"t3", "t1", "t2", "t4"}
	result := mergeUniqueTenantIDs(base, extra)

	assert.Equal(t, []string{"t1", "t2", "t3", "t4"}, result)
}

func TestMergeUniqueTenantIDs_SkipsEmptyStrings(t *testing.T) {
	t.Parallel()

	base := []string{"t1"}
	extra := []string{"", "t2", ""}
	result := mergeUniqueTenantIDs(base, extra)

	assert.Equal(t, []string{"t1", "t2"}, result)
}

func TestMergeUniqueTenantIDs_BothEmpty_ReturnsNil(t *testing.T) {
	t.Parallel()

	result := mergeUniqueTenantIDs(nil, nil)
	assert.Nil(t, result)
}

func TestMergeUniqueTenantIDs_ExtraOnlyEmpty_ReturnsBase(t *testing.T) {
	t.Parallel()

	result := mergeUniqueTenantIDs(nil, []string{""})
	// Empty strings are skipped, so base remains nil and sort.Strings(nil) is fine.
	assert.Nil(t, result)
}

// ---------------------------------------------------------------------------
// cachedTenantIDs
// ---------------------------------------------------------------------------

func TestCachedTenantIDs_NilSnapshot_ReturnsNil(t *testing.T) {
	t.Parallel()

	result := cachedTenantIDs(nil)
	assert.Nil(t, result)
}

func TestCachedTenantIDs_EmptyTenantSettings_ReturnsNil(t *testing.T) {
	t.Parallel()

	snap := &domain.Snapshot{TenantSettings: map[string]map[string]domain.EffectiveValue{}}
	result := cachedTenantIDs(snap)
	assert.Nil(t, result)
}

func TestCachedTenantIDs_ExtractsAndSorts(t *testing.T) {
	t.Parallel()

	snap := &domain.Snapshot{
		TenantSettings: map[string]map[string]domain.EffectiveValue{
			"zeta-tenant":  {"k": {Key: "k"}},
			"alpha-tenant": {"k": {Key: "k"}},
			"mid-tenant":   {"k": {Key: "k"}},
		},
	}

	result := cachedTenantIDs(snap)
	assert.Equal(t, []string{"alpha-tenant", "mid-tenant", "zeta-tenant"}, result)
}

// ---------------------------------------------------------------------------
// sortReconcilersByPhase (additional coverage)
// ---------------------------------------------------------------------------

func TestSortReconcilersByPhase_NilInput_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	result := sortReconcilersByPhase(nil)
	assert.Empty(t, result)
}

func TestSortReconcilersByPhase_SingleElement(t *testing.T) {
	t.Parallel()

	rec := testutil.NewFakeReconcilerWithPhase("only", domain.PhaseStateSync)
	result := sortReconcilersByPhase([]ports.BundleReconciler{rec})

	require.Len(t, result, 1)
	assert.Equal(t, "only", result[0].Name())
}

func TestSortReconcilersByPhase_DoesNotMutateInput(t *testing.T) {
	t.Parallel()

	sideEffect := testutil.NewFakeReconcilerWithPhase("se", domain.PhaseSideEffect)
	stateSync := testutil.NewFakeReconcilerWithPhase("ss", domain.PhaseStateSync)

	input := []ports.BundleReconciler{sideEffect, stateSync}
	_ = sortReconcilersByPhase(input)

	// Original order should be unchanged.
	assert.Equal(t, "se", input[0].Name())
	assert.Equal(t, "ss", input[1].Name())
}

// ---------------------------------------------------------------------------
// isStopped
// ---------------------------------------------------------------------------

func TestIsStopped_NotStopped_ReturnsFalse(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory})
	require.NoError(t, err)

	ds := sv.(*defaultSupervisor)
	assert.False(t, ds.isStopped())
}

func TestIsStopped_AfterStop_ReturnsTrue(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory})
	require.NoError(t, err)

	require.NoError(t, sv.Stop(context.Background()))

	ds := sv.(*defaultSupervisor)
	assert.True(t, ds.isStopped())
}

// ---------------------------------------------------------------------------
// buildBundle
// ---------------------------------------------------------------------------

func TestBuildBundle_NoPrevious_UsesFullBuild(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory})
	require.NoError(t, err)

	ds := sv.(*defaultSupervisor)
	snap := domain.Snapshot{Configs: map[string]domain.EffectiveValue{}, BuiltAt: time.Now().UTC()}

	bundle, strategy, err := ds.buildBundle(context.Background(), snap, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, bundle)
	assert.Equal(t, BuildStrategyFull, strategy)
}

func TestBuildBundle_WithPreviousAndIncrementalFactory_UsesIncremental(t *testing.T) {
	t.Parallel()

	builder, _, _ := testSupervisorDeps(t)
	incFactory := testutil.NewFakeIncrementalBundleFactory()

	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: incFactory})
	require.NoError(t, err)

	ds := sv.(*defaultSupervisor)
	prevSnap := domain.Snapshot{Configs: map[string]domain.EffectiveValue{}, BuiltAt: time.Now().UTC()}
	newSnap := domain.Snapshot{Configs: map[string]domain.EffectiveValue{}, BuiltAt: time.Now().UTC()}
	prevBundle := &testutil.FakeBundle{}

	bundle, strategy, err := ds.buildBundle(context.Background(), newSnap, prevBundle, &prevSnap)
	require.NoError(t, err)
	assert.NotNil(t, bundle)
	assert.Equal(t, BuildStrategyIncremental, strategy)
	assert.Equal(t, 1, incFactory.IncrementalCallCount())
}

func TestBuildBundle_IncrementalFails_FallsBackToFull(t *testing.T) {
	t.Parallel()

	builder, _, _ := testSupervisorDeps(t)
	incFactory := testutil.NewFakeIncrementalBundleFactory()
	incFactory.IncrementalBuildFunc = func(_ context.Context, _ domain.Snapshot, _ domain.RuntimeBundle, _ domain.Snapshot) (domain.RuntimeBundle, error) {
		return nil, errors.New("incremental failed")
	}

	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: incFactory})
	require.NoError(t, err)

	ds := sv.(*defaultSupervisor)
	prevSnap := domain.Snapshot{BuiltAt: time.Now().UTC()}
	newSnap := domain.Snapshot{BuiltAt: time.Now().UTC()}
	prevBundle := &testutil.FakeBundle{}

	bundle, strategy, err := ds.buildBundle(context.Background(), newSnap, prevBundle, &prevSnap)
	require.NoError(t, err)
	assert.NotNil(t, bundle)
	assert.Equal(t, BuildStrategyFull, strategy)
}

func TestBuildBundle_FullBuildFails_PropagatesError(t *testing.T) {
	t.Parallel()

	builder, _, factory := testSupervisorDeps(t)
	factory.SetError(errors.New("factory broken"))

	sv, err := NewSupervisor(SupervisorConfig{Builder: builder, Factory: factory})
	require.NoError(t, err)

	ds := sv.(*defaultSupervisor)
	snap := domain.Snapshot{BuiltAt: time.Now().UTC()}

	_, _, err = ds.buildBundle(context.Background(), snap, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "factory broken")
}
