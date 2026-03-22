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
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
	"github.com/LerianStudio/matcher/pkg/systemplane/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// previewConfigSnapshot
// ---------------------------------------------------------------------------

func TestPreviewConfigSnapshot_AppliesOpsToCurrentSnapshot(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	spy.snapshot = domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.workers": {Key: "app.workers", Value: 4, Default: 10, Source: "default", Revision: 1},
		},
		BuiltAt: time.Now().UTC(),
	}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	candidate, err := mgr.previewConfigSnapshot(context.Background(), []ports.WriteOp{
		{Key: "app.workers", Value: 16},
	})
	require.NoError(t, err)
	assert.Equal(t, 16, candidate.Configs["app.workers"].Value)
	assert.Equal(t, 16, candidate.Configs["app.workers"].Override)
	assert.Equal(t, "preview-override", candidate.Configs["app.workers"].Source)
	assert.Equal(t, 10, candidate.Configs["app.workers"].Default)
	assert.False(t, candidate.BuiltAt.IsZero())
}

func TestPreviewConfigSnapshot_ResetOp_ResetsToDefault(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	spy.snapshot = domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.workers": {Key: "app.workers", Value: 8, Default: 10, Override: 8, Source: "global-override", Revision: 2},
		},
		BuiltAt: time.Now().UTC(),
	}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	candidate, err := mgr.previewConfigSnapshot(context.Background(), []ports.WriteOp{
		{Key: "app.workers", Reset: true},
	})
	require.NoError(t, err)
	assert.Equal(t, 10, candidate.Configs["app.workers"].Value)
	assert.Nil(t, candidate.Configs["app.workers"].Override)
	assert.Equal(t, "default", candidate.Configs["app.workers"].Source)
}

func TestPreviewConfigSnapshot_NilValueOp_ResetsToDefault(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	spy.snapshot = domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.workers": {Key: "app.workers", Value: 8, Default: 10, Override: 8, Source: "global-override", Revision: 2},
		},
		BuiltAt: time.Now().UTC(),
	}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	candidate, err := mgr.previewConfigSnapshot(context.Background(), []ports.WriteOp{
		{Key: "app.workers", Value: nil},
	})
	require.NoError(t, err)
	assert.Equal(t, 10, candidate.Configs["app.workers"].Value)
	assert.Equal(t, "default", candidate.Configs["app.workers"].Source)
}

func TestPreviewConfigSnapshot_NoSnapshot_BuildsFresh(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	// No active snapshot — forces fresh build.
	spy.snapshot = domain.Snapshot{}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	candidate, err := mgr.previewConfigSnapshot(context.Background(), []ports.WriteOp{
		{Key: "app.workers", Value: 12},
	})
	require.NoError(t, err)
	assert.Equal(t, 12, candidate.Configs["app.workers"].Value)
}

func TestPreviewConfigSnapshot_UnknownKey_ReturnsError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	spy.snapshot = domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{},
		BuiltAt: time.Now().UTC(),
	}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	_, err := mgr.previewConfigSnapshot(context.Background(), []ports.WriteOp{
		{Key: "no.such.key", Value: "x"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestPreviewConfigSnapshot_BuildFreshFailure_PropagatesError(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "app.workers",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeInt,
		DefaultValue:     4,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "workers",
		Group:            "app",
	}))

	failStore := &failingStore{}
	builder, builderErr := NewSnapshotBuilder(reg, failStore)
	require.NoError(t, builderErr)

	spy := &spySupervisor{} // empty snapshot → BuildFull → store error
	history := testutil.NewFakeHistoryStore()

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: failStore, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	_, err := mgr.previewConfigSnapshot(context.Background(), []ports.WriteOp{
		{Key: "app.workers", Value: 8},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build fresh snapshot")
}

func TestPreviewConfigSnapshot_SetsRedactedFlag(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "auth.secret",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "default-secret",
		Secret:           true,
		RedactPolicy:     domain.RedactFull,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "secret",
		Group:            "auth",
	}))

	spy.snapshot = domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{},
		BuiltAt: time.Now().UTC(),
	}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	candidate, err := mgr.previewConfigSnapshot(context.Background(), []ports.WriteOp{
		{Key: "auth.secret", Value: "new-token"},
	})
	require.NoError(t, err)
	assert.True(t, candidate.Configs["auth.secret"].Redacted)
}

// ---------------------------------------------------------------------------
// validateConfigOp
// ---------------------------------------------------------------------------

func TestValidateConfigOp_ValidMutableKey_NoError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateConfigOp(ports.WriteOp{Key: "app.workers", Value: 8})
	assert.NoError(t, err)
}

func TestValidateConfigOp_UnknownKey_ReturnsErrKeyUnknown(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateConfigOp(ports.WriteOp{Key: "no.such.key", Value: "x"})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestValidateConfigOp_SettingKind_RejectsAsWrongKind(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateConfigOp(ports.WriteOp{Key: "ui.theme", Value: "dark"})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestValidateConfigOp_ImmutableKey_ReturnsErrKeyNotMutable(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.frozen", domain.ApplyLiveRead, false)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateConfigOp(ports.WriteOp{Key: "app.frozen", Value: 42})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyNotMutable)
}

func TestValidateConfigOp_ResetOp_SkipsValidation(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateConfigOp(ports.WriteOp{Key: "app.workers", Reset: true})
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// validateSettingOp
// ---------------------------------------------------------------------------

func TestValidateSettingOp_ValidKey_NoError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateSettingOp(ports.WriteOp{Key: "ui.theme", Value: "dark"}, domain.ScopeGlobal)
	assert.NoError(t, err)
}

func TestValidateSettingOp_UnknownKey_ReturnsError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateSettingOp(ports.WriteOp{Key: "no.such.key", Value: "x"}, domain.ScopeGlobal)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestValidateSettingOp_ConfigKind_RejectsAsWrongKind(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateSettingOp(ports.WriteOp{Key: "app.workers", Value: 8}, domain.ScopeGlobal)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestValidateSettingOp_ImmutableKey_ReturnsError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.frozen", []domain.Scope{domain.ScopeGlobal}, false)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateSettingOp(ports.WriteOp{Key: "ui.frozen", Value: "x"}, domain.ScopeGlobal)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyNotMutable)
}

func TestValidateSettingOp_ScopeNotAllowed_ReturnsError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateSettingOp(ports.WriteOp{Key: "ui.theme", Value: "dark"}, domain.ScopeGlobal)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrScopeInvalid)
}

func TestValidateSettingOp_ResetOp_SkipsValueValidation(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	err := mgr.validateSettingOp(ports.WriteOp{Key: "ui.theme", Reset: true}, domain.ScopeGlobal)
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// applyEscalation
// ---------------------------------------------------------------------------

func TestApplyEscalation_BootstrapOnly_IsNoop(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyEscalation(context.Background(), target, domain.ApplyBootstrapOnly)
	require.NoError(t, err)
	assert.Equal(t, 0, spy.publishCalls)
	assert.Equal(t, 0, spy.reconcileCalls)
	assert.Equal(t, 0, spy.reloadCalls)
}

func TestApplyEscalation_LiveRead_CallsPublishSnapshot(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyEscalation(context.Background(), target, domain.ApplyLiveRead)
	require.NoError(t, err)
	assert.Equal(t, 1, spy.publishCalls)
}

func TestApplyEscalation_WorkerReconcile_CallsReconcileCurrent(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "worker.interval", domain.ApplyWorkerReconcile, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyEscalation(context.Background(), target, domain.ApplyWorkerReconcile)
	require.NoError(t, err)
	assert.Equal(t, 1, spy.reconcileCalls)
}

func TestApplyEscalation_BundleRebuild_CallsReload(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyEscalation(context.Background(), target, domain.ApplyBundleRebuild)
	require.NoError(t, err)
	assert.Equal(t, 1, spy.reloadCalls)
	assert.Equal(t, string(domain.ApplyBundleRebuild), spy.lastReason)
}

func TestApplyEscalation_BundleRebuildAndReconcile_CallsReload(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyEscalation(context.Background(), target, domain.ApplyBundleRebuildAndReconcile)
	require.NoError(t, err)
	assert.Equal(t, 1, spy.reloadCalls)
	assert.Equal(t, string(domain.ApplyBundleRebuildAndReconcile), spy.lastReason)
}

func TestApplyEscalation_BundleRebuild_TenantScope_PassesExtraTenantIDs(t *testing.T) {
	t.Parallel()

	reg, store, history, _, builder := testManagerDeps(t)

	// Use a spy that captures the extraTenantIDs argument.
	var capturedReason string
	capturedSpy := &spySupervisor{}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: capturedSpy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "tenant-42")
	require.NoError(t, err)

	err = mgr.applyEscalation(context.Background(), target, domain.ApplyBundleRebuild)
	require.NoError(t, err)
	capturedReason = capturedSpy.lastReason
	assert.Equal(t, string(domain.ApplyBundleRebuild), capturedReason)
}

func TestApplyEscalation_UnknownBehavior_ReturnsError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyEscalation(context.Background(), target, domain.ApplyBehavior("bogus"))
	require.Error(t, err)
	assert.ErrorIs(t, err, errUnexpectedApplyBehavior)
}

// ---------------------------------------------------------------------------
// applyWithSnapshot
// ---------------------------------------------------------------------------

func TestApplyWithSnapshot_InvokesStateSync(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	var syncCalled bool

	mgrIface, mgrErr := NewManager(ManagerConfig{
		Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder,
		StateSync: func(_ context.Context, _ domain.Snapshot) {
			syncCalled = true
		},
	})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyWithSnapshot(context.Background(), target, "test-label",
		func(_ context.Context, _ domain.Snapshot, _ string) error { return nil })
	require.NoError(t, err)
	assert.True(t, syncCalled)
}

func TestApplyWithSnapshot_NilStateSync_DoesNotPanic(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyWithSnapshot(context.Background(), target, "test",
		func(_ context.Context, _ domain.Snapshot, _ string) error { return nil })
	assert.NoError(t, err)
}

func TestApplyWithSnapshot_ApplyFuncError_PropagatesError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyWithSnapshot(context.Background(), target, "fail-label",
		func(_ context.Context, _ domain.Snapshot, _ string) error { return errors.New("apply failed") })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply failed")
}

// ---------------------------------------------------------------------------
// buildActiveSnapshot
// ---------------------------------------------------------------------------

func TestBuildActiveSnapshot_ConfigTarget_BuildsConfigs(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(target, []domain.Entry{{Key: "app.workers", Value: 16}}, domain.Revision(3))

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap, err := mgr.buildActiveSnapshot(context.Background(), target)
	require.NoError(t, err)
	assert.Equal(t, 16, snap.Configs["app.workers"].Value)
	assert.False(t, snap.BuiltAt.IsZero())
}

func TestBuildActiveSnapshot_GlobalSettingTarget_BuildsGlobalSettings(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)

	target, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(target, []domain.Entry{{Key: "ui.theme", Value: "dark"}}, domain.Revision(2))

	// Seed existing tenant in snapshot so the global change cascades.
	spy.snapshot = domain.Snapshot{
		TenantSettings: map[string]map[string]domain.EffectiveValue{
			"t1": {"ui.theme": {Key: "ui.theme", Value: "light"}},
		},
	}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap, err := mgr.buildActiveSnapshot(context.Background(), target)
	require.NoError(t, err)
	assert.Equal(t, "dark", snap.GlobalSettings["ui.theme"].Value)
	// Tenant settings should also be rebuilt.
	assert.Contains(t, snap.TenantSettings, "t1")
}

func TestBuildActiveSnapshot_TenantSettingTarget_BuildsTenantSettings(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "t1")
	require.NoError(t, err)
	store.Seed(tenantTarget, []domain.Entry{{Key: "ui.theme", Value: "solarized"}}, domain.Revision(5))

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap, err := mgr.buildActiveSnapshot(context.Background(), tenantTarget)
	require.NoError(t, err)
	assert.Equal(t, "solarized", snap.TenantSettings["t1"]["ui.theme"].Value)
}

func TestBuildActiveSnapshot_InitializesNilMaps(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	// Spy returns zero snapshot — all maps are nil.
	spy.snapshot = domain.Snapshot{}

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	snap, err := mgr.buildActiveSnapshot(context.Background(), target)
	require.NoError(t, err)
	assert.NotNil(t, snap.Configs)
	assert.NotNil(t, snap.GlobalSettings)
	assert.NotNil(t, snap.TenantSettings)
}
