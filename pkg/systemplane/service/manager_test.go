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

type spySupervisor struct {
	snapshot       domain.Snapshot
	publishCalls   int
	reconcileCalls int
	reloadCalls    int
	lastReason     string
	publishErr     error
	reconcileErr   error
	reloadErr      error
}

func (s *spySupervisor) Current() domain.RuntimeBundle { return nil }

func (s *spySupervisor) Snapshot() domain.Snapshot { return s.snapshot }

func (s *spySupervisor) PublishSnapshot(_ context.Context, snap domain.Snapshot, reason string) error {
	s.publishCalls++
	s.lastReason = reason
	if s.publishErr != nil {
		return s.publishErr
	}
	s.snapshot = snap

	return nil
}

func (s *spySupervisor) ReconcileCurrent(_ context.Context, snap domain.Snapshot, reason string) error {
	s.reconcileCalls++
	s.lastReason = reason
	if s.reconcileErr != nil {
		return s.reconcileErr
	}
	s.snapshot = snap

	return nil
}

func (s *spySupervisor) Reload(_ context.Context, reason string) error {
	s.reloadCalls++
	s.lastReason = reason

	return s.reloadErr
}

func (s *spySupervisor) Stop(_ context.Context) error { return nil }

func testManagerDeps(t *testing.T) (registry.Registry, *testutil.FakeStore, *testutil.FakeHistoryStore, *spySupervisor, *SnapshotBuilder) {
	t.Helper()

	reg := registry.New()
	store := testutil.NewFakeStore()
	history := testutil.NewFakeHistoryStore()
	spy := &spySupervisor{}
	builder, builderErr := NewSnapshotBuilder(reg, store)
	require.NoError(t, builderErr)

	return reg, store, history, spy, builder
}

func registerTestConfigKey(t *testing.T, reg registry.Registry, key string, behavior domain.ApplyBehavior, mutable bool) {
	t.Helper()

	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              key,
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeInt,
		DefaultValue:     10,
		ApplyBehavior:    behavior,
		MutableAtRuntime: mutable,
		Description:      "test key " + key,
		Group:            "test",
	}))
}

func registerTestSettingKey(t *testing.T, reg registry.Registry, key string, scopes []domain.Scope, mutable bool) {
	t.Helper()

	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              key,
		Kind:             domain.KindSetting,
		AllowedScopes:    scopes,
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "default",
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: mutable,
		Description:      "test setting " + key,
		Group:            "test",
	}))
}

func TestManager_GetConfigs_ReturnsMapValues(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.timeout", domain.ApplyLiveRead, true)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetConfigs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, result.Revision)
	assert.Equal(t, 10, result.Values["app.timeout"].Value)
	assert.Equal(t, 10, result.Values["app.workers"].Value)
}

func TestManager_GetSettings_GlobalAndTenantScopes(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)

	globalTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(globalTarget, []domain.Entry{{Key: "ui.theme", Value: "dark"}}, domain.Revision(2))

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "tenant-123")
	require.NoError(t, err)
	store.Seed(tenantTarget, []domain.Entry{{Key: "ui.theme", Value: "solarized"}}, domain.Revision(4))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	globalResult, err := mgr.GetSettings(context.Background(), Subject{Scope: domain.ScopeGlobal})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(2), globalResult.Revision)
	assert.Equal(t, "dark", globalResult.Values["ui.theme"].Value)
	assert.Equal(t, "global-override", globalResult.Values["ui.theme"].Source)

	tenantResult, err := mgr.GetSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "tenant-123"})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(4), tenantResult.Revision)
	assert.Equal(t, "solarized", tenantResult.Values["ui.theme"].Value)
	assert.Equal(t, "tenant-override", tenantResult.Values["ui.theme"].Source)
}

func TestManager_GetSettings_PrefersActiveSnapshot(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)
	spy.snapshot = domain.Snapshot{
		GlobalSettings: map[string]domain.EffectiveValue{
			"ui.theme": {Key: "ui.theme", Value: "dark", Default: "default", Revision: 7, Source: "global-override"},
		},
		TenantSettings: map[string]map[string]domain.EffectiveValue{
			"tenant-123": {
				"ui.theme": {Key: "ui.theme", Value: "solarized", Default: "default", Revision: 9, Source: "tenant-override"},
			},
		},
		BuiltAt: time.Now().UTC(),
	}

	globalTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(globalTarget, []domain.Entry{{Key: "ui.theme", Value: "light"}}, domain.Revision(2))

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "tenant-123")
	require.NoError(t, err)
	store.Seed(tenantTarget, []domain.Entry{{Key: "ui.theme", Value: "light"}}, domain.Revision(4))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	globalResult, err := mgr.GetSettings(context.Background(), Subject{Scope: domain.ScopeGlobal})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(7), globalResult.Revision)
	assert.Equal(t, "dark", globalResult.Values["ui.theme"].Value)

	tenantResult, err := mgr.GetSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "tenant-123"})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(9), tenantResult.Revision)
	assert.Equal(t, "solarized", tenantResult.Values["ui.theme"].Value)
	assert.Equal(t, 0, spy.publishCalls)
	assert.Equal(t, 0, spy.reconcileCalls)
	assert.Equal(t, 0, spy.reloadCalls)
}

func TestManager_GetSettings_TenantSnapshotMissFallsBackToBuilder(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)
	spy.snapshot = domain.Snapshot{BuiltAt: time.Now().UTC()}

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "tenant-123")
	require.NoError(t, err)
	store.Seed(tenantTarget, []domain.Entry{{Key: "ui.theme", Value: "solarized"}}, domain.Revision(4))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "tenant-123"})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(4), result.Revision)
	assert.Equal(t, "solarized", result.Values["ui.theme"].Value)
}

func TestManager_PatchConfigs_LiveRead_PublishesSnapshot(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.workers", Value: 8}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), result.Revision)
	assert.Equal(t, 1, spy.publishCalls)
	assert.Equal(t, 8, spy.snapshot.Configs["app.workers"].Value)
	assert.Equal(t, domain.Revision(1), spy.snapshot.Configs["app.workers"].Revision)
}

func TestManager_PatchConfigs_LiveRead_InvokesStateSync(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)
	stateSyncCalls := 0

	mgr, mgrErr := NewManager(ManagerConfig{
		Registry:   reg,
		Store:      store,
		History:    history,
		Supervisor: spy,
		Builder:    builder,
		StateSync: func(_ context.Context, snap domain.Snapshot) {
			stateSyncCalls++
			assert.Equal(t, 8, snap.Configs["app.workers"].Value)
		},
	})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.workers", Value: 8}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stateSyncCalls)
}

func TestManager_PatchConfigs_ConfigWriteValidatorRejectsCandidate(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgr, mgrErr := NewManager(ManagerConfig{
		Registry:             reg,
		Store:                store,
		History:              history,
		Supervisor:           spy,
		Builder:              builder,
		ConfigWriteValidator: func(_ context.Context, _ domain.Snapshot) error { return errors.New("invalid config") },
	})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.workers", Value: 8}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "patch configs validation")
	assert.Equal(t, 0, spy.publishCalls)
}

func TestManager_PatchSettings_LiveRead_UpdatesTenantSnapshot(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "tenant-123"}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "ui.theme", Value: "dark"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "user-1"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), result.Revision)
	assert.Equal(t, 1, spy.publishCalls)
	assert.Equal(t, "dark", spy.snapshot.TenantSettings["tenant-123"]["ui.theme"].Value)
	assert.Equal(t, domain.Revision(1), spy.snapshot.TenantSettings["tenant-123"]["ui.theme"].Revision)
}

func TestManager_PatchSettings_GlobalScope_UpdatesCachedTenants(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)
	spy.snapshot = domain.Snapshot{
		GlobalSettings: map[string]domain.EffectiveValue{"ui.theme": {Key: "ui.theme", Value: "light", Default: "default", Revision: 0, Source: "default"}},
		TenantSettings: map[string]map[string]domain.EffectiveValue{
			"tenant-123": {"ui.theme": {Key: "ui.theme", Value: "light", Default: "default", Revision: 0, Source: "default"}},
		},
		BuiltAt: time.Now().UTC(),
	}

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeGlobal}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "ui.theme", Value: "dark"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, "dark", spy.snapshot.GlobalSettings["ui.theme"].Value)
	assert.Equal(t, "dark", spy.snapshot.TenantSettings["tenant-123"]["ui.theme"].Value)
}

func TestManager_PatchSettings_ScopeValidation(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeGlobal}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "ui.theme", Value: "dark"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrScopeInvalid)
}

func TestManager_GetSchema_RedactsSecretDefaults(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "auth.secret",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "super-secret",
		Secret:           true,
		RedactPolicy:     domain.RedactFull,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "secret",
		Group:            "auth",
	}))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetConfigSchema(context.Background())
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "****", entries[0].DefaultValue)
	assert.Equal(t, domain.RedactFull, entries[0].RedactPolicy)
}

func TestManager_GetHistory_RedactsSecrets(t *testing.T) {
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
	history.Append(ports.HistoryEntry{Revision: 1, Key: "auth.secret", Scope: domain.ScopeGlobal, OldValue: "old", NewValue: "new", ActorID: "admin", ChangedAt: time.Now().UTC()})

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetConfigHistory(context.Background(), ports.HistoryFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "****", entries[0].OldValue)
	assert.Equal(t, "****", entries[0].NewValue)
}

func TestManager_GetConfigs_PrefersActiveSnapshot(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)
	spy.snapshot = domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.workers": {Key: "app.workers", Value: 4, Default: 10, Source: "default", Revision: 1},
		},
		BuiltAt: time.Now().UTC(),
	}

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(target, []domain.Entry{{Key: "app.workers", Value: 99}}, domain.Revision(8))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetConfigs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), result.Revision)
	assert.Equal(t, 4, result.Values["app.workers"].Value)
}

func TestManager_PatchConfigs_PropagatesPublishFailure(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)
	spy.publishErr = errors.New("publish failed")

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.workers", Value: 8}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish failed")
}

func TestManager_PatchConfigs_PropagatesReconcileFailure(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "worker.interval", domain.ApplyWorkerReconcile, true)
	spy.reconcileErr = errors.New("reconcile failed")

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "worker.interval", Value: 30}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconcile failed")
}

func TestManager_PatchConfigs_BundleRebuild_TriggersReload(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.bundle", domain.ApplyBundleRebuild, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.bundle", Value: 1}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), result.Revision)
	assert.Equal(t, 1, spy.reloadCalls)
	assert.Equal(t, string(domain.ApplyBundleRebuild), spy.lastReason)
	assert.Equal(t, 0, spy.publishCalls)
	assert.Equal(t, 0, spy.reconcileCalls)
}

func TestManager_PatchConfigs_BundleRebuildAndReconcile_TriggersReload(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.bundle", domain.ApplyBundleRebuildAndReconcile, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.bundle", Value: 1}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), result.Revision)
	assert.Equal(t, 1, spy.reloadCalls)
	assert.Equal(t, string(domain.ApplyBundleRebuildAndReconcile), spy.lastReason)
	assert.Equal(t, 0, spy.publishCalls)
	assert.Equal(t, 0, spy.reconcileCalls)
}

func TestManager_ApplyEscalation_UnexpectedBehaviorReturnsError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	mgrIface, mgrCreateErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrCreateErr)

	mgr := mgrIface.(*defaultManager)
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.applyEscalation(context.Background(), target, domain.ApplyBehavior("bogus"))
	require.Error(t, err)
	assert.ErrorIs(t, err, errUnexpectedApplyBehavior)
}

func TestManager_Resync_PropagatesReloadFailure(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	spy.reloadErr = errors.New("reload failed")

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	err := mgr.Resync(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reload failed")
}
