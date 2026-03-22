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
// GetConfigs
// ---------------------------------------------------------------------------

func TestGetConfigs_NoSnapshot_BuildsFromStore(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.limit", domain.ApplyLiveRead, true)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(target, []domain.Entry{{Key: "app.limit", Value: 42}}, domain.Revision(3))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetConfigs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(3), result.Revision)
	assert.Equal(t, 42, result.Values["app.limit"].Value)
	assert.Equal(t, "global-override", result.Values["app.limit"].Source)
}

func TestGetConfigs_EmptySnapshot_BuildsFromStore(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.timeout", domain.ApplyLiveRead, true)

	// Snapshot with zero BuiltAt forces builder path.
	spy.snapshot = domain.Snapshot{}

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetConfigs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, result.Revision)
	assert.Equal(t, 10, result.Values["app.timeout"].Value) // default
}

func TestGetConfigs_SnapshotWithNilConfigs_BuildsFromStore(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.timeout", domain.ApplyLiveRead, true)

	// Snapshot has BuiltAt set but nil Configs — triggers builder path.
	spy.snapshot = domain.Snapshot{BuiltAt: time.Now().UTC(), Configs: nil}

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetConfigs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 10, result.Values["app.timeout"].Value)
}

func TestGetConfigs_ActiveSnapshot_ReturnsSnapshotValues(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	spy.snapshot = domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.workers": {Key: "app.workers", Value: 16, Default: 10, Source: "global-override", Revision: 5},
		},
		BuiltAt: time.Now().UTC(),
	}

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetConfigs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(5), result.Revision)
	assert.Equal(t, 16, result.Values["app.workers"].Value)
}

func TestGetConfigs_RedactsSecretValues(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "db.password",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "s3cret",
		Secret:           true,
		RedactPolicy:     domain.RedactFull,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "DB password",
		Group:            "db",
	}))

	spy.snapshot = domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"db.password": {Key: "db.password", Value: "actualpass", Default: "s3cret", Source: "global-override", Revision: 2},
		},
		BuiltAt: time.Now().UTC(),
	}

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetConfigs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "****", result.Values["db.password"].Value)
	assert.Equal(t, "****", result.Values["db.password"].Default)
}

func TestGetConfigs_BuilderError_Propagated(t *testing.T) {
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

	history := testutil.NewFakeHistoryStore()
	spy := &spySupervisor{} // empty snapshot forces builder path

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: failStore, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.GetConfigs(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, errStoreDown)
}

// ---------------------------------------------------------------------------
// GetSettings
// ---------------------------------------------------------------------------

func TestGetSettings_GlobalScope_NoSnapshot_BuildsFromStore(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)

	globalTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(globalTarget, []domain.Entry{{Key: "ui.theme", Value: "dark"}}, domain.Revision(3))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetSettings(context.Background(), Subject{Scope: domain.ScopeGlobal})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(3), result.Revision)
	assert.Equal(t, "dark", result.Values["ui.theme"].Value)
}

func TestGetSettings_TenantScope_NoSnapshot_BuildsFromStore(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "t1")
	require.NoError(t, err)
	store.Seed(tenantTarget, []domain.Entry{{Key: "ui.theme", Value: "solarized"}}, domain.Revision(5))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.GetSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "t1"})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(5), result.Revision)
	assert.Equal(t, "solarized", result.Values["ui.theme"].Value)
}

func TestGetSettings_BuilderError_Propagated(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "ui.theme",
		Kind:             domain.KindSetting,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "light",
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "theme",
		Group:            "ui",
	}))

	failStore := &failingStore{}
	builder, builderErr := NewSnapshotBuilder(reg, failStore)
	require.NoError(t, builderErr)

	history := testutil.NewFakeHistoryStore()
	spy := &spySupervisor{}

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: failStore, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.GetSettings(context.Background(), Subject{Scope: domain.ScopeGlobal})
	require.Error(t, err)
	assert.ErrorIs(t, err, errStoreDown)
}

// ---------------------------------------------------------------------------
// cachedSettingsFromSnapshot
// ---------------------------------------------------------------------------

func TestCachedSettingsFromSnapshot_GlobalScope_ReturnsValues(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap := domain.Snapshot{
		GlobalSettings: map[string]domain.EffectiveValue{
			"ui.theme": {Key: "ui.theme", Value: "dark", Default: "default", Revision: 3, Source: "global-override"},
		},
		BuiltAt: time.Now().UTC(),
	}

	resolved, ok := mgr.cachedSettingsFromSnapshot(snap, Subject{Scope: domain.ScopeGlobal})
	require.True(t, ok)
	assert.Equal(t, domain.Revision(3), resolved.Revision)
	assert.Equal(t, "dark", resolved.Values["ui.theme"].Value)
}

func TestCachedSettingsFromSnapshot_GlobalScope_NilGlobalSettings_ReturnsFalse(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap := domain.Snapshot{GlobalSettings: nil, BuiltAt: time.Now().UTC()}

	_, ok := mgr.cachedSettingsFromSnapshot(snap, Subject{Scope: domain.ScopeGlobal})
	assert.False(t, ok)
}

func TestCachedSettingsFromSnapshot_TenantScope_ReturnsValues(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap := domain.Snapshot{
		TenantSettings: map[string]map[string]domain.EffectiveValue{
			"t1": {"ui.theme": {Key: "ui.theme", Value: "solar", Default: "default", Revision: 7, Source: "tenant-override"}},
		},
		BuiltAt: time.Now().UTC(),
	}

	resolved, ok := mgr.cachedSettingsFromSnapshot(snap, Subject{Scope: domain.ScopeTenant, SubjectID: "t1"})
	require.True(t, ok)
	assert.Equal(t, domain.Revision(7), resolved.Revision)
	assert.Equal(t, "solar", resolved.Values["ui.theme"].Value)
}

func TestCachedSettingsFromSnapshot_TenantScope_NilTenantSettings_ReturnsFalse(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap := domain.Snapshot{TenantSettings: nil, BuiltAt: time.Now().UTC()}

	_, ok := mgr.cachedSettingsFromSnapshot(snap, Subject{Scope: domain.ScopeTenant, SubjectID: "t1"})
	assert.False(t, ok)
}

func TestCachedSettingsFromSnapshot_TenantScope_MissingTenant_ReturnsFalse(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap := domain.Snapshot{
		TenantSettings: map[string]map[string]domain.EffectiveValue{
			"other-tenant": {"ui.theme": {Key: "ui.theme", Value: "x"}},
		},
		BuiltAt: time.Now().UTC(),
	}

	_, ok := mgr.cachedSettingsFromSnapshot(snap, Subject{Scope: domain.ScopeTenant, SubjectID: "missing-tenant"})
	assert.False(t, ok)
}

func TestCachedSettingsFromSnapshot_UnknownScope_ReturnsFalse(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgrIface, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	mgr := mgrIface.(*defaultManager)

	snap := domain.Snapshot{BuiltAt: time.Now().UTC()}

	_, ok := mgr.cachedSettingsFromSnapshot(snap, Subject{Scope: domain.Scope("unknown")})
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// GetConfigSchema / GetSettingSchema
// ---------------------------------------------------------------------------

func TestGetConfigSchema_ReturnsConfigKeysOnly(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal}, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetConfigSchema(context.Background())
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "app.workers", entries[0].Key)
	assert.Equal(t, domain.KindConfig, entries[0].Kind)
}

func TestGetSettingSchema_ReturnsSettingKeysOnly(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetSettingSchema(context.Background())
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "ui.theme", entries[0].Key)
	assert.Equal(t, domain.KindSetting, entries[0].Kind)
}

func TestGetConfigSchema_RedactsSecretDefault(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "auth.token",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "my-token-value",
		Secret:           true,
		RedactPolicy:     domain.RedactFull,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "auth token",
		Group:            "auth",
	}))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetConfigSchema(context.Background())
	require.NoError(t, err)

	var found bool

	for _, entry := range entries {
		if entry.Key == "auth.token" {
			found = true
			assert.Equal(t, "****", entry.DefaultValue)
		}
	}

	assert.True(t, found, "expected auth.token in schema")
}

// ---------------------------------------------------------------------------
// GetConfigHistory / GetSettingHistory
// ---------------------------------------------------------------------------

func TestGetConfigHistory_ReturnsFilteredEntries(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	history.AppendForKind(domain.KindConfig, ports.HistoryEntry{
		Revision: 1, Key: "app.workers", Scope: domain.ScopeGlobal,
		OldValue: 4, NewValue: 8, ActorID: "admin", ChangedAt: time.Now().UTC(),
	})
	history.AppendForKind(domain.KindSetting, ports.HistoryEntry{
		Revision: 2, Key: "ui.theme", Scope: domain.ScopeGlobal,
		OldValue: "light", NewValue: "dark", ActorID: "admin", ChangedAt: time.Now().UTC(),
	})

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetConfigHistory(context.Background(), ports.HistoryFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "app.workers", entries[0].Key)
}

func TestGetSettingHistory_ReturnsFilteredEntries(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal}, true)

	history.AppendForKind(domain.KindConfig, ports.HistoryEntry{
		Revision: 1, Key: "app.workers", Scope: domain.ScopeGlobal,
		OldValue: 4, NewValue: 8, ActorID: "admin", ChangedAt: time.Now().UTC(),
	})
	history.AppendForKind(domain.KindSetting, ports.HistoryEntry{
		Revision: 2, Key: "ui.theme", Scope: domain.ScopeGlobal,
		OldValue: "light", NewValue: "dark", ActorID: "admin", ChangedAt: time.Now().UTC(),
	})

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetSettingHistory(context.Background(), ports.HistoryFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "ui.theme", entries[0].Key)
}

func TestGetConfigHistory_RedactsSecretEntries(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "auth.key",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "default-key",
		Secret:           true,
		RedactPolicy:     domain.RedactFull,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "auth key",
		Group:            "auth",
	}))

	history.AppendForKind(domain.KindConfig, ports.HistoryEntry{
		Revision: 1, Key: "auth.key", Scope: domain.ScopeGlobal,
		OldValue: "old-secret", NewValue: "new-secret", ActorID: "admin", ChangedAt: time.Now().UTC(),
	})

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetConfigHistory(context.Background(), ports.HistoryFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "****", entries[0].OldValue)
	assert.Equal(t, "****", entries[0].NewValue)
}

func TestGetSettingHistory_RedactsSecretEntries(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "db.password",
		Kind:             domain.KindSetting,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "changeme",
		Secret:           true,
		RedactPolicy:     domain.RedactFull,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
		Description:      "db password",
		Group:            "db",
	}))

	history.AppendForKind(domain.KindSetting, ports.HistoryEntry{
		Revision: 1, Key: "db.password", Scope: domain.ScopeTenant, SubjectID: "t1",
		OldValue: "old-pw", NewValue: "new-pw", ActorID: "admin", ChangedAt: time.Now().UTC(),
	})

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	entries, err := mgr.GetSettingHistory(context.Background(), ports.HistoryFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "****", entries[0].OldValue)
	assert.Equal(t, "****", entries[0].NewValue)
}

// failingHistoryStore simulates a history store that always returns errors.
type failingHistoryStore struct{}

func (failingHistoryStore) ListHistory(_ context.Context, _ ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	return nil, errors.New("history store down")
}

func TestGetConfigHistory_StoreError_Propagated(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	store := testutil.NewFakeStore()
	builder, builderErr := NewSnapshotBuilder(reg, store)
	require.NoError(t, builderErr)

	spy := &spySupervisor{}
	fHistory := failingHistoryStore{}

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: fHistory, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.GetConfigHistory(context.Background(), ports.HistoryFilter{Limit: 10})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "history store down")
}

func TestGetSettingHistory_StoreError_Propagated(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	store := testutil.NewFakeStore()
	builder, builderErr := NewSnapshotBuilder(reg, store)
	require.NoError(t, builderErr)

	spy := &spySupervisor{}
	fHistory := failingHistoryStore{}

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: fHistory, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.GetSettingHistory(context.Background(), ports.HistoryFilter{Limit: 10})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "history store down")
}

// ---------------------------------------------------------------------------
// Resync
// ---------------------------------------------------------------------------

func TestResync_Success_CallsSupervisorReload(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	err := mgr.Resync(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, spy.reloadCalls)
	assert.Equal(t, "resync", spy.lastReason)
}

func TestResync_ReloadFailure_PropagatesError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	spy.reloadErr = errors.New("reload boom")

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	err := mgr.Resync(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reload boom")
}
