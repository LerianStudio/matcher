//go:build unit

// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
	"github.com/LerianStudio/matcher/pkg/systemplane/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func configKeyDef(key string, defaultValue any, vt domain.ValueType) domain.KeyDef {
	return domain.KeyDef{
		Key:              key,
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        vt,
		DefaultValue:     defaultValue,
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
	}
}

func settingKeyDef(key string, defaultValue any, vt domain.ValueType) domain.KeyDef {
	return domain.KeyDef{
		Key:              key,
		Kind:             domain.KindSetting,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant},
		ValueType:        vt,
		DefaultValue:     defaultValue,
		RedactPolicy:     domain.RedactNone,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
	}
}

type failingStore struct{}

var errStoreDown = errors.New("store is down")

func (failingStore) Get(_ context.Context, _ domain.Target) (ports.ReadResult, error) {
	return ports.ReadResult{}, errStoreDown
}

func (failingStore) Put(_ context.Context, _ domain.Target, _ []ports.WriteOp, _ domain.Revision, _ domain.Actor, _ string) (domain.Revision, error) {
	return domain.RevisionZero, errStoreDown
}

func TestBuildConfigs_ReturnsDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	reg.MustRegister(configKeyDef("worker.interval", 60, domain.ValueTypeInt))
	reg.MustRegister(configKeyDef("app.name", "matcher", domain.ValueTypeString))

	store := testutil.NewFakeStore()
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(target, []domain.Entry{{Key: "worker.interval", Value: 30}}, domain.Revision(5))

	builder := NewSnapshotBuilder(reg, store)
	configs, rev, err := builder.BuildConfigs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(5), rev)
	assert.Equal(t, 30, configs["worker.interval"].Value)
	assert.Equal(t, 30, configs["worker.interval"].Override)
	assert.Equal(t, "global-override", configs["worker.interval"].Source)
	assert.Equal(t, domain.Revision(5), configs["worker.interval"].Revision)
	assert.Equal(t, "matcher", configs["app.name"].Value)
}

func TestBuildSettings_GlobalAndTenantScopes(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	reg.MustRegister(settingKeyDef("ui.theme", "light", domain.ValueTypeString))

	store := testutil.NewFakeStore()
	globalTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(globalTarget, []domain.Entry{{Key: "ui.theme", Value: "dark"}}, domain.Revision(2))

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "tenant-001")
	require.NoError(t, err)
	store.Seed(tenantTarget, []domain.Entry{{Key: "ui.theme", Value: "solarized"}}, domain.Revision(7))

	builder := NewSnapshotBuilder(reg, store)

	globalSettings, globalRev, err := builder.BuildSettings(context.Background(), Subject{Scope: domain.ScopeGlobal})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(2), globalRev)
	assert.Equal(t, "dark", globalSettings["ui.theme"].Value)
	assert.Equal(t, "global-override", globalSettings["ui.theme"].Source)

	tenantSettings, tenantRev, err := builder.BuildSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "tenant-001"})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(7), tenantRev)
	assert.Equal(t, "solarized", tenantSettings["ui.theme"].Value)
	assert.Equal(t, "tenant-override", tenantSettings["ui.theme"].Source)
	assert.Equal(t, domain.Revision(7), tenantSettings["ui.theme"].Revision)
}

func TestBuildSettings_TenantFallsBackToGlobalOverride(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	reg.MustRegister(settingKeyDef("ui.theme", "light", domain.ValueTypeString))
	reg.MustRegister(settingKeyDef("ui.page_size", 25, domain.ValueTypeInt))

	store := testutil.NewFakeStore()
	globalTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(globalTarget, []domain.Entry{{Key: "ui.page_size", Value: 50}}, domain.Revision(3))

	builder := NewSnapshotBuilder(reg, store)
	settings, rev, err := builder.BuildSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "tenant-001"})
	require.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, rev)
	assert.Equal(t, 50, settings["ui.page_size"].Value)
	assert.Equal(t, "global-override", settings["ui.page_size"].Source)
}

func TestBuildFull_BuildsConfigsGlobalAndTenantSettings(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	reg.MustRegister(configKeyDef("worker.interval", 60, domain.ValueTypeInt))
	reg.MustRegister(settingKeyDef("ui.theme", "light", domain.ValueTypeString))

	store := testutil.NewFakeStore()
	configTarget, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(configTarget, []domain.Entry{{Key: "worker.interval", Value: 45}}, domain.Revision(3))

	globalTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(globalTarget, []domain.Entry{{Key: "ui.theme", Value: "dark"}}, domain.Revision(5))

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "tenant-abc")
	require.NoError(t, err)
	store.Seed(tenantTarget, []domain.Entry{{Key: "ui.theme", Value: "solarized"}}, domain.Revision(8))

	builder := NewSnapshotBuilder(reg, store)
	snap, err := builder.BuildFull(context.Background(), "tenant-abc")
	require.NoError(t, err)
	assert.Equal(t, 45, snap.Configs["worker.interval"].Value)
	assert.Equal(t, "dark", snap.GlobalSettings["ui.theme"].Value)
	assert.Equal(t, "solarized", snap.TenantSettings["tenant-abc"]["ui.theme"].Value)
	assert.Equal(t, domain.Revision(8), snap.Revision)
	assert.False(t, snap.BuiltAt.IsZero())
}

func TestBuildConfigs_StoreError_Propagated(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	reg.MustRegister(configKeyDef("app.name", "matcher", domain.ValueTypeString))
	builder := NewSnapshotBuilder(reg, failingStore{})

	_, _, err := builder.BuildConfigs(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, errStoreDown)
}

func TestBuildSettings_StoreError_Propagated(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	reg.MustRegister(settingKeyDef("ui.theme", "light", domain.ValueTypeString))
	builder := NewSnapshotBuilder(reg, failingStore{})

	_, _, err := builder.BuildSettings(context.Background(), Subject{Scope: domain.ScopeGlobal})
	require.Error(t, err)
	assert.ErrorIs(t, err, errStoreDown)
}

func TestBuildSettings_RetainsRawSecretValuesInSnapshot(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "db.password",
		Kind:             domain.KindSetting,
		AllowedScopes:    []domain.Scope{domain.ScopeTenant},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "changeme",
		Secret:           true,
		RedactPolicy:     domain.RedactFull,
		ApplyBehavior:    domain.ApplyLiveRead,
		MutableAtRuntime: true,
	}))

	builder := NewSnapshotBuilder(reg, testutil.NewFakeStore())
	settings, _, err := builder.BuildSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "tenant-001"})
	require.NoError(t, err)
	assert.Equal(t, "changeme", settings["db.password"].Value)
	assert.True(t, settings["db.password"].Redacted)
}
