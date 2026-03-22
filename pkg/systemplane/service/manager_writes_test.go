//go:build unit

// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// PatchConfigs
// ---------------------------------------------------------------------------

func TestPatchConfigs_EmptyOps_ReturnsZeroResult(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchConfigs(context.Background(), PatchRequest{Ops: nil})
	require.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, result.Revision)
	assert.Equal(t, 0, spy.publishCalls)
}

func TestPatchConfigs_UnknownKey_RejectsWithErrKeyUnknown(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "nonexistent.key", Value: "v"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestPatchConfigs_SettingKey_RejectsAsWrongKind(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal}, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "ui.theme", Value: "dark"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestPatchConfigs_ImmutableKey_RejectsWithErrKeyNotMutable(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.immutable", domain.ApplyLiveRead, false)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.immutable", Value: 42}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyNotMutable)
}

func TestPatchConfigs_RevisionMismatch_RejectsWithStoreError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	// Seed store at revision 5 so expected=0 mismatches.
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(target, []domain.Entry{{Key: "app.workers", Value: 4}}, domain.Revision(5))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err = mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.workers", Value: 8}},
		ExpectedRevision: domain.RevisionZero, // mismatch with actual revision 5
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrRevisionMismatch)
}

func TestPatchConfigs_WriteValidatorRejects_DoesNotPersist(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	validatorErr := errors.New("workers must be <= 16")
	mgr, mgrErr := NewManager(ManagerConfig{
		Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder,
		ConfigWriteValidator: func(_ context.Context, _ domain.Snapshot) error {
			return validatorErr
		},
	})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.workers", Value: 32}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "patch configs validation")
	assert.Equal(t, 0, spy.publishCalls)
}

func TestPatchConfigs_BootstrapOnly_PublishesSnapshot(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.noop", domain.ApplyLiveRead, true)

	// Register a bootstrap-only key as well — escalation should reject it.
	require.NoError(t, reg.Register(domain.KeyDef{
		Key:              "boot.key",
		Kind:             domain.KindConfig,
		AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
		ValueType:        domain.ValueTypeString,
		DefaultValue:     "x",
		ApplyBehavior:    domain.ApplyBootstrapOnly,
		MutableAtRuntime: true,
		Description:      "bootstrap key",
		Group:            "boot",
	}))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "boot.key", Value: "y"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyNotMutable)
}

func TestPatchConfigs_ResetOp_BypassesValidation(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchConfigs(context.Background(), PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.workers", Reset: true}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), result.Revision)
}

// ---------------------------------------------------------------------------
// PatchSettings
// ---------------------------------------------------------------------------

func TestPatchSettings_EmptyOps_ReturnsZeroResult(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeGlobal}, PatchRequest{Ops: nil})
	require.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, result.Revision)
}

func TestPatchSettings_UnknownKey_RejectsWithErrKeyUnknown(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeGlobal}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "nonexistent.key", Value: "v"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestPatchSettings_ConfigKey_RejectsAsWrongKind(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeGlobal}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "app.workers", Value: 8}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyUnknown)
}

func TestPatchSettings_ImmutableKey_RejectsWithErrKeyNotMutable(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.frozen", []domain.Scope{domain.ScopeGlobal}, false)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeGlobal}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "ui.frozen", Value: "value"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrKeyNotMutable)
}

func TestPatchSettings_ScopeNotAllowed_RejectsWithErrScopeInvalid(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	// Only tenant scope allowed.
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

func TestPatchSettings_TenantScope_PersistsAndEscalates(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeTenant}, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeTenant, SubjectID: "t1"}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "ui.theme", Value: "dark"}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "user-1"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), result.Revision)
	assert.Equal(t, 1, spy.publishCalls)
}

func TestPatchSettings_RevisionMismatch_RejectsWithStoreError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal}, true)

	target, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	require.NoError(t, err)
	store.Seed(target, []domain.Entry{{Key: "ui.theme", Value: "dark"}}, domain.Revision(3))

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	_, err = mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeGlobal}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "ui.theme", Value: "light"}},
		ExpectedRevision: domain.RevisionZero, // actual is 3
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrRevisionMismatch)
}

func TestPatchSettings_ResetOp_BypassesValidation(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestSettingKey(t, reg, "ui.theme", []domain.Scope{domain.ScopeGlobal}, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	result, err := mgr.PatchSettings(context.Background(), Subject{Scope: domain.ScopeGlobal}, PatchRequest{
		Ops:              []ports.WriteOp{{Key: "ui.theme", Reset: true}},
		ExpectedRevision: domain.RevisionZero,
		Actor:            domain.Actor{ID: "admin"},
		Source:           "api",
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), result.Revision)
}

// ---------------------------------------------------------------------------
// ApplyChangeSignal
// ---------------------------------------------------------------------------

func TestApplyChangeSignal_ValidBehavior_Dispatches(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.ApplyChangeSignal(context.Background(), ports.ChangeSignal{
		Target:        target,
		Revision:      domain.Revision(5),
		ApplyBehavior: domain.ApplyBundleRebuild,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, spy.reloadCalls)
}

func TestApplyChangeSignal_InvalidBehavior_FallsBackToBundleRebuild(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.ApplyChangeSignal(context.Background(), ports.ChangeSignal{
		Target:        target,
		Revision:      domain.Revision(1),
		ApplyBehavior: domain.ApplyBehavior("garbage-value"),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, spy.reloadCalls)
	assert.Equal(t, string(domain.ApplyBundleRebuild), spy.lastReason)
}

func TestApplyChangeSignal_PropagatesEscalationError(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	spy.reloadErr = errors.New("reload exploded")

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.ApplyChangeSignal(context.Background(), ports.ChangeSignal{
		Target:        target,
		Revision:      domain.Revision(1),
		ApplyBehavior: domain.ApplyBundleRebuild,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reload exploded")
}

func TestApplyChangeSignal_LiveRead_PublishesSnapshot(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "app.workers", domain.ApplyLiveRead, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.ApplyChangeSignal(context.Background(), ports.ChangeSignal{
		Target:        target,
		Revision:      domain.Revision(2),
		ApplyBehavior: domain.ApplyLiveRead,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, spy.publishCalls)
}

func TestApplyChangeSignal_WorkerReconcile_CallsReconcile(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)
	registerTestConfigKey(t, reg, "worker.interval", domain.ApplyWorkerReconcile, true)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.ApplyChangeSignal(context.Background(), ports.ChangeSignal{
		Target:        target,
		Revision:      domain.Revision(2),
		ApplyBehavior: domain.ApplyWorkerReconcile,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, spy.reconcileCalls)
}

func TestApplyChangeSignal_BootstrapOnly_IsNoop(t *testing.T) {
	t.Parallel()

	reg, store, history, spy, builder := testManagerDeps(t)

	mgr, mgrErr := NewManager(ManagerConfig{Registry: reg, Store: store, History: history, Supervisor: spy, Builder: builder})
	require.NoError(t, mgrErr)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	err = mgr.ApplyChangeSignal(context.Background(), ports.ChangeSignal{
		Target:        target,
		Revision:      domain.Revision(1),
		ApplyBehavior: domain.ApplyBootstrapOnly,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, spy.publishCalls)
	assert.Equal(t, 0, spy.reconcileCalls)
	assert.Equal(t, 0, spy.reloadCalls)
}
