// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

type fakeMigrator struct {
	version     uint
	dirty       bool
	versionErr  error
	forceCalled bool
	forcedTo    int
	forceErr    error
	upErr       error
	upStartedCh chan struct{}
	upReleaseCh chan struct{}
}

func (f *fakeMigrator) Version() (uint, bool, error) {
	if f.versionErr != nil {
		return 0, false, f.versionErr
	}

	return f.version, f.dirty, nil
}

func (f *fakeMigrator) Force(version int) error {
	f.forceCalled = true
	f.forcedTo = version

	return f.forceErr
}

func (f *fakeMigrator) Up() error {
	if f.upStartedCh != nil {
		select {
		case <-f.upStartedCh:
		default:
			close(f.upStartedCh)
		}
	}

	if f.upReleaseCh != nil {
		<-f.upReleaseCh
	}

	return f.upErr
}

func (f *fakeMigrator) Close() (error, error) {
	return nil, nil
}

func TestRunMigrations_CanceledContext(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := RunMigrations(ctx, "postgres://test:test@localhost/test", "test", "migrations", logger, false)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunMigrations_EmptyPath_SkipsGracefully(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}

	err := RunMigrations(context.Background(), "postgres://test:test@localhost/test", "test", "", logger, false)
	assert.NoError(t, err, "empty migrations path should skip gracefully")
}

func TestOpenMigrationDB_CanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	db, err := openMigrationDB(ctx, "postgres://test:test@localhost/test")
	require.Error(t, err)
	require.Nil(t, db)
	require.ErrorIs(t, err, context.Canceled)
}

func TestErrDatabaseDirty(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "database is in dirty migration state", ErrDatabaseDirty.Error())
}

func TestErrDirtyMigrationState(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "database migration dirty state requires manual intervention", ErrDirtyMigrationState.Error())
}

func TestRunMigrations_InvalidDSN_ReturnsError(t *testing.T) {
	t.Parallel()

	logger := &libLog.NopLogger{}

	err := RunMigrations(context.Background(), "not-a-valid-dsn", "test", "migrations", logger, false)
	if assert.Error(t, err, "invalid DSN should return error") {
		assert.Contains(t, err.Error(), "ping migration connection", "RunMigrations should add context in TestRunMigrations_InvalidDSN_ReturnsError")
		assert.NotNil(t, errors.Unwrap(err), "RunMigrations should wrap underlying error in TestRunMigrations_InvalidDSN_ReturnsError")
	}
}

func TestRunMigrations_NilLogger_DoesNotPanic(t *testing.T) {
	t.Parallel()

	err := RunMigrations(context.Background(), "postgres://test:test@localhost/test", "test", "", nil, false)
	require.NoError(t, err)
}

func TestHandleDirtyState_ProductionRequiresManualIntervention(t *testing.T) {
	t.Parallel()

	migrator := &fakeMigrator{version: 10, dirty: true}

	err := handleDirtyState(context.Background(), migrator, &libLog.NopLogger{}, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDirtyMigrationState)
	assert.False(t, migrator.forceCalled)
}

func TestHandleDirtyState_DevelopmentAutoRecovers(t *testing.T) {
	t.Parallel()

	migrator := &fakeMigrator{version: 7, dirty: true}

	err := handleDirtyState(context.Background(), migrator, &libLog.NopLogger{}, true)
	require.NoError(t, err)
	assert.True(t, migrator.forceCalled)
	assert.Equal(t, 6, migrator.forcedTo)
}

func TestHandleDirtyState_VersionError_ReturnsWrappedError(t *testing.T) {
	t.Parallel()

	migrator := &fakeMigrator{versionErr: errors.New("boom")}

	err := handleDirtyState(context.Background(), migrator, &libLog.NopLogger{}, true)
	require.Error(t, err)
	assert.ErrorContains(t, err, "check migration version")
}

func TestHandleDirtyState_ForceError_ReturnsWrappedError(t *testing.T) {
	t.Parallel()

	migrator := &fakeMigrator{version: 3, dirty: true, forceErr: errors.New("force failed")}

	err := handleDirtyState(context.Background(), migrator, &libLog.NopLogger{}, true)
	require.Error(t, err)
	assert.ErrorContains(t, err, "force migration version 2")
}

func TestHandleDirtyState_VersionExceedsMaxInt_ReturnsErrDatabaseDirty(t *testing.T) {
	t.Parallel()

	maxIntAsUint := uint(^uint(0) >> 1)
	migrator := &fakeMigrator{version: maxIntAsUint + 1, dirty: true}

	err := handleDirtyState(context.Background(), migrator, &libLog.NopLogger{}, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDatabaseDirty)
	assert.False(t, migrator.forceCalled)
}

func TestRunMigrationsUp_ContextCanceledDuringApply(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	upStartedCh := make(chan struct{})
	upReleaseCh := make(chan struct{})
	migrator := &fakeMigrator{upStartedCh: upStartedCh, upReleaseCh: upReleaseCh}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runMigrationsUp(ctx, migrator, &libLog.NopLogger{})
	}()

	<-upStartedCh
	cancel()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(time.Second):
		t.Fatal("runMigrationsUp did not return after cancellation")
	}

	close(upReleaseCh)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestApplyMigrations_NoChange_ReturnsNil(t *testing.T) {
	t.Parallel()

	migrator := &fakeMigrator{
		version: 1,
		dirty:   false,
		upErr:   migrate.ErrNoChange,
	}

	err := applyMigrations(context.Background(), migrator, &libLog.NopLogger{}, true)
	require.NoError(t, err)
}

func TestLogMigrationVersion_VersionError_ReturnsWrappedError(t *testing.T) {
	t.Parallel()

	migrator := &fakeMigrator{versionErr: errors.New("version failed")}

	err := logMigrationVersion(context.Background(), migrator, &libLog.NopLogger{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "get migration version")
}
