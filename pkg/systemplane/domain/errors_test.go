//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentinelErrors_NonEmptyMessages(t *testing.T) {
	t.Parallel()

	sentinels := []struct {
		name string
		err  error
	}{
		{name: "ErrKeyUnknown", err: ErrKeyUnknown},
		{name: "ErrValueInvalid", err: ErrValueInvalid},
		{name: "ErrRevisionMismatch", err: ErrRevisionMismatch},
		{name: "ErrScopeInvalid", err: ErrScopeInvalid},
		{name: "ErrPermissionDenied", err: ErrPermissionDenied},
		{name: "ErrReloadFailed", err: ErrReloadFailed},
		{name: "ErrKeyNotMutable", err: ErrKeyNotMutable},
		{name: "ErrSnapshotBuildFailed", err: ErrSnapshotBuildFailed},
		{name: "ErrBundleBuildFailed", err: ErrBundleBuildFailed},
		{name: "ErrBundleSwapFailed", err: ErrBundleSwapFailed},
		{name: "ErrReconcileFailed", err: ErrReconcileFailed},
		{name: "ErrNoCurrentBundle", err: ErrNoCurrentBundle},
		{name: "ErrSupervisorStopped", err: ErrSupervisorStopped},
		{name: "ErrRegistryRequired", err: ErrRegistryRequired},
	}

	for _, tt := range sentinels {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestSentinelErrors_Identity(t *testing.T) {
	t.Parallel()

	sentinels := []struct {
		name string
		err  error
	}{
		{name: "ErrKeyUnknown", err: ErrKeyUnknown},
		{name: "ErrValueInvalid", err: ErrValueInvalid},
		{name: "ErrRevisionMismatch", err: ErrRevisionMismatch},
		{name: "ErrScopeInvalid", err: ErrScopeInvalid},
		{name: "ErrPermissionDenied", err: ErrPermissionDenied},
		{name: "ErrReloadFailed", err: ErrReloadFailed},
		{name: "ErrKeyNotMutable", err: ErrKeyNotMutable},
		{name: "ErrSnapshotBuildFailed", err: ErrSnapshotBuildFailed},
		{name: "ErrBundleBuildFailed", err: ErrBundleBuildFailed},
		{name: "ErrBundleSwapFailed", err: ErrBundleSwapFailed},
		{name: "ErrReconcileFailed", err: ErrReconcileFailed},
		{name: "ErrNoCurrentBundle", err: ErrNoCurrentBundle},
		{name: "ErrSupervisorStopped", err: ErrSupervisorStopped},
		{name: "ErrRegistryRequired", err: ErrRegistryRequired},
	}

	for _, tt := range sentinels {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.True(t, errors.Is(tt.err, tt.err))
		})
	}
}

func TestSentinelErrors_Distinct(t *testing.T) {
	t.Parallel()

	all := []error{
		ErrKeyUnknown,
		ErrValueInvalid,
		ErrRevisionMismatch,
		ErrScopeInvalid,
		ErrPermissionDenied,
		ErrReloadFailed,
		ErrKeyNotMutable,
		ErrSnapshotBuildFailed,
		ErrBundleBuildFailed,
		ErrBundleSwapFailed,
		ErrReconcileFailed,
		ErrNoCurrentBundle,
		ErrSupervisorStopped,
		ErrRegistryRequired,
	}

	for i := range all {
		for j := i + 1; j < len(all); j++ {
			assert.False(t, errors.Is(all[i], all[j]),
				"expected %q and %q to be distinct", all[i], all[j])
		}
	}
}

func TestSentinelErrors_WrappingPreservesIdentity(t *testing.T) {
	t.Parallel()

	sentinels := []struct {
		name string
		err  error
	}{
		{name: "ErrKeyUnknown", err: ErrKeyUnknown},
		{name: "ErrValueInvalid", err: ErrValueInvalid},
		{name: "ErrRevisionMismatch", err: ErrRevisionMismatch},
		{name: "ErrScopeInvalid", err: ErrScopeInvalid},
		{name: "ErrPermissionDenied", err: ErrPermissionDenied},
		{name: "ErrReloadFailed", err: ErrReloadFailed},
		{name: "ErrKeyNotMutable", err: ErrKeyNotMutable},
		{name: "ErrSnapshotBuildFailed", err: ErrSnapshotBuildFailed},
		{name: "ErrBundleBuildFailed", err: ErrBundleBuildFailed},
		{name: "ErrBundleSwapFailed", err: ErrBundleSwapFailed},
		{name: "ErrReconcileFailed", err: ErrReconcileFailed},
		{name: "ErrNoCurrentBundle", err: ErrNoCurrentBundle},
		{name: "ErrSupervisorStopped", err: ErrSupervisorStopped},
		{name: "ErrRegistryRequired", err: ErrRegistryRequired},
	}

	for _, tt := range sentinels {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wrapped := fmt.Errorf("context: %w", tt.err)
			require.True(t, errors.Is(wrapped, tt.err))
			assert.Contains(t, wrapped.Error(), tt.err.Error())
		})
	}
}
