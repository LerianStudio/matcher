// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/ports"
)

func TestErrLockAlreadyHeld(t *testing.T) {
	t.Parallel()

	require.Error(t, ports.ErrLockAlreadyHeld)
	require.Equal(t, "lock already held", ports.ErrLockAlreadyHeld.Error())
}

func TestErrLockAlreadyHeld_ErrorIs(t *testing.T) {
	t.Parallel()

	wrappedErr := fmt.Errorf("context: %w", ports.ErrLockAlreadyHeld)
	require.ErrorIs(t, wrappedErr, ports.ErrLockAlreadyHeld)

	joinedErr := errors.Join(
		ports.ErrLockAlreadyHeld,
		fmt.Errorf("additional: %w", ports.ErrLockAlreadyHeld),
	)
	require.ErrorIs(t, joinedErr, ports.ErrLockAlreadyHeld)
}

func TestLock_InterfaceDefinition(t *testing.T) {
	t.Parallel()

	var lock ports.Lock
	require.Nil(t, lock)
}

func TestRefreshableLock_InterfaceDefinition(t *testing.T) {
	t.Parallel()

	var refreshableLock ports.RefreshableLock
	require.Nil(t, refreshableLock)
}

func TestRefreshableLock_ExtendsLock(t *testing.T) {
	t.Parallel()

	var _ ports.Lock = ports.RefreshableLock(nil)
}

func TestLockManager_InterfaceDefinition(t *testing.T) {
	t.Parallel()

	var lockManager ports.LockManager
	require.Nil(t, lockManager)
}
