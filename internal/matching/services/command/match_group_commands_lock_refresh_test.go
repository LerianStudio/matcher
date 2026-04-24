// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/ports"
)

type failingRefreshLock struct {
	released atomic.Bool
}

func (l *failingRefreshLock) Release(_ context.Context) error {
	l.released.Store(true)
	return nil
}

var errRefreshFailed = errors.New("refresh failed")

func (l *failingRefreshLock) Refresh(_ context.Context, _ time.Duration) error {
	return errRefreshFailed
}

var _ ports.RefreshableLock = (*failingRefreshLock)(nil)

func TestEnsureLockFresh_RefreshFailure(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	var refreshFailed atomic.Bool

	lock := &failingRefreshLock{}

	err := uc.ensureLockFresh(context.Background(), nil, lock, &refreshFailed)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
	require.True(t, refreshFailed.Load())
}

func TestEnsureLockFresh_AlreadyFailed(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	var refreshFailed atomic.Bool

	refreshFailed.Store(true)

	lock := &failingRefreshLock{}

	err := uc.ensureLockFresh(context.Background(), nil, lock, &refreshFailed)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
}

type successfulRefreshLock struct {
	refreshCount atomic.Int32
}

func (l *successfulRefreshLock) Release(_ context.Context) error {
	return nil
}

func (l *successfulRefreshLock) Refresh(_ context.Context, _ time.Duration) error {
	l.refreshCount.Add(1)
	return nil
}

var _ ports.RefreshableLock = (*successfulRefreshLock)(nil)

func TestEnsureLockFresh_Success(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	var refreshFailed atomic.Bool

	lock := &successfulRefreshLock{}

	err := uc.ensureLockFresh(context.Background(), nil, lock, &refreshFailed)
	require.NoError(t, err)
	require.False(t, refreshFailed.Load())
	require.Equal(t, int32(1), lock.refreshCount.Load())
}

type nonRefreshableLock struct{}

func (l *nonRefreshableLock) Release(_ context.Context) error {
	return nil
}

var _ ports.Lock = (*nonRefreshableLock)(nil)

func TestEnsureLockFresh_NonRefreshableLock(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	var refreshFailed atomic.Bool

	lock := &nonRefreshableLock{}

	err := uc.ensureLockFresh(context.Background(), nil, lock, &refreshFailed)
	require.NoError(t, err)
	require.False(t, refreshFailed.Load())
}
