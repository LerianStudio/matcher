//go:build unit

package testutil

import (
	"context"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time interface check.
var _ libLog.Logger = (*TestLogger)(nil)

func TestTestLogger_SatisfiesLoggerInterface(t *testing.T) {
	t.Parallel()

	var logger libLog.Logger = &TestLogger{}
	require.NotNil(t, logger)
}

func TestTestLogger_ErrorCalled(t *testing.T) {
	t.Parallel()

	mock := &TestLogger{}
	assert.False(t, mock.ErrorCalled)

	mock.Log(context.Background(), libLog.LevelError, "test")
	assert.True(t, mock.ErrorCalled)
}

func TestTestLogger_WarnCalled(t *testing.T) {
	t.Parallel()

	mock := &TestLogger{}
	assert.False(t, mock.WarnCalled)

	mock.Log(context.Background(), libLog.LevelWarn, "test warning")
	assert.True(t, mock.WarnCalled)
}

func TestTestLogger_InfoSetsInfoFlagOnly(t *testing.T) {
	t.Parallel()

	mock := &TestLogger{}
	mock.Log(context.Background(), libLog.LevelInfo, "test info")
	assert.False(t, mock.ErrorCalled)
	assert.False(t, mock.WarnCalled)
	assert.True(t, mock.InfoCalled)
	assert.False(t, mock.DebugCalled)
	require.Len(t, mock.Messages, 1)
	assert.Equal(t, "test info", mock.Messages[0])
}

func TestTestLogger_DebugSetsDebugFlagOnly(t *testing.T) {
	t.Parallel()

	mock := &TestLogger{}
	mock.Log(context.Background(), libLog.LevelDebug, "test debug")
	assert.False(t, mock.ErrorCalled)
	assert.False(t, mock.WarnCalled)
	assert.False(t, mock.InfoCalled)
	assert.True(t, mock.DebugCalled)
	require.Len(t, mock.Messages, 1)
	assert.Equal(t, "test debug", mock.Messages[0])
}

func TestTestLogger_WithReturnsSelf(t *testing.T) {
	t.Parallel()

	mock := &TestLogger{}
	result := mock.With(libLog.String("key", "value"))
	assert.Equal(t, mock, result)
}

func TestTestLogger_WithGroupReturnsSelf(t *testing.T) {
	t.Parallel()

	mock := &TestLogger{}
	result := mock.WithGroup("group")
	assert.Equal(t, mock, result)
}

func TestTestLogger_SyncReturnsNil(t *testing.T) {
	t.Parallel()

	mock := &TestLogger{}
	err := mock.Sync(context.Background())
	assert.NoError(t, err)
}

func TestTestLogger_EnabledReturnsTrue(t *testing.T) {
	t.Parallel()

	mock := &TestLogger{}
	assert.True(t, mock.Enabled(libLog.LevelError))
	assert.True(t, mock.Enabled(libLog.LevelDebug))
}
