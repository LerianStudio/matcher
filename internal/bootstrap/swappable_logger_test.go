//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

type spyLogger struct {
	fields []libLog.Field
	groups []string
	msgs   []string
}

func (l *spyLogger) Log(_ context.Context, _ libLog.Level, msg string, _ ...libLog.Field) {
	l.msgs = append(l.msgs, msg)
}

func (l *spyLogger) With(fields ...libLog.Field) libLog.Logger {
	l.fields = append(l.fields, fields...)
	return l
}

func (l *spyLogger) WithGroup(name string) libLog.Logger {
	l.groups = append(l.groups, name)
	return l
}
func (l *spyLogger) Enabled(_ libLog.Level) bool  { return true }
func (l *spyLogger) Sync(_ context.Context) error { return nil }

func TestSwappableLogger_SwapAndCurrent(t *testing.T) {
	t.Parallel()

	first := &spyLogger{}
	second := &spyLogger{}
	logger := NewSwappableLogger(first)
	require.Same(t, first, logger.Current())

	logger.Swap(second)
	assert.Same(t, second, logger.Current())
}

func TestSwappableLogger_WithAndGroupSurviveSwap(t *testing.T) {
	t.Parallel()

	first := &spyLogger{}
	second := &spyLogger{}
	logger := NewSwappableLogger(first).WithGroup("runtime").With(libLog.String("component", "systemplane"))

	logger.(*SwappableLogger).Swap(second)
	logger.Log(context.Background(), libLog.LevelInfo, "hello")

	require.Len(t, second.msgs, 1)
	assert.Equal(t, "hello", second.msgs[0])
	assert.Len(t, second.groups, 1)
	assert.Len(t, second.fields, 1)
}
