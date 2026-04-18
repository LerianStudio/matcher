package testutil

import (
	"context"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// TestLogger is a mock logger that tracks level usage and captured messages.
// It implements libLog.Logger (v2 5-method interface) and can be used in tests across the codebase.
type TestLogger struct {
	ErrorCalled bool
	WarnCalled  bool
	InfoCalled  bool
	DebugCalled bool
	Messages    []string
}

// Log dispatches a log event. It sets tracking flags based on the level.
func (tl *TestLogger) Log(_ context.Context, level libLog.Level, msg string, _ ...libLog.Field) {
	tl.Messages = append(tl.Messages, msg)

	switch level {
	case libLog.LevelError:
		tl.ErrorCalled = true
	case libLog.LevelWarn:
		tl.WarnCalled = true
	case libLog.LevelInfo:
		tl.InfoCalled = true
	case libLog.LevelDebug:
		tl.DebugCalled = true
	case libLog.LevelUnknown:
		// No tracking needed.
	}
}

// With returns the logger with additional fields (returns self for tests).
//
//nolint:ireturn
func (tl *TestLogger) With(_ ...libLog.Field) libLog.Logger { return tl }

// WithGroup returns the logger with a named group (returns self for tests).
//
//nolint:ireturn
func (tl *TestLogger) WithGroup(_ string) libLog.Logger { return tl }

// Enabled reports whether the logger emits entries at the given level (always true for tests).
func (tl *TestLogger) Enabled(_ libLog.Level) bool { return true }

// Sync flushes any buffered log entries (no-op for tests).
func (tl *TestLogger) Sync(_ context.Context) error { return nil }
