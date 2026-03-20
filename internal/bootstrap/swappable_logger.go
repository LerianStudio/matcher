// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"sync"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

// SwappableLogger is a stable logger handle whose underlying implementation can
// be replaced at runtime without recreating existing services or handlers.
type SwappableLogger struct {
	state  *swappableLoggerState
	fields []libLog.Field
	groups []string
}

type swappableLoggerState struct {
	mu      sync.RWMutex
	current libLog.Logger
}

// NewSwappableLogger creates a logger proxy with the given initial logger.
func NewSwappableLogger(initial libLog.Logger) *SwappableLogger {
	state := &swappableLoggerState{}
	state.swap(initial)

	return &SwappableLogger{state: state}
}

// Swap replaces the underlying logger implementation.
func (logger *SwappableLogger) Swap(next libLog.Logger) {
	if logger == nil || logger.state == nil {
		return
	}

	logger.state.swap(next)
}

// Current returns the currently active logger implementation.
func (logger *SwappableLogger) Current() libLog.Logger {
	if logger == nil || logger.state == nil {
		return &libLog.NopLogger{}
	}

	return logger.state.currentLogger()
}

func (logger *SwappableLogger) Log(ctx context.Context, level libLog.Level, msg string, fields ...libLog.Field) {
	logger.effective().Log(ctx, level, msg, fields...)
}

func (logger *SwappableLogger) With(fields ...libLog.Field) libLog.Logger {
	if logger == nil {
		return &libLog.NopLogger{}
	}

	combined := make([]libLog.Field, 0, len(logger.fields)+len(fields))
	combined = append(combined, logger.fields...)
	combined = append(combined, fields...)

	return &SwappableLogger{
		state:  logger.state,
		fields: combined,
		groups: append([]string(nil), logger.groups...),
	}
}

func (logger *SwappableLogger) WithGroup(name string) libLog.Logger {
	if logger == nil {
		return &libLog.NopLogger{}
	}

	groups := append(append([]string(nil), logger.groups...), name)

	return &SwappableLogger{
		state:  logger.state,
		fields: append([]libLog.Field(nil), logger.fields...),
		groups: groups,
	}
}

func (logger *SwappableLogger) Enabled(level libLog.Level) bool {
	return logger.effective().Enabled(level)
}

func (logger *SwappableLogger) Sync(ctx context.Context) error {
	return logger.effective().Sync(ctx)
}

func (logger *SwappableLogger) effective() libLog.Logger {
	if logger == nil || logger.state == nil {
		return &libLog.NopLogger{}
	}

	effective := logger.state.currentLogger()
	for _, group := range logger.groups {
		effective = effective.WithGroup(group)
	}
	if len(logger.fields) > 0 {
		effective = effective.With(logger.fields...)
	}

	return effective
}

func (state *swappableLoggerState) swap(next libLog.Logger) {
	state.mu.Lock()
	defer state.mu.Unlock()

	if isNilInterface(next) {
		state.current = &libLog.NopLogger{}
		return
	}

	state.current = next
}

func (state *swappableLoggerState) currentLogger() libLog.Logger {
	state.mu.RLock()
	defer state.mu.RUnlock()

	if isNilInterface(state.current) {
		return &libLog.NopLogger{}
	}

	return state.current
}
