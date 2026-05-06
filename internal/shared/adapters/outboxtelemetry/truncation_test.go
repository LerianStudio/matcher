// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package outboxtelemetry

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/opentelemetry/metrics"
)

// capturingLogger records every Log call so tests can assert that side
// effects (WARN line, fields) actually fire. Implements libLog.Logger.
type capturingLogger struct {
	mu      sync.Mutex
	entries []capturedEntry
}

type capturedEntry struct {
	level  libLog.Level
	msg    string
	fields []libLog.Field
}

func (l *capturingLogger) Log(_ context.Context, level libLog.Level, msg string, fields ...libLog.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, capturedEntry{level: level, msg: msg, fields: fields})
}

func (l *capturingLogger) With(_ ...libLog.Field) libLog.Logger { return l }
func (l *capturingLogger) WithGroup(_ string) libLog.Logger     { return l }
func (l *capturingLogger) Enabled(_ libLog.Level) bool          { return true }
func (l *capturingLogger) Sync(_ context.Context) error         { return nil }

func (l *capturingLogger) snapshot() []capturedEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]capturedEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// fieldByKey returns the value of the field with the given key, or nil if absent.
func fieldByKey(fields []libLog.Field, key string) any {
	for _, f := range fields {
		if f.Key == key {
			return f.Value
		}
	}
	return nil
}

// Below: nil-tracking-fallback regression tests. These guard the
// NopLogger fallback path inside RecordAuditChangesTruncated and
// RecordIDListTruncated — when ctx has no tracking, NewTrackingFromContext
// returns a NopLogger and a default MetricFactory. Both calls must remain
// panic-free in that mode (the publish path that calls these is on the
// critical broker write path; a panic here would crash an outbox dispatcher
// goroutine and stall delivery).

func TestRecordAuditChangesTruncated_NilTracking_FallsBackToNopLogger(t *testing.T) {
	// Plain context — no tracking injected. NewTrackingFromContext returns a
	// NopLogger and the default MetricFactory; calls must not panic.
	assert.NotPanics(t, func() {
		RecordAuditChangesTruncated(context.Background(), "audit_config", uuid.New(), 2048, 1024)
	})
}

func TestRecordIDListTruncated_NilTracking_FallsBackToNopLogger(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordIDListTruncated(context.Background(), "match_confirmed", uuid.New(), 10, 5, 1024)
	})
}

func TestEmitCounterWithNilFactoryDoesNothing(t *testing.T) {
	// Explicit nil factory path: emitCounter must short-circuit without
	// touching otel global state.
	assert.NotPanics(t, func() {
		emitCounter(context.Background(), nil, "match_confirmed")
	})
}

// Below: side-effect verification tests. These assert the package's whole
// purpose — emitting a structured WARN log + (best-effort) counter increment
// when payload truncation actually happens. Without these, the public
// functions could be replaced with `return` and the nil-fallback tests
// would still pass.

func TestRecordAuditChangesTruncated_WithCapturingLogger_EmitsWarnWithFields(t *testing.T) {
	logger := &capturingLogger{}
	ctx := libCommons.ContextWithLogger(context.Background(), logger)

	entityID := uuid.New()
	RecordAuditChangesTruncated(ctx, "audit_config", entityID, 4096, 2048)

	entries := logger.snapshot()
	require.Len(t, entries, 1, "expected exactly one log entry")

	entry := entries[0]
	assert.Equal(t, libLog.LevelWarn, entry.level)
	assert.Equal(t, auditTruncationNote, entry.msg,
		"WARN line message must match auditTruncationNote (operator-grep contract)")

	assert.Equal(t, "audit_config", fieldByKey(entry.fields, "entity_type"))
	assert.Equal(t, entityID.String(), fieldByKey(entry.fields, "entity_id"))
	assert.Equal(t, 4096, fieldByKey(entry.fields, "original_size_bytes"))
	assert.Equal(t, 2048, fieldByKey(entry.fields, "max_allowed_bytes"))
}

func TestRecordIDListTruncated_WithCapturingLogger_EmitsWarnWithFields(t *testing.T) {
	logger := &capturingLogger{}
	ctx := libCommons.ContextWithLogger(context.Background(), logger)

	entityID := uuid.New()
	RecordIDListTruncated(ctx, "match_confirmed", entityID, 100, 42, 1024)

	entries := logger.snapshot()
	require.Len(t, entries, 1, "expected exactly one log entry")

	entry := entries[0]
	assert.Equal(t, libLog.LevelWarn, entry.level)
	assert.Equal(t, idListTruncationNote, entry.msg,
		"WARN line message must match idListTruncationNote (operator-grep contract)")

	assert.Equal(t, "match_confirmed", fieldByKey(entry.fields, "entity_type"))
	assert.Equal(t, entityID.String(), fieldByKey(entry.fields, "entity_id"))
	assert.Equal(t, 100, fieldByKey(entry.fields, "original_count"))
	assert.Equal(t, 42, fieldByKey(entry.fields, "truncated_count"))
	assert.Equal(t, 1024, fieldByKey(entry.fields, "max_allowed_bytes"))
}

// TestRecordAuditChangesTruncated_WithRealFactory_NoPanic exercises emitCounter's
// non-nil factory path. Uses metrics.NewNopFactory() (lib-commons no-op meter)
// so the Counter() call is exercised end-to-end without a real OTel pipeline.
// Guards against a regression where a future change to the metric definition
// (Name, Unit) breaks Counter() construction at runtime.
func TestRecordAuditChangesTruncated_WithRealFactory_NoPanic(t *testing.T) {
	factory := metrics.NewNopFactory()
	require.NotNil(t, factory)

	logger := &libLog.NopLogger{}
	ctx := libCommons.ContextWithLogger(context.Background(), logger)
	ctx = libCommons.ContextWithMetricFactory(ctx, factory)

	assert.NotPanics(t, func() {
		RecordAuditChangesTruncated(ctx, "audit_config", uuid.New(), 4096, 2048)
	})
}

// TestRecordIDListTruncated_WithRealFactory_NoPanic — same contract as above
// for the ID list path.
func TestRecordIDListTruncated_WithRealFactory_NoPanic(t *testing.T) {
	factory := metrics.NewNopFactory()
	require.NotNil(t, factory)

	logger := &libLog.NopLogger{}
	ctx := libCommons.ContextWithLogger(context.Background(), logger)
	ctx = libCommons.ContextWithMetricFactory(ctx, factory)

	assert.NotPanics(t, func() {
		RecordIDListTruncated(ctx, "match_confirmed", uuid.New(), 100, 42, 1024)
	})
}

// TestTruncationNotesRemainStable locks the public/operator-visible note
// strings. Operators alert on these via log-search; changing the literals
// silently breaks alert rules. Keep this test even if the structured-field
// tests above subsume the WARN line assertions — those tests use the
// constants by reference, this test asserts the constant values themselves.
func TestTruncationNotesRemainStable(t *testing.T) {
	assert.Equal(t, "audit diff exceeded outbox payload cap; original not persisted", AuditChangesTruncationNote)
	assert.Equal(t, "id list exceeded outbox cap; event published with truncated ids", idListTruncationNote)
	assert.Equal(t, "outbox payload truncated due to broker cap", auditTruncationNote)
}
