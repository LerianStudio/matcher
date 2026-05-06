// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package emission provides tenant-aware helpers for emitting Matcher's streaming events.
package emission

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	streaming "github.com/LerianStudio/lib-streaming"
)

// Emission errors.
var (
	ErrTenantIDMissing          = errors.New("streaming tenant ID missing from tenant-manager context")
	ErrCriticalOutboxTxRequired = errors.New("critical streaming emission requires an outbox transaction")
)

// payloadBufferPool reuses *bytes.Buffer instances for JSON encoding hot paths.
// Reduces per-emit allocations on high-frequency emitters (transaction.matched,
// match_run.completed, ingestion.completed). The pool is package-private and
// safe for concurrent use.
var payloadBufferPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

type emitOptions struct {
	outboxTx        *sql.Tx
	requireOutboxTx bool
}

// Option configures a single streaming emission call.
type Option func(*emitOptions)

// WithOutboxTx attaches the ambient SQL transaction used by CRITICAL outbox emissions.
func WithOutboxTx(tx *sql.Tx) Option {
	return func(options *emitOptions) {
		options.outboxTx = tx
	}
}

// RequireOutboxTx makes missing ambient transaction a caller error for CRITICAL events.
func RequireOutboxTx() Option {
	return func(options *emitOptions) {
		options.requireOutboxTx = true
	}
}

// TenantIDFromContext returns the canonical streaming tenant ID from tenant-manager context.
func TenantIDFromContext(ctx context.Context) (string, error) {
	tenantID := tmcore.GetTenantIDContext(ctx)
	if tenantID == "" {
		return "", fmt.Errorf("%w: %w", ErrTenantIDMissing, streaming.ErrMissingTenantID)
	}

	return tenantID, nil
}

// AddTenantID copies a map payload and adds tenant_id from tenant-manager context.
func AddTenantID(ctx context.Context, payload map[string]any) (map[string]any, error) {
	tenantID, err := TenantIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	withTenant := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		withTenant[key] = value
	}

	withTenant["tenant_id"] = tenantID

	return withTenant, nil
}

// FormatTime renders timestamps as UTC RFC3339Nano. Zero times are coerced to
// time.Now().UTC() so consumers always see a real instant. This is the single
// canonical formatter for all streaming payload time fields across matcher.
//
// Per-context streaming_events.go files keep thin wrappers that delegate here
// for backward compatibility with their unit tests; new code should call this
// directly.
func FormatTime(t time.Time) string {
	if t.IsZero() {
		t = time.Now().UTC()
	}

	return t.UTC().Format(time.RFC3339Nano)
}

// IsNilEmitter reports whether emitter is nil, including typed-nil interface values
// and emitters that explicitly identify themselves as no-ops via the IsNoop() bool
// interface.
//
// Why this exists despite lib-streaming.NewNoopEmitter() being the canonical fail-safe:
//
//  1. **Typed-nil interface trap.** A common Go bug pattern: a constructor returns
//     `(*FakeEmitter)(nil)` typed as the streaming.Emitter interface. The interface
//     itself is non-nil (it carries a type descriptor), so a bare `if emitter == nil`
//     check yields false and the caller proceeds to invoke methods on a nil pointer.
//     Test mocks and partial wiring during tests are the most common sources.
//     The reflect.Value.IsNil() branch defends against this.
//
//  2. **Self-identifying no-ops.** When a wrapper around a real emitter wants to
//     short-circuit emission cheaply (e.g., after Close), it can implement
//     `IsNoop() bool { return true }` and we skip both the reflect call and
//     downstream marshalling. Vendored types like lib-streaming's NoopEmitter
//     cannot have methods added, so this branch is opt-in for matcher-owned
//     wrappers and any future upstream additions.
//
// The function is intentionally permissive: any emitter that survives all three
// checks is treated as live. Order matters — bare nil is cheapest, IsNoop() is
// the explicit-intent path, reflect is the safety net.
func IsNilEmitter(emitter streaming.Emitter) bool {
	if emitter == nil {
		return true
	}

	if marker, ok := emitter.(interface{ IsNoop() bool }); ok {
		return marker.IsNoop()
	}

	value := reflect.ValueOf(emitter)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Emit marshals payload and emits the catalog-keyed event using the tenant ID from context.
//
// Performance note: callers on hot paths (transaction.matched, match_run.completed,
// ingestion.completed) should pass typed structs rather than map[string]any to avoid
// the reflection-heavy map encoding path. The internal encoder uses a sync.Pool of
// bytes.Buffer either way, but typed structs allow the json package to skip the
// interface-resolution dance per field.
func Emit(ctx context.Context, emitter streaming.Emitter, definitionKey, subject string, payload any, options ...Option) error {
	resolvedOptions := emitOptions{}

	for _, option := range options {
		if option != nil {
			option(&resolvedOptions)
		}
	}

	if resolvedOptions.requireOutboxTx && resolvedOptions.outboxTx == nil {
		return ErrCriticalOutboxTxRequired
	}

	if IsNilEmitter(emitter) {
		if resolvedOptions.requireOutboxTx {
			return ErrCriticalOutboxTxRequired
		}

		return nil
	}

	tenantID, err := TenantIDFromContext(ctx)
	if err != nil {
		return err
	}

	if resolvedOptions.outboxTx != nil {
		ctx = streaming.WithOutboxTx(ctx, resolvedOptions.outboxTx)
	}

	payloadBytes, err := marshalPayload(payload)
	if err != nil {
		return fmt.Errorf("marshal streaming payload for %s: %w", definitionKey, err)
	}

	request := streaming.EmitRequest{
		DefinitionKey: definitionKey,
		TenantID:      tenantID,
		Subject:       subject,
		Payload:       payloadBytes,
	}

	if err := emitter.Emit(ctx, request); err != nil {
		return fmt.Errorf("emit streaming event %s: %w", definitionKey, err)
	}

	return nil
}

// marshalPayload encodes payload using a pooled bytes.Buffer and json.Encoder
// to avoid repeated allocator pressure on hot emit paths. The returned slice
// is a copy: the buffer is returned to the pool before this function returns,
// so the caller may freely retain or mutate the result.
//
// json.Encoder appends a trailing newline; we trim it because the lib-streaming
// EmitRequest.Payload is a JSON document, not an NDJSON line.
func marshalPayload(payload any) ([]byte, error) {
	buf, ok := payloadBufferPool.Get().(*bytes.Buffer)
	if !ok {
		// Defensive: pool returned an unexpected type. Fall back to a fresh buffer
		// rather than panicking — emission must remain best-effort on warm paths.
		buf = &bytes.Buffer{}
	}

	defer func() {
		buf.Reset()
		payloadBufferPool.Put(buf)
	}()

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(payload); err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}

	encoded := buf.Bytes()
	// json.Encoder.Encode always appends '\n'; strip it for parity with json.Marshal.
	if length := len(encoded); length > 0 && encoded[length-1] == '\n' {
		encoded = encoded[:length-1]
	}

	out := make([]byte, len(encoded))
	copy(out, encoded)

	return out, nil
}
