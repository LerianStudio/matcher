// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/ports"
)

// mockBridgeHeartbeatReader is a test double for the BridgeHeartbeatReader
// interface. Kept minimal — the ports file is a pure contract and only
// needs compile-time conformance plus a handful of behaviour-via-stub
// tests to pin the contract's shape against accidental drift.
type mockBridgeHeartbeatReader struct {
	readFn func(ctx context.Context) (time.Time, error)
}

func (m *mockBridgeHeartbeatReader) ReadLastTickAt(ctx context.Context) (time.Time, error) {
	if m.readFn != nil {
		return m.readFn(ctx)
	}

	return time.Time{}, nil
}

// mockBridgeHeartbeatWriter is a test double for the BridgeHeartbeatWriter
// interface.
type mockBridgeHeartbeatWriter struct {
	writeFn func(ctx context.Context, now time.Time, ttl time.Duration) error
}

func (m *mockBridgeHeartbeatWriter) WriteLastTickAt(ctx context.Context, now time.Time, ttl time.Duration) error {
	if m.writeFn != nil {
		return m.writeFn(ctx, now, ttl)
	}

	return nil
}

// Compile-time interface compliance checks. These are the primary
// assertions for the ports file: if either signature drifts, the build
// fails here rather than at runtime on the first handler call.
var (
	_ ports.BridgeHeartbeatReader = (*mockBridgeHeartbeatReader)(nil)
	_ ports.BridgeHeartbeatWriter = (*mockBridgeHeartbeatWriter)(nil)
)

func TestBridgeHeartbeatReader_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var reader ports.BridgeHeartbeatReader = &mockBridgeHeartbeatReader{}
	assert.NotNil(t, reader)
}

func TestBridgeHeartbeatWriter_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var writer ports.BridgeHeartbeatWriter = &mockBridgeHeartbeatWriter{}
	assert.NotNil(t, writer)
}

// TestBridgeHeartbeatReader_ReadLastTickAt_PropagatesValue pins the
// documented "return value, nil on hit" contract — implementations must
// surface the stored timestamp verbatim.
func TestBridgeHeartbeatReader_ReadLastTickAt_PropagatesValue(t *testing.T) {
	t.Parallel()

	want := time.Date(2026, time.April, 17, 12, 0, 0, 0, time.UTC)

	reader := &mockBridgeHeartbeatReader{
		readFn: func(_ context.Context) (time.Time, error) {
			return want, nil
		},
	}

	got, err := reader.ReadLastTickAt(context.Background())
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// TestBridgeHeartbeatReader_ReadLastTickAt_ZeroTimeIsNotAnError pins the
// "missing key → (zero time, nil)" contract explicitly documented on the
// interface — callers must be able to treat an empty heartbeat as
// "unknown liveness" rather than an error.
func TestBridgeHeartbeatReader_ReadLastTickAt_ZeroTimeIsNotAnError(t *testing.T) {
	t.Parallel()

	reader := &mockBridgeHeartbeatReader{
		readFn: func(_ context.Context) (time.Time, error) {
			return time.Time{}, nil
		},
	}

	got, err := reader.ReadLastTickAt(context.Background())
	require.NoError(t, err)
	assert.True(t, got.IsZero(), "missing-key contract: zero time must be reported without error")
}

// TestBridgeHeartbeatReader_ReadLastTickAt_InfrastructureErrorsSurface
// pins the "genuine infra failure → error" contract. Callers differentiate
// empty-state (zero, nil) from broken-infra (zero, err) on this exact
// signature, so implementations MUST propagate errors.
func TestBridgeHeartbeatReader_ReadLastTickAt_InfrastructureErrorsSurface(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("redis unreachable")
	reader := &mockBridgeHeartbeatReader{
		readFn: func(_ context.Context) (time.Time, error) {
			return time.Time{}, sentinel
		},
	}

	_, err := reader.ReadLastTickAt(context.Background())
	require.ErrorIs(t, err, sentinel)
}

// TestBridgeHeartbeatWriter_WriteLastTickAt_PassesArguments pins the
// timestamp/TTL argument order so a swapped signature fails loudly.
func TestBridgeHeartbeatWriter_WriteLastTickAt_PassesArguments(t *testing.T) {
	t.Parallel()

	var (
		gotNow time.Time
		gotTTL time.Duration
	)

	writer := &mockBridgeHeartbeatWriter{
		writeFn: func(_ context.Context, now time.Time, ttl time.Duration) error {
			gotNow = now
			gotTTL = ttl

			return nil
		},
	}

	wantNow := time.Date(2026, time.April, 17, 12, 0, 0, 0, time.UTC)
	wantTTL := 90 * time.Second

	require.NoError(t, writer.WriteLastTickAt(context.Background(), wantNow, wantTTL))
	assert.Equal(t, wantNow, gotNow)
	assert.Equal(t, wantTTL, gotTTL)
}

// TestBridgeHeartbeatWriter_WriteLastTickAt_ErrorsPropagate pins the
// write-side error contract. The documented behaviour is that errors are
// non-fatal for the worker but MUST be visible to callers so they can log
// and move on.
func TestBridgeHeartbeatWriter_WriteLastTickAt_ErrorsPropagate(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("redis set failed")
	writer := &mockBridgeHeartbeatWriter{
		writeFn: func(_ context.Context, _ time.Time, _ time.Duration) error {
			return sentinel
		},
	}

	err := writer.WriteLastTickAt(context.Background(), time.Now().UTC(), time.Minute)
	require.ErrorIs(t, err, sentinel)
}
