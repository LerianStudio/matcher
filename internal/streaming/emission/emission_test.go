// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package emission

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	streaming "github.com/LerianStudio/lib-streaming/v2"
	"github.com/LerianStudio/lib-streaming/v2/streamingtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type typedNilEmitter struct{}

func (*typedNilEmitter) Emit(context.Context, streaming.EmitRequest) error { return nil }
func (*typedNilEmitter) Close() error                                      { return nil }
func (*typedNilEmitter) Healthy(context.Context) error                     { return nil }

type contextCapturingEmitter struct {
	ctx     context.Context
	request streaming.EmitRequest
}

func (emitter *contextCapturingEmitter) Emit(ctx context.Context, request streaming.EmitRequest) error {
	emitter.ctx = ctx
	emitter.request = request

	return nil
}

func (*contextCapturingEmitter) Close() error                  { return nil }
func (*contextCapturingEmitter) Healthy(context.Context) error { return nil }

func TestEmitUsesTenantManagerContextAndBuildsCatalogRequest(t *testing.T) {
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)
	emitter := streamingtest.NewMockEmitter()

	err := Emit(ctx, emitter, "reconciliation_context.created", "ctx-123", map[string]any{
		"context_id": "ctx-123",
		"status":     "ACTIVE",
	})
	require.NoError(t, err)

	requests := emitter.Requests()
	require.Len(t, requests, 1)
	require.Equal(t, "reconciliation_context.created", requests[0].DefinitionKey)
	require.Equal(t, tenantID, requests[0].TenantID)
	require.Equal(t, "ctx-123", requests[0].Subject)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(requests[0].Payload, &payload))
	require.Equal(t, "ctx-123", payload["context_id"])
	require.Equal(t, "ACTIVE", payload["status"])
}

func TestEmitRejectsContextsWithoutTenantManagerTenant(t *testing.T) {
	emitter := streamingtest.NewMockEmitter()

	err := Emit(context.Background(), emitter, "reconciliation_context.created", "ctx-123", map[string]any{
		"context_id": "ctx-123",
	})
	require.ErrorIs(t, err, ErrTenantIDMissing)
	streamingtest.AssertNoEvents(t, emitter)
}

func TestEmitRequiresOutboxTransactionWhenMarkedCritical(t *testing.T) {
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)
	emitter := streamingtest.NewMockEmitter()

	err := Emit(ctx, emitter, "audit_log.created", "audit-123", map[string]any{
		"audit_log_id": "audit-123",
	}, RequireOutboxTx())

	require.ErrorIs(t, err, ErrCriticalOutboxTxRequired)
	streamingtest.AssertNoEvents(t, emitter)
}

func TestEmitAttachesAmbientTransactionContextForCriticalEvents(t *testing.T) {
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)
	emitter := &contextCapturingEmitter{}
	tx := &sql.Tx{}

	err := Emit(ctx, emitter, "audit_log.created", "audit-123", map[string]any{
		"audit_log_id": "audit-123",
	}, RequireOutboxTx(), WithOutboxTx(tx))

	require.NoError(t, err)
	require.NotNil(t, emitter.ctx)
	assert.NotEqual(t, ctx, emitter.ctx)
	assert.Equal(t, "audit_log.created", emitter.request.DefinitionKey)
	assert.Equal(t, tenantID, emitter.request.TenantID)
}

func TestEmitRequiresOutboxTransactionBeforeNilEmitterNoop(t *testing.T) {
	ctx := tmcore.ContextWithTenantID(context.Background(), "018f4f95-0000-7000-8000-000000000001")

	err := Emit(ctx, nil, "audit_log.created", "audit-123", map[string]any{
		"audit_log_id": "audit-123",
	}, RequireOutboxTx())

	require.ErrorIs(t, err, ErrCriticalOutboxTxRequired)
}

func TestEmitCriticalNilEmitterWithTransactionFailsClosed(t *testing.T) {
	ctx := tmcore.ContextWithTenantID(context.Background(), "018f4f95-0000-7000-8000-000000000001")

	err := Emit(ctx, nil, "audit_log.created", "audit-123", map[string]any{
		"audit_log_id": "audit-123",
	}, RequireOutboxTx(), WithOutboxTx(&sql.Tx{}))

	require.ErrorIs(t, err, ErrCriticalOutboxTxRequired)
}

func TestEmitCriticalTypedNilEmitterWithTransactionFailsClosed(t *testing.T) {
	ctx := tmcore.ContextWithTenantID(context.Background(), "018f4f95-0000-7000-8000-000000000001")
	var emitter *typedNilEmitter

	err := Emit(ctx, emitter, "audit_log.created", "audit-123", map[string]any{
		"audit_log_id": "audit-123",
	}, RequireOutboxTx(), WithOutboxTx(&sql.Tx{}))

	require.ErrorIs(t, err, ErrCriticalOutboxTxRequired)
}

func TestIsNilEmitterDetectsTypedNilEmitter(t *testing.T) {
	var emitter *typedNilEmitter

	require.True(t, IsNilEmitter(emitter))
	require.False(t, IsNilEmitter(streamingtest.NewMockEmitter()))
}

// noopMarkerEmitter exercises the IsNoop() interface bypass branch in IsNilEmitter.
type noopMarkerEmitter struct {
	noop bool
}

func (e *noopMarkerEmitter) Emit(context.Context, streaming.EmitRequest) error { return nil }
func (*noopMarkerEmitter) Close() error                                        { return nil }
func (*noopMarkerEmitter) Healthy(context.Context) error                       { return nil }
func (e *noopMarkerEmitter) IsNoop() bool                                      { return e.noop }

func TestIsNilEmitterCoversAllThreeBranches(t *testing.T) {
	t.Run("bare nil interface returns true", func(t *testing.T) {
		require.True(t, IsNilEmitter(nil))
	})

	t.Run("IsNoop returns true short-circuits before reflect", func(t *testing.T) {
		require.True(t, IsNilEmitter(&noopMarkerEmitter{noop: true}))
	})

	t.Run("IsNoop returns false treats emitter as live", func(t *testing.T) {
		require.False(t, IsNilEmitter(&noopMarkerEmitter{noop: false}))
	})

	t.Run("typed nil pointer falls through to reflect branch", func(t *testing.T) {
		var emitter *typedNilEmitter
		require.True(t, IsNilEmitter(emitter))
	})

	t.Run("real emitter without IsNoop is live", func(t *testing.T) {
		require.False(t, IsNilEmitter(streamingtest.NewMockEmitter()))
	})
}

func TestFormatTimeUsesUTCAndCoercesZero(t *testing.T) {
	t.Run("non-zero time is normalized to UTC RFC3339Nano", func(t *testing.T) {
		input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

		formatted := FormatTime(input)

		parsed, err := time.Parse(time.RFC3339Nano, formatted)
		require.NoError(t, err)
		require.Equal(t, input.UTC(), parsed)
	})

	t.Run("zero time is coerced to a real instant", func(t *testing.T) {
		formatted := FormatTime(time.Time{})

		parsed, err := time.Parse(time.RFC3339Nano, formatted)
		require.NoError(t, err)
		require.False(t, parsed.IsZero())
	})
}

func TestEmitAcceptsTypedStructPayloads(t *testing.T) {
	type matchRunCompletedPayload struct {
		MatchRunID string `json:"match_run_id"`
		Status     string `json:"status"`
	}

	tenantID := "018f4f95-0000-7000-8000-000000000001"
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)
	emitter := streamingtest.NewMockEmitter()

	err := Emit(ctx, emitter, "match_run.completed", "run-1", matchRunCompletedPayload{
		MatchRunID: "run-1",
		Status:     "COMPLETED",
	})
	require.NoError(t, err)

	requests := emitter.Requests()
	require.Len(t, requests, 1)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(requests[0].Payload, &decoded))
	require.Equal(t, "run-1", decoded["match_run_id"])
	require.Equal(t, "COMPLETED", decoded["status"])
}

func TestAddTenantIDCopiesPayloadAndUsesTenantManagerContext(t *testing.T) {
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)
	original := map[string]any{"context_id": "ctx-123"}

	withTenant, err := AddTenantID(ctx, original)
	require.NoError(t, err)

	require.Equal(t, tenantID, withTenant["tenant_id"])
	require.Equal(t, "ctx-123", withTenant["context_id"])
	require.NotContains(t, original, "tenant_id")
}
