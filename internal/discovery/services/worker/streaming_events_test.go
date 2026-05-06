// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-streaming/streamingtest"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

func TestFormatBridgeTimeUsesUTCAndRFC3339Nano(t *testing.T) {
	input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

	formatted := formatBridgeTime(input)

	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	require.NoError(t, err)
	assert.Equal(t, input.UTC(), parsed)
}

// TestEmitExtractionBridgeFailedAttachesSanitizedErrorCodeAndTenantContext
// verifies that emitExtractionBridgeFailed, on a bridge-terminal failure,
// emits exactly one streaming event with:
//
//   - the stable, catalog-aligned bridge_last_error_code constant
//     (no leak of the producer-side error message into a code field),
//   - the tenant ID from the tenant-manager context,
//   - timestamps formatted via emission.FormatTime (RFC3339Nano UTC),
//   - the catalog-required identifying fields (extraction_request_id,
//     connection_id, fetcher_job_id, status, attempts).
//
// The bridge_failed catalog policy is fallback_on_circuit_open, so no
// outbox tx is required for this emission path.
func TestEmitExtractionBridgeFailedAttachesSanitizedErrorCodeAndTenantContext(t *testing.T) {
	mockEmitter := streamingtest.NewMockEmitter()
	worker := &BridgeWorker{
		streamEmitter: mockEmitter,
		tracer:        noop.NewTracerProvider().Tracer("test"),
	}

	tenantID := "018f4f95-0000-7000-8000-000000000001"
	ctx := tmcore.ContextWithTenantID(context.Background(), tenantID)

	failedAt := time.Date(2026, time.April, 1, 12, 30, 45, 123456789, time.UTC)
	extractionID := uuid.MustParse("018f4f95-1111-7000-8000-000000000002")
	connectionID := uuid.MustParse("018f4f95-2222-7000-8000-000000000003")

	extraction := &entities.ExtractionRequest{
		ID:                     extractionID,
		ConnectionID:           connectionID,
		FetcherJobID:           "fetcher-job-abc",
		Status:                 vo.ExtractionStatusComplete,
		BridgeAttempts:         5,
		BridgeLastError:        vo.BridgeErrorClassIntegrityFailed,
		BridgeLastErrorMessage: "internal: object hash mismatch xyz/abc — DO NOT LEAK",
		BridgeFailedAt:         failedAt,
	}

	worker.emitExtractionBridgeFailed(ctx, extraction)

	requests := mockEmitter.Requests()
	require.Len(t, requests, 1, "expected exactly one streaming emission")
	streamingtest.AssertEventEmitted(t, mockEmitter, "extraction_request.bridge_failed")
	streamingtest.AssertTenantID(t, mockEmitter, tenantID)

	request := requests[0]
	assert.Equal(t, "extraction_request.bridge_failed", request.DefinitionKey)
	assert.Equal(t, tenantID, request.TenantID)
	assert.Equal(t, extractionID.String(), request.Subject,
		"subject is the extraction request ID for downstream routing")

	var payload map[string]any
	require.NoError(t, json.Unmarshal(request.Payload, &payload))

	// Catalog-aligned identifying fields.
	assert.Equal(t, extractionID.String(), payload["extraction_request_id"])
	assert.Equal(t, connectionID.String(), payload["connection_id"])
	assert.Equal(t, "fetcher-job-abc", payload["fetcher_job_id"])
	assert.Equal(t, "COMPLETE", payload["status"])
	assert.Equal(t, float64(5), payload["bridge_attempts"],
		"json.Unmarshal decodes integer fields as float64")

	// The stable error code constant — NOT the producer's free-text message.
	assert.Equal(t, ExtractionBridgeFailedCode, payload["bridge_last_error_code"])
	assert.Equal(t, "EXTRACTION_BRIDGE_FAILED", payload["bridge_last_error_code"],
		"the constant is locked to the catalog-defined external code")

	// bridge_last_error carries the structured class (NOT the free-text
	// message), so the operator's narrative stays out of the wire format.
	assert.Equal(t, string(vo.BridgeErrorClassIntegrityFailed), payload["bridge_last_error"])
	assert.NotContains(t, payload, "bridge_last_error_message",
		"unsanitized producer message must not be on the wire")

	// Timestamps formatted via emission.FormatTime — RFC3339Nano in UTC.
	assert.Equal(t, emission.FormatTime(failedAt), payload["bridge_failed_at"])
	parsed, err := time.Parse(time.RFC3339Nano, payload["bridge_failed_at"].(string))
	require.NoError(t, err)
	assert.Equal(t, failedAt.UTC(), parsed)
}

// TestEmitExtractionBridgeFailedNoOpForNilExtraction verifies the early
// return in emitExtractionBridgeFailed: a nil extraction must not produce
// any emission (defensive symmetry with the worker's pollCycle, which can
// hand a nil row through if a downstream load returned no result).
func TestEmitExtractionBridgeFailedNoOpForNilExtraction(t *testing.T) {
	mockEmitter := streamingtest.NewMockEmitter()
	worker := &BridgeWorker{
		streamEmitter: mockEmitter,
		tracer:        noop.NewTracerProvider().Tracer("test"),
	}
	ctx := tmcore.ContextWithTenantID(context.Background(), "018f4f95-0000-7000-8000-000000000001")

	worker.emitExtractionBridgeFailed(ctx, nil)

	streamingtest.AssertNoEvents(t, mockEmitter)
}
