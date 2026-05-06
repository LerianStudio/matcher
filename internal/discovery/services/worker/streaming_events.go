// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

// ExtractionBridgeFailedCode is the stable external error code emitted on bridge
// worker failures. The producer-side error message is intentionally discarded
// at this boundary to avoid leaking internal text into the catalog-defined schema.
const ExtractionBridgeFailedCode = "EXTRACTION_BRIDGE_FAILED"

func (worker *BridgeWorker) emitExtractionBridgeFailed(ctx context.Context, extraction *entities.ExtractionRequest) {
	if extraction == nil {
		return
	}

	payload := map[string]any{
		"extraction_request_id":  extraction.ID.String(),
		"connection_id":          extraction.ConnectionID.String(),
		"fetcher_job_id":         extraction.FetcherJobID,
		"status":                 extraction.Status.String(),
		"bridge_last_error":      string(extraction.BridgeLastError),
		"bridge_last_error_code": ExtractionBridgeFailedCode,
		"bridge_attempts":        extraction.BridgeAttempts,
		"bridge_failed_at":       formatBridgeTime(extraction.BridgeFailedAt),
	}

	if err := emission.Emit(ctx, worker.streamEmitter, "extraction_request.bridge_failed", extraction.ID.String(), payload); err != nil {
		_, span := worker.tracer.Start(ctx, "discovery.bridge.emit_bridge_failed")
		libOpentelemetry.HandleSpanError(span, "failed to emit streaming event extraction_request.bridge_failed", err)
		span.End()
	}
}

func (dw *DiscoveryWorker) emitFetcherConnectionSynced(ctx context.Context, span trace.Span, conn *entities.FetcherConnection) {
	if conn == nil {
		return
	}

	dw.emitDiscoveryPayload(ctx, span, "fetcher_connection.synced", conn.ID.String(), map[string]any{
		"connection_id":         conn.ID.String(),
		"fetcher_connection_id": conn.FetcherConnID,
		"config_name":           conn.ConfigName,
		"database_type":         conn.DatabaseType,
		"status":                string(conn.Status),
		"schema_discovered":     conn.SchemaDiscovered,
		"last_seen_at":          formatDiscoveryWorkerTime(conn.LastSeenAt),
		"updated_at":            formatDiscoveryWorkerTime(conn.UpdatedAt),
	})
}

func (dw *DiscoveryWorker) emitFetcherConnectionUnreachable(ctx context.Context, span trace.Span, conn *entities.FetcherConnection, previousStatus string) {
	if conn == nil {
		return
	}

	dw.emitDiscoveryPayload(ctx, span, "fetcher_connection.unreachable", conn.ID.String(), map[string]any{
		"connection_id":         conn.ID.String(),
		"fetcher_connection_id": conn.FetcherConnID,
		"previous_status":       previousStatus,
		"status":                string(conn.Status),
		"schema_discovered":     false,
		"updated_at":            formatDiscoveryWorkerTime(conn.UpdatedAt),
	})
}

func (dw *DiscoveryWorker) emitDiscoveryPayload(ctx context.Context, span trace.Span, definitionKey, subject string, payload map[string]any) {
	if err := emission.Emit(ctx, dw.streamEmitter, definitionKey, subject, payload); err != nil && span != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
	}
}

// formatBridgeTime delegates to emission.FormatTime; preserved as a thin wrapper
// for backward compatibility with existing unit tests.
func formatBridgeTime(value time.Time) string {
	return emission.FormatTime(value)
}

// formatDiscoveryWorkerTime delegates to emission.FormatTime; preserved as a
// thin wrapper for symmetry with formatBridgeTime.
func formatDiscoveryWorkerTime(value time.Time) string {
	return emission.FormatTime(value)
}
