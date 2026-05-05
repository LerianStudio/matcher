// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

// ExtractionRequestFailedCode is the stable external error code for failed
// extraction request lifecycle events. The producer-side error is intentionally
// discarded at this boundary to avoid leaking internal error text into the
// catalog-defined external schema.
const ExtractionRequestFailedCode = "EXTRACTION_REQUEST_FAILED"

func (uc *UseCase) emitFetcherConnectionSynced(ctx context.Context, span trace.Span, conn *entities.FetcherConnection) {
	if conn == nil {
		return
	}

	uc.emitDiscoveryPayload(ctx, span, "fetcher_connection.synced", conn.ID.String(), map[string]any{
		"connection_id":         conn.ID.String(),
		"fetcher_connection_id": conn.FetcherConnID,
		"config_name":           conn.ConfigName,
		"database_type":         conn.DatabaseType,
		"status":                string(conn.Status),
		"schema_discovered":     conn.SchemaDiscovered,
		"last_seen_at":          formatDiscoveryTime(conn.LastSeenAt),
		"updated_at":            formatDiscoveryTime(conn.UpdatedAt),
	})
}

func (uc *UseCase) emitFetcherConnectionUnreachable(ctx context.Context, span trace.Span, conn *entities.FetcherConnection, previousStatus string) {
	if conn == nil {
		return
	}

	uc.emitDiscoveryPayload(ctx, span, "fetcher_connection.unreachable", conn.ID.String(), map[string]any{
		"connection_id":         conn.ID.String(),
		"fetcher_connection_id": conn.FetcherConnID,
		"previous_status":       previousStatus,
		"status":                string(conn.Status),
		"schema_discovered":     false,
		"updated_at":            formatDiscoveryTime(conn.UpdatedAt),
	})
}

func (uc *UseCase) emitExtractionRequestCreated(ctx context.Context, span trace.Span, req *entities.ExtractionRequest) {
	if req == nil {
		return
	}

	uc.emitDiscoveryPayload(ctx, span, "extraction_request.created", req.ID.String(), extractionPayload(req, map[string]any{
		"table_count": len(req.Tables),
		"has_filters": len(req.Filters) > 0,
		"start_date":  req.StartDate,
		"end_date":    req.EndDate,
		"created_at":  formatDiscoveryTime(req.CreatedAt),
	}))
}

func (uc *UseCase) emitExtractionRequestSubmitted(ctx context.Context, span trace.Span, req *entities.ExtractionRequest) {
	if req == nil {
		return
	}

	uc.emitDiscoveryPayload(ctx, span, "extraction_request.submitted", req.ID.String(), extractionPayload(req, map[string]any{
		"fetcher_job_id":  req.FetcherJobID,
		"previous_status": "PENDING",
		"submitted_at":    formatDiscoveryTime(req.UpdatedAt),
		"updated_at":      formatDiscoveryTime(req.UpdatedAt),
	}))
}

func (uc *UseCase) emitExtractionTerminal(ctx context.Context, span trace.Span, req *entities.ExtractionRequest, previousStatus string) {
	if req == nil {
		return
	}

	var definitionKey string

	extra := map[string]any{
		"fetcher_job_id":  req.FetcherJobID,
		"previous_status": previousStatus,
		"updated_at":      formatDiscoveryTime(req.UpdatedAt),
	}

	switch req.Status.String() {
	case "COMPLETE":
		definitionKey = "extraction_request.completed"
		extra["has_result"] = req.ResultPath != ""
		extra["completed_at"] = formatDiscoveryTime(req.UpdatedAt)
	case "FAILED":
		definitionKey = "extraction_request.failed"
		extra["error_code"] = ExtractionRequestFailedCode
		extra["failed_at"] = formatDiscoveryTime(req.UpdatedAt)
	case "CANCELLED":
		definitionKey = "extraction_request.cancelled"
		extra["cancelled_at"] = formatDiscoveryTime(req.UpdatedAt)
	default:
		return
	}

	uc.emitDiscoveryPayload(ctx, span, definitionKey, req.ID.String(), extractionPayload(req, extra))
}

func (orch *BridgeExtractionOrchestrator) emitExtractionBridged(ctx context.Context, span trace.Span, extraction *entities.ExtractionRequest, outcome *sharedPorts.BridgeExtractionOutcome) {
	if extraction == nil || outcome == nil {
		return
	}

	payload := extractionPayload(extraction, map[string]any{
		"fetcher_job_id":    extraction.FetcherJobID,
		"ingestion_job_id":  outcome.IngestionJobID.String(),
		"transaction_count": outcome.TransactionCount,
		"bridge_attempts":   extraction.BridgeAttempts,
		"bridged_at":        formatDiscoveryTime(time.Now().UTC()),
	})

	if err := emission.Emit(ctx, orch.streamEmitter, "extraction_request.bridged", extraction.ID.String(), payload); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to emit streaming event extraction_request.bridged", err)
		} else {
			emitWarnNoSpan(ctx, "extraction_request.bridged", err)
		}
	}
}

func extractionPayload(req *entities.ExtractionRequest, extra map[string]any) map[string]any {
	// payload_version is intentionally omitted: lib-streaming attaches event.SchemaVersion
	// from the catalog as a CloudEvents extension automatically, so duplicating it
	// in-band invites drift between in-band and envelope-level versioning.
	payload := map[string]any{
		"extraction_request_id": req.ID.String(),
		"connection_id":         req.ConnectionID.String(),
		"status":                req.Status.String(),
	}
	for key, value := range extra {
		payload[key] = value
	}

	return payload
}

func (uc *UseCase) emitDiscoveryPayload(ctx context.Context, span trace.Span, definitionKey, subject string, payload map[string]any) {
	if err := emission.Emit(ctx, uc.streamEmitter, definitionKey, subject, payload); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
		} else {
			emitWarnNoSpan(ctx, definitionKey, err)
		}
	}
}

// emitWarnNoSpan logs IMPORTANT-tier emission failures when no active span is
// available to attribute the error to. Matcher does not silently drop emit
// failures: the pattern mirrors configuration.emitConfigurationEvent.
func emitWarnNoSpan(ctx context.Context, definitionKey string, err error) {
	// NewTrackingFromContext is the canonical accessor in lib-commons/v5; it
	// returns (logger, tracer, requestID, metricsFactory) and only the logger
	// is needed here. lib-commons itself uses the same nolint at its own
	// call sites (see commons/net/http/ratelimit/middleware.go).
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed; see lib-commons NewTrackingFromContext signature
	if logger == nil {
		return
	}

	// Broker/client errors can carry connection or tenant context; route the
	// log through SafeError so the production-mode redactor strips sensitive
	// fields, matching the rest of the service-layer error logs in matcher.
	libLog.SafeError(logger, ctx, "failed to emit streaming event "+definitionKey+" without span", err, runtime.IsProductionMode())
}

// formatDiscoveryTime delegates to emission.FormatTime; preserved as a thin
// wrapper for backward compatibility with existing unit tests.
func formatDiscoveryTime(value time.Time) string {
	return emission.FormatTime(value)
}
