// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

// IngestionFailedCode is the stable external error code for ingestion failures.
// The producer-side error is intentionally discarded at this boundary.
const IngestionFailedCode = "INGESTION_FAILED"

func (uc *UseCase) emitIngestionEvent(ctx context.Context, span trace.Span, definitionKey string, job *entities.IngestionJob, extra map[string]any) {
	if job == nil {
		return
	}

	// ingestion.completed is one of matcher's highest-frequency emit sites
	// (one per ingested file with hundreds of rows). The payload remains a
	// map rather than a typed struct because callers in lifecycle_commands.go
	// extend the map with caller-specific fields (transaction_count,
	// date_range_start, date_range_end) that vary by caller. The hot-path
	// optimisation lives in emission.Emit's pooled encoder; switching to a
	// typed struct here would either drop caller-specific fields or force
	// every caller to learn the typed shape, which trades one allocation
	// problem for a coupling problem. See emission.go marshalPayload for
	// the buffer-pool that keeps allocations predictable on this path.
	payload := map[string]any{
		"job_id":      job.ID.String(),
		"context_id":  job.ContextID.String(),
		"source_id":   job.SourceID.String(),
		"status":      string(job.Status),
		"total_rows":  job.Metadata.TotalRows,
		"failed_rows": job.Metadata.FailedRows,
	}
	if job.CompletedAt != nil {
		terminalAt := formatIngestionTime(*job.CompletedAt)

		switch definitionKey {
		case "ingestion.completed":
			payload["completed_at"] = terminalAt
		case "ingestion.failed":
			payload["failed_at"] = terminalAt
		}
	}

	if job.Metadata.Error != "" {
		payload["error_code"] = IngestionFailedCode
	}

	for key, value := range extra {
		payload[key] = value
	}

	if err := emission.Emit(ctx, uc.streamEmitter, definitionKey, job.ID.String(), payload); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
		}
	}
}

func (uc *UseCase) emitTransactionIgnored(ctx context.Context, span trace.Span, existing, updated *shared.Transaction, contextID string) {
	if updated == nil {
		return
	}

	previousStatus := ""
	if existing != nil {
		previousStatus = string(existing.Status)
	}

	payload := map[string]any{
		"transaction_id":    updated.ID.String(),
		"ingestion_job_id":  updated.IngestionJobID.String(),
		"context_id":        contextID,
		"source_id":         updated.SourceID.String(),
		"previous_status":   previousStatus,
		"status":            string(updated.Status),
		"extraction_status": string(updated.ExtractionStatus),
		"updated_at":        formatIngestionTime(updated.UpdatedAt),
	}

	if err := emission.Emit(ctx, uc.streamEmitter, "transaction.ignored", updated.ID.String(), payload); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to emit streaming event transaction.ignored", err)
		}
	}
}

// formatIngestionTime delegates to emission.FormatTime; preserved as a thin
// wrapper for backward compatibility with existing unit tests.
func formatIngestionTime(value time.Time) string {
	return emission.FormatTime(value)
}
