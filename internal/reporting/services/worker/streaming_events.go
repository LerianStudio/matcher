// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	reportingStreamingPayload "github.com/LerianStudio/matcher/internal/reporting/services/streamingpayload"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

func (worker *ExportWorker) emitExportJobEvent(ctx context.Context, span trace.Span, definitionKey string, job *entities.ExportJob) {
	if job == nil {
		return
	}

	if err := emission.Emit(ctx, worker.streamEmitter, definitionKey, job.ID.String(), buildExportJobPayloadForEvent(definitionKey, job)); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
	}
}

func (worker *CleanupWorker) emitExportJobExpired(ctx context.Context, span trace.Span, job *entities.ExportJob) {
	if job == nil {
		return
	}

	if err := emission.Emit(ctx, worker.streamEmitter, "export_job.expired", job.ID.String(), buildExportJobPayloadForEvent("export_job.expired", job)); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit streaming event export_job.expired", err)
	}
}

func buildExportJobPayloadForEvent(definitionKey string, job *entities.ExportJob) map[string]any {
	return reportingStreamingPayload.ExportJob(definitionKey, job)
}
