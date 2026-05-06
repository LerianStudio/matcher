// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package streamingpayload centralizes reporting streaming event payload builders.
package streamingpayload

import (
	"time"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

// ExportJobFailedCode is the stable external error code for export job failures.
// The producer-side error is intentionally discarded at this boundary to avoid
// leaking internal error text into the catalog-defined external schema.
const ExportJobFailedCode = "EXPORT_JOB_FAILED"

// ExportJob builds the external streaming payload for export job lifecycle events.
func ExportJob(definitionKey string, job *entities.ExportJob) map[string]any {
	payload := map[string]any{
		"export_job_id":  job.ID.String(),
		"tenant_id":      job.TenantID.String(),
		"context_id":     job.ContextID.String(),
		"report_type":    string(job.ReportType),
		"format":         string(job.Format),
		"status":         string(job.Status),
		"schema_version": "1.0.0",
		"created_at":     FormatTime(job.CreatedAt),
		"expires_at":     FormatTime(job.ExpiresAt),
		"updated_at":     FormatTime(job.UpdatedAt),
	}

	if job.FileName != "" {
		payload["file_name"] = job.FileName
	}

	if job.SHA256 != "" {
		payload["sha256"] = job.SHA256
	}

	if job.RecordsWritten > 0 {
		payload["records_written"] = job.RecordsWritten
	}

	if job.BytesWritten > 0 {
		payload["bytes_written"] = job.BytesWritten
	}

	if job.Attempts > 0 {
		payload["attempts"] = job.Attempts
	}

	if job.Error != "" {
		payload["error_code"] = ExportJobFailedCode
	}

	if job.FinishedAt != nil {
		payload["finished_at"] = FormatTime(*job.FinishedAt)
	}

	if definitionKey == "export_job.expired" {
		payload["expired_at"] = FormatTime(job.UpdatedAt)
	}

	return payload
}

// FormatTime delegates to emission.FormatTime; preserved as a thin wrapper
// for backward compatibility with existing unit tests that import it as
// reportingStreamingPayload.FormatTime.
func FormatTime(value time.Time) string {
	return emission.FormatTime(value)
}
