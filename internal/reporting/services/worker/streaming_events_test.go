// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	reportingStreamingPayload "github.com/LerianStudio/matcher/internal/reporting/services/streamingpayload"
)

func TestBuildExportJobPayloadIncludesRequiredAndOptionalFields(t *testing.T) {
	finishedAt := time.Date(2026, time.May, 4, 13, 11, 12, 13, time.UTC)
	createdAt := finishedAt.Add(-time.Hour)
	job := &entities.ExportJob{
		ID:             uuid.New(),
		TenantID:       uuid.New(),
		ContextID:      uuid.New(),
		ReportType:     entities.ExportReportTypeSummary,
		Format:         entities.ExportFormatCSV,
		Status:         entities.ExportJobStatusSucceeded,
		FileName:       "summary.csv",
		FileKey:        "exports/summary.csv",
		SHA256:         "abc123",
		RecordsWritten: 42,
		BytesWritten:   4096,
		Attempts:       2,
		CreatedAt:      createdAt,
		ExpiresAt:      createdAt.Add(24 * time.Hour),
		UpdatedAt:      finishedAt,
		FinishedAt:     &finishedAt,
	}

	payload := buildExportJobPayloadForEvent("export_job.succeeded", job)

	assert.Equal(t, job.ID.String(), payload["export_job_id"])
	assert.Equal(t, job.TenantID.String(), payload["tenant_id"])
	assert.Equal(t, job.ContextID.String(), payload["context_id"])
	assert.Equal(t, "SUMMARY", payload["report_type"])
	assert.Equal(t, "CSV", payload["format"])
	assert.Equal(t, "SUCCEEDED", payload["status"])
	assert.Equal(t, "summary.csv", payload["file_name"])
	assert.NotContains(t, payload, "file_key")
	assert.Equal(t, "abc123", payload["sha256"])
	assert.Equal(t, int64(42), payload["records_written"])
	assert.Equal(t, int64(4096), payload["bytes_written"])
	assert.Equal(t, 2, payload["attempts"])
	assert.Equal(t, reportingStreamingPayload.FormatTime(finishedAt), payload["finished_at"])
	assert.NotContains(t, payload, "expired_at")
}

func TestBuildExportJobPayloadForExpiredUsesUpdatedAtAndOmitsFileKey(t *testing.T) {
	finishedAt := time.Date(2026, time.May, 4, 13, 11, 12, 13, time.UTC)
	expiredAt := finishedAt.Add(2 * time.Hour)
	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeSummary,
		Format:     entities.ExportFormatCSV,
		Status:     entities.ExportJobStatusExpired,
		FileName:   "summary.csv",
		FileKey:    "exports/summary.csv",
		CreatedAt:  finishedAt.Add(-time.Hour),
		ExpiresAt:  finishedAt,
		UpdatedAt:  expiredAt,
		FinishedAt: &finishedAt,
	}

	payload := buildExportJobPayloadForEvent("export_job.expired", job)

	assert.Equal(t, reportingStreamingPayload.FormatTime(expiredAt), payload["expired_at"])
	assert.NotContains(t, payload, "file_key")
}

func TestBuildExportJobPayloadForFailedUsesSanitizedErrorCode(t *testing.T) {
	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeSummary,
		Format:     entities.ExportFormatCSV,
		Status:     entities.ExportJobStatusFailed,
		Error:      "s3 credential for bucket leaked in raw error",
		CreatedAt:  time.Date(2026, time.May, 4, 12, 0, 0, 0, time.UTC),
		ExpiresAt:  time.Date(2026, time.May, 5, 12, 0, 0, 0, time.UTC),
		UpdatedAt:  time.Date(2026, time.May, 4, 12, 30, 0, 0, time.UTC),
	}

	payload := buildExportJobPayloadForEvent("export_job.failed", job)

	assert.Equal(t, "EXPORT_JOB_FAILED", payload["error_code"])
	assert.NotContains(t, payload["error_code"], "credential")
}

func TestFormatReportingTimeUsesUTCAndRFC3339Nano(t *testing.T) {
	input := time.Date(2026, time.May, 4, 10, 11, 12, 13, time.FixedZone("BRT", -3*60*60))

	formatted := reportingStreamingPayload.FormatTime(input)

	parsed, err := time.Parse(time.RFC3339Nano, formatted)
	require.NoError(t, err)
	assert.Equal(t, input.UTC(), parsed)
}
