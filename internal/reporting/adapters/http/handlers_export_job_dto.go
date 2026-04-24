// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

// CreateExportJobRequest represents the request body for creating an export job.
type CreateExportJobRequest struct {
	// ReportType specifies the type of report to export
	// @enum MATCHED,UNMATCHED,SUMMARY,VARIANCE,EXCEPTIONS,MATCHES,UNMATCHED_TRANSACTIONS
	ReportType string `json:"reportType" validate:"required,oneof=MATCHED UNMATCHED SUMMARY VARIANCE EXCEPTIONS MATCHES UNMATCHED_TRANSACTIONS matched unmatched summary variance exceptions matches unmatched_transactions" example:"MATCHED"`
	// Format specifies the export file format (server normalizes to uppercase)
	Format string `json:"format" validate:"required,oneof=CSV JSON XML csv json xml" enums:"CSV,JSON,XML,csv,json,xml" example:"CSV"`
	// DateFrom is the start date for the report (YYYY-MM-DD)
	DateFrom string `json:"dateFrom" validate:"required" example:"2025-01-01"`
	// DateTo is the end date for the report (YYYY-MM-DD)
	DateTo string `json:"dateTo" validate:"required" example:"2025-01-31"`
	// SourceID optionally filters to a specific source
	SourceID *string `json:"sourceId,omitempty" validate:"omitempty,uuid" example:"550e8400-e29b-41d4-a716-446655440000"`
}

// CreateExportJobResponse represents the response for creating an export job.
type CreateExportJobResponse struct {
	JobID     string `json:"jobId"     example:"550e8400-e29b-41d4-a716-446655440000"`
	Status    string `json:"status"    example:"QUEUED"    enums:"QUEUED"`
	StatusURL string `json:"statusUrl" example:"/v1/contexts/550e8400-e29b-41d4-a716-446655440000/export-jobs/550e8400-e29b-41d4-a716-446655440001"`
}

// parsedExportJobRequest holds validated and parsed request data.
type parsedExportJobRequest struct {
	reportType entities.ExportReportType
	format     entities.ExportFormat
	dateFrom   time.Time
	dateTo     time.Time
	sourceID   *uuid.UUID
}

// parseExportJobRequest parses and applies business rules to the request.
// Note: Struct validation (required, oneof, uuid) is done by libHTTP.ParseBodyAndValidate.
func parseExportJobRequest(req *CreateExportJobRequest) (*parsedExportJobRequest, string, error) {
	normalizedReportType, ok := normalizeReportTypeAlias(req.ReportType)
	if !ok {
		return nil, "invalid report_type: must be MATCHED, UNMATCHED, SUMMARY, VARIANCE, EXCEPTIONS, MATCHES, or UNMATCHED_TRANSACTIONS", entities.ErrInvalidReportType
	}

	format := entities.ExportFormat(strings.ToUpper(strings.TrimSpace(req.Format)))

	if !format.IsValid() {
		return nil, "invalid format: must be CSV, JSON, XML, or PDF", entities.ErrInvalidExportFormat
	}

	if format == entities.ExportFormatPDF {
		return nil, "PDF format not supported for async export", ErrPDFNotSupportedAsync
	}

	if normalizedReportType == entities.ExportReportTypeSummary {
		return nil, "SUMMARY report type not supported for async export", ErrSummaryNotSupportedAsync
	}

	if normalizedReportType == entities.ExportReportTypeExceptions {
		return nil, "EXCEPTIONS report type is not yet supported for async export", ErrExceptionsNotSupportedAsync
	}

	dateFrom, err := time.Parse(time.DateOnly, req.DateFrom)
	if err != nil {
		return nil, "invalid date_from format", fmt.Errorf("invalid date_from format: %w", err)
	}

	dateTo, err := time.Parse(time.DateOnly, req.DateTo)
	if err != nil {
		return nil, "invalid date_to format", fmt.Errorf("invalid date_to format: %w", err)
	}

	if dateFrom.After(dateTo) {
		return nil, "dateFrom must be before or equal to dateTo", ErrDateRangeInvalid
	}

	if dateTo.Sub(dateFrom).Hours()/hoursPerDay > float64(maxAsyncExportDateRangeDays) {
		return nil, fmt.Sprintf("date range exceeds maximum of %d days for export jobs", maxAsyncExportDateRangeDays), ErrAsyncExportDateRangeExceeded
	}

	dateTo = dateTo.Add(hoursPerDay*time.Hour - time.Nanosecond)

	var sourceID *uuid.UUID

	if req.SourceID != nil && *req.SourceID != "" {
		parsed, err := uuid.Parse(*req.SourceID)
		if err != nil {
			return nil, "invalid source_id", fmt.Errorf("invalid source_id: %w", err)
		}

		sourceID = &parsed
	}

	return &parsedExportJobRequest{
		reportType: normalizedReportType,
		format:     format,
		dateFrom:   dateFrom,
		dateTo:     dateTo,
		sourceID:   sourceID,
	}, "", nil
}

func normalizeReportTypeAlias(reportType string) (entities.ExportReportType, bool) {
	switch strings.ToUpper(strings.TrimSpace(reportType)) {
	case string(entities.ExportReportTypeMatched), "MATCHES":
		return entities.ExportReportTypeMatched, true
	case string(entities.ExportReportTypeUnmatched), "UNMATCHED_TRANSACTIONS":
		return entities.ExportReportTypeUnmatched, true
	case string(entities.ExportReportTypeSummary):
		return entities.ExportReportTypeSummary, true
	case string(entities.ExportReportTypeVariance):
		return entities.ExportReportTypeVariance, true
	case string(entities.ExportReportTypeExceptions):
		return entities.ExportReportTypeExceptions, true
	default:
		return "", false
	}
}

// ExportJobResponse represents an export job in API responses.
type ExportJobResponse struct {
	ID             string  `json:"id"                   example:"550e8400-e29b-41d4-a716-446655440000"`
	ReportType     string  `json:"reportType"           example:"MATCHED"    enums:"MATCHED,UNMATCHED,SUMMARY,VARIANCE"`
	Format         string  `json:"format"               example:"CSV"        enums:"CSV,JSON,XML,PDF"`
	Status         string  `json:"status"               example:"SUCCEEDED"  enums:"QUEUED,RUNNING,SUCCEEDED,FAILED,EXPIRED,CANCELED"`
	RecordsWritten int64   `json:"recordsWritten"       example:"4250"`
	BytesWritten   int64   `json:"bytesWritten"         example:"524288"`
	FileName       *string `json:"fileName,omitempty"   example:"matched_report_2025-01-31.csv"`
	Error          *string `json:"error,omitempty"       example:"timeout exceeded"`
	CreatedAt      string  `json:"createdAt"            example:"2025-01-15T10:30:00Z"`
	StartedAt      *string `json:"startedAt,omitempty"  example:"2025-01-15T10:30:05Z"`
	FinishedAt     *string `json:"finishedAt,omitempty" example:"2025-01-15T10:35:00Z"`
	ExpiresAt      string  `json:"expiresAt"            example:"2025-01-16T10:30:00Z"`
	DownloadURL    *string `json:"downloadUrl,omitempty" example:"https://storage.example.com/exports/matched_report.csv?token=abc"`
}

// ExportJobListResponse represents a list of export jobs.
//
// The export-jobs list endpoints use forward-only cursor pagination: clients
// walk results by passing the returned nextCursor as the cursor query param on
// the next request. prevCursor is intentionally omitted — the underlying
// keyset query is unidirectional by creation timestamp and exposing a reverse
// cursor would publish a capability the service does not honour.
type ExportJobListResponse struct {
	Items      []*ExportJobResponse `json:"items"                validate:"omitempty,max=200" maxItems:"200"`
	NextCursor string               `json:"nextCursor,omitempty" example:"eyJpZCI6IjEyMyJ9"`
	Limit      int                  `json:"limit"                example:"20"                                minimum:"1" maximum:"200"`
	HasMore    bool                 `json:"hasMore"              example:"true"`
}

// DownloadExportJobResponse represents the response for downloading an export file.
type DownloadExportJobResponse struct {
	// Presigned URL to download the export file
	DownloadURL string `json:"downloadUrl"  example:"https://storage.example.com/exports/report.csv?token=abc"`
	// Original file name of the export
	FileName string `json:"fileName"     example:"matched_report.csv"`
	// SHA-256 checksum of the file for integrity verification
	SHA256 string `json:"sha256"       example:"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`
	// Duration in seconds until the download URL expires
	ExpiresIn int `json:"expiresIn"    example:"3600"`
}
