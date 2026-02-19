// Package entities defines reporting domain types and aggregation logic.
package entities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Clock is a function type that returns the current time.
// This allows time to be injected for testing purposes.
type Clock func() time.Time

// DefaultClock returns the current UTC time.
func DefaultClock() time.Time {
	return time.Now().UTC()
}

// ExportJobOption is a functional option for configuring ExportJob creation.
type ExportJobOption func(*exportJobOptions)

type exportJobOptions struct {
	clock Clock
}

// WithClock sets a custom clock for the export job.
// This is primarily used for testing to provide deterministic timestamps.
func WithClock(clock Clock) ExportJobOption {
	return func(opts *exportJobOptions) {
		opts.clock = clock
	}
}

// Export job status constants.
const (
	ExportJobStatusQueued    = "QUEUED"
	ExportJobStatusRunning   = "RUNNING"
	ExportJobStatusSucceeded = "SUCCEEDED"
	ExportJobStatusFailed    = "FAILED"
	ExportJobStatusExpired   = "EXPIRED"
	ExportJobStatusCanceled  = "CANCELED"
)

// Export format constants.
const (
	ExportFormatCSV  = "CSV"
	ExportFormatJSON = "JSON"
	ExportFormatXML  = "XML"
	ExportFormatPDF  = "PDF"
)

// Export report type constants.
const (
	ExportReportTypeMatched   = "MATCHED"
	ExportReportTypeUnmatched = "UNMATCHED"
	ExportReportTypeSummary   = "SUMMARY"
	ExportReportTypeVariance  = "VARIANCE"
)

// DefaultExportExpiry defines the default expiration time for export files.
const DefaultExportExpiry = 7 * 24 * time.Hour

// DefaultPresignExpiry defines the default expiration time for presigned download URLs.
const DefaultPresignExpiry = 1 * time.Hour

// ErrInvalidExportFormat is returned when an unsupported export format is requested.
var ErrInvalidExportFormat = errors.New("invalid export format")

// ErrInvalidReportType is returned when an unsupported report type is requested.
var ErrInvalidReportType = errors.New("invalid report type")

// ExportJob represents an async export job for large report exports.
type ExportJob struct {
	ID             uuid.UUID
	TenantID       uuid.UUID
	ContextID      uuid.UUID
	ReportType     string
	Format         string
	Filter         ExportJobFilter
	Status         string
	RecordsWritten int64
	BytesWritten   int64
	FileKey        string
	FileName       string
	SHA256         string
	Error          string
	Attempts       int
	NextRetryAt    *time.Time
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	ExpiresAt      time.Time
	UpdatedAt      time.Time

	// clock is used for time operations. Not persisted.
	clock Clock
}

// now returns the current time using the job's clock.
// Falls back to DefaultClock if no clock is set.
func (j *ExportJob) now() time.Time {
	if j.clock != nil {
		return j.clock()
	}

	return DefaultClock()
}

// SetClock updates the clock used by this job.
// This is primarily used for testing to simulate time progression.
func (j *ExportJob) SetClock(clock Clock) {
	j.clock = clock
}

// ExportJobFilter contains the original filter parameters for the export.
type ExportJobFilter struct {
	DateFrom time.Time  `json:"dateFrom"`
	DateTo   time.Time  `json:"dateTo"`
	SourceID *uuid.UUID `json:"sourceId,omitempty"`
	Status   *string    `json:"status,omitempty"`
}

// ToJSON serializes the filter to JSON bytes.
func (f ExportJobFilter) ToJSON() ([]byte, error) {
	return json.Marshal(f)
}

// ExportJobFilterFromJSON deserializes filter from JSON bytes.
func ExportJobFilterFromJSON(data []byte) (ExportJobFilter, error) {
	var filter ExportJobFilter
	if err := json.Unmarshal(data, &filter); err != nil {
		return ExportJobFilter{}, err
	}

	return filter, nil
}

// NewExportJob creates a new export job with default values.
// Optional ExportJobOption parameters can be passed to customize behavior (e.g., WithClock for testing).
func NewExportJob(
	ctx context.Context,
	tenantID, contextID uuid.UUID,
	reportType, format string,
	filter ExportJobFilter,
	opts ...ExportJobOption,
) (*ExportJob, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "reporting.export_job.new")

	if err := asserter.That(ctx, IsValidExportFormat(format), "invalid export format", "format", format); err != nil {
		return nil, fmt.Errorf("export job format: %w", err)
	}

	if err := asserter.That(ctx, IsValidReportType(reportType), "invalid report type", "report_type", reportType); err != nil {
		return nil, fmt.Errorf("export job report type: %w", err)
	}

	options := &exportJobOptions{
		clock: DefaultClock,
	}

	for _, opt := range opts {
		opt(options)
	}

	now := options.clock()

	return &ExportJob{
		ID:         uuid.New(),
		TenantID:   tenantID,
		ContextID:  contextID,
		ReportType: reportType,
		Format:     format,
		Filter:     filter,
		Status:     ExportJobStatusQueued,
		CreatedAt:  now,
		ExpiresAt:  now.Add(DefaultExportExpiry),
		UpdatedAt:  now,
		clock:      options.clock,
	}, nil
}

// MarkRunning transitions the job to running status.
func (j *ExportJob) MarkRunning() {
	now := j.now()
	j.Status = ExportJobStatusRunning
	j.StartedAt = &now
	j.Attempts++
	j.UpdatedAt = now
}

// MarkSucceeded transitions the job to succeeded status with file details.
//
//nolint:varnamelen // receiver name matches consistent pattern
func (j *ExportJob) MarkSucceeded(
	fileKey, fileName, sha256 string,
	recordsWritten, bytesWritten int64,
) {
	now := j.now()
	j.Status = ExportJobStatusSucceeded
	j.FileKey = fileKey
	j.FileName = fileName
	j.SHA256 = sha256
	j.RecordsWritten = recordsWritten
	j.BytesWritten = bytesWritten
	j.FinishedAt = &now
	j.UpdatedAt = now
}

// MarkFailed transitions the job to failed status with error message.
func (j *ExportJob) MarkFailed(errMsg string) {
	now := j.now()
	j.Status = ExportJobStatusFailed
	j.Error = errMsg
	j.FinishedAt = &now
	j.UpdatedAt = now
}

// MarkForRetry transitions the job back to queued status for retry.
// The nextRetryAt specifies when the job should be retried.
//
//nolint:varnamelen // receiver name 'j' consistent with other ExportJob methods
func (j *ExportJob) MarkForRetry(errMsg string, nextRetryAt time.Time) {
	now := j.now()
	j.Status = ExportJobStatusQueued
	j.Error = errMsg
	j.StartedAt = nil
	j.NextRetryAt = &nextRetryAt
	j.UpdatedAt = now
}

// MarkExpired transitions the job to expired status.
func (j *ExportJob) MarkExpired() {
	now := j.now()
	j.Status = ExportJobStatusExpired
	j.UpdatedAt = now
}

// MarkCanceled transitions the job to canceled status.
func (j *ExportJob) MarkCanceled() {
	now := j.now()
	j.Status = ExportJobStatusCanceled
	j.FinishedAt = &now
	j.UpdatedAt = now
}

// UpdateProgress updates the records and bytes written counters.
func (j *ExportJob) UpdateProgress(recordsWritten, bytesWritten int64) {
	j.RecordsWritten = recordsWritten
	j.BytesWritten = bytesWritten
	j.UpdatedAt = j.now()
}

// IsTerminal returns true if the job is in a terminal state.
func (j *ExportJob) IsTerminal() bool {
	switch j.Status {
	case ExportJobStatusSucceeded,
		ExportJobStatusFailed,
		ExportJobStatusExpired,
		ExportJobStatusCanceled:
		return true
	default:
		return false
	}
}

// IsDownloadable returns true if the job has a file ready for download.
func (j *ExportJob) IsDownloadable() bool {
	return j.Status == ExportJobStatusSucceeded && j.FileKey != ""
}

// IsValidExportFormat checks if the format is supported.
func IsValidExportFormat(format string) bool {
	switch format {
	case ExportFormatCSV, ExportFormatJSON, ExportFormatXML, ExportFormatPDF:
		return true
	default:
		return false
	}
}

// IsValidReportType checks if the report type is supported.
func IsValidReportType(reportType string) bool {
	switch reportType {
	case ExportReportTypeMatched,
		ExportReportTypeUnmatched,
		ExportReportTypeSummary,
		ExportReportTypeVariance:
		return true
	default:
		return false
	}
}

// IsStreamableFormat returns true if the format supports streaming (no memory limit).
func IsStreamableFormat(format string) bool {
	switch format {
	case ExportFormatCSV, ExportFormatJSON, ExportFormatXML:
		return true
	default:
		return false
	}
}

// GenerateFileName creates a standardized file name for the export.
func GenerateFileName(
	reportType, format string,
	contextID uuid.UUID,
	dateFrom, dateTo time.Time,
) string {
	dateRange := dateFrom.Format("20060102") + "-" + dateTo.Format("20060102")

	var ext string

	switch format {
	case ExportFormatCSV:
		ext = "csv"
	case ExportFormatJSON:
		ext = "json"
	case ExportFormatXML:
		ext = "xml"
	case ExportFormatPDF:
		ext = "pdf"
	default:
		ext = "dat"
	}

	return reportType + "_" + contextID.String()[:8] + "_" + dateRange + "." + ext
}
