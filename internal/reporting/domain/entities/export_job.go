// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package entities defines reporting domain types and aggregation logic.
package entities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

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

// ExportJobStatus represents the status of an export job.
type ExportJobStatus string

// Export job status constants.
const (
	ExportJobStatusQueued    ExportJobStatus = "QUEUED"
	ExportJobStatusRunning   ExportJobStatus = "RUNNING"
	ExportJobStatusSucceeded ExportJobStatus = "SUCCEEDED"
	ExportJobStatusFailed    ExportJobStatus = "FAILED"
	ExportJobStatusExpired   ExportJobStatus = "EXPIRED"
	ExportJobStatusCanceled  ExportJobStatus = "CANCELED"
)

// IsValid returns true if the status is a known export job status.
func (s ExportJobStatus) IsValid() bool {
	switch s {
	case ExportJobStatusQueued,
		ExportJobStatusRunning,
		ExportJobStatusSucceeded,
		ExportJobStatusFailed,
		ExportJobStatusExpired,
		ExportJobStatusCanceled:
		return true
	default:
		return false
	}
}

// ExportFormat represents the file format of an export job.
type ExportFormat string

// Export format constants.
const (
	ExportFormatCSV  ExportFormat = "CSV"
	ExportFormatJSON ExportFormat = "JSON"
	ExportFormatXML  ExportFormat = "XML"
	ExportFormatPDF  ExportFormat = "PDF"
)

// IsValid returns true if the format is a known export format.
func (f ExportFormat) IsValid() bool {
	switch f {
	case ExportFormatCSV, ExportFormatJSON, ExportFormatXML, ExportFormatPDF:
		return true
	default:
		return false
	}
}

// ExportReportType represents the type of report in an export job.
type ExportReportType string

// Export report type constants.
const (
	ExportReportTypeMatched    ExportReportType = "MATCHED"
	ExportReportTypeUnmatched  ExportReportType = "UNMATCHED"
	ExportReportTypeSummary    ExportReportType = "SUMMARY"
	ExportReportTypeVariance   ExportReportType = "VARIANCE"
	ExportReportTypeExceptions ExportReportType = "EXCEPTIONS"
)

// IsValid returns true if the report type is a known export report type.
func (r ExportReportType) IsValid() bool {
	switch r {
	case ExportReportTypeMatched,
		ExportReportTypeUnmatched,
		ExportReportTypeSummary,
		ExportReportTypeVariance,
		ExportReportTypeExceptions:
		return true
	default:
		return false
	}
}

// DefaultExportExpiry defines the default expiration time for export files.
const DefaultExportExpiry = 7 * 24 * time.Hour

// DefaultPresignExpiry defines the default expiration time for presigned download URLs.
const DefaultPresignExpiry = 1 * time.Hour

// validExportJobTransitions defines the allowed state transitions for export jobs.
var validExportJobTransitions = map[ExportJobStatus]map[ExportJobStatus]bool{
	ExportJobStatusQueued: {
		ExportJobStatusRunning:  true,
		ExportJobStatusFailed:   true, // last-resort failure when requeue persistence fails
		ExportJobStatusCanceled: true,
		ExportJobStatusExpired:  true, // cleanup worker expires stale queued jobs
	},
	ExportJobStatusRunning: {
		ExportJobStatusSucceeded: true,
		ExportJobStatusFailed:    true,
		ExportJobStatusQueued:    true, // retry resets to queued
		ExportJobStatusCanceled:  true,
		ExportJobStatusExpired:   true, // cleanup worker expires stale running jobs
	},
	ExportJobStatusFailed: {
		ExportJobStatusQueued:  true, // retry
		ExportJobStatusExpired: true, // cleanup worker expires old failed jobs
	},
	ExportJobStatusSucceeded: {
		ExportJobStatusExpired: true, // file expiry lifecycle
	},
	// Terminal states with no outgoing transitions: Expired, Canceled.
}

// ErrInvalidExportJobTransition indicates an invalid state transition was attempted.
var ErrInvalidExportJobTransition = errors.New("invalid export job status transition")

// ErrInvalidExportFormat is returned when an unsupported export format is requested.
var ErrInvalidExportFormat = errors.New("invalid export format")

// ErrInvalidReportType is returned when an unsupported report type is requested.
var ErrInvalidReportType = errors.New("invalid report type")

// ErrInvalidExportJobStatus is returned when an unsupported export job status is provided.
var ErrInvalidExportJobStatus = errors.New("invalid export job status")

// ExportJob represents an async export job for large report exports.
type ExportJob struct {
	ID             uuid.UUID
	TenantID       uuid.UUID
	ContextID      uuid.UUID
	ReportType     ExportReportType
	Format         ExportFormat
	Filter         ExportJobFilter
	Status         ExportJobStatus
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
func (job *ExportJob) now() time.Time {
	if job.clock != nil {
		return job.clock()
	}

	return DefaultClock()
}

// SetClock updates the clock used by this job.
// This is primarily used for testing to simulate time progression.
func (job *ExportJob) SetClock(clock Clock) {
	if job == nil {
		return
	}

	job.clock = clock
}

// ExportJobFilter contains the original filter parameters for the export.
type ExportJobFilter struct {
	DateFrom time.Time        `json:"dateFrom"`
	DateTo   time.Time        `json:"dateTo"`
	SourceID *uuid.UUID       `json:"sourceId,omitempty"`
	Status   *ExportJobStatus `json:"status,omitempty"`
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

	if filter.Status != nil && !filter.Status.IsValid() {
		return ExportJobFilter{}, fmt.Errorf("filter status: %w", ErrInvalidExportJobStatus)
	}

	return filter, nil
}

// NewExportJob creates a new export job with default values.
// Optional ExportJobOption parameters can be passed to customize behavior (e.g., WithClock for testing).
func NewExportJob(
	ctx context.Context,
	tenantID, contextID uuid.UUID,
	reportType ExportReportType,
	format ExportFormat,
	filter ExportJobFilter,
	opts ...ExportJobOption,
) (*ExportJob, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "reporting.export_job.new")

	if err := asserter.That(ctx, tenantID != uuid.Nil, "tenant id is required"); err != nil {
		return nil, fmt.Errorf("export job tenant id: %w", err)
	}

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, fmt.Errorf("export job context id: %w", err)
	}

	if err := asserter.That(ctx, format.IsValid(), "invalid export format", "format", string(format)); err != nil {
		return nil, fmt.Errorf("export job format: %w", err)
	}

	if err := asserter.That(ctx, reportType.IsValid(), "invalid report type", "report_type", string(reportType)); err != nil {
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

// validateTransition checks whether transitioning from the current status to next is allowed.
func (job *ExportJob) validateTransition(next ExportJobStatus) error {
	if job == nil {
		return nil
	}

	allowed, exists := validExportJobTransitions[job.Status]
	if !exists || !allowed[next] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidExportJobTransition, job.Status, next)
	}

	return nil
}

// MarkRunning transitions the job to running status.
func (job *ExportJob) MarkRunning() error {
	if job == nil {
		return nil
	}

	if err := job.validateTransition(ExportJobStatusRunning); err != nil {
		return err
	}

	now := job.now()
	job.Status = ExportJobStatusRunning
	job.StartedAt = pointers.Time(now)
	job.Attempts++
	job.UpdatedAt = now

	return nil
}

// MarkSucceeded transitions the job to succeeded status with file details.
func (job *ExportJob) MarkSucceeded(
	fileKey, fileName, sha256 string,
	recordsWritten, bytesWritten int64,
) error {
	if job == nil {
		return nil
	}

	if err := job.validateTransition(ExportJobStatusSucceeded); err != nil {
		return err
	}

	now := job.now()
	job.Status = ExportJobStatusSucceeded
	job.FileKey = fileKey
	job.FileName = fileName
	job.SHA256 = sha256
	job.RecordsWritten = recordsWritten
	job.BytesWritten = bytesWritten
	job.FinishedAt = pointers.Time(now)
	job.UpdatedAt = now

	return nil
}

// MarkFailed transitions the job to failed status with error message.
func (job *ExportJob) MarkFailed(errMsg string) error {
	if job == nil {
		return nil
	}

	if err := job.validateTransition(ExportJobStatusFailed); err != nil {
		return err
	}

	now := job.now()
	job.Status = ExportJobStatusFailed
	job.Error = errMsg
	job.FinishedAt = pointers.Time(now)
	job.UpdatedAt = now

	return nil
}

// MarkForRetry transitions the job back to queued status for retry.
// The nextRetryAt specifies when the job should be retried.
func (job *ExportJob) MarkForRetry(errMsg string, nextRetryAt time.Time) error {
	if job == nil {
		return nil
	}

	if err := job.validateTransition(ExportJobStatusQueued); err != nil {
		return err
	}

	now := job.now()
	job.Status = ExportJobStatusQueued
	job.Error = errMsg
	job.StartedAt = nil
	job.NextRetryAt = pointers.Time(nextRetryAt)
	job.UpdatedAt = now

	return nil
}

// MarkExpired transitions the job to expired status.
func (job *ExportJob) MarkExpired() error {
	if job == nil {
		return nil
	}

	if err := job.validateTransition(ExportJobStatusExpired); err != nil {
		return err
	}

	now := job.now()
	job.Status = ExportJobStatusExpired
	job.UpdatedAt = now

	return nil
}

// MarkCanceled transitions the job to canceled status.
func (job *ExportJob) MarkCanceled() error {
	if job == nil {
		return nil
	}

	if err := job.validateTransition(ExportJobStatusCanceled); err != nil {
		return err
	}

	now := job.now()
	job.Status = ExportJobStatusCanceled
	job.FinishedAt = pointers.Time(now)
	job.UpdatedAt = now

	return nil
}

// UpdateProgress updates the records and bytes written counters.
func (job *ExportJob) UpdateProgress(recordsWritten, bytesWritten int64) {
	if job == nil {
		return
	}

	job.RecordsWritten = recordsWritten
	job.BytesWritten = bytesWritten
	job.UpdatedAt = job.now()
}

// IsTerminal returns true if the job is in a terminal state.
func (job *ExportJob) IsTerminal() bool {
	if job == nil {
		return false
	}

	switch job.Status {
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
func (job *ExportJob) IsDownloadable() bool {
	if job == nil {
		return false
	}

	return job.Status == ExportJobStatusSucceeded && job.FileKey != ""
}

// IsStreamableFormat returns true if the format supports streaming (no memory limit).
func IsStreamableFormat(format ExportFormat) bool {
	switch format {
	case ExportFormatCSV, ExportFormatJSON, ExportFormatXML:
		return true
	default:
		return false
	}
}

// GenerateFileName creates a standardized file name for the export.
func GenerateFileName(
	reportType ExportReportType,
	format ExportFormat,
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

	return string(reportType) + "_" + contextID.String()[:8] + "_" + dateRange + "." + ext
}
