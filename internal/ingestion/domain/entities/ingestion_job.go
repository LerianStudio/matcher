package entities

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Ingestion job validation and state transition errors.
var (
	ErrJobNil                    = errors.New("ingestion job is nil")
	ErrContextIDRequired         = errors.New("context_id is required")
	ErrSourceIDRequired          = errors.New("source_id is required")
	ErrFileNameRequired          = errors.New("file name is required")
	ErrFileSizeInvalid           = errors.New("file size must be non-negative")
	ErrTotalRowsInvalid          = errors.New("total rows must be non-negative")
	ErrFailedRowsInvalid         = errors.New("failed rows must be non-negative")
	ErrFailedRowsExceedTotal     = errors.New("failed rows cannot exceed total rows")
	ErrErrorMessageRequired      = errors.New("error message is required")
	ErrJobMustBeQueued           = errors.New("job must be queued to start")
	ErrJobMustBeProcessing       = errors.New("job must be processing to complete")
	ErrJobMustBeProcessingToFail = errors.New("job must be in processing state to fail")
	ErrCompletedJobCannotFail    = errors.New("completed job cannot fail")
)

// RowError represents a parsing error for a specific row.
type RowError struct {
	Row     int    `json:"row"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

// JobMetadata stores additional job info in JSONB column
// Matches schema: migrations/000001_init_schema.up.sql:71-81.
type JobMetadata struct {
	FileName     string     `json:"fileName,omitempty"     example:"transactions_2024.csv"`
	FileSize     int64      `json:"fileSize,omitempty"     example:"1048576"                     minimum:"0"`
	TotalRows    int        `json:"totalRows,omitempty"    example:"1000"                        minimum:"0"`
	FailedRows   int        `json:"failedRows,omitempty"   example:"5"                           minimum:"0"`
	Error        string     `json:"error,omitempty"        example:"row 15: invalid date format"`
	ErrorDetails []RowError `json:"errorDetails,omitempty"`
}

// IngestionJob represents a batch data import operation.
type IngestionJob struct {
	ID          uuid.UUID
	ContextID   uuid.UUID
	SourceID    uuid.UUID
	Status      value_objects.JobStatus
	StartedAt   time.Time
	CompletedAt *time.Time
	Metadata    JobMetadata
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewIngestionJob creates a new ingestion job entity.
func NewIngestionJob(
	ctx context.Context,
	contextID, sourceID uuid.UUID,
	fileName string,
	fileSize int64,
) (*IngestionJob, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "ingestion.ingestion_job.new")

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, ErrContextIDRequired
	}

	if err := asserter.That(ctx, sourceID != uuid.Nil, "source id is required"); err != nil {
		return nil, ErrSourceIDRequired
	}

	trimmedFileName := strings.TrimSpace(fileName)
	if err := asserter.NotEmpty(ctx, trimmedFileName, "file name is required"); err != nil {
		return nil, ErrFileNameRequired
	}

	if err := asserter.That(ctx, fileSize >= 0, "file size must be non-negative", "file_size", fileSize); err != nil {
		return nil, ErrFileSizeInvalid
	}

	now := time.Now().UTC()

	return &IngestionJob{
		ID:        uuid.New(),
		ContextID: contextID,
		SourceID:  sourceID,
		Status:    value_objects.JobStatusQueued,
		Metadata: JobMetadata{
			FileName: trimmedFileName,
			FileSize: fileSize,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// Start transitions the job from QUEUED to PROCESSING state.
func (job *IngestionJob) Start(ctx context.Context) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "ingestion.ingestion_job.start")
	if err := asserter.NotNil(ctx, job, "ingestion job is required"); err != nil {
		return ErrJobNil
	}

	if job.Status != value_objects.JobStatusQueued {
		return ErrJobMustBeQueued
	}

	now := time.Now().UTC()
	job.Status = value_objects.JobStatusProcessing
	job.StartedAt = now
	job.UpdatedAt = now

	return nil
}

// Complete transitions the job to COMPLETED state with row counts.
func (job *IngestionJob) Complete(ctx context.Context, total, failed int) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "ingestion.ingestion_job.complete")
	if err := asserter.NotNil(ctx, job, "ingestion job is required"); err != nil {
		return ErrJobNil
	}

	if job.Status != value_objects.JobStatusProcessing {
		return ErrJobMustBeProcessing
	}

	if total < 0 {
		return ErrTotalRowsInvalid
	}

	if failed < 0 {
		return ErrFailedRowsInvalid
	}

	if failed > total {
		return ErrFailedRowsExceedTotal
	}

	now := time.Now().UTC()
	job.Status = value_objects.JobStatusCompleted
	job.CompletedAt = pointers.Time(now)
	job.Metadata.TotalRows = total
	job.Metadata.FailedRows = failed
	job.UpdatedAt = now

	return nil
}

// Fail transitions the job from PROCESSING to FAILED state with error message.
// Returns nil (idempotent) if the job is already FAILED.
// Returns ErrJobMustBeProcessingToFail for any other state (QUEUED, COMPLETED).
func (job *IngestionJob) Fail(ctx context.Context, errMsg string) error {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "ingestion.ingestion_job.fail")
	if err := asserter.NotNil(ctx, job, "ingestion job is required"); err != nil {
		return ErrJobNil
	}

	// Idempotent: already failed is a no-op.
	if job.Status == value_objects.JobStatusFailed {
		return nil
	}

	// Only PROCESSING jobs can transition to FAILED.
	if job.Status != value_objects.JobStatusProcessing {
		return ErrJobMustBeProcessingToFail
	}

	trimmed := strings.TrimSpace(errMsg)
	if err := asserter.NotEmpty(ctx, trimmed, "error message is required"); err != nil {
		return ErrErrorMessageRequired
	}

	now := time.Now().UTC()
	job.Status = value_objects.JobStatusFailed
	job.CompletedAt = pointers.Time(now)
	job.Metadata.Error = trimmed
	job.UpdatedAt = now

	return nil
}

// MetadataJSON returns metadata as JSON bytes for DB storage.
func (job *IngestionJob) MetadataJSON() ([]byte, error) {
	if job == nil {
		return json.Marshal(nil)
	}

	data, err := json.Marshal(job.Metadata)
	if err != nil {
		return nil, err
	}

	return data, nil
}
