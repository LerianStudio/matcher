// Package entities provides domain entities for the ingestion bounded context.
package entities

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Event type constants for ingestion events.
const (
	EventTypeIngestionCompleted = "ingestion.completed"
	EventTypeIngestionFailed    = "ingestion.failed"
)

// ErrNilJob is returned when a nil job is passed to event constructors.
var ErrNilJob = errors.New("job is nil")

// IngestionCompletedEvent published when a job finishes successfully.
type IngestionCompletedEvent struct {
	EventType        string    `json:"eventType"`
	JobID            uuid.UUID `json:"jobId"`
	ContextID        uuid.UUID `json:"contextId"`
	SourceID         uuid.UUID `json:"sourceId"`
	TransactionCount int       `json:"transactionCount"`
	DateRangeStart   time.Time `json:"dateRangeStart"`
	DateRangeEnd     time.Time `json:"dateRangeEnd"`
	TotalRows        int       `json:"totalRows"`
	FailedRows       int       `json:"failedRows"`
	CompletedAt      time.Time `json:"completedAt"`
	Timestamp        time.Time `json:"timestamp"`
}

// NewIngestionCompletedEvent creates a new ingestion completed event.
// Returns ErrNilJob if job is nil.
func NewIngestionCompletedEvent(
	ctx context.Context,
	job *IngestionJob,
	transactionCount int,
	dateRangeStart, dateRangeEnd time.Time,
	totalRows, failedRows int,
) (*IngestionCompletedEvent, error) {
	if job == nil {
		return nil, ErrNilJob
	}

	// ctx reserved for future observability (span events, correlation IDs)
	_ = ctx

	completedAt := time.Now().UTC()
	if job.CompletedAt != nil {
		completedAt = *job.CompletedAt
	}

	return &IngestionCompletedEvent{
		EventType:        EventTypeIngestionCompleted,
		JobID:            job.ID,
		ContextID:        job.ContextID,
		SourceID:         job.SourceID,
		TransactionCount: transactionCount,
		DateRangeStart:   dateRangeStart,
		DateRangeEnd:     dateRangeEnd,
		TotalRows:        totalRows,
		FailedRows:       failedRows,
		CompletedAt:      completedAt,
		Timestamp:        time.Now().UTC(),
	}, nil
}

// IngestionFailedEvent published when a job fails.
type IngestionFailedEvent struct {
	EventType string    `json:"eventType"`
	JobID     uuid.UUID `json:"jobId"`
	ContextID uuid.UUID `json:"contextId"`
	SourceID  uuid.UUID `json:"sourceId"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

// NewIngestionFailedEvent creates a new ingestion failed event.
// Returns ErrNilJob if job is nil.
func NewIngestionFailedEvent(
	ctx context.Context,
	job *IngestionJob,
) (*IngestionFailedEvent, error) {
	if job == nil {
		return nil, ErrNilJob
	}

	// ctx reserved for future observability (span events, correlation IDs)
	_ = ctx

	return &IngestionFailedEvent{
		EventType: EventTypeIngestionFailed,
		JobID:     job.ID,
		ContextID: job.ContextID,
		SourceID:  job.SourceID,
		Error:     job.Metadata.Error,
		Timestamp: time.Now().UTC(),
	}, nil
}
