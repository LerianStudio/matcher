// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package entities provides domain entities for the ingestion bounded context.
package entities

import (
	"context"
	"errors"
	"time"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Event type constants for ingestion events.
// Re-exported from the shared kernel so that event routing in the outbox dispatcher
// can reference them without importing ingestion/domain/entities directly.
const (
	EventTypeIngestionCompleted = sharedDomain.EventTypeIngestionCompleted
	EventTypeIngestionFailed    = sharedDomain.EventTypeIngestionFailed
)

// ErrNilJob is returned when a nil job is passed to event constructors.
var ErrNilJob = errors.New("job is nil")

// IngestionCompletedEvent is a type alias for the shared kernel IngestionCompletedEvent.
// The canonical definition lives in internal/shared/domain.
type IngestionCompletedEvent = sharedDomain.IngestionCompletedEvent

// IngestionFailedEvent is a type alias for the shared kernel IngestionFailedEvent.
// The canonical definition lives in internal/shared/domain.
type IngestionFailedEvent = sharedDomain.IngestionFailedEvent

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
