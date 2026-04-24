// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package shared provides shared domain types used across bounded contexts.
package shared

import (
	"time"

	"github.com/google/uuid"
)

// Ingestion event type constants.
// These are shared so the outbox dispatcher can route events by type without
// directly importing ingestion/domain/entities.
const (
	EventTypeIngestionCompleted = "ingestion.completed"
	EventTypeIngestionFailed    = "ingestion.failed"
)

// IngestionCompletedEvent is published when an ingestion job finishes successfully.
// This shared type allows the outbox dispatcher to unmarshal and route ingestion
// completion events without importing ingestion/domain/entities.
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

// IngestionFailedEvent is published when an ingestion job fails.
// This shared type allows the outbox dispatcher to unmarshal and route ingestion
// failure events without importing ingestion/domain/entities.
type IngestionFailedEvent struct {
	EventType string    `json:"eventType"`
	JobID     uuid.UUID `json:"jobId"`
	ContextID uuid.UUID `json:"contextId"`
	SourceID  uuid.UUID `json:"sourceId"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

// NOTE: The IngestionEventPublisher interface has been moved to internal/shared/ports/
// for consistency with other port interfaces. All new code should import it from there:
//
//	import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
