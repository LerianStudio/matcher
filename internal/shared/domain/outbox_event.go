// Package shared provides shared domain types used across bounded contexts.
package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// ErrOutboxEventRequired is returned when an outbox event is nil.
// This sentinel is defined in the shared kernel so that contexts that import
// shared outbox types can reference it without importing outbox/domain/entities.
var ErrOutboxEventRequired = errors.New("outbox event is required")

// ErrOutboxEventPayloadInvalidJSON is returned when the outbox payload is not valid JSON.
var ErrOutboxEventPayloadInvalidJSON = errors.New("outbox event payload must be valid JSON")

// OutboxEventStatus is a typed enumeration for outbox event lifecycle states.
// Using a named type prevents accidental assignment of arbitrary strings to
// OutboxEvent.Status and enables exhaustive switch analysis by static linters.
type OutboxEventStatus string

// Outbox event status constants shared across all bounded contexts.
const (
	OutboxStatusPending    OutboxEventStatus = "PENDING"
	OutboxStatusProcessing OutboxEventStatus = "PROCESSING"
	OutboxStatusPublished  OutboxEventStatus = "PUBLISHED"
	OutboxStatusFailed     OutboxEventStatus = "FAILED"
	OutboxStatusInvalid    OutboxEventStatus = "INVALID" // Permanent validation failures (non-retryable)
)

// IsValid reports whether s is a recognised OutboxEventStatus value.
func (s OutboxEventStatus) IsValid() bool {
	switch s {
	case OutboxStatusPending, OutboxStatusProcessing, OutboxStatusPublished, OutboxStatusFailed, OutboxStatusInvalid:
		return true
	default:
		return false
	}
}

// OutboxEvent represents an event stored in the outbox for reliable delivery.
// This is the shared kernel representation used by all bounded contexts that
// publish or consume outbox events, avoiding direct imports between contexts.
type OutboxEvent struct {
	ID          uuid.UUID
	EventType   string
	AggregateID uuid.UUID
	Payload     []byte
	Status      OutboxEventStatus
	Attempts    int
	PublishedAt *time.Time
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewOutboxEvent creates a new OutboxEvent with the given parameters.
func NewOutboxEvent(
	ctx context.Context,
	eventType string,
	aggregateID uuid.UUID,
	payload []byte,
) (*OutboxEvent, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "outbox.outbox_event.new")

	if err := asserter.NotEmpty(ctx, eventType, "event type is required"); err != nil {
		return nil, fmt.Errorf("outbox event type: %w", err)
	}

	if err := asserter.That(ctx, aggregateID != uuid.Nil, "aggregate id is required"); err != nil {
		return nil, fmt.Errorf("outbox event aggregate id: %w", err)
	}

	if err := asserter.That(ctx, len(payload) > 0, "payload is required"); err != nil {
		return nil, fmt.Errorf("outbox event payload: %w", err)
	}

	if err := asserter.That(ctx, json.Valid(payload), "payload must be valid JSON"); err != nil {
		return nil, ErrOutboxEventPayloadInvalidJSON
	}

	now := time.Now().UTC()

	return &OutboxEvent{
		ID:          uuid.New(),
		EventType:   eventType,
		AggregateID: aggregateID,
		Payload:     payload,
		Status:      OutboxStatusPending,
		Attempts:    0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}
