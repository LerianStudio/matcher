// Package entities defines outbox domain types and validation logic.
package entities

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// ErrOutboxEventRequired is returned when an outbox event is nil.
var ErrOutboxEventRequired = errors.New("outbox event is required")

// Outbox event status constants.
const (
	OutboxStatusPending    = "PENDING"
	OutboxStatusProcessing = "PROCESSING"
	OutboxStatusPublished  = "PUBLISHED"
	OutboxStatusFailed     = "FAILED"
	OutboxStatusInvalid    = "INVALID" // Permanent validation failures (non-retryable)
)

// OutboxEvent represents an event stored in the outbox for reliable delivery.
type OutboxEvent struct {
	ID          uuid.UUID
	EventType   string
	AggregateID uuid.UUID
	Payload     []byte
	Status      string
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
