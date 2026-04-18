// Package shared provides shared domain types used across bounded contexts.
package shared

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// OutboxEvent is a type alias for the canonical lib-commons/v5 outbox event.
// All bounded contexts use this alias so the canonical outbox internals
// remain transparent to callers.
type OutboxEvent = outbox.OutboxEvent

// OutboxEventStatus is a type alias for the canonical outbox status type.
type OutboxEventStatus = outbox.OutboxEventStatus

// Outbox event status constants re-exported from the canonical package.
const (
	OutboxStatusPending    = outbox.OutboxStatusPending
	OutboxStatusProcessing = outbox.OutboxStatusProcessing
	OutboxStatusPublished  = outbox.OutboxStatusPublished
	OutboxStatusFailed     = outbox.OutboxStatusFailed
	OutboxStatusInvalid    = outbox.OutboxStatusInvalid
)

// Sentinel errors re-exported from the canonical package for backward compatibility.
var (
	ErrOutboxEventRequired           = outbox.ErrOutboxEventRequired
	ErrOutboxEventPayloadInvalidJSON = outbox.ErrOutboxEventPayloadNotJSON
)

// ParseOutboxEventStatus validates and converts a raw status string into the
// typed outbox lifecycle used by lib-commons.
func ParseOutboxEventStatus(raw string) (OutboxEventStatus, error) {
	return outbox.ParseOutboxEventStatus(raw)
}

// ValidateOutboxTransition validates a lifecycle transition using the canonical
// typed outbox status rules.
func ValidateOutboxTransition(fromRaw, toRaw string) error {
	return outbox.ValidateOutboxTransition(fromRaw, toRaw)
}

// OutboxStatusOf returns the typed lifecycle status for an event.
func OutboxStatusOf(event *OutboxEvent) (OutboxEventStatus, error) {
	if event == nil {
		return "", ErrOutboxEventRequired
	}

	return ParseOutboxEventStatus(event.Status)
}

// NewOutboxEvent creates a new OutboxEvent via the canonical lib-commons constructor.
// This is a thin wrapper that preserves the existing call-site signature used
// throughout matcher's bounded contexts.
func NewOutboxEvent(
	ctx context.Context,
	eventType string,
	aggregateID uuid.UUID,
	payload []byte,
) (*OutboxEvent, error) {
	event, err := outbox.NewOutboxEvent(ctx, eventType, aggregateID, payload)
	if err != nil {
		return nil, fmt.Errorf("new outbox event: %w", err)
	}

	return event, nil
}
