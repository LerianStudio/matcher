// Package entities defines outbox domain types and validation logic.
// The canonical type definitions live in the shared kernel (internal/shared/domain)
// and are re-exported here as type aliases for backward compatibility.
package entities

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ErrOutboxEventRequired is returned when an outbox event is nil.
// Re-exported from the shared kernel.
var ErrOutboxEventRequired = sharedDomain.ErrOutboxEventRequired

// OutboxEventStatus is a type alias for the shared kernel typed enum.
// Re-exported here for backward compatibility with outbox-internal consumers.
type OutboxEventStatus = sharedDomain.OutboxEventStatus

// Outbox event status constants re-exported from the shared kernel.
const (
	OutboxStatusPending    = sharedDomain.OutboxStatusPending
	OutboxStatusProcessing = sharedDomain.OutboxStatusProcessing
	OutboxStatusPublished  = sharedDomain.OutboxStatusPublished
	OutboxStatusFailed     = sharedDomain.OutboxStatusFailed
	OutboxStatusInvalid    = sharedDomain.OutboxStatusInvalid
)

// OutboxEvent is a type alias for the shared kernel OutboxEvent.
// All bounded contexts that need outbox types should use the shared kernel directly:
//
//	import sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
//
// This alias exists for backward compatibility with code that already imports
// this package. No new code should import outbox/domain/entities from outside
// the outbox bounded context.
type OutboxEvent = sharedDomain.OutboxEvent

// NewOutboxEvent creates a new OutboxEvent. Delegates to the shared kernel constructor.
func NewOutboxEvent(ctx context.Context, eventType string, tenantID uuid.UUID, payload []byte) (*OutboxEvent, error) {
	event, err := sharedDomain.NewOutboxEvent(ctx, eventType, tenantID, payload)
	if err != nil {
		return nil, fmt.Errorf("new outbox event: %w", err)
	}

	return event, nil
}
