// Package shared provides shared domain entities for cross-context communication.
package shared

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	// EventTypeMatchConfirmed is the event type for match confirmed events.
	// Used for outbox routing and message queue topics.
	EventTypeMatchConfirmed = "matching.match_confirmed"

	// EventTypeMatchUnmatched is the event type for match unmatched (revocation) events.
	// Published when a previously confirmed match group is revoked, so downstream
	// systems can compensate for the original confirmation.
	EventTypeMatchUnmatched = "matching.match_unmatched"

	// EventTypeAuditLogCreated is the event type for audit log creation events.
	// Used for outbox routing to the governance context.
	EventTypeAuditLogCreated = "governance.audit_log_created"
)

// MatchConfirmedEvent represents a confirmed match event for cross-context communication.
// This shared type allows the outbox context to publish matching events without
// directly importing the matching context.
type MatchConfirmedEvent struct {
	EventType      string      `json:"eventType"`
	TenantID       uuid.UUID   `json:"tenantId"`
	TenantSlug     string      `json:"tenantSlug"` // Optional: may be empty in single-tenant mode
	ContextID      uuid.UUID   `json:"contextId"`
	RunID          uuid.UUID   `json:"runId"`
	MatchID        uuid.UUID   `json:"matchId"`
	RuleID         uuid.UUID   `json:"ruleId"`
	TransactionIDs []uuid.UUID `json:"transactionIds"`
	Confidence     int         `json:"confidence"`
	ConfirmedAt    time.Time   `json:"confirmedAt"`
	Timestamp      time.Time   `json:"timestamp"`
}

// ID returns the stable idempotency identifier for this event.
func (e MatchConfirmedEvent) ID() uuid.UUID {
	return e.MatchID
}

// MatchUnmatchedEvent represents a compensating event when a confirmed match group is revoked.
// Published so downstream systems can undo any actions taken upon the original confirmation.
// This shared type allows the outbox context to publish matching events without
// directly importing the matching context.
type MatchUnmatchedEvent struct {
	EventType      string      `json:"eventType"`
	TenantID       uuid.UUID   `json:"tenantId"`
	TenantSlug     string      `json:"tenantSlug"`
	ContextID      uuid.UUID   `json:"contextId"`
	RunID          uuid.UUID   `json:"runId"`
	MatchID        uuid.UUID   `json:"matchId"`
	RuleID         uuid.UUID   `json:"ruleId"`
	TransactionIDs []uuid.UUID `json:"transactionIds"`
	Reason         string      `json:"reason"`
	Timestamp      time.Time   `json:"timestamp"`
}

// ID returns the stable idempotency identifier for this event.
func (e MatchUnmatchedEvent) ID() uuid.UUID {
	return e.MatchID
}

// MatchEventPublisher publishes match-related events to message queues.
// This interface is defined in shared to allow cross-context access without direct coupling.
type MatchEventPublisher interface {
	PublishMatchConfirmed(ctx context.Context, event *MatchConfirmedEvent) error
	PublishMatchUnmatched(ctx context.Context, event *MatchUnmatchedEvent) error
}

// AuditLogCreatedEvent represents an audit event for cross-context communication.
// This shared type allows contexts to publish audit events without directly
// importing the governance context.
type AuditLogCreatedEvent struct {
	// UniqueID is the per-instance event identifier for idempotency.
	// Each audit event gets its own UUID to avoid collisions when multiple
	// audit entries reference the same entity.
	UniqueID   uuid.UUID      `json:"id"`
	EventType  string         `json:"eventType"`
	TenantID   uuid.UUID      `json:"tenantId"`
	EntityType string         `json:"entityType"`
	EntityID   uuid.UUID      `json:"entityId"`
	Action     string         `json:"action"`
	Actor      *string        `json:"actor,omitempty"`
	Changes    map[string]any `json:"changes,omitempty"`
	OccurredAt time.Time      `json:"occurredAt"`
	Timestamp  time.Time      `json:"timestamp"`
}

// ID returns the per-instance idempotency identifier for this event.
// This uses UniqueID (not EntityID) so that multiple audit entries
// on the same entity do not collide.
func (e AuditLogCreatedEvent) ID() uuid.UUID {
	return e.UniqueID
}

// AuditEventPublisher publishes audit-related events to the outbox.
type AuditEventPublisher interface {
	PublishAuditLogCreated(ctx context.Context, event *AuditLogCreatedEvent) error
}
