// Package ports defines outbound interfaces for the configuration bounded context.
package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
)

//go:generate mockgen -destination=mocks/audit_publisher_mock.go -package=mocks . AuditPublisher

// AuditEvent represents an audit event for configuration operations.
type AuditEvent struct {
	EntityType string
	EntityID   uuid.UUID
	Action     string
	Actor      string
	OccurredAt time.Time
	Changes    map[string]any
}

// AuditPublisher publishes configuration audit events.
type AuditPublisher interface {
	Publish(ctx context.Context, event AuditEvent) error
}
