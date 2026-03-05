// Package audit provides adapters for publishing configuration audit events.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ErrNilOutboxRepository is returned when the outbox repository is nil.
var ErrNilOutboxRepository = errors.New("outbox repository is required")

// OutboxPublisher publishes configuration audit events to the outbox for asynchronous processing.
type OutboxPublisher struct {
	outboxRepo sharedPorts.OutboxRepository
}

// NewOutboxPublisher creates a new outbox-based audit publisher.
func NewOutboxPublisher(repo sharedPorts.OutboxRepository) (*OutboxPublisher, error) {
	if repo == nil {
		return nil, ErrNilOutboxRepository
	}

	return &OutboxPublisher{outboxRepo: repo}, nil
}

// Publish publishes a configuration audit event to the outbox.
func (pub *OutboxPublisher) Publish(ctx context.Context, event ports.AuditEvent) error {
	if pub == nil || pub.outboxRepo == nil {
		return ErrNilOutboxRepository
	}

	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return fmt.Errorf("parse tenant id: %w", err)
	}

	var actor *string
	if event.Actor != "" {
		actor = &event.Actor
	}

	auditEvent := sharedDomain.AuditLogCreatedEvent{
		UniqueID:   uuid.New(),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   tenantID,
		EntityType: event.EntityType,
		EntityID:   event.EntityID,
		Action:     event.Action,
		Actor:      actor,
		Changes:    event.Changes,
		OccurredAt: event.OccurredAt,
		Timestamp:  time.Now().UTC(),
	}

	payload, err := json.Marshal(auditEvent)
	if err != nil {
		return fmt.Errorf("marshal audit event: %w", err)
	}

	outboxEvent, err := sharedDomain.NewOutboxEvent(
		ctx,
		sharedDomain.EventTypeAuditLogCreated,
		event.EntityID,
		payload,
	)
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	if _, err := pub.outboxRepo.Create(ctx, outboxEvent); err != nil {
		return fmt.Errorf("persist outbox event: %w", err)
	}

	return nil
}

var _ ports.AuditPublisher = (*OutboxPublisher)(nil)
