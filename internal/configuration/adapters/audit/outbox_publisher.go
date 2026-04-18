// Package audit provides adapters for publishing configuration audit events.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ErrNilOutboxRepository is returned when the outbox repository is nil.
var ErrNilOutboxRepository = errors.New("outbox repository is required")

// maxAuditChangesBytes caps the serialized size of AuditLogCreatedEvent.Changes
// at 900 KiB to stay under the v5 outbox 1 MiB payload cap while leaving headroom
// for envelope fields (tenant id, entity id, actor, timestamps, etc.). When
// Changes exceeds this limit the map is replaced with a truncation marker so
// the triggering business operation does not fail on audit enqueue.
const maxAuditChangesBytes = 900 * 1024

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

	changes := truncateAuditChangesIfTooLarge(ctx, event.Changes, event.EntityID)

	auditEvent := sharedDomain.AuditLogCreatedEvent{
		UniqueID:   uuid.New(),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   tenantID,
		EntityType: event.EntityType,
		EntityID:   event.EntityID,
		Action:     event.Action,
		Actor:      actor,
		Changes:    changes,
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

// truncateAuditChangesIfTooLarge enforces the 1 MiB outbox payload cap on the
// audit event Changes map. When the serialized size exceeds the cap the map is
// replaced with a truncation marker that preserves metadata about the original
// payload so operators can trace truncation events in the audit log. Returning
// a modified map (rather than failing) ensures that a verbose config diff does
// not block the triggering business operation.
func truncateAuditChangesIfTooLarge(
	ctx context.Context,
	changes map[string]any,
	entityID uuid.UUID,
) map[string]any {
	if len(changes) == 0 {
		return changes
	}

	changesBytes, err := json.Marshal(changes)
	if err != nil || len(changesBytes) <= maxAuditChangesBytes {
		return changes
	}

	originalSize := len(changesBytes)

	//nolint:dogsled // only logger needed here; tracer/headerID/metrics are irrelevant for this truncation-warn log line.
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	logger.Log(ctx, libLog.LevelWarn,
		"audit payload truncated due to size cap",
		libLog.String("entity_id", entityID.String()),
		libLog.Int("original_size_bytes", originalSize),
		libLog.Int("max_allowed_bytes", maxAuditChangesBytes),
	)

	return map[string]any{
		"_truncated":    true,
		"_originalSize": originalSize,
		"_note":         "audit diff exceeded 1MiB outbox cap; original not persisted",
		"_maxAllowed":   maxAuditChangesBytes,
	}
}
