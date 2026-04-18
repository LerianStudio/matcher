// Package audit provides adapters for publishing exception audit events.
package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const entityTypeException = "exception"

// Sentinel errors for outbox publisher.
var (
	ErrNilOutboxRepository = errors.New("outbox repository is required")
	ErrTenantIDRequired    = errors.New("tenant id required for audit logging")
)

// OutboxPublisher publishes exception audit events to the outbox for asynchronous processing.
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

// PublishExceptionEvent publishes an exception audit event to the outbox.
func (pub *OutboxPublisher) PublishExceptionEvent(ctx context.Context, event ports.AuditEvent) error {
	if pub == nil || pub.outboxRepo == nil {
		return ErrNilOutboxRepository
	}

	outboxEvent, err := pub.buildOutboxEvent(ctx, event)
	if err != nil {
		return err
	}

	if _, err := pub.outboxRepo.Create(ctx, outboxEvent); err != nil {
		return fmt.Errorf("persist outbox event: %w", err)
	}

	return nil
}

// buildOutboxEvent constructs the outbox event payload from an audit event.
func (pub *OutboxPublisher) buildOutboxEvent(
	ctx context.Context,
	event ports.AuditEvent,
) (*sharedDomain.OutboxEvent, error) {
	tenantIDStr := auth.GetTenantID(ctx)
	if tenantIDStr == "" {
		tenantIDStr = auth.GetDefaultTenantID()
	}

	if tenantIDStr == "" {
		return nil, ErrTenantIDRequired
	}

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return nil, fmt.Errorf("parse tenant id: %w", err)
	}

	actorHash := event.GetActorHash()

	var actor *string
	if actorHash != "" {
		actor = &actorHash
	}

	changes := buildOutboxChangesMap(event)

	auditEvent := sharedDomain.AuditLogCreatedEvent{
		UniqueID:   uuid.New(),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   tenantID,
		EntityType: entityTypeException,
		EntityID:   event.ExceptionID,
		Action:     event.Action,
		Actor:      actor,
		Changes:    changes,
		OccurredAt: event.OccurredAt,
		Timestamp:  time.Now().UTC(),
	}

	payload, err := json.Marshal(auditEvent)
	if err != nil {
		return nil, fmt.Errorf("marshal audit event: %w", err)
	}

	outboxEvent, err := sharedDomain.NewOutboxEvent(
		ctx,
		sharedDomain.EventTypeAuditLogCreated,
		event.ExceptionID,
		payload,
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox event: %w", err)
	}

	return outboxEvent, nil
}

// PublishExceptionEventWithTx publishes an exception audit event within the provided
// database transaction, enabling atomic commits with the business operation.
// When tx is non-nil the outbox repository's CreateWithTx is used so that the
// audit row is committed (or rolled back) together with the caller's transaction.
// When tx is nil, a warning is logged and the method falls back to the
// non-transactional PublishExceptionEvent path.
func (pub *OutboxPublisher) PublishExceptionEventWithTx(
	ctx context.Context,
	tx *sql.Tx,
	event ports.AuditEvent,
) error {
	if tx == nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		if logger == nil {
			logger = &libLog.NopLogger{}
		}

		logger.Log(ctx, libLog.LevelWarn,
			"PublishExceptionEventWithTx called with nil tx; falling back to non-transactional publish",
			libLog.String("exception_id", event.ExceptionID.String()),
			libLog.String("action", event.Action),
		)

		return pub.PublishExceptionEvent(ctx, event)
	}

	outboxEvent, err := pub.buildOutboxEvent(ctx, event)
	if err != nil {
		return err
	}

	if _, err := pub.outboxRepo.CreateWithTx(ctx, tx, outboxEvent); err != nil {
		return fmt.Errorf("persist outbox event with tx: %w", err)
	}

	return nil
}

func buildOutboxChangesMap(event ports.AuditEvent) map[string]any {
	actorHash := event.GetActorHash()

	changes := map[string]any{
		"exception_id": event.ExceptionID.String(),
		"action":       event.Action,
		"occurred_at":  event.OccurredAt,
	}

	if actorHash != "" {
		changes["actor_hash"] = actorHash
	}

	if event.Notes != "" {
		changes["notes"] = event.Notes
	}

	if event.ReasonCode != nil {
		changes["reason_code"] = *event.ReasonCode
	}

	if len(event.Metadata) > 0 {
		changes["metadata"] = event.Metadata
	}

	return changes
}

var _ ports.AuditPublisher = (*OutboxPublisher)(nil)
