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

// auditExceptionEntityType labels truncation metrics emitted from this
// publisher so operators can distinguish exception-diff truncation from
// the configuration-context publisher.
const auditExceptionEntityType = "audit_exception"

// Sentinel errors for outbox publisher.
var (
	ErrNilOutboxRepository = errors.New("outbox repository is required")
	ErrTenantIDRequired    = errors.New("tenant id required for audit logging")
)

// OutboxPublisher publishes exception audit events to the outbox for
// asynchronous processing. The optional saltProvider keys the actor hash
// via HMAC-SHA-256; when nil or returning an empty string, actor hashes
// degrade to unsalted SHA-256 truncations for backwards compatibility.
// See ports.HashActor for the exact semantics.
type OutboxPublisher struct {
	outboxRepo   sharedPorts.OutboxRepository
	saltProvider ports.SaltProvider
}

// PublisherOption configures NewOutboxPublisher.
type PublisherOption func(*OutboxPublisher)

// WithSaltProvider wires a SaltProvider into the publisher so that actor
// hashes written to the audit outbox are HMAC-keyed by the provider's
// tenant-aware salt lookup. Passing a nil provider is a no-op; hashes
// remain unsalted. Callers should register a real provider in production
// to close the offline rainbow-table attack vector on exfiltrated audit
// rows.
func WithSaltProvider(provider ports.SaltProvider) PublisherOption {
	return func(pub *OutboxPublisher) {
		pub.saltProvider = provider
	}
}

// NewOutboxPublisher creates a new outbox-based audit publisher.
func NewOutboxPublisher(repo sharedPorts.OutboxRepository, opts ...PublisherOption) (*OutboxPublisher, error) {
	if repo == nil {
		return nil, ErrNilOutboxRepository
	}

	pub := &OutboxPublisher{outboxRepo: repo}

	for _, opt := range opts {
		if opt != nil {
			opt(pub)
		}
	}

	return pub, nil
}

// resolveSalt returns the salt for ctx via the configured provider. Nil or
// missing providers yield an empty string, which HashActor treats as the
// unsalted fallback.
func (pub *OutboxPublisher) resolveSalt(ctx context.Context) string {
	if pub == nil || pub.saltProvider == nil {
		return ""
	}

	return pub.saltProvider.SaltFor(ctx)
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

// buildOutboxEvent constructs the outbox event payload from an audit
// event using the single-marshal gating strategy: the complete
// AuditLogCreatedEvent is serialized once, and only if the resulting
// payload exceeds the broker's per-event cap does Changes get swapped
// for a truncation marker and re-marshaled.
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

	salt := pub.resolveSalt(ctx)

	actorHash := event.ResolveActorHash(salt)

	var actor *string
	if actorHash != "" {
		actor = &actorHash
	}

	auditEvent := sharedDomain.AuditLogCreatedEvent{
		UniqueID:   uuid.New(),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   tenantID,
		EntityType: entityTypeException,
		EntityID:   event.ExceptionID,
		Action:     event.Action,
		Actor:      actor,
		Changes:    buildOutboxChangesMap(event, salt),
		OccurredAt: event.OccurredAt,
		Timestamp:  time.Now().UTC(),
	}

	payload, err := marshalOrTruncate(ctx, &auditEvent)
	if err != nil {
		return nil, err
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

func buildOutboxChangesMap(event ports.AuditEvent, salt string) map[string]any {
	actorHash := event.ResolveActorHash(salt)

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

// marshalOrTruncate serializes the audit event and, if the resulting
// payload exceeds the broker's cap, replaces Changes with a truncation
// marker and re-marshals. The final payload is returned or a marshal
// error wrapped for the caller.
func marshalOrTruncate(
	ctx context.Context,
	event *sharedDomain.AuditLogCreatedEvent,
) ([]byte, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal audit event: %w", err)
	}

	if len(payload) <= sharedDomain.DefaultOutboxMaxPayloadBytes {
		return payload, nil
	}

	originalSize := len(payload)

	sharedDomain.RecordOutboxTruncation(
		ctx,
		auditExceptionEntityType,
		event.EntityID,
		originalSize,
		sharedDomain.DefaultOutboxMaxPayloadBytes,
	)

	event.Changes = sharedDomain.BuildAuditChangesTruncationMarker(
		originalSize,
		sharedDomain.DefaultOutboxMaxPayloadBytes,
	)

	payload, err = json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal truncated audit event: %w", err)
	}

	return payload, nil
}
