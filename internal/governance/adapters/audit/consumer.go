// Package audit provides adapters for consuming audit events.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// defaultDedupWindow is the default time window used to detect duplicate event deliveries.
// The outbox pattern may deliver the same event twice on retry; if an audit log
// with the same entity_type + entity_id + action exists within this window,
// the duplicate is silently skipped.
//
// TUNING: This value should be greater than the outbox dispatcher's retry interval
// (currently configured via IDEMPOTENCY_RETRY_WINDOW). If you change the retry
// configuration, review this constant. The fail-open design ensures that if the
// window is too short, the worst case is a duplicate audit entry (harmless for
// compliance) rather than a missed event.
const defaultDedupWindow = 5 * time.Second

const dedupScanLimit = 10

// Sentinel errors for audit consumer validation.
var (
	ErrNilAuditRepository = errors.New("audit log repository is required")
	ErrAuditEventRequired = errors.New("audit event is required")
)

// Consumer processes audit events and persists them to the audit log.
type Consumer struct {
	auditLogRepo repositories.AuditLogRepository
	dedupWindow  time.Duration
}

// ConsumerConfig controls consumer behavior.
type ConsumerConfig struct {
	DedupWindow time.Duration
}

// NewConsumer creates a new audit event consumer.
func NewConsumer(repo repositories.AuditLogRepository, cfgOpt ...ConsumerConfig) (*Consumer, error) {
	if repo == nil {
		return nil, ErrNilAuditRepository
	}

	cfg := ConsumerConfig{DedupWindow: defaultDedupWindow}
	if len(cfgOpt) > 0 {
		cfg = cfgOpt[0]
		if cfg.DedupWindow <= 0 {
			cfg.DedupWindow = defaultDedupWindow
		}
	}

	return &Consumer{auditLogRepo: repo, dedupWindow: cfg.DedupWindow}, nil
}

// PublishAuditLogCreated processes an audit log created event and persists it.
// This implements sharedDomain.AuditEventPublisher.
func (consumer *Consumer) PublishAuditLogCreated(
	ctx context.Context,
	event *sharedDomain.AuditLogCreatedEvent,
) error {
	if consumer == nil || consumer.auditLogRepo == nil {
		return ErrNilAuditRepository
	}

	if event == nil {
		return ErrAuditEventRequired
	}

	ctx = context.WithValue(ctx, auth.TenantIDKey, event.TenantID.String())

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("commons.noop")
	}

	ctx, span := tracer.Start(ctx, "governance.audit.publish_created")
	defer span.End()

	// Idempotency guard: the outbox pattern may deliver the same event twice on retry.
	// Check if an audit log with the same entity + action was recently created.
	// This uses a small time window to avoid false positives from legitimate rapid operations.
	if consumer.isDuplicateDelivery(ctx, logger, event) {
		logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("skipping duplicate audit event for entity %s/%s action=%s",
			event.EntityType, event.EntityID, event.Action))

		return nil
	}

	changes, err := buildChangesPayload(event)
	if err != nil {
		wrappedErr := fmt.Errorf("build changes payload: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to build changes payload", wrappedErr)

		return wrappedErr
	}

	auditLog, err := entities.NewAuditLog(
		ctx,
		event.TenantID,
		event.EntityType,
		event.EntityID,
		event.Action,
		event.Actor,
		changes,
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create audit log entity: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create audit log entity", wrappedErr)

		return wrappedErr
	}

	if _, err := consumer.auditLogRepo.Create(ctx, auditLog); err != nil {
		wrappedErr := fmt.Errorf("persist audit log: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to persist audit log", wrappedErr)

		return wrappedErr
	}

	return nil
}

// isDuplicateDelivery checks whether a matching audit log was recently persisted.
// It queries the most recent audit log for the same entity and compares the action
// and timestamp. If the latest entry matches within the dedup window, the event is
// considered a duplicate delivery and should be skipped.
//
// Errors from the query are intentionally swallowed — a failed dedup check must not
// prevent a legitimate audit event from being persisted.
func (consumer *Consumer) isDuplicateDelivery(
	ctx context.Context,
	logger libLog.Logger,
	event *sharedDomain.AuditLogCreatedEvent,
) bool {
	recent, _, err := consumer.auditLogRepo.ListByEntity(
		ctx, event.EntityType, event.EntityID, nil, dedupScanLimit,
	)
	if err != nil {
		if logger != nil {
			logger.Log(
				ctx,
				libLog.LevelWarn,
				fmt.Sprintf("dedup check failed for entity %s/%s action=%s: %v", event.EntityType, event.EntityID, event.Action, err),
			)
		}

		return false
	}

	if len(recent) == 0 {
		return false
	}

	now := time.Now().UTC()

	for _, candidate := range recent {
		if candidate == nil {
			continue
		}

		if now.Sub(candidate.CreatedAt) >= consumer.dedupWindow {
			// Results are sorted descending by CreatedAt, so once we are outside
			// the dedup window all subsequent rows are also outside the window.
			break
		}

		if candidate.Action == event.Action {
			return true
		}
	}

	return false
}

func buildChangesPayload(event *sharedDomain.AuditLogCreatedEvent) ([]byte, error) {
	payload := map[string]any{
		"entity_type": event.EntityType,
		"entity_id":   event.EntityID.String(),
		"action":      event.Action,
		"occurred_at": event.OccurredAt,
	}

	if event.Actor != nil && *event.Actor != "" {
		payload["actor"] = *event.Actor
	}

	if event.Changes != nil {
		payload["changes"] = event.Changes
	}

	return json.Marshal(payload)
}

var _ sharedDomain.AuditEventPublisher = (*Consumer)(nil)
