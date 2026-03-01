// Package rabbitmq provides RabbitMQ-based event publishing for matching.
//
// # Event Flow (Outbound Only)
//
// Matcher publishes domain events to RabbitMQ for external consumers. This is a
// fire-and-forget pattern -- Matcher does NOT consume messages from these queues.
//
// # Published Events
//
// All events are published to the "matcher.events" topic exchange with routing keys
// following the "<context>.<action>" convention.
//
// ## matching.match_confirmed (routing key: "matching.match_confirmed")
//
// Published when transactions are successfully matched/reconciled (auto or manual).
//
// Payload (MatchConfirmedEvent):
//
//	{
//	  "eventType":      "matching.match_confirmed",
//	  "tenantId":       "<uuid>",
//	  "tenantSlug":     "<string>",        // may be empty in single-tenant mode
//	  "contextId":      "<uuid>",          // reconciliation context
//	  "runId":          "<uuid>",          // match run that produced the group
//	  "matchId":        "<uuid>",          // match group ID (also the idempotency key)
//	  "ruleId":         "<uuid>",          // rule that produced the match (Nil for manual)
//	  "transactionIds": ["<uuid>", ...],   // sorted list of matched transaction IDs
//	  "confidence":     <int 0-100>,       // confidence score from rule evaluation
//	  "confirmedAt":    "<RFC3339>",       // when the match was confirmed
//	  "timestamp":      "<RFC3339>"        // event creation timestamp (UTC)
//	}
//
// ## matching.match_unmatched (routing key: "matching.match_unmatched")
//
// Published when a previously confirmed match group is revoked (unmatch operation).
// Downstream systems should use this as a compensating event to undo any actions
// taken upon the original match_confirmed event.
//
// Payload (MatchUnmatchedEvent):
//
//	{
//	  "eventType":      "matching.match_unmatched",
//	  "tenantId":       "<uuid>",
//	  "tenantSlug":     "<string>",
//	  "contextId":      "<uuid>",
//	  "runId":          "<uuid>",
//	  "matchId":        "<uuid>",          // same match group ID as the original confirmation
//	  "ruleId":         "<uuid>",
//	  "transactionIds": ["<uuid>", ...],
//	  "reason":         "<string>",        // human-readable revocation reason (max 1024 chars)
//	  "timestamp":      "<RFC3339>"
//	}
//
// # AMQP Headers
//
// Every published message includes:
//   - idempotency_key: The match group UUID, ensuring exactly-once processing
//   - traceparent / tracestate: W3C Trace Context for distributed tracing
//   - DeliveryMode: Persistent (survives broker restarts)
//   - ContentType: application/json
//
// # Known / Planned Consumers
//
// The following Lerian ecosystem services are known or planned consumers of these events.
// Integration status is tracked per-service.
//
//   - Midaz (ledger service): Marks ledger transactions as reconciled upon match_confirmed;
//     reverses reconciliation marks upon match_unmatched. Status: planned.
//   - Settlement service: Initiates fund transfer workflows for confirmed matches.
//     Status: planned (TBD -- depends on settlement service roadmap).
//   - Webhook dispatcher: Forwards match events to external partner callback URLs.
//     Status: planned (TBD -- depends on notification service availability).
//   - Analytics / BI pipeline: Ingests reconciliation metrics for dashboards and reporting.
//     Status: planned (TBD -- may consume directly or via CDC).
//
// Last reviewed: 2026-02
//
// # Dead Letter Queue
//
// Messages that fail consumer processing are routed to "matcher.events.dlx" exchange
// and land in the "matcher.events.dlq" queue via a catch-all "#" binding. See
// internal/shared/adapters/rabbitmq/dlq.go for DLQ topology.
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"
	libRabbitmq "github.com/LerianStudio/lib-uncommons/v2/uncommons/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

var (
	errRabbitMQChannelRequired        = errors.New("rabbitmq channel is required")
	errPublisherNotInit               = errors.New("rabbitmq publisher not initialized")
	errEventRequired                  = errors.New("match confirmed event is required")
	errUnmatchedEventRequired         = errors.New("match unmatched event is required")
	errIdempotencyKeyRequired         = errors.New("idempotency key is required")
	errConfirmableSetupFailed         = errors.New("failed to setup confirmable publisher")
	errTenantIDRequiredForMultiTenant = errors.New("tenant ID is required in multi-tenant mode")
	errRabbitMQManagerRequired        = errors.New("rabbitmq multi-tenant manager is required")
)

const (
	routingKeyMatchConfirmed = sharedDomain.EventTypeMatchConfirmed
	routingKeyMatchUnmatched = sharedDomain.EventTypeMatchUnmatched
)

type amqpChannel interface {
	ExchangeDeclare(
		name, kind string,
		durable, autoDelete, internal, noWait bool,
		args amqp.Table,
	) error
	PublishWithContext(
		ctx context.Context,
		exchange, key string,
		mandatory, immediate bool,
		msg amqp.Publishing,
	) error
}

// EventPublisher publishes matching events to RabbitMQ with publisher confirms.
// It supports both single-tenant mode (using a shared confirmable publisher) and
// multi-tenant mode (using per-tenant vhost isolation via RabbitMQMultiTenantManager).
type EventPublisher struct {
	conn                 *libRabbitmq.RabbitMQConnection
	ch                   amqpChannel
	confirmablePublisher *sharedRabbitmq.ConfirmablePublisher

	// rabbitmqManager provides per-tenant channels in multi-tenant mode.
	// Nil when operating in single-tenant mode.
	rabbitmqManager sharedRabbitmq.RabbitMQMultiTenantManager

	propagator propagation.TextMapPropagator
}

// NewEventPublisherFromChannel creates a matching event publisher using a dedicated AMQP
// channel. Each publisher MUST own its own channel because AMQP publisher confirms are
// channel-scoped. Sharing a channel between publishers corrupts delivery tag tracking
// and leads to silent message loss.
//
// The caller is responsible for opening the channel (e.g., conn.Connection.Channel())
// and closing it when the publisher is no longer needed.
//
// This constructor is used in single-tenant mode. For multi-tenant mode with
// vhost isolation, use NewEventPublisherMultiTenant instead.
func NewEventPublisherFromChannel(
	ch *amqp.Channel,
	opts ...sharedRabbitmq.ConfirmablePublisherOption,
) (*EventPublisher, error) {
	if ch == nil {
		return nil, errRabbitMQChannelRequired
	}

	confirmablePublisher, err := sharedRabbitmq.NewConfirmablePublisherFromChannel(ch, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errConfirmableSetupFailed, err)
	}

	return newEventPublisher(nil, ch, otel.GetTextMapPropagator(), confirmablePublisher)
}

// NewEventPublisherMultiTenant creates an event publisher for multi-tenant mode
// using per-tenant vhost isolation. Each tenant's messages are published to their
// own RabbitMQ vhost via the provided manager.
//
// In this mode, broker confirms are not used because each publish gets a fresh
// channel from the tenant pool. The X-Tenant-ID header is always included for
// audit and tracing purposes.
//
// Layer 1 (Vhost Isolation): Provided by RabbitMQMultiTenantManager.GetChannel()
// Layer 2 (Audit Header): X-Tenant-ID header added to all messages.
func NewEventPublisherMultiTenant(manager sharedRabbitmq.RabbitMQMultiTenantManager) (*EventPublisher, error) {
	if manager == nil {
		return nil, errRabbitMQManagerRequired
	}

	return &EventPublisher{
		rabbitmqManager: manager,
		propagator:      otel.GetTextMapPropagator(),
	}, nil
}

func newEventPublisher(
	conn *libRabbitmq.RabbitMQConnection,
	ch amqpChannel,
	propagator propagation.TextMapPropagator,
	confirmablePublisher *sharedRabbitmq.ConfirmablePublisher,
) (*EventPublisher, error) {
	if ch == nil {
		return nil, errRabbitMQChannelRequired
	}

	if propagator == nil {
		propagator = otel.GetTextMapPropagator()
	}

	if err := ch.ExchangeDeclare(sharedRabbitmq.ExchangeName, sharedRabbitmq.ExchangeType, true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	dlqChannel, ok := ch.(sharedRabbitmq.AMQPChannel)
	if ok {
		if err := sharedRabbitmq.DeclareDLQTopology(dlqChannel); err != nil {
			return nil, fmt.Errorf("failed to declare dlq topology: %w", err)
		}
	}

	return &EventPublisher{
		conn:                 conn,
		ch:                   ch,
		confirmablePublisher: confirmablePublisher,
		propagator:           propagator,
	}, nil
}

// PublishMatchConfirmed publishes a MatchConfirmed event with broker confirmation.
func (publisher *EventPublisher) PublishMatchConfirmed(
	ctx context.Context,
	event *sharedDomain.MatchConfirmedEvent,
) error {
	if publisher == nil {
		return errPublisherNotInit
	}

	// Must have either single-tenant publisher or multi-tenant manager
	if publisher.confirmablePublisher == nil && publisher.rabbitmqManager == nil {
		return errPublisherNotInit
	}

	if event == nil {
		return errEventRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	span := trace.SpanFromContext(ctx)
	if tracer != nil {
		ctx, span = tracer.Start(ctx, "rabbitmq.publish_matching_event")
		defer span.End()
	}

	body, err := json.Marshal(event)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to marshal match event", err)
		return fmt.Errorf("failed to marshal match event: %w", err)
	}

	idempotencyKey := event.ID()
	if idempotencyKey == uuid.Nil {
		return errIdempotencyKeyRequired
	}

	headers := amqp.Table{
		"idempotency_key": idempotencyKey.String(),
	}

	// Layer 2: Inject X-Tenant-ID header for audit/tracing (always, when available)
	tenantID := strings.TrimSpace(auth.GetTenantID(ctx))
	if tenantID != "" {
		headers["X-Tenant-ID"] = tenantID
	}

	// Inject trace context
	carrier := propagation.MapCarrier{}
	publisher.propagator.Inject(ctx, carrier)

	for k, v := range carrier {
		headers[k] = v
	}

	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		MessageId:    idempotencyKey.String(),
		Headers:      headers,
	}

	// Layer 1: Choose publish path based on mode
	if publisher.rabbitmqManager != nil {
		return publisher.publishMultiTenant(ctx, routingKeyMatchConfirmed, tenantID, msg, span, logger, event.MatchID.String())
	}

	// Single-tenant mode: use confirmable publisher
	if err := publisher.confirmablePublisher.Publish(ctx, sharedRabbitmq.ExchangeName, routingKeyMatchConfirmed, false, false, msg); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to publish match event with confirm", err)

		logger.With(libLog.Any("exchange", sharedRabbitmq.ExchangeName), libLog.Any("routing_key", routingKeyMatchConfirmed), libLog.Any("message_id", msg.MessageId), libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to publish match event")

		return fmt.Errorf("failed to publish match event: %w", err)
	}

	logger.With(libLog.Any("exchange", sharedRabbitmq.ExchangeName), libLog.Any("routing_key", routingKeyMatchConfirmed), libLog.Any("message_id", msg.MessageId), libLog.Any("match_id", event.MatchID.String())).Log(ctx, libLog.LevelDebug, "published match confirmed event")

	return nil
}

// PublishMatchUnmatched publishes a MatchUnmatched (revocation) event with broker confirmation.
func (publisher *EventPublisher) PublishMatchUnmatched(
	ctx context.Context,
	event *sharedDomain.MatchUnmatchedEvent,
) error {
	if publisher == nil {
		return errPublisherNotInit
	}

	// Must have either single-tenant publisher or multi-tenant manager
	if publisher.confirmablePublisher == nil && publisher.rabbitmqManager == nil {
		return errPublisherNotInit
	}

	if event == nil {
		return errUnmatchedEventRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	span := trace.SpanFromContext(ctx)
	if tracer != nil {
		ctx, span = tracer.Start(ctx, "rabbitmq.publish_match_unmatched_event")
		defer span.End()
	}

	body, err := json.Marshal(event)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to marshal match unmatched event", err)
		return fmt.Errorf("failed to marshal match unmatched event: %w", err)
	}

	idempotencyKey := event.ID()
	if idempotencyKey == uuid.Nil {
		return errIdempotencyKeyRequired
	}

	headers := amqp.Table{
		"idempotency_key": idempotencyKey.String(),
	}

	// Layer 2: Inject X-Tenant-ID header for audit/tracing (always, when available)
	tenantID := strings.TrimSpace(auth.GetTenantID(ctx))
	if tenantID != "" {
		headers["X-Tenant-ID"] = tenantID
	}

	// Inject trace context
	carrier := propagation.MapCarrier{}
	publisher.propagator.Inject(ctx, carrier)

	for k, v := range carrier {
		headers[k] = v
	}

	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		MessageId:    idempotencyKey.String(),
		Headers:      headers,
	}

	// Layer 1: Choose publish path based on mode
	if publisher.rabbitmqManager != nil {
		return publisher.publishMultiTenant(ctx, routingKeyMatchUnmatched, tenantID, msg, span, logger, event.MatchID.String())
	}

	// Single-tenant mode: use confirmable publisher
	if err := publisher.confirmablePublisher.Publish(ctx, sharedRabbitmq.ExchangeName, routingKeyMatchUnmatched, false, false, msg); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to publish match unmatched event with confirm", err)

		logger.With(libLog.Any("exchange", sharedRabbitmq.ExchangeName), libLog.Any("routing_key", routingKeyMatchUnmatched), libLog.Any("message_id", msg.MessageId), libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to publish match unmatched event")

		return fmt.Errorf("failed to publish match unmatched event: %w", err)
	}

	logger.With(libLog.Any("exchange", sharedRabbitmq.ExchangeName), libLog.Any("routing_key", routingKeyMatchUnmatched), libLog.Any("message_id", msg.MessageId), libLog.Any("match_id", event.MatchID.String())).Log(ctx, libLog.LevelDebug, "published match unmatched event")

	return nil
}

// publishMultiTenant publishes a message using the multi-tenant manager (vhost isolation).
func (publisher *EventPublisher) publishMultiTenant(
	ctx context.Context,
	routingKey string,
	tenantID string,
	msg amqp.Publishing,
	span trace.Span,
	logger libLog.Logger,
	matchID string,
) error {
	if tenantID == "" {
		return errTenantIDRequiredForMultiTenant
	}

	// Get tenant-specific channel from the manager
	ch, err := publisher.rabbitmqManager.GetChannel(ctx, tenantID)
	if err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to get tenant channel", err)
		}

		if logger != nil {
			logger.With(
				libLog.Any("error", err.Error()),
				libLog.Any("tenant_id", tenantID),
			).Log(ctx, libLog.LevelError, "failed to get tenant channel")
		}

		return fmt.Errorf("get tenant channel: %w", err)
	}

	// Declare exchange on tenant's vhost (idempotent operation)
	if err := ch.ExchangeDeclare(
		sharedRabbitmq.ExchangeName,
		sharedRabbitmq.ExchangeType,
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,
	); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to declare exchange on tenant channel", err)
		}

		return fmt.Errorf("declare exchange on tenant channel: %w", err)
	}

	// Publish to tenant's vhost
	if err := ch.PublishWithContext(
		ctx,
		sharedRabbitmq.ExchangeName,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to publish to tenant vhost", err)
		}

		if logger != nil {
			logger.With(
				libLog.Any("error", err.Error()),
				libLog.Any("tenant_id", tenantID),
				libLog.Any("routing_key", routingKey),
			).Log(ctx, libLog.LevelError, "failed to publish to tenant vhost")
		}

		return fmt.Errorf("publish to tenant vhost: %w", err)
	}

	if logger != nil {
		logger.With(
			libLog.Any("exchange", sharedRabbitmq.ExchangeName),
			libLog.Any("routing_key", routingKey),
			libLog.Any("message_id", msg.MessageId),
			libLog.Any("match_id", matchID),
			libLog.Any("tenant_id", tenantID),
		).Log(ctx, libLog.LevelDebug, "published match event to tenant vhost")
	}

	return nil
}

// Close gracefully stops the internal confirmable publisher.
// In multi-tenant mode (rabbitmqManager != nil), this is a no-op since
// channels are managed by the tenant manager pool.
func (publisher *EventPublisher) Close() error {
	if publisher == nil {
		return nil
	}

	// Multi-tenant mode: channels are managed by the pool, nothing to close
	if publisher.rabbitmqManager != nil {
		return nil
	}

	// Single-tenant mode: close the confirmable publisher
	if publisher.confirmablePublisher == nil {
		return nil
	}

	if err := publisher.confirmablePublisher.Close(); err != nil {
		return fmt.Errorf("confirmable publisher close: %w", err)
	}

	return nil
}

var _ matchingPorts.MatchEventPublisher = (*EventPublisher)(nil)
