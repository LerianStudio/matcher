// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

var (
	errRabbitMQChannelRequired = errors.New("rabbitmq channel is required")
	errRabbitMQManagerRequired = errors.New("rabbitmq tenant manager is required")
	errPublisherNotInit        = errors.New("rabbitmq publisher not initialized")
	errEventRequired           = errors.New("match confirmed event is required")
	errUnmatchedEventRequired  = errors.New("match unmatched event is required")
	errIdempotencyKeyRequired  = errors.New("idempotency key is required")
	errConfirmableSetupFailed  = errors.New("failed to setup confirmable publisher")
	errTenantIDRequired        = errors.New("tenant ID is required in multi-tenant mode")
	errBrokerNacked            = errors.New("broker nacked publish")
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
//
// Two operational modes are supported:
//
// Single-tenant mode (default): Uses a static AMQP channel via ConfirmablePublisher.
// All messages go through the same connection and vhost.
//
// Multi-tenant mode: Uses tmrabbitmq.Manager for Layer 1 vhost isolation.
// Each publish call resolves a tenant-specific channel from a per-tenant vhost connection.
// Layer 2 (X-Tenant-ID header) is also active, providing defense-in-depth.
type EventPublisher struct {
	conn                 *libRabbitmq.RabbitMQConnection
	ch                   amqpChannel
	confirmablePublisher *sharedRabbitmq.ConfirmablePublisher
	propagator           propagation.TextMapPropagator
	rmqManager           *tmrabbitmq.Manager // per-tenant vhost manager (nil in single-tenant mode)
	multiTenant          bool                // true when using per-tenant vhost isolation
	declaredExchanges    sync.Map            // tracks tenant exchanges already declared (key: "tenantID:exchange")
}

// NewEventPublisherFromChannel creates a matching event publisher using a dedicated AMQP
// channel. Each publisher MUST own its own channel because AMQP publisher confirms are
// channel-scoped. Sharing a channel between publishers corrupts delivery tag tracking
// and leads to silent message loss.
//
// The caller is responsible for opening the channel (e.g., conn.Connection.Channel())
// and closing it when the publisher is no longer needed.
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

// NewMultiTenantEventPublisher creates a matching event publisher that uses
// per-tenant RabbitMQ vhosts for Layer 1 isolation. Each publish call resolves
// a tenant-specific channel via the tmrabbitmq.Manager.
//
// Layer 2 (X-Tenant-ID header) is also active, providing defense-in-depth.
// The manager's connection pool handles connection reuse across publish calls.
func NewMultiTenantEventPublisher(mgr *tmrabbitmq.Manager) (*EventPublisher, error) {
	if mgr == nil {
		return nil, errRabbitMQManagerRequired
	}

	return &EventPublisher{
		rmqManager:  mgr,
		multiTenant: true,
		propagator:  otel.GetTextMapPropagator(),
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

// publishMultiTenant resolves a per-tenant channel from the vhost manager and publishes
// a single message. The channel is opened and closed per-publish to avoid long-lived
// channel state management (the Manager's connection pool handles connection reuse).
//
// Exchange declarations are cached per tenant:exchange pair using sync.Map to avoid
// redundant broker round-trips. The declaration is only performed on first use per
// tenant exchange; subsequent publishes skip the ExchangeDeclare call entirely.
func (publisher *EventPublisher) publishMultiTenant(
	ctx context.Context,
	tenantID string,
	exchange, routingKey string,
	msg amqp.Publishing,
) (err error) {
	ch, chErr := publisher.rmqManager.GetChannel(ctx, tenantID)
	if chErr != nil {
		return fmt.Errorf("get tenant channel: %w", chErr)
	}

	defer func() {
		if closeErr := ch.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close tenant channel: %w", closeErr)
		}
	}()

	// Enable publisher confirms for delivery guarantee parity with single-tenant path.
	if confirmErr := ch.Confirm(false); confirmErr != nil {
		return fmt.Errorf("enable confirm mode on tenant channel: %w", confirmErr)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// Declare exchange and DLQ topology on the tenant vhost channel only on first
	// use per tenant. ExchangeDeclare is idempotent but incurs a broker round-trip;
	// caching avoids this overhead on every publish. The cache is stored AFTER
	// successful declaration to prevent a concurrent goroutine from skipping
	// declaration for an undeclared exchange.
	cacheKey := tenantID + ":" + exchange
	if _, loaded := publisher.declaredExchanges.Load(cacheKey); !loaded {
		if declareErr := ch.ExchangeDeclare(exchange, sharedRabbitmq.ExchangeType, true, false, false, false, nil); declareErr != nil {
			return fmt.Errorf("declare exchange on tenant vhost: %w", declareErr)
		}

		if dlqErr := sharedRabbitmq.DeclareDLQTopology(ch); dlqErr != nil {
			return fmt.Errorf("declare DLQ topology on tenant vhost: %w", dlqErr)
		}

		publisher.declaredExchanges.Store(cacheKey, true)
	}

	if pubErr := ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg); pubErr != nil {
		return fmt.Errorf("publish to exchange %q on tenant %s: %w", exchange, tenantID, pubErr)
	}

	// Wait for broker confirmation.
	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("exchange %q tenant %s: %w", exchange, tenantID, errBrokerNacked)
		}

		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for publish confirm: %w", ctx.Err())
	}
}

// buildPublishHeaders constructs AMQP headers with the idempotency key, optional tenant ID,
// and W3C trace context propagation fields.
func (publisher *EventPublisher) buildPublishHeaders(ctx context.Context, idempotencyKey uuid.UUID) amqp.Table {
	headers := amqp.Table{
		"idempotency_key": idempotencyKey.String(),
	}

	if tenantID, ok := auth.LookupTenantID(ctx); ok {
		headers["X-Tenant-ID"] = tenantID
	}

	carrier := propagation.MapCarrier{}
	publisher.propagator.Inject(ctx, carrier)

	for k, v := range carrier {
		headers[k] = v
	}

	return headers
}

// publishEvent handles the common publish-or-multi-tenant dispatch, logging, and error reporting
// for both confirmed and unmatched event types.
func (publisher *EventPublisher) publishEvent(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	routingKey string,
	msg amqp.Publishing,
	matchID string,
) error {
	if publisher.multiTenant {
		tenantID, ok := auth.LookupTenantID(ctx)
		if !ok || tenantID == "" {
			return errTenantIDRequired
		}

		if err := publisher.publishMultiTenant(ctx, tenantID, sharedRabbitmq.ExchangeName, routingKey, msg); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to publish event via tenant vhost", err)

			logger.With(libLog.Any("exchange", sharedRabbitmq.ExchangeName), libLog.Any("routing_key", routingKey), libLog.Any("tenant_id", tenantID), libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to publish event via tenant vhost")

			return fmt.Errorf("failed to publish event via tenant vhost: %w", err)
		}

		logger.With(libLog.Any("exchange", sharedRabbitmq.ExchangeName), libLog.Any("routing_key", routingKey), libLog.Any("message_id", msg.MessageId), libLog.Any("match_id", matchID), libLog.Any("tenant_id", tenantID)).Log(ctx, libLog.LevelDebug, "published event via tenant vhost")

		return nil
	}

	if err := publisher.confirmablePublisher.Publish(ctx, sharedRabbitmq.ExchangeName, routingKey, false, false, msg); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to publish event with confirm", err)

		logger.With(libLog.Any("exchange", sharedRabbitmq.ExchangeName), libLog.Any("routing_key", routingKey), libLog.Any("message_id", msg.MessageId), libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to publish event")

		return fmt.Errorf("failed to publish event: %w", err)
	}

	logger.With(libLog.Any("exchange", sharedRabbitmq.ExchangeName), libLog.Any("routing_key", routingKey), libLog.Any("message_id", msg.MessageId), libLog.Any("match_id", matchID)).Log(ctx, libLog.LevelDebug, "published event")

	return nil
}

// PublishMatchConfirmed publishes a MatchConfirmed event with broker confirmation.
func (publisher *EventPublisher) PublishMatchConfirmed(
	ctx context.Context,
	event *sharedDomain.MatchConfirmedEvent,
) error {
	if publisher == nil {
		return errPublisherNotInit
	}

	if !publisher.multiTenant && publisher.confirmablePublisher == nil {
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

	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		MessageId:    idempotencyKey.String(),
		Headers:      publisher.buildPublishHeaders(ctx, idempotencyKey),
	}

	return publisher.publishEvent(ctx, span, logger, routingKeyMatchConfirmed, msg, event.MatchID.String())
}

// PublishMatchUnmatched publishes a MatchUnmatched (revocation) event with broker confirmation.
func (publisher *EventPublisher) PublishMatchUnmatched(
	ctx context.Context,
	event *sharedDomain.MatchUnmatchedEvent,
) error {
	if publisher == nil {
		return errPublisherNotInit
	}

	if !publisher.multiTenant && publisher.confirmablePublisher == nil {
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

	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		MessageId:    idempotencyKey.String(),
		Headers:      publisher.buildPublishHeaders(ctx, idempotencyKey),
	}

	return publisher.publishEvent(ctx, span, logger, routingKeyMatchUnmatched, msg, event.MatchID.String())
}

// Close gracefully stops the internal confirmable publisher.
func (publisher *EventPublisher) Close() error {
	if publisher == nil {
		return nil
	}

	// Multi-tenant mode: the manager is closed by bootstrap, not by individual publishers.
	if publisher.multiTenant {
		return nil
	}

	if publisher.confirmablePublisher == nil {
		return nil
	}

	if err := publisher.confirmablePublisher.Close(); err != nil {
		return fmt.Errorf("confirmable publisher close: %w", err)
	}

	return nil
}

var _ matchingPorts.MatchEventPublisher = (*EventPublisher)(nil)
