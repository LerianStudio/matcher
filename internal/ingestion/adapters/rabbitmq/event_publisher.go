// Package rabbitmq provides RabbitMQ-based event publishing for ingestion.
//
// # Event Flow (Outbound Only)
//
// Matcher publishes domain events to RabbitMQ for external consumers. This is a
// fire-and-forget pattern - Matcher does NOT consume messages from these queues.
//
// # Published Events
//
//   - ingestion.completed: Published when a file/batch ingestion finishes successfully.
//     External systems (e.g., Midaz, notification services) can react to trigger
//     downstream workflows like ledger updates or user notifications.
//
//   - ingestion.failed: Published when ingestion fails. External systems can use this
//     to trigger alerts, retry logic, or compensating actions.
//
// # Consumers (External Services)
//
// TODO(integrations): Document which Lerian services consume these events:
//   - Midaz ledger service (for transaction sync?)
//   - Notification service (for webhook/email alerts?)
//   - Audit service (for compliance logging?)
//
// # Trace Propagation
//
// All messages include W3C Trace Context headers (traceparent, tracestate) so that
// consuming services can link their traces back to the originating Matcher operation.
//
// # Dead Letter Queue (DLQ) Topology
//
// The publisher declares the shared DLQ topology via `sharedRabbitmq.DeclareDLQTopology`
// in this file. Undeliverable, expired, or rejected messages for `ingestion.completed`
// and `ingestion.failed` are routed to the DLX (`matcher.events.dlx`) and end up in the
// DLQ (`matcher.events.dlq`) via a catch-all `#` binding. Queues that publish or consume
// these events should be configured with the DLX args (`x-dead-letter-exchange`) so
// failures are redirected consistently. There is no TTL-based retry configured by
// default in this topology; operators should monitor `matcher.events.dlq`, inspect
// payloads/headers for failure context, and reprocess by requeueing or replaying
// messages once the underlying issue is addressed.
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

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
)

var (
	errRabbitMQChannelRequired        = errors.New("rabbitmq channel is required")
	errPublisherNotInit               = errors.New("rabbitmq publisher not initialized")
	errNilEvent                       = errors.New("event is required")
	errConfirmableSetupFailed         = errors.New("failed to setup confirmable publisher")
	errTenantIDRequiredForMultiTenant = errors.New("tenant ID is required in multi-tenant mode")
	errRabbitMQManagerRequired        = errors.New("rabbitmq multi-tenant manager is required")
)

const (
	routingKeyIngestionCompleted = "ingestion.completed"
	routingKeyIngestionFailed    = "ingestion.failed"
)

// EventPublisher publishes ingestion events to RabbitMQ with publisher confirms.
// It supports both single-tenant mode (using a shared confirmable publisher) and
// multi-tenant mode (using per-tenant vhost isolation via RabbitMQMultiTenantManager).
type EventPublisher struct {
	// confirmablePublisher is used in single-tenant mode with broker confirms.
	// Nil when operating in multi-tenant mode.
	confirmablePublisher *sharedRabbitmq.ConfirmablePublisher

	// rabbitmqManager provides per-tenant channels in multi-tenant mode.
	// Nil when operating in single-tenant mode.
	rabbitmqManager sharedRabbitmq.RabbitMQMultiTenantManager

	// propagator is used for W3C trace context propagation.
	propagator propagation.TextMapPropagator
}

// NewEventPublisherFromChannel creates an event publisher using a dedicated AMQP channel.
// Each publisher MUST own its own channel because AMQP publisher confirms are
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

	err := ch.ExchangeDeclare(
		sharedRabbitmq.ExchangeName,
		sharedRabbitmq.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	if err := sharedRabbitmq.DeclareDLQTopology(ch); err != nil {
		return nil, fmt.Errorf("failed to declare dlq topology: %w", err)
	}

	confirmablePublisher, err := sharedRabbitmq.NewConfirmablePublisherFromChannel(ch, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errConfirmableSetupFailed, err)
	}

	return &EventPublisher{
		confirmablePublisher: confirmablePublisher,
		propagator:           otel.GetTextMapPropagator(),
	}, nil
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

// PublishIngestionCompleted publishes an ingestion completed event.
func (publisher *EventPublisher) PublishIngestionCompleted(
	ctx context.Context,
	event *entities.IngestionCompletedEvent,
) error {
	if event == nil {
		return errNilEvent
	}

	return publisher.publish(ctx, routingKeyIngestionCompleted, event.JobID, event)
}

// PublishIngestionFailed publishes an ingestion failed event.
func (publisher *EventPublisher) PublishIngestionFailed(
	ctx context.Context,
	event *entities.IngestionFailedEvent,
) error {
	if event == nil {
		return errNilEvent
	}

	return publisher.publish(ctx, routingKeyIngestionFailed, event.JobID, event)
}

func (publisher *EventPublisher) publish(
	ctx context.Context,
	routingKey string,
	idempotencyKey uuid.UUID,
	payload any,
) error {
	if publisher == nil {
		return errPublisherNotInit
	}

	// Must have either single-tenant publisher or multi-tenant manager
	if publisher.confirmablePublisher == nil && publisher.rabbitmqManager == nil {
		return errPublisherNotInit
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	span := trace.SpanFromContext(ctx)
	if tracer != nil {
		ctx, span = tracer.Start(ctx, "rabbitmq.publish_ingestion_event")
		defer span.End()
	}

	body, err := json.Marshal(payload)
	if err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to marshal event", err)
		}

		return fmt.Errorf("failed to marshal event: %w", err)
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
		return publisher.publishMultiTenant(ctx, routingKey, tenantID, msg, span, logger)
	}

	// Single-tenant mode: use confirmable publisher
	if err := publisher.confirmablePublisher.Publish(ctx, sharedRabbitmq.ExchangeName, routingKey, false, false, msg); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to publish event with confirm", err)
		}

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to publish event")

		return fmt.Errorf("failed to publish event: %w", err)
	}

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

	if ch == nil {
		return fmt.Errorf("get tenant channel: nil channel returned for tenant %s", tenantID)
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
		return fmt.Errorf("close confirmable publisher: %w", err)
	}

	return nil
}
