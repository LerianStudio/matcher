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

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
)

var (
	errRabbitMQChannelRequired = errors.New("rabbitmq channel is required")
	errPublisherNotInit        = errors.New("rabbitmq publisher not initialized")
	errNilEvent                = errors.New("event is required")
	errConfirmableSetupFailed  = errors.New("failed to setup confirmable publisher")
)

const (
	routingKeyIngestionCompleted = "ingestion.completed"
	routingKeyIngestionFailed    = "ingestion.failed"
)

// EventPublisher publishes ingestion events to RabbitMQ with publisher confirms.
type EventPublisher struct {
	confirmablePublisher *sharedRabbitmq.ConfirmablePublisher
	propagator           propagation.TextMapPropagator
}

// NewEventPublisherFromChannel creates an event publisher using a dedicated AMQP channel.
// Each publisher MUST own its own channel because AMQP publisher confirms are
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
	if publisher == nil || publisher.confirmablePublisher == nil {
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

	if err := publisher.confirmablePublisher.Publish(ctx, sharedRabbitmq.ExchangeName, routingKey, false, false, msg); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to publish event with confirm", err)
		}

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to publish event")

		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}

// Close gracefully stops the internal confirmable publisher.
func (publisher *EventPublisher) Close() error {
	if publisher == nil || publisher.confirmablePublisher == nil {
		return nil
	}

	if err := publisher.confirmablePublisher.Close(); err != nil {
		return fmt.Errorf("close confirmable publisher: %w", err)
	}

	return nil
}
