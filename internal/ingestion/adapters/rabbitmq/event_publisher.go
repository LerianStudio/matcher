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
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
)

var (
	errRabbitMQChannelRequired = errors.New("rabbitmq channel is required")
	errRabbitMQManagerRequired = errors.New("rabbitmq tenant manager is required")
	errPublisherNotInit        = errors.New("rabbitmq publisher not initialized")
	errNilEvent                = errors.New("event is required")
	errConfirmableSetupFailed  = errors.New("failed to setup confirmable publisher")
	errTenantIDRequired        = errors.New("tenant ID is required in multi-tenant mode")
	errBrokerNacked            = errors.New("broker nacked publish")
	errNilTenantChannel        = errors.New("nil channel returned for tenant")
)

const (
	routingKeyIngestionCompleted = "ingestion.completed"
	routingKeyIngestionFailed    = "ingestion.failed"
)

// EventPublisher publishes ingestion events to RabbitMQ with publisher confirms.
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
	confirmablePublisher *sharedRabbitmq.ConfirmablePublisher
	propagator           propagation.TextMapPropagator
	rmqManager           *tmrabbitmq.Manager // per-tenant vhost manager (nil in single-tenant mode)
	multiTenant          bool                // true when using per-tenant vhost isolation
	declaredExchanges    sync.Map            // tracks tenant exchanges already declared (key: "tenantID:exchange")
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

// NewMultiTenantEventPublisher creates an ingestion event publisher that uses
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

	if ch == nil {
		return fmt.Errorf("get tenant channel: %w: %s", errNilTenantChannel, tenantID)
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

// dispatchEvent handles the publish-or-multi-tenant dispatch, logging, and error reporting.
func (publisher *EventPublisher) dispatchEvent(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	routingKey string,
	msg amqp.Publishing,
) error {
	if publisher.multiTenant {
		tenantID, ok := auth.LookupTenantID(ctx)
		if !ok || tenantID == "" {
			return errTenantIDRequired
		}

		if err := publisher.publishMultiTenant(ctx, tenantID, sharedRabbitmq.ExchangeName, routingKey, msg); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to publish event via tenant vhost", err)

			logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to publish event via tenant vhost")

			return fmt.Errorf("failed to publish event via tenant vhost: %w", err)
		}

		return nil
	}

	if err := publisher.confirmablePublisher.Publish(ctx, sharedRabbitmq.ExchangeName, routingKey, false, false, msg); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to publish event with confirm", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to publish event")

		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
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

	if !publisher.multiTenant && publisher.confirmablePublisher == nil {
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
		libOpentelemetry.HandleSpanError(span, "failed to marshal event", err)

		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		MessageId:    idempotencyKey.String(),
		Headers:      publisher.buildPublishHeaders(ctx, idempotencyKey),
	}

	return publisher.dispatchEvent(ctx, span, logger, routingKey, msg)
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
		return fmt.Errorf("close confirmable publisher: %w", err)
	}

	return nil
}
