// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Publish sends a message and waits for broker confirmation.
// Returns nil on ack, error on nack, timeout, or channel close.
//
// Publish is serialized: only one goroutine may be in the publish+confirm
// sequence at a time. This prevents confirm cross-talk where goroutine A
// could consume goroutine B's confirmation from the shared channel.
func (pub *ConfirmablePublisher) Publish(
	ctx context.Context,
	exchange, routingKey string,
	mandatory, immediate bool,
	msg amqp.Publishing,
) error {
	// Serialize the entire publish+waitForConfirm sequence so each caller
	// receives exactly its own broker confirmation.
	pub.publishMu.Lock()
	defer pub.publishMu.Unlock()

	pub.mu.RLock()

	if pub.closed {
		pub.mu.RUnlock()
		return ErrPublisherClosed
	}

	if sharedPorts.IsNilValue(pub.ch) {
		pub.mu.RUnlock()
		return ErrPublisherNotReady
	}

	publishChannel := pub.ch
	confirms := pub.confirms
	closedCh := pub.closedCh
	confirmTimeout := pub.confirmTimeout
	pub.mu.RUnlock()

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only tracer needed here; publisher uses caller's logger
	ctx, span := tracer.Start(ctx, "rabbitmq.publish")

	defer span.End()

	span.SetAttributes(
		attribute.String("exchange", exchange),
		attribute.String("routing_key", routingKey),
		attribute.Int64("confirm_timeout_ms", confirmTimeout.Milliseconds()),
	)

	if err := publishChannel.PublishWithContext(ctx, exchange, routingKey, mandatory, immediate, msg); err != nil {
		wrappedErr := fmt.Errorf("publish: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to publish", wrappedErr)

		return wrappedErr
	}

	if err := waitForConfirm(ctx, confirms, closedCh, confirmTimeout); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to confirm publish", err)

		return err
	}

	return nil
}

// waitForConfirm waits for broker confirmation of the last published message.
func waitForConfirm(
	ctx context.Context,
	confirms <-chan amqp.Confirmation,
	closedCh <-chan struct{},
	confirmTimeout time.Duration,
) error {
	timeout := time.NewTimer(confirmTimeout)
	defer timeout.Stop()

	select {
	case confirmed, ok := <-confirms:
		if !ok {
			return ErrPublisherClosed
		}

		if !confirmed.Ack {
			return fmt.Errorf("%w: delivery_tag=%d", ErrPublishNacked, confirmed.DeliveryTag)
		}

		return nil

	case <-closedCh:
		return ErrPublisherClosed

	case <-timeout.C:
		return ErrConfirmTimeout

	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	}
}
