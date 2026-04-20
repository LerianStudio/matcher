// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	amqp "github.com/rabbitmq/amqp091-go"

	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"

	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
)

type eventPublisherFnOverrides struct {
	openDedicatedChannelFn                  func(*libRabbitmq.RabbitMQConnection) (*amqp.Channel, error)
	newMatchingEventPublisherFromChannelFn  func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*matchingRabbitmq.EventPublisher, error)
	newIngestionEventPublisherFromChannelFn func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*ingestionRabbitmq.EventPublisher, error)
	closeAMQPChannelFn                      func(*amqp.Channel) error
	closeMatchingEventPublisherFn           func(*matchingRabbitmq.EventPublisher) error
	closeIngestionEventPublisherFn          func(*ingestionRabbitmq.EventPublisher) error
}

func setEventPublisherFnsForTest(overrides eventPublisherFnOverrides) func() {
	eventPublisherFnMu.Lock()
	previous := eventPublisherFnOverrides{
		openDedicatedChannelFn:                  openDedicatedChannelFn,
		newMatchingEventPublisherFromChannelFn:  newMatchingEventPublisherFromChannelFn,
		newIngestionEventPublisherFromChannelFn: newIngestionEventPublisherFromChannelFn,
		closeAMQPChannelFn:                      closeAMQPChannelFn,
		closeMatchingEventPublisherFn:           closeMatchingEventPublisherFn,
		closeIngestionEventPublisherFn:          closeIngestionEventPublisherFn,
	}

	if overrides.openDedicatedChannelFn != nil {
		openDedicatedChannelFn = overrides.openDedicatedChannelFn
	}

	if overrides.newMatchingEventPublisherFromChannelFn != nil {
		newMatchingEventPublisherFromChannelFn = overrides.newMatchingEventPublisherFromChannelFn
	}

	if overrides.newIngestionEventPublisherFromChannelFn != nil {
		newIngestionEventPublisherFromChannelFn = overrides.newIngestionEventPublisherFromChannelFn
	}

	if overrides.closeAMQPChannelFn != nil {
		closeAMQPChannelFn = overrides.closeAMQPChannelFn
	}

	if overrides.closeMatchingEventPublisherFn != nil {
		closeMatchingEventPublisherFn = overrides.closeMatchingEventPublisherFn
	}

	if overrides.closeIngestionEventPublisherFn != nil {
		closeIngestionEventPublisherFn = overrides.closeIngestionEventPublisherFn
	}
	eventPublisherFnMu.Unlock()

	return func() {
		eventPublisherFnMu.Lock()
		openDedicatedChannelFn = previous.openDedicatedChannelFn
		newMatchingEventPublisherFromChannelFn = previous.newMatchingEventPublisherFromChannelFn
		newIngestionEventPublisherFromChannelFn = previous.newIngestionEventPublisherFromChannelFn
		closeAMQPChannelFn = previous.closeAMQPChannelFn
		closeMatchingEventPublisherFn = previous.closeMatchingEventPublisherFn
		closeIngestionEventPublisherFn = previous.closeIngestionEventPublisherFn
		eventPublisherFnMu.Unlock()
	}
}
