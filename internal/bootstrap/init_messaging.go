// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/LerianStudio/lib-commons/v5/commons/errgroup"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/rabbitmq"

	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
)

// logCloseErr calls closeFn and logs any error at LevelError. It is a no-op
// when closeFn returns nil. This helper exists to flatten the nested
// "if err; if logger" pattern that otherwise inflates cognitive complexity.
func logCloseErr(ctx context.Context, logger libLog.Logger, msg string, closeFn func() error) {
	if err := closeFn(); err != nil {
		libLog.SafeError(logger, ctx, msg, err, runtime.IsProductionMode())
	}
}

// openChannelForPublisher opens a dedicated AMQP channel, checking for
// cancellation first. Extracted from initEventPublishers to reduce complexity.
func openChannelForPublisher(
	groupCtx context.Context,
	name string,
	conn *libRabbitmq.RabbitMQConnection,
	openFn func(*libRabbitmq.RabbitMQConnection) (*amqp.Channel, error),
) (*amqp.Channel, error) {
	select {
	case <-groupCtx.Done():
		return nil, fmt.Errorf("open dedicated channel for %s publisher: %w", name, groupCtx.Err())
	default:
	}

	ch, err := openFn(conn)
	if err != nil {
		return nil, fmt.Errorf("open dedicated channel for %s publisher: %w", name, err)
	}

	return ch, nil
}

// cleanupPublishersOnFailure closes publishers and channels when
// initEventPublishers encounters an error partway through setup.
func cleanupPublishersOnFailure(
	ctx context.Context,
	logger libLog.Logger,
	publishers EventPublisherFactory,
	matchingPublisher *matchingRabbitmq.EventPublisher,
	ingestionPublisher *ingestionRabbitmq.EventPublisher,
	matchingChannel, ingestionChannel *amqp.Channel,
) {
	if publishers == nil {
		publishers = DefaultEventPublisherFactory()
	}

	logCloseErr(ctx, logger, "failed to close matching publisher during cleanup", func() error {
		return publishers.CloseMatchingEventPublisher(matchingPublisher)
	})

	logCloseErr(ctx, logger, "failed to close ingestion publisher during cleanup", func() error {
		return publishers.CloseIngestionEventPublisher(ingestionPublisher)
	})

	if matchingChannel != nil {
		logCloseErr(ctx, logger, "failed to close matching channel", func() error {
			return publishers.CloseAMQPChannel(matchingChannel)
		})
	}

	if ingestionChannel != nil {
		logCloseErr(ctx, logger, "failed to close ingestion channel", func() error {
			return publishers.CloseAMQPChannel(ingestionChannel)
		})
	}
}

// initEventPublishers creates dedicated AMQP channels and event publishers for
// matching and ingestion modules. Channels are opened in parallel since they are
// independent protocol exchanges on the same connection.
//
// Channel isolation: each publisher gets its own dedicated AMQP channel.
// AMQP publisher confirms are channel-scoped -- calling Confirm(false) on a
// channel resets the delivery tag counter and confirmation state for that channel.
// If two ConfirmablePublishers shared the same channel, the second Confirm call
// would corrupt the first publisher's tracking, leading to silent message loss.
//
// We open fresh channels from the underlying *amqp.Connection so each publisher
// has isolated delivery tags and confirm sequences.
func initEventPublishers(
	ctx context.Context,
	rabbitMQConnection *libRabbitmq.RabbitMQConnection,
	logger libLog.Logger,
	rmqManager *tmrabbitmq.Manager,
	publishers EventPublisherFactory,
) (*matchingRabbitmq.EventPublisher, *ingestionRabbitmq.EventPublisher, error) {
	if publishers == nil {
		publishers = DefaultEventPublisherFactory()
	}

	// Multi-tenant mode: use per-tenant vhost channels via tmrabbitmq.Manager.
	// No dedicated AMQP channels or confirmable publishers are needed because
	// each publish call resolves a tenant-specific channel on demand.
	if rmqManager != nil {
		matchingPublisher, err := matchingRabbitmq.NewMultiTenantEventPublisher(rmqManager)
		if err != nil {
			return nil, nil, fmt.Errorf("create multi-tenant matching event publisher: %w", err)
		}

		ingestionPublisher, err := ingestionRabbitmq.NewMultiTenantEventPublisher(rmqManager)
		if err != nil {
			return nil, nil, fmt.Errorf("create multi-tenant ingestion event publisher: %w", err)
		}

		return matchingPublisher, ingestionPublisher, nil
	}

	// Single-tenant mode: existing behavior with dedicated AMQP channels.
	success := false

	var matchingPublisher *matchingRabbitmq.EventPublisher

	var ingestionPublisher *ingestionRabbitmq.EventPublisher

	// Open both AMQP channels in parallel — independent protocol exchanges.
	var matchingChannel, ingestionChannel *amqp.Channel

	defer func() {
		if !success {
			cleanupPublishersOnFailure(ctx, logger, publishers, matchingPublisher, ingestionPublisher, matchingChannel, ingestionChannel)
		}
	}()

	channelGroup, groupCtx := errgroup.WithContext(ctx)
	channelGroup.SetLogger(logger)

	channelGroup.Go(func() error {
		ch, err := openChannelForPublisher(groupCtx, "matching", rabbitMQConnection, publishers.OpenDedicatedChannel)
		if err != nil {
			return err
		}

		matchingChannel = ch

		return nil
	})

	channelGroup.Go(func() error {
		ch, err := openChannelForPublisher(groupCtx, "ingestion", rabbitMQConnection, publishers.OpenDedicatedChannel)
		if err != nil {
			return err
		}

		ingestionChannel = ch

		return nil
	})

	if err := channelGroup.Wait(); err != nil {
		return nil, nil, fmt.Errorf("open AMQP channels: %w", err)
	}

	// Enable auto-recovery: if the AMQP channel dies (e.g., broker restart,
	// network partition), the publisher automatically reopens a new channel
	// from the underlying connection with exponential backoff.
	channelRecoveryProvider := sharedRabbitmq.WithAutoRecovery(func() (sharedRabbitmq.ConfirmableChannel, error) {
		ch, err := publishers.OpenDedicatedChannel(rabbitMQConnection)
		if err != nil {
			return nil, fmt.Errorf("open dedicated channel for publisher recovery: %w", err)
		}

		return ch, nil
	})

	matchingPublisher, err := publishers.NewMatchingEventPublisher(matchingChannel, channelRecoveryProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("create matching event publisher: %w", err)
	}

	ingestionPublisher, err = publishers.NewIngestionEventPublisher(ingestionChannel, channelRecoveryProvider)
	if err != nil {
		return nil, nil, fmt.Errorf("create ingestion event publisher: %w", err)
	}

	success = true

	return matchingPublisher, ingestionPublisher, nil
}
