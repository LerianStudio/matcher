// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"

	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
)

// fakeInfraConnector is a test double for InfraConnector. Each field is a
// function hook; nil hooks fall back to safe zero-value behaviour so tests
// only stub the methods they care about. This pattern replaces the
// package-level *Fn vars the refactor removed.
type fakeInfraConnector struct {
	runMigrations         func(ctx context.Context, dsn, db, path string, logger libLog.Logger, allowDirty bool) error
	connectPostgres       func(ctx context.Context, client *libPostgres.Client) error
	ensureRabbitChannel   func(conn *libRabbitmq.RabbitMQConnection) error
	initTelemetry         func(cfg *Config, logger libLog.Logger) *libOpentelemetry.Telemetry
	initAuthBoundary      func() (libLog.Logger, error)
	newS3Client           func(ctx context.Context, cfg reportingStorage.S3Config) (*reportingStorage.S3Client, error)
}

func (f *fakeInfraConnector) RunMigrations(
	ctx context.Context,
	primaryDSN string,
	dbName string,
	migrationsPath string,
	logger libLog.Logger,
	allowDirtyRecovery bool,
) error {
	if f == nil || f.runMigrations == nil {
		return nil
	}

	return f.runMigrations(ctx, primaryDSN, dbName, migrationsPath, logger, allowDirtyRecovery)
}

func (f *fakeInfraConnector) ConnectPostgres(ctx context.Context, client *libPostgres.Client) error {
	if f == nil || f.connectPostgres == nil {
		return nil
	}

	return f.connectPostgres(ctx, client)
}

func (f *fakeInfraConnector) EnsureRabbitChannel(conn *libRabbitmq.RabbitMQConnection) error {
	if f == nil || f.ensureRabbitChannel == nil {
		return nil
	}

	return f.ensureRabbitChannel(conn)
}

func (f *fakeInfraConnector) InitTelemetry(cfg *Config, logger libLog.Logger) *libOpentelemetry.Telemetry {
	if f == nil || f.initTelemetry == nil {
		// Preserve the production contract: InitTelemetry returns a non-nil
		// Telemetry when telemetry is disabled. Tests that expect a concrete
		// instance should supply their own hook.
		return InitTelemetry(cfg, logger)
	}

	return f.initTelemetry(cfg, logger)
}

func (f *fakeInfraConnector) InitializeAuthBoundaryLogger() (libLog.Logger, error) {
	if f == nil || f.initAuthBoundary == nil {
		return libLog.NewNop(), nil
	}

	return f.initAuthBoundary()
}

func (f *fakeInfraConnector) NewS3Client(
	ctx context.Context,
	cfg reportingStorage.S3Config,
) (*reportingStorage.S3Client, error) {
	if f == nil || f.newS3Client == nil {
		return nil, nil
	}

	return f.newS3Client(ctx, cfg)
}

// fakeEventPublisherFactory is a test double for EventPublisherFactory.
// Like fakeInfraConnector, each field is a function hook that tests set to
// shape behaviour. Nil hooks are safe no-ops.
type fakeEventPublisherFactory struct {
	openDedicatedChannel        func(conn *libRabbitmq.RabbitMQConnection) (*amqp.Channel, error)
	newMatchingPublisher        func(ch *amqp.Channel, opts ...sharedRabbitmq.ConfirmablePublisherOption) (*matchingRabbitmq.EventPublisher, error)
	newIngestionPublisher       func(ch *amqp.Channel, opts ...sharedRabbitmq.ConfirmablePublisherOption) (*ingestionRabbitmq.EventPublisher, error)
	closeAMQPChannel            func(ch *amqp.Channel) error
	closeMatchingPublisher      func(publisher *matchingRabbitmq.EventPublisher) error
	closeIngestionPublisher     func(publisher *ingestionRabbitmq.EventPublisher) error
}

func (f *fakeEventPublisherFactory) OpenDedicatedChannel(
	conn *libRabbitmq.RabbitMQConnection,
) (*amqp.Channel, error) {
	if f == nil || f.openDedicatedChannel == nil {
		return nil, nil
	}

	return f.openDedicatedChannel(conn)
}

func (f *fakeEventPublisherFactory) NewMatchingEventPublisher(
	ch *amqp.Channel,
	opts ...sharedRabbitmq.ConfirmablePublisherOption,
) (*matchingRabbitmq.EventPublisher, error) {
	if f == nil || f.newMatchingPublisher == nil {
		return nil, nil
	}

	return f.newMatchingPublisher(ch, opts...)
}

func (f *fakeEventPublisherFactory) NewIngestionEventPublisher(
	ch *amqp.Channel,
	opts ...sharedRabbitmq.ConfirmablePublisherOption,
) (*ingestionRabbitmq.EventPublisher, error) {
	if f == nil || f.newIngestionPublisher == nil {
		return nil, nil
	}

	return f.newIngestionPublisher(ch, opts...)
}

func (f *fakeEventPublisherFactory) CloseAMQPChannel(ch *amqp.Channel) error {
	if f == nil || f.closeAMQPChannel == nil {
		return nil
	}

	return f.closeAMQPChannel(ch)
}

func (f *fakeEventPublisherFactory) CloseMatchingEventPublisher(
	publisher *matchingRabbitmq.EventPublisher,
) error {
	if f == nil || f.closeMatchingPublisher == nil {
		return nil
	}

	return f.closeMatchingPublisher(publisher)
}

func (f *fakeEventPublisherFactory) CloseIngestionEventPublisher(
	publisher *ingestionRabbitmq.EventPublisher,
) error {
	if f == nil || f.closeIngestionPublisher == nil {
		return nil
	}

	return f.closeIngestionPublisher(publisher)
}
