// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	libZap "github.com/LerianStudio/lib-commons/v5/commons/zap"

	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// InfraConnector abstracts the infrastructure-level primitives the bootstrap
// wiring depends on: migrations, database/broker connect calls, telemetry
// initialization, the auth boundary logger, and object-storage client
// construction. Injecting this interface (instead of package-level function
// variables) lets tests exercise InitServersWithOptions and its helpers with
// fakes, without mutating process-global state.
type InfraConnector interface {
	// RunMigrations applies database migrations against the primary DSN.
	RunMigrations(
		ctx context.Context,
		primaryDSN string,
		dbName string,
		migrationsPath string,
		logger libLog.Logger,
		allowDirtyRecovery bool,
	) error

	// ConnectPostgres connects the lib-commons Postgres client (primary +
	// replica handles). Invoked concurrently with EnsureRabbitChannel so the
	// implementation must be safe under independent goroutines sharing the
	// connector but not the client.
	ConnectPostgres(ctx context.Context, client *libPostgres.Client) error

	// EnsureRabbitChannel opens (or reuses) the shared AMQP channel on the
	// provided RabbitMQ connection.
	EnsureRabbitChannel(conn *libRabbitmq.RabbitMQConnection) error

	// InitTelemetry initialises OpenTelemetry exporters for the current
	// process. The returned *Telemetry is always non-nil in the default
	// implementation (see observability.InitTelemetry).
	InitTelemetry(cfg *Config, logger libLog.Logger) *libOpentelemetry.Telemetry

	// InitializeAuthBoundaryLogger builds the dedicated logger used by the
	// auth middleware. Splitting this from the application logger lets auth
	// stay observable even when the bootstrap logger is swapped for tests.
	InitializeAuthBoundaryLogger() (libLog.Logger, error)

	// NewS3Client constructs the reporting S3 client used for exports,
	// archival, and object-storage health checks. Returning a concrete
	// *reportingStorage.S3Client keeps runtime type assertions at call sites
	// unchanged.
	NewS3Client(ctx context.Context, cfg reportingStorage.S3Config) (*reportingStorage.S3Client, error)
}

// EventPublisherFactory encapsulates the dedicated-AMQP-channel lifecycle and
// publisher constructors/destructors used by matching and ingestion modules.
// Tests inject this interface to exercise initEventPublishers and
// cleanupPublishersOnFailure without opening real AMQP channels.
type EventPublisherFactory interface {
	// OpenDedicatedChannel opens a fresh AMQP channel on the provided
	// RabbitMQ connection. Each publisher gets its own channel because
	// AMQP publisher confirms are channel-scoped.
	OpenDedicatedChannel(conn *libRabbitmq.RabbitMQConnection) (*amqp.Channel, error)

	// NewMatchingEventPublisher wraps the provided AMQP channel in a
	// matching EventPublisher with optional confirmable-publisher options
	// (e.g., auto-recovery).
	NewMatchingEventPublisher(
		ch *amqp.Channel,
		opts ...sharedRabbitmq.ConfirmablePublisherOption,
	) (*matchingRabbitmq.EventPublisher, error)

	// NewIngestionEventPublisher wraps the provided AMQP channel in an
	// ingestion EventPublisher.
	NewIngestionEventPublisher(
		ch *amqp.Channel,
		opts ...sharedRabbitmq.ConfirmablePublisherOption,
	) (*ingestionRabbitmq.EventPublisher, error)

	// CloseAMQPChannel safely closes an AMQP channel. Nil channels are a
	// no-op.
	CloseAMQPChannel(ch *amqp.Channel) error

	// CloseMatchingEventPublisher safely closes a matching publisher. Nil
	// publishers are a no-op.
	CloseMatchingEventPublisher(publisher *matchingRabbitmq.EventPublisher) error

	// CloseIngestionEventPublisher safely closes an ingestion publisher.
	// Nil publishers are a no-op.
	CloseIngestionEventPublisher(publisher *ingestionRabbitmq.EventPublisher) error
}

// defaultInfraConnector is the production implementation wiring each method
// back to its lib-commons / package-level counterpart. Bootstrap uses this
// when no connector is supplied via Options.
type defaultInfraConnector struct{}

// DefaultInfraConnector returns the production InfraConnector implementation.
// Constructed once per call (the struct is stateless), so callers are free to
// reuse the return value or instantiate it per bootstrap.
func DefaultInfraConnector() InfraConnector {
	return defaultInfraConnector{}
}

// RunMigrations invokes the package-level migration runner.
func (defaultInfraConnector) RunMigrations(
	ctx context.Context,
	primaryDSN string,
	dbName string,
	migrationsPath string,
	logger libLog.Logger,
	allowDirtyRecovery bool,
) error {
	return RunMigrations(ctx, primaryDSN, dbName, migrationsPath, logger, allowDirtyRecovery)
}

// ConnectPostgres delegates to the lib-commons Postgres client.
func (defaultInfraConnector) ConnectPostgres(ctx context.Context, client *libPostgres.Client) error {
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}

	return nil
}

// EnsureRabbitChannel delegates to the lib-commons RabbitMQ connection.
func (defaultInfraConnector) EnsureRabbitChannel(conn *libRabbitmq.RabbitMQConnection) error {
	if err := conn.EnsureChannel(); err != nil {
		return fmt.Errorf("ensure rabbitmq channel: %w", err)
	}

	return nil
}

// InitTelemetry delegates to the package-level InitTelemetry so production
// behaviour is unchanged after the refactor.
func (defaultInfraConnector) InitTelemetry(cfg *Config, logger libLog.Logger) *libOpentelemetry.Telemetry {
	return InitTelemetry(cfg, logger)
}

// InitializeAuthBoundaryLogger constructs a fresh zap logger for the auth
// boundary. The config is pinned to production/info with the app's OTel
// library name so audit output is structured JSON regardless of the main
// application logger's runtime configuration — downstream compliance
// tooling consumes these logs and expects a stable machine-parseable format
// in every environment.
func (defaultInfraConnector) InitializeAuthBoundaryLogger() (libLog.Logger, error) {
	logger, err := libZap.New(libZap.Config{
		Environment:     libZap.EnvironmentProduction,
		Level:           defaultLoggerLevel,
		OTelLibraryName: constants.ApplicationName,
	})
	if err != nil {
		return nil, fmt.Errorf("build auth boundary logger: %w", err)
	}

	return logger, nil
}

// NewS3Client delegates to the reporting storage package.
func (defaultInfraConnector) NewS3Client(
	ctx context.Context,
	cfg reportingStorage.S3Config,
) (*reportingStorage.S3Client, error) {
	client, err := reportingStorage.NewS3Client(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("build s3 client: %w", err)
	}

	return client, nil
}

// defaultEventPublisherFactory is the production implementation delegating to
// the matching / ingestion rabbitmq packages.
type defaultEventPublisherFactory struct{}

// DefaultEventPublisherFactory returns the production EventPublisherFactory
// implementation.
func DefaultEventPublisherFactory() EventPublisherFactory {
	return defaultEventPublisherFactory{}
}

// OpenDedicatedChannel delegates to the package-level openDedicatedChannel
// helper.
func (defaultEventPublisherFactory) OpenDedicatedChannel(
	conn *libRabbitmq.RabbitMQConnection,
) (*amqp.Channel, error) {
	return openDedicatedChannel(conn)
}

// NewMatchingEventPublisher delegates to matchingRabbitmq.NewEventPublisherFromChannel.
func (defaultEventPublisherFactory) NewMatchingEventPublisher(
	ch *amqp.Channel,
	opts ...sharedRabbitmq.ConfirmablePublisherOption,
) (*matchingRabbitmq.EventPublisher, error) {
	publisher, err := matchingRabbitmq.NewEventPublisherFromChannel(ch, opts...)
	if err != nil {
		return nil, fmt.Errorf("build matching publisher: %w", err)
	}

	return publisher, nil
}

// NewIngestionEventPublisher delegates to ingestionRabbitmq.NewEventPublisherFromChannel.
func (defaultEventPublisherFactory) NewIngestionEventPublisher(
	ch *amqp.Channel,
	opts ...sharedRabbitmq.ConfirmablePublisherOption,
) (*ingestionRabbitmq.EventPublisher, error) {
	publisher, err := ingestionRabbitmq.NewEventPublisherFromChannel(ch, opts...)
	if err != nil {
		return nil, fmt.Errorf("build ingestion publisher: %w", err)
	}

	return publisher, nil
}

// CloseAMQPChannel closes the channel if non-nil.
func (defaultEventPublisherFactory) CloseAMQPChannel(ch *amqp.Channel) error {
	if ch == nil {
		return nil
	}

	if err := ch.Close(); err != nil {
		return fmt.Errorf("close rabbitmq channel: %w", err)
	}

	return nil
}

// CloseMatchingEventPublisher closes the publisher if non-nil.
func (defaultEventPublisherFactory) CloseMatchingEventPublisher(
	publisher *matchingRabbitmq.EventPublisher,
) error {
	if publisher == nil {
		return nil
	}

	if err := publisher.Close(); err != nil {
		return fmt.Errorf("close matching publisher: %w", err)
	}

	return nil
}

// CloseIngestionEventPublisher closes the publisher if non-nil.
func (defaultEventPublisherFactory) CloseIngestionEventPublisher(
	publisher *ingestionRabbitmq.EventPublisher,
) error {
	if publisher == nil {
		return nil
	}

	if err := publisher.Close(); err != nil {
		return fmt.Errorf("close ingestion publisher: %w", err)
	}

	return nil
}
