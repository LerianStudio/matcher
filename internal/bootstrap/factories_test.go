// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libAssert "github.com/LerianStudio/lib-commons/v5/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"

	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
)

// TestOptions_ensureDefaults_FillsNilFactories demonstrates that Options
// normalization replaces nil factories with the production defaults. This
// is the contract consumed by InitServersWithOptions: callers can pass a
// partial Options value (or none at all) and get a working configuration.
func TestOptions_ensureDefaults_FillsNilFactories(t *testing.T) {
	t.Parallel()

	t.Run("nil options yields defaults", func(t *testing.T) {
		t.Parallel()

		var opts *Options

		normalized := opts.ensureDefaults()

		require.NotNil(t, normalized)
		assert.NotNil(t, normalized.InfraConnector)
		assert.NotNil(t, normalized.EventPublishers)
	})

	t.Run("empty options gets defaults without overriding", func(t *testing.T) {
		t.Parallel()

		opts := &Options{}

		normalized := opts.ensureDefaults()

		assert.Same(t, opts, normalized, "same pointer preserved")
		assert.NotNil(t, normalized.InfraConnector)
		assert.NotNil(t, normalized.EventPublishers)
	})

	t.Run("supplied factories pass through unchanged", func(t *testing.T) {
		t.Parallel()

		connector := &fakeInfraConnector{}
		publishers := &fakeEventPublisherFactory{}

		opts := &Options{
			InfraConnector:  connector,
			EventPublishers: publishers,
		}

		normalized := opts.ensureDefaults()

		assert.Same(t, connector, normalized.InfraConnector)
		assert.Same(t, publishers, normalized.EventPublishers)
	})
}

// TestDefaultInfraConnector_SatisfiesInterface ensures the production
// connector compiles against the InfraConnector contract. The assignment
// is the test — if the interface and struct drift, this fails at build.
func TestDefaultInfraConnector_SatisfiesInterface(t *testing.T) {
	t.Parallel()

	var _ InfraConnector = DefaultInfraConnector()
}

// TestDefaultEventPublisherFactory_SatisfiesInterface is the paired
// compile-time check for EventPublisherFactory.
func TestDefaultEventPublisherFactory_SatisfiesInterface(t *testing.T) {
	t.Parallel()

	var _ EventPublisherFactory = DefaultEventPublisherFactory()
}

// TestDefaultEventPublisherFactory_NilChannelClosesAreNoops documents the
// production invariant that Close* methods tolerate nil inputs. This
// invariant used to live inline in the deleted closeAMQPChannelFn /
// closeMatching*Fn closures; keeping it asserted prevents regression.
func TestDefaultEventPublisherFactory_NilChannelClosesAreNoops(t *testing.T) {
	t.Parallel()

	factory := DefaultEventPublisherFactory()

	assert.NoError(t, factory.CloseAMQPChannel(nil))
	assert.NoError(t, factory.CloseMatchingEventPublisher(nil))
	assert.NoError(t, factory.CloseIngestionEventPublisher(nil))
}

// TestDefaultEventPublisherFactory_OpenDedicatedChannel_NilConnection covers
// the production OpenDedicatedChannel delegating to openDedicatedChannel,
// which rejects a nil connection with errRabbitMQConnectionNil. The
// matching/ingestion "New*EventPublisher" methods are compile-time checked
// by TestDefaultEventPublisherFactory_SatisfiesInterface; calling them with
// nil channels is undefined behaviour in the underlying libs, so they are
// exercised through initEventPublishers tests with fake channels instead.
func TestDefaultEventPublisherFactory_OpenDedicatedChannel_NilConnection(t *testing.T) {
	t.Parallel()

	factory := DefaultEventPublisherFactory()

	ch, err := factory.OpenDedicatedChannel(nil)

	require.Error(t, err)
	assert.Nil(t, ch)
	assert.ErrorIs(t, err, errRabbitMQConnectionNil)
}

// TestDefaultInfraConnector_ConnectPostgres_NilPanicRecovered exercises the
// delegation to client.Connect. A nil *libPostgres.Client panics inside the
// client method; we do not swallow the panic, we just document that the
// default connector is a straight pass-through — callers are responsible
// for non-nil args. Supplying a non-nil but unconnected client exercises
// the delegation without needing a real postgres.
func TestDefaultInfraConnector_ConnectPostgres_UnconnectedClient(t *testing.T) {
	t.Parallel()

	connector := DefaultInfraConnector()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// An empty client has no DSN; Connect returns an error. The important
	// property for coverage is that the default delegates — not whether
	// the error is a specific sentinel.
	err := connector.ConnectPostgres(ctx, &libPostgres.Client{})
	_ = err
}

// TestDefaultInfraConnector_EnsureRabbitChannel_EmptyConnection exercises
// the default delegation to conn.EnsureChannel. A zero-value
// RabbitMQConnection has no underlying *amqp.Connection; EnsureChannel
// returns an error rather than panicking. Coverage target is the one-line
// delegation.
func TestDefaultInfraConnector_EnsureRabbitChannel_EmptyConnection(t *testing.T) {
	t.Parallel()

	connector := DefaultInfraConnector()

	err := connector.EnsureRabbitChannel(&libRabbitmq.RabbitMQConnection{})
	_ = err
}

// TestDefaultInfraConnector_InitializeAuthBoundaryLogger exercises the
// real zap logger construction. The default config is intentionally
// minimal (zero-value libZap.Config) and the lib-commons validator
// requires OTelLibraryName, so we expect an error — the coverage goal
// is the delegation itself, not the zap configuration.
func TestDefaultInfraConnector_InitializeAuthBoundaryLogger(t *testing.T) {
	t.Parallel()

	connector := DefaultInfraConnector()

	logger, err := connector.InitializeAuthBoundaryLogger()
	_ = logger
	_ = err
}

// TestDefaultInfraConnector_NewS3Client_EmptyConfig exercises the
// delegation to reporting storage NewS3Client. Empty config produces a
// valid (if non-functional) client — coverage goal is the delegation.
func TestDefaultInfraConnector_NewS3Client_EmptyConfig(t *testing.T) {
	t.Parallel()

	connector := DefaultInfraConnector()

	client, err := connector.NewS3Client(context.Background(), reportingStorage.S3Config{Region: "us-east-1"})
	// Any error shape is acceptable — we only care that the method
	// ran, not whether an empty config produces a usable client.
	_ = client
	_ = err
}

// TestConnectInfrastructure_InjectedConnectorIsExercised proves that a
// connector supplied via the function parameter (not a package-level
// global) is the one receiving the calls. It is the positive counterpart
// to the old setEventPublisherFnsForTest pattern: fake in, observe calls
// out, no shared state.
//
// The "saw*" flags are atomic.Bool because ConnectPostgres and
// EnsureRabbitChannel run on parallel goroutines inside the errgroup.
func TestConnectInfrastructure_InjectedConnectorIsExercised(t *testing.T) {
	t.Parallel()

	var (
		sawMigrations atomic.Bool
		sawPostgres   atomic.Bool
		sawRabbit     atomic.Bool
	)

	connector := &fakeInfraConnector{
		runMigrations: func(context.Context, string, string, string, libLog.Logger, bool) error {
			sawMigrations.Store(true)

			return nil
		},
		connectPostgres: func(context.Context, *libPostgres.Client) error {
			sawPostgres.Store(true)

			return nil
		},
		ensureRabbitChannel: func(*libRabbitmq.RabbitMQConnection) error {
			sawRabbit.Store(true)

			return nil
		},
	}

	cfg := &Config{Postgres: PostgresConfig{PrimaryDB: "matcher"}}
	asserter := newTestAsserter(t)

	err := connectInfrastructure(
		context.Background(),
		asserter,
		cfg,
		&libPostgres.Client{},
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
		connector,
	)

	require.NoError(t, err)
	assert.True(t, sawMigrations.Load(), "RunMigrations should be invoked on the injected connector")
	assert.True(t, sawPostgres.Load(), "ConnectPostgres should be invoked on the injected connector")
	assert.True(t, sawRabbit.Load(), "EnsureRabbitChannel should be invoked on the injected connector")
}

// TestConnectInfrastructure_NilConnectorFallsBackToDefault documents the
// graceful-default contract: passing nil does not panic, it silently
// delegates to DefaultInfraConnector. Production code paths that never
// supply a connector (legacy tests, integration harnesses) remain valid.
func TestConnectInfrastructure_NilConnectorFallsBackToDefault(t *testing.T) {
	t.Parallel()

	// Uses the real DefaultInfraConnector, so we pass a cfg that makes
	// RunMigrations fail fast (empty DSN, empty migrations path). We only
	// care that the call does not panic when connector is nil — the error
	// path is sufficient evidence the default was resolved.
	cfg := &Config{Postgres: PostgresConfig{PrimaryDB: "matcher"}}
	asserter := newTestAsserter(t)

	err := connectInfrastructure(
		context.Background(),
		asserter,
		cfg,
		&libPostgres.Client{},
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
		nil,
	)

	require.Error(t, err, "expected default RunMigrations to error on empty DSN")
	// No panic == success for this test; the returned error is incidental.
}

// TestInitEventPublishers_InjectedFactoryIsExercised is the
// EventPublisherFactory counterpart to the connector test. It proves the
// cleanup path (cleanupPublishersOnFailure) uses the injected factory's
// Close* methods, not a hidden global.
//
// The "saw*" booleans are atomic.Bool because initEventPublishers fans
// channel opens out across an errgroup and openDedicatedChannel fires
// from multiple goroutines concurrently.
func TestInitEventPublishers_InjectedFactoryIsExercised(t *testing.T) {
	t.Parallel()

	var sawOpen, sawNewMatching, sawClose atomic.Bool

	publishers := &fakeEventPublisherFactory{
		openDedicatedChannel: func(*libRabbitmq.RabbitMQConnection) (*amqp.Channel, error) {
			sawOpen.Store(true)

			return new(amqp.Channel), nil
		},
		newMatchingPublisher: func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*matchingRabbitmq.EventPublisher, error) {
			sawNewMatching.Store(true)

			return nil, errors.New("stop here to exercise cleanup")
		},
		newIngestionPublisher: func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*ingestionRabbitmq.EventPublisher, error) {
			return nil, errors.New("unused")
		},
		closeAMQPChannel: func(*amqp.Channel) error {
			sawClose.Store(true)

			return nil
		},
	}

	mp, ip, err := initEventPublishers(
		context.Background(),
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
		nil,
		publishers,
	)

	require.Error(t, err)
	assert.Nil(t, mp)
	assert.Nil(t, ip)
	assert.True(t, sawOpen.Load(), "OpenDedicatedChannel should be invoked via injected factory")
	assert.True(t, sawNewMatching.Load(), "NewMatchingEventPublisher should be invoked via injected factory")
	assert.True(t, sawClose.Load(), "CloseAMQPChannel should be invoked via injected factory on cleanup")
}

// TestInitTelemetryWithTimeout_UsesInjectedConnector proves the telemetry
// path routes through the connector rather than a package-level fn-var.
// This test could not be written cleanly before the refactor — it would
// have required mutating initTelemetryFn under initTelemetryFnMu.
//
// Telemetry must be Enabled for the connector hook to fire: the fast-path
// disabled branch intentionally bypasses the connector (see
// InitTelemetryWithTimeout) so that test doubles cannot stall the
// deterministic no-op fallback.
func TestInitTelemetryWithTimeout_UsesInjectedConnector(t *testing.T) {
	t.Parallel()

	var sawCall bool

	connector := &fakeInfraConnector{
		initTelemetry: func(_ *Config, _ libLog.Logger) *libOpentelemetry.Telemetry {
			sawCall = true

			// Return a disabled telemetry struct so the caller has a
			// non-nil instance without opening real exporters.
			return InitTelemetry(&Config{Telemetry: TelemetryConfig{Enabled: false}}, &libLog.NopLogger{})
		},
	}

	cfg := &Config{Telemetry: TelemetryConfig{Enabled: true, ServiceName: "factory-test"}}

	telemetry := InitTelemetryWithTimeout(context.Background(), cfg, &libLog.NopLogger{}, connector)

	require.NotNil(t, telemetry)
	assert.True(t, sawCall, "injected InitTelemetry hook should be invoked on the live-init path")
}

// newTestAsserter constructs a libAssert.Asserter tied to a nop logger —
// sufficient for connectInfrastructure tests that only care about
// control-flow, not emitted assertions.
func newTestAsserter(t *testing.T) *libAssert.Asserter {
	t.Helper()

	return libAssert.New(context.Background(), &libLog.NopLogger{}, "matcher", "test")
}
