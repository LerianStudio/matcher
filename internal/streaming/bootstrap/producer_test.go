// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	streaming "github.com/LerianStudio/lib-streaming/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	streamingbootstrap "github.com/LerianStudio/matcher/internal/streaming/bootstrap"
	streamingcatalog "github.com/LerianStudio/matcher/internal/streaming/catalog"
)

func TestNewEmitterDisabledReturnsNonNilNoopEmitter(t *testing.T) {
	bundle, err := streamingbootstrap.NewEmitter(context.Background(), streaming.Config{Enabled: false})
	require.NoError(t, err)
	require.NotNil(t, bundle.Emitter)
	require.Nil(t, bundle.App)
	require.Len(t, bundle.Catalog.Definitions(), 46)
}

func TestNewEmitterEnabledWithoutBrokersReturnsNoopEmitter(t *testing.T) {
	bundle, err := streamingbootstrap.NewEmitter(context.Background(), streaming.Config{Enabled: true})
	require.NoError(t, err)
	require.NotNil(t, bundle.Emitter)
	require.Nil(t, bundle.App)
	require.NoError(t, bundle.Emitter.Healthy(context.Background()))
}

func TestNewEmitterWithCatalogRejectsEnabledProducerWithoutCatalog(t *testing.T) {
	_, err := streamingbootstrap.NewEmitterWithCatalog(context.Background(), enabledConfig(), streaming.Catalog{})
	require.Error(t, err)
	require.True(t, errors.Is(err, streamingbootstrap.ErrStreamingCatalogEmpty))
}

func TestNewEmitterWithCatalogEnabledBuildsProducerWithCatalog(t *testing.T) {
	catalog, err := streamingcatalog.NewCatalog()
	require.NoError(t, err)

	bundle, err := streamingbootstrap.NewEmitterWithCatalog(context.Background(), enabledConfig(), catalog)
	require.NoError(t, err)
	require.NotNil(t, bundle.Emitter)
	require.NotNil(t, bundle.App)
	require.Len(t, bundle.Catalog.Definitions(), 46)
	require.NoError(t, bundle.Emitter.Close())
}

func TestNewEmitterWithCatalogRejectsCriticalPolicyDowngrade(t *testing.T) {
	catalog, err := streamingcatalog.NewCatalog()
	require.NoError(t, err)

	disabled := false
	tests := []struct {
		name     string
		override streaming.DeliveryPolicyOverride
	}{
		{
			name: "disabled critical event",
			override: streaming.DeliveryPolicyOverride{
				Enabled: &disabled,
			},
		},
		{
			name: "removes outbox durability",
			override: streaming.DeliveryPolicyOverride{
				Outbox: streaming.OutboxModeNever,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := enabledConfig()
			cfg.PolicyOverrides = map[string]streaming.DeliveryPolicyOverride{
				"audit_log.created": tt.override,
			}

			_, err := streamingbootstrap.NewEmitterWithCatalog(context.Background(), cfg, catalog)
			require.ErrorIs(t, err, streamingbootstrap.ErrCriticalDeliveryPolicyOverridden)
		})
	}
}

func TestNewEmitterWithCatalogAllowsImportantPolicyOverride(t *testing.T) {
	catalog, err := streamingcatalog.NewCatalog()
	require.NoError(t, err)

	cfg := enabledConfig()
	cfg.PolicyOverrides = map[string]streaming.DeliveryPolicyOverride{
		"reconciliation_context.created": {Outbox: streaming.OutboxModeNever},
	}

	bundle, err := streamingbootstrap.NewEmitterWithCatalog(context.Background(), cfg, catalog)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bundle.Emitter.Close()) })
}

func TestRegisterOutboxRelayEnabledProducerRegistersStreamingEventType(t *testing.T) {
	catalog, err := streamingcatalog.NewCatalog()
	require.NoError(t, err)

	bundle, err := streamingbootstrap.NewEmitterWithCatalog(context.Background(), enabledConfig(), catalog)
	require.NoError(t, err)
	require.NotNil(t, bundle.Emitter)
	t.Cleanup(func() { require.NoError(t, bundle.Emitter.Close()) })

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, streamingbootstrap.RegisterOutboxRelay(bundle, registry))
	require.NoError(t, bundle.Emitter.Close())

	err = registry.Handle(context.Background(), streamingOutboxRow(t))
	require.Error(t, err)
	assert.ErrorIs(t, err, streaming.ErrEmitterClosed)
}

func TestRegisterOutboxRelayPreservesExistingRegistryHandlers(t *testing.T) {
	catalog, err := streamingcatalog.NewCatalog()
	require.NoError(t, err)

	bundle, err := streamingbootstrap.NewEmitterWithCatalog(context.Background(), enabledConfig(), catalog)
	require.NoError(t, err)
	require.NotNil(t, bundle.Emitter)
	t.Cleanup(func() { require.NoError(t, bundle.Emitter.Close()) })

	registry := outbox.NewHandlerRegistry()
	called := false
	require.NoError(t, registry.Register("matcher.existing.handler", func(_ context.Context, _ *outbox.OutboxEvent) error {
		called = true
		return nil
	}))

	require.NoError(t, streamingbootstrap.RegisterOutboxRelay(bundle, registry))

	require.NoError(t, registry.Handle(context.Background(), &outbox.OutboxEvent{
		ID:        uuid.New(),
		EventType: "matcher.existing.handler",
		Payload:   []byte(`{"ok":true}`),
	}))
	assert.True(t, called)
}

func TestRegisterOutboxRelayDisabledStreamingNoops(t *testing.T) {
	bundle, err := streamingbootstrap.NewEmitter(context.Background(), streaming.Config{Enabled: false})
	require.NoError(t, err)
	require.NotNil(t, bundle.Emitter)

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, streamingbootstrap.RegisterOutboxRelay(bundle, registry))

	err = registry.Handle(context.Background(), streamingOutboxRow(t))
	require.Error(t, err)
	assert.ErrorIs(t, err, outbox.ErrHandlerNotRegistered)
}

func TestRegisterOutboxRelayDisabledPolicyRowDoesNotPublish(t *testing.T) {
	catalog, err := streamingcatalog.NewCatalog()
	require.NoError(t, err)

	bundle, err := streamingbootstrap.NewEmitterWithCatalog(context.Background(), enabledConfig(), catalog)
	require.NoError(t, err)
	require.NotNil(t, bundle.Emitter)
	t.Cleanup(func() { require.NoError(t, bundle.Emitter.Close()) })

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, streamingbootstrap.RegisterOutboxRelay(bundle, registry))
	require.NoError(t, bundle.Emitter.Close())

	row := streamingOutboxRow(t)
	var envelope streaming.OutboxEnvelope
	require.NoError(t, json.Unmarshal(row.Payload, &envelope))
	envelope.Policy.Enabled = false
	payload, err := json.Marshal(envelope)
	require.NoError(t, err)
	row.Payload = payload

	err = registry.Handle(context.Background(), row)
	require.ErrorIs(t, err, streaming.ErrEventDisabled)
	require.NotErrorIs(t, err, streaming.ErrEmitterClosed)
}

func streamingOutboxRow(t *testing.T) *outbox.OutboxEvent {
	t.Helper()

	event := streaming.Event{
		TenantID:     "00000000-0000-0000-0000-000000000001",
		ResourceType: "reconciliation_context",
		EventType:    "created",
		Source:       "matcher",
		Subject:      "ctx-1",
		Payload:      json.RawMessage(`{"context_id":"ctx-1"}`),
	}
	event.ApplyDefaults()

	envelope := streaming.OutboxEnvelope{
		Version:       1,
		Topic:         event.Topic(),
		DefinitionKey: "reconciliation_context.created",
		AggregateID:   uuid.New(),
		Policy:        streaming.DefaultDeliveryPolicy(),
		Event:         event,
	}
	payload, err := json.Marshal(envelope)
	require.NoError(t, err)

	return &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   streaming.StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}
}

func enabledConfig() streaming.Config {
	return streaming.Config{
		Enabled:               true,
		Brokers:               []string{"localhost:9092"},
		ClientID:              "matcher-unit-test",
		BatchLingerMs:         5,
		BatchMaxBytes:         1_048_576,
		MaxBufferedRecords:    10_000,
		Compression:           "lz4",
		RecordRetries:         10,
		RecordDeliveryTimeout: 30 * time.Second,
		RequiredAcks:          "all",
		CBFailureRatio:        0.5,
		CBMinRequests:         10,
		CBTimeout:             30 * time.Second,
		CloseTimeout:          30 * time.Second,
		CloudEventsSource:     "matcher",
	}
}
