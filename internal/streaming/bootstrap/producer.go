// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package bootstrap wires Matcher's lib-streaming catalog, producer, and outbox relay.
package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/opentelemetry/metrics"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	streaming "github.com/LerianStudio/lib-streaming/v2"

	streamingcatalog "github.com/LerianStudio/matcher/internal/streaming/catalog"
)

// Producer bootstrap errors.
var (
	ErrStreamingCatalogEmpty            = errors.New("streaming catalog must contain at least one event definition")
	ErrCriticalDeliveryPolicyOverridden = errors.New("critical streaming delivery policy overrides are not allowed")
)

// ProducerOptions contains optional dependencies for constructing the streaming producer.
type ProducerOptions struct {
	Logger                libLog.Logger
	MetricsFactory        *metrics.MetricsFactory
	Tracer                trace.Tracer
	CircuitBreakerManager circuitbreaker.Manager
	OutboxRepository      outbox.OutboxRepository
	OutboxWriter          streaming.OutboxWriter
}

// ProducerBundle carries the emitter and optional lifecycle app created during bootstrap.
type ProducerBundle struct {
	Emitter streaming.Emitter
	App     libCommons.App
	Catalog streaming.Catalog
	Config  streaming.Config
}

// RegisterOutboxRelay registers the streaming producer's outbox replay handler when enabled.
func RegisterOutboxRelay(bundle ProducerBundle, registry *outbox.HandlerRegistry) error {
	producer, ok := bundle.Emitter.(*streaming.Producer)
	if !ok {
		return nil
	}

	innerRegistry := outbox.NewHandlerRegistry()
	if err := producer.RegisterOutboxRelay(innerRegistry); err != nil {
		return fmt.Errorf("register streaming outbox relay: %w", err)
	}

	if err := registry.Register(streaming.StreamingOutboxEventType, func(ctx context.Context, row *outbox.OutboxEvent) error {
		disabled, err := isDisabledStreamingOutboxRow(row)
		if err != nil {
			return err
		}

		if disabled {
			return streaming.ErrEventDisabled
		}

		return innerRegistry.Handle(ctx, row)
	}); err != nil {
		return fmt.Errorf("register guarded streaming outbox relay: %w", err)
	}

	return nil
}

func isDisabledStreamingOutboxRow(row *outbox.OutboxEvent) (bool, error) {
	if row == nil || row.EventType != streaming.StreamingOutboxEventType {
		return false, nil
	}

	var probe struct {
		Version int                       `json:"version"`
		Policy  *streaming.DeliveryPolicy `json:"policy"`
	}

	unmarshalErr := json.Unmarshal(row.Payload, &probe)
	if unmarshalErr != nil {
		return false, fmt.Errorf("inspect streaming outbox envelope: %w: %w", streaming.ErrInvalidOutboxEnvelope, unmarshalErr)
	}

	if probe.Version == 0 || probe.Policy == nil {
		return false, nil
	}

	policy := probe.Policy.Normalize()
	if err := policy.Validate(); err != nil {
		return false, fmt.Errorf("validate disabled streaming outbox policy: %w", err)
	}

	return !policy.Enabled, nil
}

// NewEmitter builds Matcher's canonical streaming catalog and creates an emitter bundle.
func NewEmitter(ctx context.Context, cfg streaming.Config, options ...ProducerOptions) (ProducerBundle, error) {
	catalog, err := streamingcatalog.NewCatalog()
	if err != nil {
		return ProducerBundle{}, fmt.Errorf("build streaming catalog: %w", err)
	}

	return NewEmitterWithCatalog(ctx, cfg, catalog, options...)
}

// NewEmitterWithCatalog creates an emitter bundle using a caller-supplied catalog.
func NewEmitterWithCatalog(
	ctx context.Context,
	cfg streaming.Config,
	catalog streaming.Catalog,
	options ...ProducerOptions,
) (ProducerBundle, error) {
	if len(catalog.Definitions()) == 0 {
		return ProducerBundle{}, ErrStreamingCatalogEmpty
	}

	if err := ValidateCriticalPolicyOverrides(catalog, cfg.PolicyOverrides); err != nil {
		return ProducerBundle{}, err
	}

	if !cfg.Enabled || len(cfg.Brokers) == 0 {
		return ProducerBundle{
			Emitter: streaming.NewNoopEmitter(),
			Catalog: catalog,
			Config:  cfg,
		}, nil
	}

	opts := buildProducerOptions(catalog, firstProducerOptions(options))

	producer, err := streaming.NewProducer(ctx, cfg, opts...)
	if err != nil {
		return ProducerBundle{}, fmt.Errorf("construct streaming producer: %w", err)
	}

	return ProducerBundle{
		Emitter: producer,
		App:     producer,
		Catalog: catalog,
		Config:  cfg,
	}, nil
}

// ValidateCriticalPolicyOverrides rejects runtime overrides that would change
// canonical CRITICAL delivery policies. Matcher treats CRITICAL events as
// catalog-owned: they must remain enabled, direct=skip, outbox=always, and
// dlq=on_routable_failure so compliance facts are durably enqueued.
func ValidateCriticalPolicyOverrides(catalog streaming.Catalog, overrides map[string]streaming.DeliveryPolicyOverride) error {
	if len(overrides) == 0 {
		return nil
	}

	criticalPolicies := make(map[string]streaming.DeliveryPolicy)

	for _, definition := range catalog.Definitions() {
		if definition.DefaultPolicy.Direct == streaming.DirectModeSkip && definition.DefaultPolicy.Outbox == streaming.OutboxModeAlways {
			criticalPolicies[definition.Key] = definition.DefaultPolicy
		}
	}

	for key, override := range overrides {
		defaultPolicy, ok := criticalPolicies[key]
		if !ok {
			continue
		}

		if criticalOverrideChangesPolicy(defaultPolicy, override) {
			return fmt.Errorf("%w: %s", ErrCriticalDeliveryPolicyOverridden, key)
		}
	}

	return nil
}

func criticalOverrideChangesPolicy(defaultPolicy streaming.DeliveryPolicy, override streaming.DeliveryPolicyOverride) bool {
	if override.Enabled != nil && *override.Enabled != defaultPolicy.Enabled {
		return true
	}

	if override.Direct != "" && override.Direct != defaultPolicy.Direct {
		return true
	}

	if override.Outbox != "" && override.Outbox != defaultPolicy.Outbox {
		return true
	}

	return override.DLQ != "" && override.DLQ != defaultPolicy.DLQ
}

func firstProducerOptions(options []ProducerOptions) ProducerOptions {
	if len(options) == 0 {
		return ProducerOptions{}
	}

	return options[0]
}

func buildProducerOptions(catalog streaming.Catalog, options ProducerOptions) []streaming.EmitterOption {
	producerOptions := []streaming.EmitterOption{streaming.WithCatalog(catalog)}

	if options.Logger != nil {
		producerOptions = append(producerOptions, streaming.WithLogger(options.Logger))
	}

	if options.MetricsFactory != nil {
		producerOptions = append(producerOptions, streaming.WithMetricsFactory(options.MetricsFactory))
	}

	if options.Tracer != nil {
		producerOptions = append(producerOptions, streaming.WithTracer(options.Tracer))
	}

	if options.CircuitBreakerManager != nil {
		producerOptions = append(producerOptions, streaming.WithCircuitBreakerManager(options.CircuitBreakerManager))
	}

	if options.OutboxRepository != nil {
		producerOptions = append(producerOptions, streaming.WithOutboxRepository(options.OutboxRepository))
	}

	if options.OutboxWriter != nil {
		producerOptions = append(producerOptions, streaming.WithOutboxWriter(options.OutboxWriter))
	}

	return producerOptions
}
