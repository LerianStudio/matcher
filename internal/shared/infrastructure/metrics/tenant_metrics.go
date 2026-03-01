// Package metrics provides multi-tenant observability metrics for the Matcher service.
// These metrics track tenant-specific connection pools, message processing, and errors.
package metrics

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// TenantMetrics holds the 4 canonical multi-tenant metrics.
// When MULTI_TENANT_ENABLED=false, these use no-op implementations for zero overhead.
type TenantMetrics struct {
	// ConnectionsTotal tracks total tenant connections created.
	ConnectionsTotal metric.Int64Counter

	// ConnectionErrorsTotal tracks connection failures per tenant.
	ConnectionErrorsTotal metric.Int64Counter

	// ConsumersActive tracks active message consumers (gauge via UpDownCounter).
	ConsumersActive metric.Int64UpDownCounter

	// MessagesProcessed tracks messages processed per tenant.
	MessagesProcessed metric.Int64Counter
}

var (
	// singleton for multi-tenant metrics to avoid duplicate registrations.
	tenantMetricsSingleton *TenantMetrics
	tenantMetricsOnce      sync.Once
)

// MeterName is the OpenTelemetry meter name for tenant metrics.
const MeterName = "matcher.tenant"

// NewTenantMetrics creates TenantMetrics with real OpenTelemetry instrumentation.
// This is used when MULTI_TENANT_ENABLED=true.
//
// The function returns a singleton instance to avoid duplicate metric registrations
// which would cause OpenTelemetry errors.
func NewTenantMetrics(meter metric.Meter) (*TenantMetrics, error) {
	var initErr error

	tenantMetricsOnce.Do(func() {
		tenantMetricsSingleton, initErr = initTenantMetrics(meter)
	})

	if initErr != nil {
		return nil, initErr
	}

	return tenantMetricsSingleton, nil
}

func initTenantMetrics(meter metric.Meter) (*TenantMetrics, error) {
	connectionsTotal, err := meter.Int64Counter(
		"tenant_connections_total",
		metric.WithDescription("Total tenant connections created"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create tenant_connections_total counter: %w", err)
	}

	connectionErrorsTotal, err := meter.Int64Counter(
		"tenant_connection_errors_total",
		metric.WithDescription("Connection failures per tenant"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create tenant_connection_errors_total counter: %w", err)
	}

	consumersActive, err := meter.Int64UpDownCounter(
		"tenant_consumers_active",
		metric.WithDescription("Active message consumers per tenant"),
		metric.WithUnit("{consumer}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create tenant_consumers_active counter: %w", err)
	}

	messagesProcessed, err := meter.Int64Counter(
		"tenant_messages_processed_total",
		metric.WithDescription("Messages processed per tenant"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create tenant_messages_processed_total counter: %w", err)
	}

	return &TenantMetrics{
		ConnectionsTotal:      connectionsTotal,
		ConnectionErrorsTotal: connectionErrorsTotal,
		ConsumersActive:       consumersActive,
		MessagesProcessed:     messagesProcessed,
	}, nil
}

// NewNoOpTenantMetrics creates TenantMetrics with no-op implementations.
// This is used when MULTI_TENANT_ENABLED=false for zero overhead.
func NewNoOpTenantMetrics() *TenantMetrics {
	noopMeter := noop.NewMeterProvider().Meter(MeterName)

	// No-op meter cannot fail, so we ignore errors
	connectionsTotal, _ := noopMeter.Int64Counter("tenant_connections_total")
	connectionErrorsTotal, _ := noopMeter.Int64Counter("tenant_connection_errors_total")
	consumersActive, _ := noopMeter.Int64UpDownCounter("tenant_consumers_active")
	messagesProcessed, _ := noopMeter.Int64Counter("tenant_messages_processed_total")

	return &TenantMetrics{
		ConnectionsTotal:      connectionsTotal,
		ConnectionErrorsTotal: connectionErrorsTotal,
		ConsumersActive:       consumersActive,
		MessagesProcessed:     messagesProcessed,
	}
}

// RecordConnection records a successful tenant connection.
func (tm *TenantMetrics) RecordConnection(ctx context.Context, tenantID string) {
	if tm == nil || tm.ConnectionsTotal == nil {
		return
	}

	tm.ConnectionsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
	))
}

// RecordConnectionError records a tenant connection failure.
func (tm *TenantMetrics) RecordConnectionError(ctx context.Context, tenantID, errorType string) {
	if tm == nil || tm.ConnectionErrorsTotal == nil {
		return
	}

	tm.ConnectionErrorsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("error_type", errorType),
	))
}

// ConsumerStarted increments the active consumer count for a tenant.
func (tm *TenantMetrics) ConsumerStarted(ctx context.Context, tenantID, queueName string) {
	if tm == nil || tm.ConsumersActive == nil {
		return
	}

	tm.ConsumersActive.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("queue", queueName),
	))
}

// ConsumerStopped decrements the active consumer count for a tenant.
func (tm *TenantMetrics) ConsumerStopped(ctx context.Context, tenantID, queueName string) {
	if tm == nil || tm.ConsumersActive == nil {
		return
	}

	tm.ConsumersActive.Add(ctx, -1, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("queue", queueName),
	))
}

// RecordMessageProcessed records a message processed for a tenant.
func (tm *TenantMetrics) RecordMessageProcessed(ctx context.Context, tenantID, messageType, status string) {
	if tm == nil || tm.MessagesProcessed == nil {
		return
	}

	tm.MessagesProcessed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("message_type", messageType),
		attribute.String("status", status),
	))
}

// NewTenantMetricsFromConfig creates TenantMetrics based on multi-tenant configuration.
// When multiTenantEnabled is true, returns real metrics; otherwise returns no-op metrics.
func NewTenantMetricsFromConfig(multiTenantEnabled bool) (*TenantMetrics, error) {
	if !multiTenantEnabled {
		return NewNoOpTenantMetrics(), nil
	}

	meter := otel.Meter(MeterName)

	return NewTenantMetrics(meter)
}
