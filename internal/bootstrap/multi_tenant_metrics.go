// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// MultiTenantMetrics holds canonical multi-tenant operational metrics.
// When multi-tenant mode is disabled, all metrics use a no-op meter
// to ensure zero overhead.
type MultiTenantMetrics struct {
	connectionsTotal  metric.Int64Counter
	connectionErrors  metric.Int64Counter
	consumersActive   metric.Int64Gauge
	messagesProcessed metric.Int64Counter
}

// NewMultiTenantMetrics creates multi-tenant metrics using either the global
// OTel meter (when enabled) or a no-op meter (when disabled) for zero overhead.
func NewMultiTenantMetrics(enabled bool) (*MultiTenantMetrics, error) {
	var meter metric.Meter
	if enabled {
		meter = otel.Meter("matcher.multi_tenant")
	} else {
		meter = noop.Meter{}
	}

	mtMetrics := &MultiTenantMetrics{}

	var err error

	mtMetrics.connectionsTotal, err = meter.Int64Counter("tenant_connections_total",
		metric.WithDescription("Total tenant database connections created"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return nil, fmt.Errorf("create tenant_connections_total counter: %w", err)
	}

	mtMetrics.connectionErrors, err = meter.Int64Counter("tenant_connection_errors_total",
		metric.WithDescription("Connection failures per tenant"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, fmt.Errorf("create tenant_connection_errors_total counter: %w", err)
	}

	mtMetrics.consumersActive, err = meter.Int64Gauge("tenant_consumers_active",
		metric.WithDescription("Active message consumers by tenant"),
		metric.WithUnit("{consumer}"))
	if err != nil {
		return nil, fmt.Errorf("create tenant_consumers_active gauge: %w", err)
	}

	mtMetrics.messagesProcessed, err = meter.Int64Counter("tenant_messages_processed_total",
		metric.WithDescription("Messages processed per tenant"),
		metric.WithUnit("{message}"))
	if err != nil {
		return nil, fmt.Errorf("create tenant_messages_processed_total counter: %w", err)
	}

	return mtMetrics, nil
}

// RecordConnection increments the tenant connection counter.
// Safe to call on a nil receiver (no-op).
func (m *MultiTenantMetrics) RecordConnection(ctx context.Context, tenantID, status string) {
	if m == nil {
		return
	}

	m.connectionsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("status", status),
	))
}

// RecordConnectionError increments the tenant connection error counter.
// Safe to call on a nil receiver (no-op).
func (m *MultiTenantMetrics) RecordConnectionError(ctx context.Context, tenantID, errorType string) {
	if m == nil {
		return
	}

	m.connectionErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("error_type", errorType),
	))
}

// SetActiveConsumers records the current number of active consumers for a tenant.
// Safe to call on a nil receiver (no-op).
func (m *MultiTenantMetrics) SetActiveConsumers(ctx context.Context, tenantID, queueName string, count int64) {
	if m == nil {
		return
	}

	m.consumersActive.Record(ctx, count, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("queue_name", queueName),
	))
}

// RecordMessageProcessed increments the tenant message processing counter.
// Safe to call on a nil receiver (no-op).
func (m *MultiTenantMetrics) RecordMessageProcessed(ctx context.Context, tenantID, queueName, status string) {
	if m == nil {
		return
	}

	m.messagesProcessed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("queue_name", queueName),
		attribute.String("status", status),
	))
}
