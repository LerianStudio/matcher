//go:build unit

package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestInitTenantMetrics_ValidMeter_CreatesMetrics(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter(MeterName)

	// Use initTenantMetrics directly to avoid singleton issues in tests
	metrics, err := initTenantMetrics(meter)

	require.NoError(t, err)
	require.NotNil(t, metrics)
	assert.NotNil(t, metrics.ConnectionsTotal)
	assert.NotNil(t, metrics.ConnectionErrorsTotal)
	assert.NotNil(t, metrics.ConsumersActive)
	assert.NotNil(t, metrics.MessagesProcessed)
}

func TestNewNoOpTenantMetrics_CreatesNoOpMetrics(t *testing.T) {
	t.Parallel()

	metrics := NewNoOpTenantMetrics()

	require.NotNil(t, metrics)
	assert.NotNil(t, metrics.ConnectionsTotal)
	assert.NotNil(t, metrics.ConnectionErrorsTotal)
	assert.NotNil(t, metrics.ConsumersActive)
	assert.NotNil(t, metrics.MessagesProcessed)
}

func TestNewNoOpTenantMetrics_DoesNotPanicOnIncrement(t *testing.T) {
	t.Parallel()

	metrics := NewNoOpTenantMetrics()
	ctx := context.Background()

	// These should not panic
	assert.NotPanics(t, func() {
		metrics.RecordConnection(ctx, "test-tenant")
	})
	assert.NotPanics(t, func() {
		metrics.RecordConnectionError(ctx, "test-tenant", "timeout")
	})
	assert.NotPanics(t, func() {
		metrics.ConsumerStarted(ctx, "test-tenant", "test-queue")
	})
	assert.NotPanics(t, func() {
		metrics.ConsumerStopped(ctx, "test-tenant", "test-queue")
	})
	assert.NotPanics(t, func() {
		metrics.RecordMessageProcessed(ctx, "test-tenant", "event", "success")
	})
}

func TestTenantMetrics_RecordConnection_WithNilMetrics_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var nilMetrics *TenantMetrics

	assert.NotPanics(t, func() {
		nilMetrics.RecordConnection(ctx, "test-tenant")
	})
}

func TestTenantMetrics_RecordConnectionError_WithNilMetrics_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var nilMetrics *TenantMetrics

	assert.NotPanics(t, func() {
		nilMetrics.RecordConnectionError(ctx, "test-tenant", "timeout")
	})
}

func TestTenantMetrics_ConsumerStarted_WithNilMetrics_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var nilMetrics *TenantMetrics

	assert.NotPanics(t, func() {
		nilMetrics.ConsumerStarted(ctx, "test-tenant", "queue")
	})
}

func TestTenantMetrics_ConsumerStopped_WithNilMetrics_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var nilMetrics *TenantMetrics

	assert.NotPanics(t, func() {
		nilMetrics.ConsumerStopped(ctx, "test-tenant", "queue")
	})
}

func TestTenantMetrics_RecordMessageProcessed_WithNilMetrics_DoesNotPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var nilMetrics *TenantMetrics

	assert.NotPanics(t, func() {
		nilMetrics.RecordMessageProcessed(ctx, "test-tenant", "event", "success")
	})
}

func TestNewTenantMetricsFromConfig_MultiTenantDisabled_ReturnsNoOpMetrics(t *testing.T) {
	t.Parallel()

	metrics, err := NewTenantMetricsFromConfig(false)

	require.NoError(t, err)
	require.NotNil(t, metrics)
	assert.NotNil(t, metrics.ConnectionsTotal)
	assert.NotNil(t, metrics.ConnectionErrorsTotal)
	assert.NotNil(t, metrics.ConsumersActive)
	assert.NotNil(t, metrics.MessagesProcessed)
}

func TestNewTenantMetricsFromConfig_MultiTenantEnabled_ReturnsRealMetrics(t *testing.T) {
	t.Parallel()

	// NewTenantMetricsFromConfig uses global otel.Meter which uses noop provider by default
	// This test verifies the function returns valid metrics without error
	metrics, err := NewTenantMetricsFromConfig(true)

	require.NoError(t, err)
	require.NotNil(t, metrics)
	assert.NotNil(t, metrics.ConnectionsTotal)
	assert.NotNil(t, metrics.ConnectionErrorsTotal)
	assert.NotNil(t, metrics.ConsumersActive)
	assert.NotNil(t, metrics.MessagesProcessed)
}

func TestTenantMetrics_RecordConnection_WithValidMetrics(t *testing.T) {
	t.Parallel()

	metrics := NewNoOpTenantMetrics()
	ctx := context.Background()

	// Should not panic and complete successfully
	metrics.RecordConnection(ctx, "tenant-123")
}

func TestTenantMetrics_RecordConnectionError_WithValidMetrics(t *testing.T) {
	t.Parallel()

	metrics := NewNoOpTenantMetrics()
	ctx := context.Background()

	// Should not panic and complete successfully
	metrics.RecordConnectionError(ctx, "tenant-123", "connection_refused")
}

func TestTenantMetrics_ConsumerStarted_WithValidMetrics(t *testing.T) {
	t.Parallel()

	metrics := NewNoOpTenantMetrics()
	ctx := context.Background()

	// Should not panic and complete successfully
	metrics.ConsumerStarted(ctx, "tenant-123", "events.queue")
}

func TestTenantMetrics_ConsumerStopped_WithValidMetrics(t *testing.T) {
	t.Parallel()

	metrics := NewNoOpTenantMetrics()
	ctx := context.Background()

	// Should not panic and complete successfully
	metrics.ConsumerStopped(ctx, "tenant-123", "events.queue")
}

func TestTenantMetrics_RecordMessageProcessed_WithValidMetrics(t *testing.T) {
	t.Parallel()

	metrics := NewNoOpTenantMetrics()
	ctx := context.Background()

	// Should not panic and complete successfully
	metrics.RecordMessageProcessed(ctx, "tenant-123", "match.created", "success")
}

func TestTenantMetrics_RecordMessageProcessed_VariousStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tenantID    string
		messageType string
		status      string
	}{
		{
			name:        "success status",
			tenantID:    "tenant-1",
			messageType: "ingestion.completed",
			status:      "success",
		},
		{
			name:        "error status",
			tenantID:    "tenant-2",
			messageType: "matching.failed",
			status:      "error",
		},
		{
			name:        "retry status",
			tenantID:    "tenant-3",
			messageType: "exception.created",
			status:      "retry",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			metrics := NewNoOpTenantMetrics()
			ctx := context.Background()

			assert.NotPanics(t, func() {
				metrics.RecordMessageProcessed(ctx, tt.tenantID, tt.messageType, tt.status)
			})
		})
	}
}

func TestTenantMetrics_NilFields_DoNotPanic(t *testing.T) {
	t.Parallel()

	// Create metrics with nil fields
	metrics := &TenantMetrics{
		ConnectionsTotal:      nil,
		ConnectionErrorsTotal: nil,
		ConsumersActive:       nil,
		MessagesProcessed:     nil,
	}
	ctx := context.Background()

	assert.NotPanics(t, func() {
		metrics.RecordConnection(ctx, "tenant")
	})
	assert.NotPanics(t, func() {
		metrics.RecordConnectionError(ctx, "tenant", "err")
	})
	assert.NotPanics(t, func() {
		metrics.ConsumerStarted(ctx, "tenant", "q")
	})
	assert.NotPanics(t, func() {
		metrics.ConsumerStopped(ctx, "tenant", "q")
	})
	assert.NotPanics(t, func() {
		metrics.RecordMessageProcessed(ctx, "tenant", "type", "status")
	})
}

func TestNewTenantMetrics_SingletonBehavior(t *testing.T) {
	// Not parallel - tests singleton behavior

	// First call should create and return metrics
	meter := noop.NewMeterProvider().Meter(MeterName)
	metrics1, err1 := NewTenantMetrics(meter)

	require.NoError(t, err1)
	require.NotNil(t, metrics1)

	// Second call should return the same instance
	metrics2, err2 := NewTenantMetrics(meter)

	require.NoError(t, err2)
	require.NotNil(t, metrics2)

	// Should be the same pointer (singleton)
	assert.Equal(t, metrics1, metrics2)
}
