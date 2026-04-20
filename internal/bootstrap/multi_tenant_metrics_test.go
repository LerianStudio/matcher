// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMultiTenantMetrics_Enabled(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(true)

	require.NoError(t, err)
	require.NotNil(t, m)
	assert.NotNil(t, m.connectionsTotal, "connectionsTotal should be initialized")
	assert.NotNil(t, m.connectionErrors, "connectionErrors should be initialized")
	assert.NotNil(t, m.consumersActive, "consumersActive should be initialized")
	assert.NotNil(t, m.messagesProcessed, "messagesProcessed should be initialized")
}

func TestNewMultiTenantMetrics_Disabled(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(false)

	require.NoError(t, err)
	require.NotNil(t, m, "disabled metrics should still return a non-nil struct")
	assert.NotNil(t, m.connectionsTotal, "noop connectionsTotal should be non-nil")
	assert.NotNil(t, m.connectionErrors, "noop connectionErrors should be non-nil")
	assert.NotNil(t, m.consumersActive, "noop consumersActive should be non-nil")
	assert.NotNil(t, m.messagesProcessed, "noop messagesProcessed should be non-nil")
}

func TestMultiTenantMetrics_NilReceiver_NoPanic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(*MultiTenantMetrics)
	}{
		{
			name: "RecordConnection",
			fn: func(m *MultiTenantMetrics) {
				m.RecordConnection(context.Background(), "tenant-a", "success")
			},
		},
		{
			name: "RecordConnectionError",
			fn: func(m *MultiTenantMetrics) {
				m.RecordConnectionError(context.Background(), "tenant-a", "timeout")
			},
		},
		{
			name: "SetActiveConsumers",
			fn: func(m *MultiTenantMetrics) {
				m.SetActiveConsumers(context.Background(), "tenant-a", "events", 5)
			},
		},
		{
			name: "RecordMessageProcessed",
			fn: func(m *MultiTenantMetrics) {
				m.RecordMessageProcessed(context.Background(), "tenant-a", "events", "success")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var m *MultiTenantMetrics

			assert.NotPanics(t, func() {
				tt.fn(m)
			}, "nil receiver should not panic")
		})
	}
}

func TestMultiTenantMetrics_RecordConnection_WithContext(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(true)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		m.RecordConnection(context.Background(), "tenant-abc", "success")
		m.RecordConnection(context.Background(), "tenant-abc", "failure")
	}, "recording connections with valid context should not panic")
}

func TestMultiTenantMetrics_RecordConnectionError_WithContext(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(true)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		m.RecordConnectionError(context.Background(), "tenant-abc", "timeout")
		m.RecordConnectionError(context.Background(), "tenant-abc", "refused")
	}, "recording connection errors with valid context should not panic")
}

func TestMultiTenantMetrics_SetActiveConsumers_WithContext(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(true)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		m.SetActiveConsumers(context.Background(), "tenant-abc", "events.queue", 3)
		m.SetActiveConsumers(context.Background(), "tenant-abc", "events.queue", 0)
	}, "setting active consumers with valid context should not panic")
}

func TestMultiTenantMetrics_RecordMessageProcessed_WithContext(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(true)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		m.RecordMessageProcessed(context.Background(), "tenant-abc", "events.queue", "success")
		m.RecordMessageProcessed(context.Background(), "tenant-abc", "events.queue", "error")
	}, "recording messages processed with valid context should not panic")
}

func TestMultiTenantMetrics_Disabled_ZeroOverhead(t *testing.T) {
	t.Parallel()

	m, err := NewMultiTenantMetrics(false)
	require.NoError(t, err)

	ctx := context.Background()

	assert.NotPanics(t, func() {
		m.RecordConnection(ctx, "tenant-a", "success")
		m.RecordConnectionError(ctx, "tenant-a", "timeout")
		m.SetActiveConsumers(ctx, "tenant-a", "queue", 10)
		m.RecordMessageProcessed(ctx, "tenant-a", "queue", "success")
	}, "disabled metrics should accept calls without panic (zero overhead)")
}
