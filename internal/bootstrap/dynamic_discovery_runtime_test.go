//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface satisfaction checks.
var (
	_ discoveryPorts.SchemaCache         = (*providerBackedSchemaCache)(nil)
	_ discoveryPorts.SchemaCache         = (*dynamicSchemaCache)(nil)
	_ discoveryPorts.ExtractionJobPoller = (*dynamicExtractionPoller)(nil)
)

// --- newProviderBackedSchemaCache ---

func TestNewProviderBackedSchemaCache_NilProvider(t *testing.T) {
	t.Parallel()

	cache := newProviderBackedSchemaCache(nil, false)

	require.NotNil(t, cache, "nil provider must produce a safe no-op cache, not nil")
	_, isNoop := cache.(*noopSchemaCache)
	assert.True(t, isNoop, "nil provider must produce a *noopSchemaCache")
}

func TestNewProviderBackedSchemaCache_NonNilProvider(t *testing.T) {
	t.Parallel()

	cache := newProviderBackedSchemaCache(&fakeInfraProvider{}, true)

	require.NotNil(t, cache, "non-nil provider must produce non-nil cache")
}

// --- newDynamicSchemaCache ---

func TestNewDynamicSchemaCache_NilInner(t *testing.T) {
	t.Parallel()

	cache := newDynamicSchemaCache(nil, func() time.Duration { return time.Minute })

	require.NotNil(t, cache, "nil inner must return a safe no-op cache, not nil")
	_, isNoop := cache.(*noopSchemaCache)
	assert.True(t, isNoop, "nil inner must produce a *noopSchemaCache")
}

func TestNewDynamicSchemaCache_NilTTLGetter(t *testing.T) {
	t.Parallel()

	inner := &fakeSchemaCacheInner{}
	cache := newDynamicSchemaCache(inner, nil)

	// When ttlGetter is nil, the original inner is returned unchanged.
	assert.Equal(t, inner, cache, "nil ttlGetter must return inner as-is")
}

func TestNewDynamicSchemaCache_ValidArgs(t *testing.T) {
	t.Parallel()

	inner := &fakeSchemaCacheInner{}
	cache := newDynamicSchemaCache(inner, func() time.Duration { return 5 * time.Minute })

	require.NotNil(t, cache)
	// The returned cache should be a dynamicSchemaCache wrapper, not the inner.
	_, ok := cache.(*dynamicSchemaCache)
	assert.True(t, ok, "valid args must produce a *dynamicSchemaCache wrapper")
}

func TestDynamicSchemaCache_GetSchema_DelegatesToInner(t *testing.T) {
	t.Parallel()

	expectedSchema := &sharedPorts.FetcherSchema{ConnectionID: "conn-1"}
	inner := &fakeSchemaCacheInner{getResult: expectedSchema}
	cache := newDynamicSchemaCache(inner, func() time.Duration { return time.Minute })

	schema, err := cache.GetSchema(context.Background(), "conn-1")

	require.NoError(t, err)
	assert.Equal(t, expectedSchema, schema)
}

func TestDynamicSchemaCache_GetSchema_PropagatesError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("cache miss")
	inner := &fakeSchemaCacheInner{getErr: expectedErr}
	cache := newDynamicSchemaCache(inner, func() time.Duration { return time.Minute })

	schema, err := cache.GetSchema(context.Background(), "conn-1")

	require.Error(t, err)
	assert.Nil(t, schema)
	assert.ErrorIs(t, err, expectedErr)
}

func TestDynamicSchemaCache_SetSchema_UsesDynamicTTL(t *testing.T) {
	t.Parallel()

	inner := &fakeSchemaCacheInner{}
	dynamicTTL := 10 * time.Minute
	cache := newDynamicSchemaCache(inner, func() time.Duration { return dynamicTTL })

	schema := &sharedPorts.FetcherSchema{ConnectionID: "conn-1"}
	originalTTL := 1 * time.Minute

	err := cache.SetSchema(context.Background(), "conn-1", schema, originalTTL)

	require.NoError(t, err)
	// The inner should have received the dynamic TTL, not the original.
	assert.Equal(t, dynamicTTL, inner.lastSetTTL, "SetSchema must use dynamic TTL when > 0")
}

func TestDynamicSchemaCache_SetSchema_FallsBackToOriginalTTL(t *testing.T) {
	t.Parallel()

	inner := &fakeSchemaCacheInner{}
	// Dynamic TTL getter returns 0 — should not override.
	cache := newDynamicSchemaCache(inner, func() time.Duration { return 0 })

	schema := &sharedPorts.FetcherSchema{ConnectionID: "conn-1"}
	originalTTL := 5 * time.Minute

	err := cache.SetSchema(context.Background(), "conn-1", schema, originalTTL)

	require.NoError(t, err)
	assert.Equal(t, originalTTL, inner.lastSetTTL, "SetSchema must use original TTL when dynamic TTL is 0")
}

func TestDynamicSchemaCache_InvalidateSchema_DelegatesToInner(t *testing.T) {
	t.Parallel()

	inner := &fakeSchemaCacheInner{}
	cache := newDynamicSchemaCache(inner, func() time.Duration { return time.Minute })

	err := cache.InvalidateSchema(context.Background(), "conn-1")

	require.NoError(t, err)
	assert.Equal(t, "conn-1", inner.lastInvalidateID)
}

func TestDynamicSchemaCache_InvalidateSchema_PropagatesError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("invalidate failed")
	inner := &fakeSchemaCacheInner{invalidateErr: expectedErr}
	cache := newDynamicSchemaCache(inner, func() time.Duration { return time.Minute })

	err := cache.InvalidateSchema(context.Background(), "conn-1")

	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
}

// --- newDynamicExtractionPoller ---

func TestNewDynamicExtractionPoller_NilConfigGetter(t *testing.T) {
	t.Parallel()

	poller := newDynamicExtractionPoller(nil, nil, nil, nil)

	require.NotNil(t, poller, "nil configGetter must produce a safe no-op poller, not nil")
	_, isNoop := poller.(*noopExtractionPoller)
	assert.True(t, isNoop, "nil configGetter must produce a *noopExtractionPoller")
}

func TestNewDynamicExtractionPoller_ValidArgs(t *testing.T) {
	t.Parallel()

	poller := newDynamicExtractionPoller(nil, nil, func() discoveryWorker.ExtractionPollerConfig {
		return discoveryWorker.ExtractionPollerConfig{}
	}, nil)

	require.NotNil(t, poller)
}

func TestDynamicExtractionPoller_PollUntilComplete_NilReceiver(t *testing.T) {
	t.Parallel()

	var poller *dynamicExtractionPoller
	failCalled := false

	poller.PollUntilComplete(context.Background(), uuid.New(),
		func(_ context.Context, _ string) error { return nil },
		func(_ context.Context, errMsg string) {
			failCalled = true
			assert.Contains(t, errMsg, "extraction poller unavailable")
		},
	)

	assert.True(t, failCalled, "onFailed must be called when poller is nil")
}

func TestDynamicExtractionPoller_PollUntilComplete_NilOnFailed(t *testing.T) {
	t.Parallel()

	var poller *dynamicExtractionPoller

	// Should not panic when onFailed is nil.
	assert.NotPanics(t, func() {
		poller.PollUntilComplete(context.Background(), uuid.New(),
			func(_ context.Context, _ string) error { return nil },
			nil,
		)
	})
}

// --- Fakes ---

type fakeInfraProvider struct {
	sharedPorts.InfrastructureProvider
}

type fakeSchemaCacheInner struct {
	getResult        *sharedPorts.FetcherSchema
	getErr           error
	lastSetTTL       time.Duration
	invalidateErr    error
	lastInvalidateID string
}

func (f *fakeSchemaCacheInner) GetSchema(_ context.Context, _ string) (*sharedPorts.FetcherSchema, error) {
	return f.getResult, f.getErr
}

func (f *fakeSchemaCacheInner) SetSchema(_ context.Context, _ string, _ *sharedPorts.FetcherSchema, ttl time.Duration) error {
	f.lastSetTTL = ttl

	return nil
}

func (f *fakeSchemaCacheInner) InvalidateSchema(_ context.Context, connID string) error {
	f.lastInvalidateID = connID

	return f.invalidateErr
}
