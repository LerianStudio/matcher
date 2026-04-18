// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
)

// noopSystemplaneStore is a minimal in-memory stub satisfying
// systemplane.TestStore. Every operation is a no-op; List returns empty,
// Get always reports not-found, Subscribe blocks until context cancel, and
// Close is idempotent. This is enough to wire a real systemplane.Client for
// tests that only need the resolver's "key not registered" fallback path.
type noopSystemplaneStore struct{}

func (*noopSystemplaneStore) List(_ context.Context) ([]systemplane.TestEntry, error) {
	return nil, nil
}

func (*noopSystemplaneStore) Get(_ context.Context, _, _ string) (systemplane.TestEntry, bool, error) {
	return systemplane.TestEntry{}, false, nil
}

func (*noopSystemplaneStore) Set(_ context.Context, _ systemplane.TestEntry) error {
	return nil
}

func (*noopSystemplaneStore) Subscribe(ctx context.Context, _ func(systemplane.TestEvent)) error {
	<-ctx.Done()
	return ctx.Err()
}

func (*noopSystemplaneStore) Close() error {
	return nil
}

// newResolverWithNilClient constructs a resolver with an explicit nil client
// to exercise the (resolver != nil, resolver.client == nil) branch on every
// method. The exported constructor returns nil when the client is nil, so we
// inject manually.
func newResolverWithNilClient() *runtimeSettingsResolver {
	return &runtimeSettingsResolver{client: nil}
}

// newResolverWithUnregisteredKeys builds a resolver backed by a real
// systemplane.Client with no keys registered. Every Get lookup falls through
// to the not-found path, causing the resolver to return the fallback.
func newResolverWithUnregisteredKeys(t *testing.T) *runtimeSettingsResolver {
	t.Helper()

	client, err := systemplane.NewForTesting(&noopSystemplaneStore{})
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Close() })

	return newRuntimeSettingsResolver(client)
}

// --- newRuntimeSettingsResolver ---

func TestNewRuntimeSettingsResolver_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newRuntimeSettingsResolver(nil)

	assert.Nil(t, resolver, "nil client must produce nil resolver to signal 'no override source'")
}

func TestNewRuntimeSettingsResolver_NonNilClient(t *testing.T) {
	t.Parallel()

	client, err := systemplane.NewForTesting(&noopSystemplaneStore{})
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Close() })

	resolver := newRuntimeSettingsResolver(client)

	require.NotNil(t, resolver)
	assert.Equal(t, client, resolver.client)
}

// --- rateLimit ---

func TestRuntimeSettings_RateLimit_NilResolver(t *testing.T) {
	t.Parallel()

	var resolver *runtimeSettingsResolver

	fallback := RateLimitConfig{Enabled: true, Max: 100, ExpirySec: 60}
	got := resolver.rateLimit(fallback)

	assert.Equal(t, fallback, got)
}

func TestRuntimeSettings_RateLimit_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newResolverWithNilClient()
	fallback := RateLimitConfig{Enabled: true, Max: 100, ExpirySec: 60}

	got := resolver.rateLimit(fallback)

	assert.Equal(t, fallback, got)
}

func TestRuntimeSettings_RateLimit_UnregisteredKeys(t *testing.T) {
	resolver := newResolverWithUnregisteredKeys(t)

	fallback := RateLimitConfig{
		Enabled:           true,
		Max:               100,
		ExpirySec:         60,
		ExportMax:         10,
		ExportExpirySec:   60,
		DispatchMax:       50,
		DispatchExpirySec: 60,
	}

	got := resolver.rateLimit(fallback)

	assert.Equal(t, fallback, got, "all fallback fields must be preserved when keys are unregistered")
}

// --- idempotencyRetryWindow ---

func TestRuntimeSettings_IdempotencyRetryWindow_NilResolver(t *testing.T) {
	t.Parallel()

	var resolver *runtimeSettingsResolver

	got := resolver.idempotencyRetryWindow(5 * time.Minute)

	assert.Equal(t, 5*time.Minute, got)
}

func TestRuntimeSettings_IdempotencyRetryWindow_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newResolverWithNilClient()

	got := resolver.idempotencyRetryWindow(5 * time.Minute)

	assert.Equal(t, 5*time.Minute, got)
}

func TestRuntimeSettings_IdempotencyRetryWindow_UnregisteredKey(t *testing.T) {
	resolver := newResolverWithUnregisteredKeys(t)

	got := resolver.idempotencyRetryWindow(5 * time.Minute)

	assert.Equal(t, 5*time.Minute, got)
}

// --- idempotencySuccessTTL ---

func TestRuntimeSettings_IdempotencySuccessTTL_NilResolver(t *testing.T) {
	t.Parallel()

	var resolver *runtimeSettingsResolver

	got := resolver.idempotencySuccessTTL(168 * time.Hour)

	assert.Equal(t, 168*time.Hour, got)
}

func TestRuntimeSettings_IdempotencySuccessTTL_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newResolverWithNilClient()

	got := resolver.idempotencySuccessTTL(168 * time.Hour)

	assert.Equal(t, 168*time.Hour, got)
}

func TestRuntimeSettings_IdempotencySuccessTTL_UnregisteredKey(t *testing.T) {
	resolver := newResolverWithUnregisteredKeys(t)

	got := resolver.idempotencySuccessTTL(168 * time.Hour)

	assert.Equal(t, 168*time.Hour, got)
}

// --- callbackRateLimitPerMinute ---

func TestRuntimeSettings_CallbackRateLimitPerMinute_NilResolver(t *testing.T) {
	t.Parallel()

	var resolver *runtimeSettingsResolver

	got := resolver.callbackRateLimitPerMinute(60)

	assert.Equal(t, 60, got)
}

func TestRuntimeSettings_CallbackRateLimitPerMinute_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newResolverWithNilClient()

	got := resolver.callbackRateLimitPerMinute(60)

	assert.Equal(t, 60, got)
}

func TestRuntimeSettings_CallbackRateLimitPerMinute_UnregisteredKey(t *testing.T) {
	resolver := newResolverWithUnregisteredKeys(t)

	got := resolver.callbackRateLimitPerMinute(60)

	assert.Equal(t, 60, got)
}

// --- webhookTimeout ---

func TestRuntimeSettings_WebhookTimeout_NilResolver(t *testing.T) {
	t.Parallel()

	var resolver *runtimeSettingsResolver

	got := resolver.webhookTimeout(30 * time.Second)

	assert.Equal(t, 30*time.Second, got)
}

func TestRuntimeSettings_WebhookTimeout_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newResolverWithNilClient()

	got := resolver.webhookTimeout(30 * time.Second)

	assert.Equal(t, 30*time.Second, got)
}

func TestRuntimeSettings_WebhookTimeout_UnregisteredKey(t *testing.T) {
	resolver := newResolverWithUnregisteredKeys(t)

	got := resolver.webhookTimeout(30 * time.Second)

	assert.Equal(t, 30*time.Second, got)
}

// --- dedupeTTL ---

func TestRuntimeSettings_DedupeTTL_NilResolver(t *testing.T) {
	t.Parallel()

	var resolver *runtimeSettingsResolver

	got := resolver.dedupeTTL(10 * time.Minute)

	assert.Equal(t, 10*time.Minute, got)
}

func TestRuntimeSettings_DedupeTTL_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newResolverWithNilClient()

	got := resolver.dedupeTTL(10 * time.Minute)

	assert.Equal(t, 10*time.Minute, got)
}

func TestRuntimeSettings_DedupeTTL_UnregisteredKey(t *testing.T) {
	resolver := newResolverWithUnregisteredKeys(t)

	got := resolver.dedupeTTL(10 * time.Minute)

	assert.Equal(t, 10*time.Minute, got)
}

// --- exportPresignExpiry ---

func TestRuntimeSettings_ExportPresignExpiry_NilResolver(t *testing.T) {
	t.Parallel()

	var resolver *runtimeSettingsResolver

	got := resolver.exportPresignExpiry(1 * time.Hour)

	assert.Equal(t, 1*time.Hour, got)
}

func TestRuntimeSettings_ExportPresignExpiry_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newResolverWithNilClient()

	got := resolver.exportPresignExpiry(1 * time.Hour)

	assert.Equal(t, 1*time.Hour, got)
}

func TestRuntimeSettings_ExportPresignExpiry_UnregisteredKey(t *testing.T) {
	resolver := newResolverWithUnregisteredKeys(t)

	got := resolver.exportPresignExpiry(1 * time.Hour)

	assert.Equal(t, 1*time.Hour, got)
}

// --- archivalPresignExpiry ---

func TestRuntimeSettings_ArchivalPresignExpiry_NilResolver(t *testing.T) {
	t.Parallel()

	var resolver *runtimeSettingsResolver

	got := resolver.archivalPresignExpiry(2 * time.Hour)

	assert.Equal(t, 2*time.Hour, got)
}

func TestRuntimeSettings_ArchivalPresignExpiry_NilClient(t *testing.T) {
	t.Parallel()

	resolver := newResolverWithNilClient()

	got := resolver.archivalPresignExpiry(2 * time.Hour)

	assert.Equal(t, 2*time.Hour, got)
}

func TestRuntimeSettings_ArchivalPresignExpiry_UnregisteredKey(t *testing.T) {
	resolver := newResolverWithUnregisteredKeys(t)

	got := resolver.archivalPresignExpiry(2 * time.Hour)

	assert.Equal(t, 2*time.Hour, got)
}
