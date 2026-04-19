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

// --- Clamp + happy-path + <=0 fallback branches ---
//
// The tests above exercise only the nil/unregistered-key paths. The blocks
// that follow register real systemplane keys, set values, and verify:
//   - happy path: Set(value) → resolver returns value (not fallback)
//   - clamp: Set(exceeds-max) → resolver returns the clamp constant
//   - <=0: Set(0 or -1) → resolver returns the fallback
//
// Keys are registered via RegisterMatcherKeys which seeds the same defaults
// the production resolver sees at startup; Set exercises the runtime mutation
// path that the resolver is designed to surface.

// newResolverWithMatcherKeys builds a resolver backed by a real
// systemplane.Client with the complete matcher key set registered against
// cfg. The client is started so Set is callable — tests that want to set
// values via the admin path go through this helper.
func newResolverWithMatcherKeys(t *testing.T, cfg *Config) (*runtimeSettingsResolver, *systemplane.Client) {
	t.Helper()

	// Reuse the shared helper from systemplane_overrides_test.go which
	// handles Register + Start + Close.
	client := newStartedTestClient(t, cfg)

	return newRuntimeSettingsResolver(client), client
}

// TestRuntimeSettings_WebhookTimeout_HappyPath asserts a set value below the
// clamp is returned as-is.
func TestRuntimeSettings_WebhookTimeout_HappyPath(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "webhook.timeout_sec", 45)

	got := resolver.webhookTimeout(30 * time.Second)

	assert.Equal(t, 45*time.Second, got, "admin-set webhook timeout must reach the caller")
}

// TestRuntimeSettings_WebhookTimeout_ClampedAboveMax asserts a value above
// maxWebhookTimeoutSec is trimmed to the clamp constant, not returned raw.
// This protects downstream HTTP clients from runaway admin writes.
func TestRuntimeSettings_WebhookTimeout_ClampedAboveMax(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "webhook.timeout_sec", 9999)

	got := resolver.webhookTimeout(30 * time.Second)

	assert.Equal(t, time.Duration(maxWebhookTimeoutSec)*time.Second, got,
		"value exceeding max must clamp to maxWebhookTimeoutSec, not pass through")
}

// TestRuntimeSettings_WebhookTimeout_FallbackOnZero asserts an explicit
// zero-valued admin write routes through the seconds<=0 branch and returns
// the fallback — preventing an accidental "disable timeout" admin write from
// crashing the webhook caller with a 0-duration deadline.
func TestRuntimeSettings_WebhookTimeout_FallbackOnZero(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "webhook.timeout_sec", 0)

	got := resolver.webhookTimeout(30 * time.Second)

	assert.Equal(t, 30*time.Second, got)
}

// TestRuntimeSettings_WebhookTimeout_FallbackOnNegative asserts a negative
// admin write also routes through the <=0 branch.
func TestRuntimeSettings_WebhookTimeout_FallbackOnNegative(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "webhook.timeout_sec", -1)

	got := resolver.webhookTimeout(30 * time.Second)

	assert.Equal(t, 30*time.Second, got)
}

// TestRuntimeSettings_ExportPresignExpiry_HappyPath asserts a set value
// below the clamp is returned as-is.
func TestRuntimeSettings_ExportPresignExpiry_HappyPath(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "export_worker.presign_expiry_sec", 7200)

	got := resolver.exportPresignExpiry(1 * time.Hour)

	assert.Equal(t, 2*time.Hour, got)
}

// TestRuntimeSettings_ExportPresignExpiry_ClampedAboveMax asserts an
// extremely-large admin write (e.g., mistake on the unit) is trimmed to the
// S3 7-day ceiling instead of being sent to the signer with invalid bounds.
func TestRuntimeSettings_ExportPresignExpiry_ClampedAboveMax(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "export_worker.presign_expiry_sec", 9_999_999)

	got := resolver.exportPresignExpiry(1 * time.Hour)

	assert.Equal(t, time.Duration(maxPresignExpirySec)*time.Second, got,
		"value exceeding 7-day S3 cap must clamp to maxPresignExpirySec")
}

// TestRuntimeSettings_ExportPresignExpiry_FallbackOnZero covers the
// seconds<=0 branch.
func TestRuntimeSettings_ExportPresignExpiry_FallbackOnZero(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "export_worker.presign_expiry_sec", 0)

	got := resolver.exportPresignExpiry(1 * time.Hour)

	assert.Equal(t, 1*time.Hour, got)
}

// TestRuntimeSettings_ExportPresignExpiry_FallbackOnNegative covers the
// seconds<=0 branch for negative values.
func TestRuntimeSettings_ExportPresignExpiry_FallbackOnNegative(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "export_worker.presign_expiry_sec", -10)

	got := resolver.exportPresignExpiry(1 * time.Hour)

	assert.Equal(t, 1*time.Hour, got)
}

// TestRuntimeSettings_ArchivalPresignExpiry_HappyPath asserts a set value
// below the clamp is returned as-is.
func TestRuntimeSettings_ArchivalPresignExpiry_HappyPath(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "archival.presign_expiry_sec", 10800)

	got := resolver.archivalPresignExpiry(2 * time.Hour)

	assert.Equal(t, 3*time.Hour, got)
}

// TestRuntimeSettings_ArchivalPresignExpiry_ClampedAboveMax covers the
// clamp branch.
func TestRuntimeSettings_ArchivalPresignExpiry_ClampedAboveMax(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "archival.presign_expiry_sec", 9_999_999)

	got := resolver.archivalPresignExpiry(2 * time.Hour)

	assert.Equal(t, time.Duration(maxPresignExpirySec)*time.Second, got)
}

// TestRuntimeSettings_ArchivalPresignExpiry_FallbackOnZero covers the
// seconds<=0 branch.
func TestRuntimeSettings_ArchivalPresignExpiry_FallbackOnZero(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "archival.presign_expiry_sec", 0)

	got := resolver.archivalPresignExpiry(2 * time.Hour)

	assert.Equal(t, 2*time.Hour, got)
}

// TestRuntimeSettings_IdempotencyRetryWindow_HappyPath asserts a set value
// propagates as seconds.
func TestRuntimeSettings_IdempotencyRetryWindow_HappyPath(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "idempotency.retry_window_sec", 120)

	got := resolver.idempotencyRetryWindow(5 * time.Minute)

	assert.Equal(t, 2*time.Minute, got)
}

// TestRuntimeSettings_IdempotencyRetryWindow_FallbackOnZero covers the
// seconds<=0 branch.
func TestRuntimeSettings_IdempotencyRetryWindow_FallbackOnZero(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "idempotency.retry_window_sec", 0)

	got := resolver.idempotencyRetryWindow(5 * time.Minute)

	assert.Equal(t, 5*time.Minute, got)
}

// TestRuntimeSettings_IdempotencyRetryWindow_FallbackOnNegative covers the
// seconds<=0 branch for negative values.
func TestRuntimeSettings_IdempotencyRetryWindow_FallbackOnNegative(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "idempotency.retry_window_sec", -1)

	got := resolver.idempotencyRetryWindow(5 * time.Minute)

	assert.Equal(t, 5*time.Minute, got)
}

// TestRuntimeSettings_IdempotencySuccessTTL_HappyPath asserts a set value
// propagates as hours (the units-aware branch — distinct from seconds).
func TestRuntimeSettings_IdempotencySuccessTTL_HappyPath(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "idempotency.success_ttl_hours", 72)

	got := resolver.idempotencySuccessTTL(168 * time.Hour)

	assert.Equal(t, 72*time.Hour, got)
}

// TestRuntimeSettings_IdempotencySuccessTTL_FallbackOnZero covers the
// hours<=0 branch.
func TestRuntimeSettings_IdempotencySuccessTTL_FallbackOnZero(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "idempotency.success_ttl_hours", 0)

	got := resolver.idempotencySuccessTTL(168 * time.Hour)

	assert.Equal(t, 168*time.Hour, got)
}

// TestRuntimeSettings_IdempotencySuccessTTL_FallbackOnNegative covers the
// hours<=0 branch for negative values.
func TestRuntimeSettings_IdempotencySuccessTTL_FallbackOnNegative(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "idempotency.success_ttl_hours", -24)

	got := resolver.idempotencySuccessTTL(168 * time.Hour)

	assert.Equal(t, 168*time.Hour, got)
}

// TestRuntimeSettings_CallbackRateLimit_HappyPath asserts a set per-minute
// cap propagates. This getter has no clamp/<=0 branch — a single happy-path
// test exhausts the remaining uncovered assignment in the method.
func TestRuntimeSettings_CallbackRateLimit_HappyPath(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "callback_rate_limit.per_minute", 240)

	got := resolver.callbackRateLimitPerMinute(60)

	assert.Equal(t, 240, got)
}

// TestRuntimeSettings_DedupeTTL_HappyPath asserts a set dedupe TTL
// propagates as seconds.
func TestRuntimeSettings_DedupeTTL_HappyPath(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "deduplication.ttl_sec", 1800)

	got := resolver.dedupeTTL(10 * time.Minute)

	assert.Equal(t, 30*time.Minute, got)
}

// TestRuntimeSettings_DedupeTTL_FallbackOnZero covers the seconds<=0 branch.
func TestRuntimeSettings_DedupeTTL_FallbackOnZero(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "deduplication.ttl_sec", 0)

	got := resolver.dedupeTTL(10 * time.Minute)

	assert.Equal(t, 10*time.Minute, got)
}

// TestRuntimeSettings_DedupeTTL_FallbackOnNegative covers the seconds<=0
// branch for negative values.
func TestRuntimeSettings_DedupeTTL_FallbackOnNegative(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "deduplication.ttl_sec", -5)

	got := resolver.dedupeTTL(10 * time.Minute)

	assert.Equal(t, 10*time.Minute, got)
}

// TestRuntimeSettings_RateLimit_HappyPath asserts every rate_limit.* field
// propagates through the single resolver.rateLimit() call. RateLimit is a
// composite struct so we verify the full mutation surface in one test.
func TestRuntimeSettings_RateLimit_HappyPath(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	resolver, client := newResolverWithMatcherKeys(t, base)

	setMatcherKey(t, client, "rate_limit.enabled", false)
	setMatcherKey(t, client, "rate_limit.max", 333)
	setMatcherKey(t, client, "rate_limit.expiry_sec", 45)
	setMatcherKey(t, client, "rate_limit.export_max", 20)
	setMatcherKey(t, client, "rate_limit.export_expiry_sec", 90)
	setMatcherKey(t, client, "rate_limit.dispatch_max", 444)
	setMatcherKey(t, client, "rate_limit.dispatch_expiry_sec", 120)
	setMatcherKey(t, client, "rate_limit.admin_max", 8)
	setMatcherKey(t, client, "rate_limit.admin_expiry_sec", 180)

	fallback := RateLimitConfig{
		Enabled:           true,
		Max:               100,
		ExpirySec:         60,
		ExportMax:         10,
		ExportExpirySec:   60,
		DispatchMax:       50,
		DispatchExpirySec: 60,
		AdminMax:          5,
		AdminExpirySec:    60,
	}

	got := resolver.rateLimit(fallback)

	assert.Equal(t, RateLimitConfig{
		Enabled:           false,
		Max:               333,
		ExpirySec:         45,
		ExportMax:         20,
		ExportExpirySec:   90,
		DispatchMax:       444,
		DispatchExpirySec: 120,
		AdminMax:          8,
		AdminExpirySec:    180,
	}, got, "every admin-set rate_limit field must reach the resolver caller")
}
