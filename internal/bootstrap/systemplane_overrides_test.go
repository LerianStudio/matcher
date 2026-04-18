// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
)

// newStartedTestClient builds a systemplane.Client backed by a noop store,
// registers every matcher key against `cfg`, and starts the client so
// Set/Get/OnChange are usable. Returned client is closed in t.Cleanup.
func newStartedTestClient(t *testing.T, cfg *Config) *systemplane.Client {
	t.Helper()

	client, err := systemplane.NewForTesting(&noopSystemplaneStore{})
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Close() })

	require.NoError(t, RegisterMatcherKeys(client, cfg))
	require.NoError(t, client.Start(context.Background()))

	return client
}

// setMatcherKey is a convenience wrapper around client.Set with the matcher
// namespace and a test actor.
func setMatcherKey(t *testing.T, client *systemplane.Client, key string, value any) {
	t.Helper()

	require.NoErrorf(t,
		client.Set(context.Background(), systemplaneNamespace, key, value, "unit-test"),
		"systemplane.Set %q=%v failed", key, value,
	)
}

// TestApplySystemplaneOverrides_NilClientPassesThrough asserts that a nil
// client returns base unchanged (no override source).
func TestApplySystemplaneOverrides_NilClientPassesThrough(t *testing.T) {
	t.Parallel()

	base := *defaultConfig()
	base.RateLimit.Max = 4242

	got := applySystemplaneOverrides(base, nil)

	assert.Equal(t, base, got, "nil client must not mutate base")
}

// TestApplySystemplaneOverrides_RegisteredDefaultsMatchBase verifies the
// baseline: when systemplane has only the env-seeded defaults, applying
// overrides returns a Config identical to base (modulo in-place copy). This
// is the success path for the headline bug — env-seeded defaults beat the
// old compile-time 100.
func TestApplySystemplaneOverrides_RegisteredDefaultsMatchBase(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	base.RateLimit.Max = 7777 // simulates MATCHER_RATE_LIMIT_MAX=7777
	base.Webhook.TimeoutSec = 99
	base.Idempotency.RetryWindowSec = 321

	client := newStartedTestClient(t, base)

	got := applySystemplaneOverrides(*base, client)

	assert.Equal(t, 7777, got.RateLimit.Max, "env-seeded rate_limit.max must survive round-trip")
	assert.Equal(t, 99, got.Webhook.TimeoutSec, "env-seeded webhook.timeout_sec must survive round-trip")
	assert.Equal(t, 321, got.Idempotency.RetryWindowSec, "env-seeded idempotency.retry_window_sec must survive round-trip")
}

// TestApplySystemplaneOverrides_RateLimit confirms admin-written rate_limit
// values propagate through applySystemplaneOverrides into Config.
func TestApplySystemplaneOverrides_RateLimit(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	setMatcherKey(t, client, "rate_limit.enabled", false)
	setMatcherKey(t, client, "rate_limit.max", 500)
	setMatcherKey(t, client, "rate_limit.expiry_sec", 45)
	setMatcherKey(t, client, "rate_limit.export_max", 25)
	setMatcherKey(t, client, "rate_limit.export_expiry_sec", 90)
	setMatcherKey(t, client, "rate_limit.dispatch_max", 750)
	setMatcherKey(t, client, "rate_limit.dispatch_expiry_sec", 120)

	got := applySystemplaneOverrides(*base, client)

	assert.False(t, got.RateLimit.Enabled)
	assert.Equal(t, 500, got.RateLimit.Max)
	assert.Equal(t, 45, got.RateLimit.ExpirySec)
	assert.Equal(t, 25, got.RateLimit.ExportMax)
	assert.Equal(t, 90, got.RateLimit.ExportExpirySec)
	assert.Equal(t, 750, got.RateLimit.DispatchMax)
	assert.Equal(t, 120, got.RateLimit.DispatchExpirySec)
}

// TestApplySystemplaneOverrides_Idempotency confirms idempotency overrides
// (including the HMAC secret) propagate.
func TestApplySystemplaneOverrides_Idempotency(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	setMatcherKey(t, client, "idempotency.retry_window_sec", 900)
	setMatcherKey(t, client, "idempotency.success_ttl_hours", 72)
	setMatcherKey(t, client, "idempotency.hmac_secret", "rotated-secret-material")

	got := applySystemplaneOverrides(*base, client)

	assert.Equal(t, 900, got.Idempotency.RetryWindowSec)
	assert.Equal(t, 72, got.Idempotency.SuccessTTLHours)
	assert.Equal(t, "rotated-secret-material", got.Idempotency.HMACSecret)
}

// TestApplySystemplaneOverrides_Fetcher covers the fetcher integration toggle
// + URL + timeouts as an end-to-end example of the new wiring.
func TestApplySystemplaneOverrides_Fetcher(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	setMatcherKey(t, client, "fetcher.enabled", true)
	setMatcherKey(t, client, "fetcher.url", "https://fetcher.prod:4006")
	setMatcherKey(t, client, "fetcher.allow_private_ips", true)
	setMatcherKey(t, client, "fetcher.health_timeout_sec", 9)
	setMatcherKey(t, client, "fetcher.request_timeout_sec", 60)
	setMatcherKey(t, client, "fetcher.discovery_interval_sec", 180)
	setMatcherKey(t, client, "fetcher.schema_cache_ttl_sec", 900)
	setMatcherKey(t, client, "fetcher.extraction_poll_sec", 15)
	setMatcherKey(t, client, "fetcher.extraction_timeout_sec", 1800)

	got := applySystemplaneOverrides(*base, client)

	assert.True(t, got.Fetcher.Enabled)
	assert.Equal(t, "https://fetcher.prod:4006", got.Fetcher.URL)
	assert.True(t, got.Fetcher.AllowPrivateIPs)
	assert.Equal(t, 9, got.Fetcher.HealthTimeoutSec)
	assert.Equal(t, 60, got.Fetcher.RequestTimeoutSec)
	assert.Equal(t, 180, got.Fetcher.DiscoveryIntervalSec)
	assert.Equal(t, 900, got.Fetcher.SchemaCacheTTLSec)
	assert.Equal(t, 15, got.Fetcher.ExtractionPollSec)
	assert.Equal(t, 1800, got.Fetcher.ExtractionTimeoutSec)
}

// TestApplySystemplaneOverrides_Workers spans export_worker, cleanup_worker,
// archival, and scheduler.
func TestApplySystemplaneOverrides_Workers(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	setMatcherKey(t, client, "export_worker.enabled", false)
	setMatcherKey(t, client, "export_worker.poll_interval_sec", 15)
	setMatcherKey(t, client, "export_worker.page_size", 500)
	setMatcherKey(t, client, "export_worker.presign_expiry_sec", 7200)

	setMatcherKey(t, client, "cleanup_worker.enabled", false)
	setMatcherKey(t, client, "cleanup_worker.interval_sec", 1800)
	setMatcherKey(t, client, "cleanup_worker.batch_size", 250)
	setMatcherKey(t, client, "cleanup_worker.grace_period_sec", 7200)

	setMatcherKey(t, client, "scheduler.interval_sec", 30)

	setMatcherKey(t, client, "archival.enabled", true)
	setMatcherKey(t, client, "archival.interval_hours", 6)
	setMatcherKey(t, client, "archival.hot_retention_days", 14)
	setMatcherKey(t, client, "archival.warm_retention_months", 6)
	setMatcherKey(t, client, "archival.cold_retention_months", 36)
	setMatcherKey(t, client, "archival.batch_size", 1000)
	setMatcherKey(t, client, "archival.partition_lookahead", 5)
	setMatcherKey(t, client, "archival.storage_bucket", "override-archives")
	setMatcherKey(t, client, "archival.storage_prefix", "override/prefix")
	setMatcherKey(t, client, "archival.storage_class", "DEEP_ARCHIVE")
	setMatcherKey(t, client, "archival.presign_expiry_sec", 14400)

	got := applySystemplaneOverrides(*base, client)

	// Export worker.
	assert.False(t, got.ExportWorker.Enabled)
	assert.Equal(t, 15, got.ExportWorker.PollIntervalSec)
	assert.Equal(t, 500, got.ExportWorker.PageSize)
	assert.Equal(t, 7200, got.ExportWorker.PresignExpirySec)

	// Cleanup worker.
	assert.False(t, got.CleanupWorker.Enabled)
	assert.Equal(t, 1800, got.CleanupWorker.IntervalSec)
	assert.Equal(t, 250, got.CleanupWorker.BatchSize)
	assert.Equal(t, 7200, got.CleanupWorker.GracePeriodSec)

	// Scheduler.
	assert.Equal(t, 30, got.Scheduler.IntervalSec)

	// Archival.
	assert.True(t, got.Archival.Enabled)
	assert.Equal(t, 6, got.Archival.IntervalHours)
	assert.Equal(t, 14, got.Archival.HotRetentionDays)
	assert.Equal(t, 6, got.Archival.WarmRetentionMonths)
	assert.Equal(t, 36, got.Archival.ColdRetentionMonths)
	assert.Equal(t, 1000, got.Archival.BatchSize)
	assert.Equal(t, 5, got.Archival.PartitionLookahead)
	assert.Equal(t, "override-archives", got.Archival.StorageBucket)
	assert.Equal(t, "override/prefix", got.Archival.StoragePrefix)
	assert.Equal(t, "DEEP_ARCHIVE", got.Archival.StorageClass)
	assert.Equal(t, 14400, got.Archival.PresignExpirySec)
}

// TestApplySystemplaneOverrides_ObjectStorage covers the object storage knobs
// that feed worker adapters.
func TestApplySystemplaneOverrides_ObjectStorage(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	setMatcherKey(t, client, "object_storage.endpoint", "https://s3.custom.internal")
	setMatcherKey(t, client, "object_storage.region", "eu-west-1")
	setMatcherKey(t, client, "object_storage.bucket", "override-bucket")
	setMatcherKey(t, client, "object_storage.access_key_id", "rotated-AK")
	setMatcherKey(t, client, "object_storage.secret_access_key", "rotated-SK")
	setMatcherKey(t, client, "object_storage.use_path_style", false)
	setMatcherKey(t, client, "object_storage.allow_insecure_endpoint", true)

	got := applySystemplaneOverrides(*base, client)

	assert.Equal(t, "https://s3.custom.internal", got.ObjectStorage.Endpoint)
	assert.Equal(t, "eu-west-1", got.ObjectStorage.Region)
	assert.Equal(t, "override-bucket", got.ObjectStorage.Bucket)
	assert.Equal(t, "rotated-AK", got.ObjectStorage.AccessKeyID)
	assert.Equal(t, "rotated-SK", got.ObjectStorage.SecretAccessKey)
	assert.False(t, got.ObjectStorage.UsePathStyle)
	assert.True(t, got.ObjectStorage.AllowInsecure)
}

// TestApplySystemplaneOverrides_Tenancy covers a large block of mixed
// string/int/bool keys.
func TestApplySystemplaneOverrides_Tenancy(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	setMatcherKey(t, client, "tenancy.default_tenant_id", "99999999-9999-9999-9999-999999999999")
	setMatcherKey(t, client, "tenancy.default_tenant_slug", "override")
	setMatcherKey(t, client, "tenancy.multi_tenant_enabled", true)
	setMatcherKey(t, client, "tenancy.multi_tenant_url", "https://tm.internal")
	setMatcherKey(t, client, "tenancy.multi_tenant_environment", "staging")
	setMatcherKey(t, client, "tenancy.multi_tenant_redis_host", "redis-tm")
	setMatcherKey(t, client, "tenancy.multi_tenant_redis_port", "7000")
	setMatcherKey(t, client, "tenancy.multi_tenant_redis_password", "override-pw")
	setMatcherKey(t, client, "tenancy.multi_tenant_redis_tls", true)
	setMatcherKey(t, client, "tenancy.multi_tenant_max_tenant_pools", 200)
	setMatcherKey(t, client, "tenancy.multi_tenant_idle_timeout_sec", 600)
	setMatcherKey(t, client, "tenancy.multi_tenant_timeout", 45)
	setMatcherKey(t, client, "tenancy.multi_tenant_circuit_breaker_threshold", 7)
	setMatcherKey(t, client, "tenancy.multi_tenant_circuit_breaker_timeout_sec", 60)
	setMatcherKey(t, client, "tenancy.multi_tenant_service_api_key", "tm-api-override")
	setMatcherKey(t, client, "tenancy.multi_tenant_cache_ttl_sec", 240)
	setMatcherKey(t, client, "tenancy.multi_tenant_connections_check_interval_sec", 45)

	got := applySystemplaneOverrides(*base, client)

	assert.Equal(t, "99999999-9999-9999-9999-999999999999", got.Tenancy.DefaultTenantID)
	assert.Equal(t, "override", got.Tenancy.DefaultTenantSlug)
	assert.True(t, got.Tenancy.MultiTenantEnabled)
	assert.Equal(t, "https://tm.internal", got.Tenancy.MultiTenantURL)
	assert.Equal(t, "staging", got.Tenancy.MultiTenantEnvironment)
	assert.Equal(t, "redis-tm", got.Tenancy.MultiTenantRedisHost)
	assert.Equal(t, "7000", got.Tenancy.MultiTenantRedisPort)
	assert.Equal(t, "override-pw", got.Tenancy.MultiTenantRedisPassword)
	assert.True(t, got.Tenancy.MultiTenantRedisTLS)
	assert.Equal(t, 200, got.Tenancy.MultiTenantMaxTenantPools)
	assert.Equal(t, 600, got.Tenancy.MultiTenantIdleTimeoutSec)
	assert.Equal(t, 45, got.Tenancy.MultiTenantTimeout)
	assert.Equal(t, 7, got.Tenancy.MultiTenantCircuitBreakerThreshold)
	assert.Equal(t, 60, got.Tenancy.MultiTenantCircuitBreakerTimeoutSec)
	assert.Equal(t, "tm-api-override", got.Tenancy.MultiTenantServiceAPIKey)
	assert.Equal(t, 240, got.Tenancy.MultiTenantCacheTTLSec)
	assert.Equal(t, 45, got.Tenancy.MultiTenantConnectionsCheckIntervalSec)
}

// TestApplySystemplaneOverrides_ServerAndCORS exercises server/cors/tls keys.
func TestApplySystemplaneOverrides_ServerAndCORS(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	setMatcherKey(t, client, "app.env_name", "override-env")
	setMatcherKey(t, client, "server.address", ":8080")
	setMatcherKey(t, client, "server.body_limit_bytes", 16*1024*1024)
	setMatcherKey(t, client, "cors.allowed_origins", "https://app.example.com")
	setMatcherKey(t, client, "cors.allowed_methods", "GET,POST,DELETE")
	setMatcherKey(t, client, "cors.allowed_headers", "X-Custom,Content-Type")
	setMatcherKey(t, client, "server.tls_cert_file", "/override/cert.pem")
	setMatcherKey(t, client, "server.tls_key_file", "/override/key.pem")
	setMatcherKey(t, client, "server.tls_terminated_upstream", true)
	setMatcherKey(t, client, "server.trusted_proxies", "10.0.0.0/8")

	got := applySystemplaneOverrides(*base, client)

	assert.Equal(t, "override-env", got.App.EnvName)
	assert.Equal(t, ":8080", got.Server.Address)
	assert.Equal(t, 16*1024*1024, got.Server.BodyLimitBytes)
	assert.Equal(t, "https://app.example.com", got.Server.CORSAllowedOrigins)
	assert.Equal(t, "GET,POST,DELETE", got.Server.CORSAllowedMethods)
	assert.Equal(t, "X-Custom,Content-Type", got.Server.CORSAllowedHeaders)
	assert.Equal(t, "/override/cert.pem", got.Server.TLSCertFile)
	assert.Equal(t, "/override/key.pem", got.Server.TLSKeyFile)
	assert.True(t, got.Server.TLSTerminatedUpstream)
	assert.Equal(t, "10.0.0.0/8", got.Server.TrustedProxies)
}

// TestApplySystemplaneOverrides_PoolKnobs covers the tunable postgres/redis
// pool sizes.
func TestApplySystemplaneOverrides_PoolKnobs(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	setMatcherKey(t, client, "postgres.max_open_conns", 75)
	setMatcherKey(t, client, "postgres.max_idle_conns", 15)
	setMatcherKey(t, client, "postgres.conn_max_lifetime_mins", 60)
	setMatcherKey(t, client, "postgres.conn_max_idle_time_mins", 10)
	setMatcherKey(t, client, "postgres.query_timeout_sec", 45)

	setMatcherKey(t, client, "redis.pool_size", 50)
	setMatcherKey(t, client, "redis.min_idle_conns", 5)
	setMatcherKey(t, client, "redis.read_timeout_ms", 5000)
	setMatcherKey(t, client, "redis.write_timeout_ms", 4000)

	got := applySystemplaneOverrides(*base, client)

	assert.Equal(t, 75, got.Postgres.MaxOpenConnections)
	assert.Equal(t, 15, got.Postgres.MaxIdleConnections)
	assert.Equal(t, 60, got.Postgres.ConnMaxLifetimeMins)
	assert.Equal(t, 10, got.Postgres.ConnMaxIdleTimeMins)
	assert.Equal(t, 45, got.Postgres.QueryTimeoutSec)
	assert.Equal(t, 50, got.Redis.PoolSize)
	assert.Equal(t, 5, got.Redis.MinIdleConn)
	assert.Equal(t, 5000, got.Redis.ReadTimeoutMs)
	assert.Equal(t, 4000, got.Redis.WriteTimeoutMs)
}

// TestApplySystemplaneOverrides_MiscAreas covers auth, telemetry, swagger,
// deduplication, callback_rate_limit, webhook, m2m, infrastructure, outbox.
func TestApplySystemplaneOverrides_MiscAreas(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	// Auth.
	setMatcherKey(t, client, "auth.enabled", true)
	setMatcherKey(t, client, "auth.host", "https://auth.override.example")
	setMatcherKey(t, client, "auth.token_secret", "rotated-jwt-secret")

	// Telemetry.
	setMatcherKey(t, client, "telemetry.enabled", true)
	setMatcherKey(t, client, "telemetry.service_name", "matcher-override")
	setMatcherKey(t, client, "telemetry.library_name", "github.com/example/override")
	setMatcherKey(t, client, "telemetry.service_version", "9.9.9")
	setMatcherKey(t, client, "telemetry.deployment_env", "qa")
	setMatcherKey(t, client, "telemetry.collector_endpoint", "otel-collector:4317")
	setMatcherKey(t, client, "telemetry.db_metrics_interval_sec", 30)

	// Swagger.
	setMatcherKey(t, client, "swagger.enabled", true)
	setMatcherKey(t, client, "swagger.host", "api.override.example")
	setMatcherKey(t, client, "swagger.schemes", "http,https")

	// Infrastructure.
	setMatcherKey(t, client, "infrastructure.connect_timeout_sec", 45)
	setMatcherKey(t, client, "infrastructure.health_check_timeout_sec", 10)

	// Outbox.
	setMatcherKey(t, client, "outbox.retry_window_sec", 600)
	setMatcherKey(t, client, "outbox.dispatch_interval_sec", 5)

	// Deduplication.
	setMatcherKey(t, client, "deduplication.ttl_sec", 7200)

	// Callback rate limit.
	setMatcherKey(t, client, "callback_rate_limit.per_minute", 120)

	// Webhook.
	setMatcherKey(t, client, "webhook.timeout_sec", 45)

	// M2M.
	setMatcherKey(t, client, "m2m.m2m_target_service", "override-service")
	setMatcherKey(t, client, "m2m.m2m_credential_cache_ttl_sec", 900)
	setMatcherKey(t, client, "m2m.aws_region", "ap-southeast-2")

	got := applySystemplaneOverrides(*base, client)

	assert.True(t, got.Auth.Enabled)
	assert.Equal(t, "https://auth.override.example", got.Auth.Host)
	assert.Equal(t, "rotated-jwt-secret", got.Auth.TokenSecret)

	assert.True(t, got.Telemetry.Enabled)
	assert.Equal(t, "matcher-override", got.Telemetry.ServiceName)
	assert.Equal(t, "github.com/example/override", got.Telemetry.LibraryName)
	assert.Equal(t, "9.9.9", got.Telemetry.ServiceVersion)
	assert.Equal(t, "qa", got.Telemetry.DeploymentEnv)
	assert.Equal(t, "otel-collector:4317", got.Telemetry.CollectorEndpoint)
	assert.Equal(t, 30, got.Telemetry.DBMetricsIntervalSec)

	assert.True(t, got.Swagger.Enabled)
	assert.Equal(t, "api.override.example", got.Swagger.Host)
	assert.Equal(t, "http,https", got.Swagger.Schemes)

	assert.Equal(t, 45, got.Infrastructure.ConnectTimeoutSec)
	assert.Equal(t, 10, got.Infrastructure.HealthCheckTimeoutSec)

	assert.Equal(t, 600, got.Outbox.RetryWindowSec)
	assert.Equal(t, 5, got.Outbox.DispatchIntervalSec)

	assert.Equal(t, 7200, got.Dedupe.TTLSec)
	assert.Equal(t, 120, got.CallbackRateLimit.PerMinute)
	assert.Equal(t, 45, got.Webhook.TimeoutSec)

	assert.Equal(t, "override-service", got.M2M.M2MTargetService)
	assert.Equal(t, 900, got.M2M.M2MCredentialCacheTTLSec)
	assert.Equal(t, "ap-southeast-2", got.M2M.AWSRegion)
}

// TestWatchedSystemplaneKeys_CoversMatcherDefs ensures every key in
// matcherKeyDefs has a corresponding watch entry. Drift here means admin PUTs
// for newly-added keys would not reload the Config snapshot, leaving stale
// values in ConfigManager.Get.
func TestWatchedSystemplaneKeys_CoversMatcherDefs(t *testing.T) {
	t.Parallel()

	watched := make(map[string]struct{}, len(watchedSystemplaneKeys()))
	for _, k := range watchedSystemplaneKeys() {
		watched[k] = struct{}{}
	}

	var missing []string

	for _, d := range matcherKeyDefs(defaultConfig()) {
		if _, ok := watched[d.key]; !ok {
			missing = append(missing, d.key)
		}
	}

	sort.Strings(missing)

	assert.Emptyf(t, missing,
		"watchedSystemplaneKeys missing %d registered keys; admin PUTs for these will not trigger Config reload: %v",
		len(missing), missing,
	)
}

// TestWatchedSystemplaneKeys_NoStaleEntries ensures watchedSystemplaneKeys
// doesn't reference keys that are no longer registered.
func TestWatchedSystemplaneKeys_NoStaleEntries(t *testing.T) {
	t.Parallel()

	registered := make(map[string]struct{})
	for _, d := range matcherKeyDefs(defaultConfig()) {
		registered[d.key] = struct{}{}
	}

	var stale []string

	for _, k := range watchedSystemplaneKeys() {
		if _, ok := registered[k]; !ok {
			stale = append(stale, k)
		}
	}

	sort.Strings(stale)

	assert.Emptyf(t, stale,
		"watchedSystemplaneKeys contains %d unregistered keys; OnChange subscriptions will fail silently: %v",
		len(stale), stale,
	)
}

// TestWatchedSystemplaneKeys_NoDuplicates catches a common editing mistake
// that would register redundant OnChange subscriptions.
func TestWatchedSystemplaneKeys_NoDuplicates(t *testing.T) {
	t.Parallel()

	seen := make(map[string]int)
	for _, k := range watchedSystemplaneKeys() {
		seen[k]++
	}

	for k, count := range seen {
		assert.Equalf(t, 1, count, "watchedSystemplaneKeys has duplicate entry %q (count=%d)", k, count)
	}
}
