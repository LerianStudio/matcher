// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMatcherKeyDefs_AllKeysUnique ensures no duplicate key registration would occur.
func TestMatcherKeyDefs_AllKeysUnique(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefs(defaultConfig())
	seen := make(map[string]int)

	for _, d := range defs {
		seen[d.key]++
	}

	for key, count := range seen {
		assert.Equalf(t, 1, count, "key %q registered %d times", key, count)
	}
}

// TestMatcherKeyDefs_AllKeysHaveDescriptions ensures every key has a non-empty description.
func TestMatcherKeyDefs_AllKeysHaveDescriptions(t *testing.T) {
	t.Parallel()

	for _, d := range matcherKeyDefs(defaultConfig()) {
		assert.NotEmptyf(t, d.description, "key %q has empty description", d.key)
	}
}

// TestMatcherKeyDefs_NoBootstrapOnlyKeys confirms Wave 1 H5 + H6 key removals stuck.
// Bootstrap-only keys (credentials, connection identities, log level) must NOT be
// registered on the systemplane because they require a process restart to take effect —
// registering them would mislead operators (admin API appears to accept changes while
// live traffic continues to use the boot-time value).
func TestMatcherKeyDefs_NoBootstrapOnlyKeys(t *testing.T) {
	t.Parallel()

	forbiddenPrefixes := []string{
		"postgres.primary_",
		"postgres.replica_",
		"redis.host", "redis.password", "redis.db", "redis.tls", "redis.protocol",
		"redis.master_name", "redis.ca_cert", "redis.dial_timeout_ms",
		"rabbitmq.url", "rabbitmq.host", "rabbitmq.port", "rabbitmq.user",
		"rabbitmq.password", "rabbitmq.vhost", "rabbitmq.health_url",
		"rabbitmq.allow_insecure",
		"app.log_level",
		"postgres.connect_timeout_sec",
		"postgres.migrations_path",
	}

	for _, d := range matcherKeyDefs(defaultConfig()) {
		for _, fp := range forbiddenPrefixes {
			assert.Falsef(t,
				strings.HasPrefix(d.key, fp) || d.key == strings.TrimSuffix(fp, "_"),
				"bootstrap-only key %q must not be registered (matched forbidden %q)",
				d.key, fp,
			)
		}
	}
}

// --- validatePositiveInt ---
//
// NOTE: validatePositiveInt supports only `int` and `float64` types.
// `int64` and other numeric types fall through to the default case
// and return an error. Tests reflect that actual behaviour.

func TestValidatePositiveInt_Valid(t *testing.T) {
	t.Parallel()

	for _, v := range []any{1, 100, float64(42)} {
		require.NoError(t, validatePositiveInt(v))
	}
}

func TestValidatePositiveInt_Invalid(t *testing.T) {
	t.Parallel()

	for _, v := range []any{0, -1, "not-a-number", nil} {
		require.Error(t, validatePositiveInt(v))
	}
}

// TestValidatePositiveInt_Int64Rejected documents that int64 is not accepted.
// If this behaviour ever changes to accept int64, update the positive/invalid
// lists above accordingly.
func TestValidatePositiveInt_Int64Rejected(t *testing.T) {
	t.Parallel()

	require.Error(t, validatePositiveInt(int64(5000)))
}

// --- validateFetcherURL ---

func TestValidateFetcherURL_EmptyAllowed(t *testing.T) {
	t.Parallel()

	// Empty is permitted: Fetcher integration is separately gated by
	// `fetcher.enabled` (default false). See systemplane_keys.go comment.
	require.NoError(t, validateFetcherURL(""))
}

func TestValidateFetcherURL_HTTPAndHTTPSAccepted(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateFetcherURL("https://fetcher.example.com"))
	require.NoError(t, validateFetcherURL("http://localhost:4006"))
}

func TestValidateFetcherURL_Malformed(t *testing.T) {
	t.Parallel()

	// "not-a-url" parses but is not absolute (no scheme).
	// "ftp://example.com" is absolute but uses a non-HTTP scheme.
	// "://broken" fails url.Parse outright.
	for _, u := range []string{"not-a-url", "ftp://example.com", "://broken"} {
		require.Errorf(t, validateFetcherURL(u), "expected error for URL %q", u)
	}
}

func TestValidateFetcherURL_NonString(t *testing.T) {
	t.Parallel()

	require.Error(t, validateFetcherURL(42))
	require.Error(t, validateFetcherURL(nil))
}

// --- matcherKeyDefs(cfg) env-seeded defaults ---
//
// The signature change matcherKeyDefs() -> matcherKeyDefs(cfg) ensures env
// overrides like MATCHER_RATE_LIMIT_MAX=10000 propagate to the registered
// systemplane default. Without this seeding, SystemplaneGetInt returned the
// compile-time default with ok=true, silently dropping env overrides.
//
// These tests cover representative keys spanning int/string/bool/duration-like
// fields across several Config sub-structs.

// findKeyDef returns the matcherKeyDef with the given dotted key, or fails the
// test if no such key is registered.
func findKeyDef(t *testing.T, defs []matcherKeyDef, key string) matcherKeyDef {
	t.Helper()

	for _, d := range defs {
		if d.key == key {
			return d
		}
	}

	t.Fatalf("matcherKeyDefs missing key %q", key)

	return matcherKeyDef{}
}

// TestMatcherKeyDefs_DefaultsReflectCfgRateLimit verifies that non-default
// rate_limit values in cfg flow through to the registered key defaults. This
// is the bug the refactor fixes: previously the registered default was the
// compile-time constant 100, which silently overrode env values.
func TestMatcherKeyDefs_DefaultsReflectCfgRateLimit(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.Enabled = false
	cfg.RateLimit.Max = 12345
	cfg.RateLimit.ExpirySec = 789
	cfg.RateLimit.ExportMax = 42
	cfg.RateLimit.ExportExpirySec = 17
	cfg.RateLimit.DispatchMax = 9876
	cfg.RateLimit.DispatchExpirySec = 54

	defs := matcherKeyDefs(cfg)

	assert.Equal(t, false, findKeyDef(t, defs, "rate_limit.enabled").defaultValue)
	assert.Equal(t, 12345, findKeyDef(t, defs, "rate_limit.max").defaultValue)
	assert.Equal(t, 789, findKeyDef(t, defs, "rate_limit.expiry_sec").defaultValue)
	assert.Equal(t, 42, findKeyDef(t, defs, "rate_limit.export_max").defaultValue)
	assert.Equal(t, 17, findKeyDef(t, defs, "rate_limit.export_expiry_sec").defaultValue)
	assert.Equal(t, 9876, findKeyDef(t, defs, "rate_limit.dispatch_max").defaultValue)
	assert.Equal(t, 54, findKeyDef(t, defs, "rate_limit.dispatch_expiry_sec").defaultValue)
}

// TestMatcherKeyDefs_DefaultsReflectCfgServer exercises string and bool fields
// across the server/cors/tls categories.
func TestMatcherKeyDefs_DefaultsReflectCfgServer(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Server.Address = ":9999"
	cfg.Server.BodyLimitBytes = 1024 * 1024
	cfg.Server.CORSAllowedOrigins = "https://custom.example.com"
	cfg.Server.CORSAllowedMethods = "GET,POST"
	cfg.Server.TLSCertFile = "/etc/certs/server.crt"
	cfg.Server.TLSKeyFile = "/etc/certs/server.key"
	cfg.Server.TLSTerminatedUpstream = true
	cfg.Server.TrustedProxies = "10.0.0.0/8,172.16.0.0/12"

	defs := matcherKeyDefs(cfg)

	assert.Equal(t, ":9999", findKeyDef(t, defs, "server.address").defaultValue)
	assert.Equal(t, 1024*1024, findKeyDef(t, defs, "server.body_limit_bytes").defaultValue)
	assert.Equal(t, "https://custom.example.com", findKeyDef(t, defs, "cors.allowed_origins").defaultValue)
	assert.Equal(t, "GET,POST", findKeyDef(t, defs, "cors.allowed_methods").defaultValue)
	assert.Equal(t, "/etc/certs/server.crt", findKeyDef(t, defs, "server.tls_cert_file").defaultValue)
	assert.Equal(t, "/etc/certs/server.key", findKeyDef(t, defs, "server.tls_key_file").defaultValue)
	assert.Equal(t, true, findKeyDef(t, defs, "server.tls_terminated_upstream").defaultValue)
	assert.Equal(t, "10.0.0.0/8,172.16.0.0/12", findKeyDef(t, defs, "server.trusted_proxies").defaultValue)
}

// TestMatcherKeyDefs_DefaultsReflectCfgIdempotency covers duration-like int
// fields (seconds/hours) that feed cfg.IdempotencyRetryWindow() accessors.
func TestMatcherKeyDefs_DefaultsReflectCfgIdempotency(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Idempotency.RetryWindowSec = 600
	cfg.Idempotency.SuccessTTLHours = 48
	cfg.Idempotency.HMACSecret = "test-secret-long-enough-to-look-real"

	defs := matcherKeyDefs(cfg)

	assert.Equal(t, 600, findKeyDef(t, defs, "idempotency.retry_window_sec").defaultValue)
	assert.Equal(t, 48, findKeyDef(t, defs, "idempotency.success_ttl_hours").defaultValue)
	assert.Equal(t, "test-secret-long-enough-to-look-real", findKeyDef(t, defs, "idempotency.hmac_secret").defaultValue)
}

// TestMatcherKeyDefs_DefaultsReflectCfgFetcher spans int/string/bool fields
// across the fetcher integration.
func TestMatcherKeyDefs_DefaultsReflectCfgFetcher(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Fetcher.Enabled = true
	cfg.Fetcher.URL = "https://fetcher.internal:8443"
	cfg.Fetcher.AllowPrivateIPs = true
	cfg.Fetcher.HealthTimeoutSec = 7
	cfg.Fetcher.RequestTimeoutSec = 45
	cfg.Fetcher.DiscoveryIntervalSec = 120
	cfg.Fetcher.SchemaCacheTTLSec = 600
	cfg.Fetcher.ExtractionPollSec = 10
	cfg.Fetcher.ExtractionTimeoutSec = 1200

	defs := matcherKeyDefs(cfg)

	assert.Equal(t, true, findKeyDef(t, defs, "fetcher.enabled").defaultValue)
	assert.Equal(t, "https://fetcher.internal:8443", findKeyDef(t, defs, "fetcher.url").defaultValue)
	assert.Equal(t, true, findKeyDef(t, defs, "fetcher.allow_private_ips").defaultValue)
	assert.Equal(t, 7, findKeyDef(t, defs, "fetcher.health_timeout_sec").defaultValue)
	assert.Equal(t, 45, findKeyDef(t, defs, "fetcher.request_timeout_sec").defaultValue)
	assert.Equal(t, 120, findKeyDef(t, defs, "fetcher.discovery_interval_sec").defaultValue)
	assert.Equal(t, 600, findKeyDef(t, defs, "fetcher.schema_cache_ttl_sec").defaultValue)
	assert.Equal(t, 10, findKeyDef(t, defs, "fetcher.extraction_poll_sec").defaultValue)
	assert.Equal(t, 1200, findKeyDef(t, defs, "fetcher.extraction_timeout_sec").defaultValue)
}

// TestMatcherKeyDefs_DefaultsReflectCfgArchival covers the largest config
// group and includes storage-class/bucket strings.
func TestMatcherKeyDefs_DefaultsReflectCfgArchival(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Archival.Enabled = true
	cfg.Archival.IntervalHours = 48
	cfg.Archival.HotRetentionDays = 30
	cfg.Archival.WarmRetentionMonths = 12
	cfg.Archival.ColdRetentionMonths = 60
	cfg.Archival.BatchSize = 10000
	cfg.Archival.PartitionLookahead = 7
	cfg.Archival.StorageBucket = "company-audit"
	cfg.Archival.StoragePrefix = "audit/prod"
	cfg.Archival.StorageClass = "STANDARD_IA"
	cfg.Archival.PresignExpirySec = 7200

	defs := matcherKeyDefs(cfg)

	assert.Equal(t, true, findKeyDef(t, defs, "archival.enabled").defaultValue)
	assert.Equal(t, 48, findKeyDef(t, defs, "archival.interval_hours").defaultValue)
	assert.Equal(t, 30, findKeyDef(t, defs, "archival.hot_retention_days").defaultValue)
	assert.Equal(t, 12, findKeyDef(t, defs, "archival.warm_retention_months").defaultValue)
	assert.Equal(t, 60, findKeyDef(t, defs, "archival.cold_retention_months").defaultValue)
	assert.Equal(t, 10000, findKeyDef(t, defs, "archival.batch_size").defaultValue)
	assert.Equal(t, 7, findKeyDef(t, defs, "archival.partition_lookahead").defaultValue)
	assert.Equal(t, "company-audit", findKeyDef(t, defs, "archival.storage_bucket").defaultValue)
	assert.Equal(t, "audit/prod", findKeyDef(t, defs, "archival.storage_prefix").defaultValue)
	assert.Equal(t, "STANDARD_IA", findKeyDef(t, defs, "archival.storage_class").defaultValue)
	assert.Equal(t, 7200, findKeyDef(t, defs, "archival.presign_expiry_sec").defaultValue)
}

// TestMatcherKeyDefs_NilCfgDefaults documents that nil cfg is tolerated and
// produces the same result as defaultConfig(), so in-process tests that call
// matcherKeyDefs without a Config still work.
func TestMatcherKeyDefs_NilCfgDefaults(t *testing.T) {
	t.Parallel()

	nilDefs := matcherKeyDefs(nil)
	defDefs := matcherKeyDefs(defaultConfig())

	require.Len(t, nilDefs, len(defDefs), "nil-cfg and defaultConfig-cfg must return the same number of keys")

	byKey := make(map[string]any, len(defDefs))
	for _, d := range defDefs {
		byKey[d.key] = d.defaultValue
	}

	for _, d := range nilDefs {
		want, ok := byKey[d.key]
		require.True(t, ok, "nil-cfg produced unknown key %q", d.key)
		assert.Equalf(t, want, d.defaultValue, "nil-cfg default mismatch for key %q", d.key)
	}
}

// TestRegisterMatcherKeys_NilCfgRejected asserts the exported constructor
// refuses a nil cfg with a wrapped ErrConfigNil.
func TestRegisterMatcherKeys_NilCfgRejected(t *testing.T) {
	t.Parallel()

	err := RegisterMatcherKeys(nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNil)
}
