// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for the canonical /readyz handler and its collaborators. See
// health_check.go for the contract in prose; every assertion below maps to
// one bullet of that contract.

func TestNewHealthDependencies_AllNil(t *testing.T) {
	t.Parallel()

	deps := NewHealthDependencies(nil, nil, nil, nil, nil)

	assert.NotNil(t, deps)
	assert.Nil(t, deps.Postgres)
	assert.Nil(t, deps.PostgresReplica)
	assert.Nil(t, deps.Redis)
	assert.Nil(t, deps.RabbitMQ)
	assert.Nil(t, deps.ObjectStorage)

	// Verify defaults: Redis, replica, and object storage are optional.
	assert.True(t, deps.RedisOptional)
	assert.True(t, deps.PostgresReplicaOptional)
	assert.True(t, deps.ObjectStorageOptional)

	// Postgres and RabbitMQ are NOT optional by default.
	assert.False(t, deps.PostgresOptional)
	assert.False(t, deps.RabbitMQOptional)
}

// TestReadinessHandler_UsesRuntimeConfigGetter verifies that a runtime config
// getter supersedes the initial config. With required deps unresolved, the
// handler fails closed (top-level "unhealthy" + HTTP 503). The runtime cfg's
// deployment_mode also reaches the response envelope.
func TestReadinessHandler_UsesRuntimeConfigGetter(t *testing.T) {
	t.Parallel()

	initialCfg := &Config{App: AppConfig{EnvName: "development", Mode: "local"}}
	runtimeCfg := &Config{App: AppConfig{EnvName: "production", Mode: "saas"}}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(initialCfg, func() *Config { return runtimeCfg }, nil, nil, nil))

	req := httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))

	// checks is always populated now (no env-gating).
	require.NotNil(t, response.Checks)
	assert.Equal(t, statusUnhealthy, response.Status)
	assert.Equal(t, checkStatusDown, response.Checks["postgres"].Status)
	assert.Equal(t, "saas", response.DeploymentMode, "runtime cfg mode must win")
}

// TestReadinessHandler_UsesRuntimeTimeoutFromConfigGetter verifies the
// runtime config's HealthCheckTimeoutSec bounds per-check latency. A hung
// probe must surface as "down" quickly, not after the default 5s.
func TestReadinessHandler_UsesRuntimeTimeoutFromConfigGetter(t *testing.T) {
	t.Parallel()

	initialCfg := &Config{
		App:            AppConfig{EnvName: "development"},
		Infrastructure: InfrastructureConfig{HealthCheckTimeoutSec: 5},
	}
	runtimeCfg := &Config{
		App:            AppConfig{EnvName: "development"},
		Infrastructure: InfrastructureConfig{HealthCheckTimeoutSec: 1},
	}
	deps := &HealthDependencies{
		PostgresCheck: func(ctx context.Context) error {
			<-ctx.Done()

			return ctx.Err()
		},
	}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(initialCfg, func() *Config { return runtimeCfg }, nil, deps, nil))

	started := time.Now()
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody), 2000)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Less(t, time.Since(started), 2*time.Second)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	assert.Equal(t, "unhealthy", response.Status)
	require.NotNil(t, response.Checks)
	assert.Equal(t, "down", response.Checks["postgres"].Status)
}

// TestReadinessHandler_WhenDraining returns 503 with an empty checks map
// and does NOT probe deps — fail-fast shutdown behaviour.
func TestReadinessHandler_WhenDraining_ReturnsServiceUnavailable(t *testing.T) {
	t.Parallel()

	state := &readinessState{}
	state.beginDraining()

	// If drain short-circuit is broken we would probe this check and
	// time-out. Setting a hung check proves we never got there.
	deps := &HealthDependencies{
		PostgresCheck: func(ctx context.Context) error {
			<-ctx.Done()

			return ctx.Err()
		},
	}

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "development"}}
	app.Get("/readyz", readinessHandler(cfg, nil, state.isDraining, deps, nil))

	started := time.Now()
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Less(t, time.Since(started), 750*time.Millisecond,
		"draining short-circuit must not run checks")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	assert.Equal(t, "unhealthy", response.Status)
	assert.NotNil(t, response.Checks)
	assert.Empty(t, response.Checks, "draining response must carry an empty checks map")
	assert.NotEmpty(t, response.Version)
	assert.NotEmpty(t, response.DeploymentMode)
}

// TestReadinessHandler_EnvelopeHasVersionAndMode is a smoke test for the
// top-level fields the skill mandates.
func TestReadinessHandler_EnvelopeHasVersionAndMode(t *testing.T) {
	t.Parallel()

	cfg := &Config{App: AppConfig{EnvName: "development", Mode: "byoc"}}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(cfg, nil, nil, nil, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	assert.NotEmpty(t, response.Version)
	assert.Equal(t, "byoc", response.DeploymentMode)
}

// TestReadinessHandler_CacheServesRepeatRequests verifies that rapid /readyz
// hits on the same handler are served from the 250ms TTL cache — the dep
// probes do not re-run. Counts probe invocations to distinguish cache hits
// from fresh probes.
func TestReadinessHandler_CacheServesRepeatRequests(t *testing.T) {
	t.Parallel()

	var probeCount int64

	cfg := &Config{App: AppConfig{EnvName: "development"}}
	deps := &HealthDependencies{
		PostgresCheck: func(context.Context) error {
			atomic.AddInt64(&probeCount, 1)
			return nil
		},
		RabbitMQCheck: func(context.Context) error { return nil },
		// Mark postgres required (default); others optional so they don't trip.
		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(cfg, nil, nil, deps, nil))

	// First request: fresh probe.
	resp1, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	_ = resp1.Body.Close()

	firstCount := atomic.LoadInt64(&probeCount)
	require.Equal(t, int64(1), firstCount, "first /readyz must run the postgres probe once")

	// Second request within 250ms: cache hit, no probe increment.
	resp2, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	_ = resp2.Body.Close()

	secondCount := atomic.LoadInt64(&probeCount)
	assert.Equal(t, firstCount, secondCount, "second /readyz within TTL must be served from cache (probe should not re-run)")
}

// TestReadinessHandler_CacheRefreshesAfterTTL verifies that a request after
// the 250ms TTL window triggers a fresh probe.
func TestReadinessHandler_CacheRefreshesAfterTTL(t *testing.T) {
	t.Parallel()

	var probeCount int64

	cfg := &Config{App: AppConfig{EnvName: "development"}}
	deps := &HealthDependencies{
		PostgresCheck: func(context.Context) error {
			atomic.AddInt64(&probeCount, 1)
			return nil
		},
		RabbitMQCheck:           func(context.Context) error { return nil },
		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(cfg, nil, nil, deps, nil))

	resp1, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	_ = resp1.Body.Close()
	require.Equal(t, int64(1), atomic.LoadInt64(&probeCount))

	// Sleep past TTL (300ms > 250ms).
	time.Sleep(300 * time.Millisecond)

	resp2, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	_ = resp2.Body.Close()

	assert.Equal(t, int64(2), atomic.LoadInt64(&probeCount),
		"request after TTL must re-run the probe (not cached)")
}

// TestReadinessHandler_DrainBypassesCache verifies that draining short-
// circuits before the cache lookup so SIGTERM flips /readyz 503 immediately
// even if a cached healthy response is still inside its TTL.
func TestReadinessHandler_DrainBypassesCache(t *testing.T) {
	t.Parallel()

	cfg := &Config{App: AppConfig{EnvName: "development"}}
	deps := &HealthDependencies{
		PostgresCheck:           func(context.Context) error { return nil },
		RabbitMQCheck:           func(context.Context) error { return nil },
		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	state := &readinessState{}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(cfg, nil, state.isDraining, deps, nil))

	// Warm the cache with a healthy response.
	resp1, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	_ = resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)

	// Flip draining flag. Immediate subsequent request must see 503 + empty
	// checks map, NOT the cached healthy response.
	state.beginDraining()

	resp2, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode,
		"drain must bypass cache regardless of TTL state")

	body, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	assert.Equal(t, statusUnhealthy, response.Status)
	assert.Empty(t, response.Checks, "drain response must carry empty checks, not cached healthy map")
}

// TestReadinessHandler_SurfacesTLSPosture exercises the per-dep TLS-posture
// helpers end-to-end: with a cfg that configures every dep and checks that
// pass, the response's CheckResult.TLS field reflects the configured posture.
func TestReadinessHandler_SurfacesTLSPosture(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		App: AppConfig{EnvName: "development", Mode: "local"},
		Postgres: PostgresConfig{
			PrimaryHost: "pg.example.com", PrimaryPort: "5432",
			PrimaryUser: "u", PrimaryPassword: "p", PrimaryDB: "db",
			PrimarySSLMode: "require",
			// Distinct replica → exercise postgresReplicaTLSPosture path.
			ReplicaHost: "replica.example.com", ReplicaSSLMode: "require",
		},
		Redis:    RedisConfig{Host: "redis.example.com:6380", TLS: true},
		RabbitMQ: RabbitMQConfig{URI: "amqps", Host: "rabbit.example.com", Port: "5671", User: "u"},
		// Non-empty endpoint exercises objectStorageTLSPosture.
		ObjectStorage: ObjectStorageConfig{Endpoint: "https://s3.example.com"},
	}

	deps := &HealthDependencies{
		PostgresCheck:        func(context.Context) error { return nil },
		PostgresReplicaCheck: func(context.Context) error { return nil },
		RedisCheck:           func(context.Context) error { return nil },
		RabbitMQCheck:        func(context.Context) error { return nil },
		ObjectStorageCheck:   func(context.Context) error { return nil },
	}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(cfg, nil, nil, deps, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	assert.Equal(t, "healthy", response.Status)

	for _, name := range []string{"postgres", "postgres_replica", "redis", "rabbitmq", "object_storage"} {
		entry, ok := response.Checks[name]
		require.True(t, ok, "expected check for %s", name)
		assert.Equal(t, "up", entry.Status)
		require.NotNil(t, entry.TLS, "TLS posture must be reported for %s", name)
		assert.True(t, *entry.TLS, "TLS posture for %s must be true", name)
	}
}

// TestEvaluateReadinessChecks_ParallelBoundedByMaxCheckLatency asserts that
// evaluateReadinessChecks fans every per-dep probe out in parallel, so the
// handler's worst-case latency is max(per-check timeout) — NOT
// n_deps × per_check_timeout. With 5 deps each sleeping 100ms, sequential
// execution would take ~500ms; parallel execution stays well under 200ms.
// This regression test guards K8s readinessProbe.periodSeconds=5 from probe
// backup under degraded deps.
func TestEvaluateReadinessChecks_ParallelBoundedByMaxCheckLatency(t *testing.T) {
	t.Parallel()

	const (
		perCheckSleep = 100 * time.Millisecond
		// Generous upper bound — parallel fan-out should finish in ~100ms
		// plus goroutine scheduling overhead. Sequential execution would
		// take 5×100ms = 500ms, far above this ceiling.
		parallelCeiling = 400 * time.Millisecond
	)

	sleepyCheck := func(ctx context.Context) error {
		select {
		case <-time.After(perCheckSleep):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	deps := &HealthDependencies{
		PostgresCheck:        sleepyCheck,
		PostgresReplicaCheck: sleepyCheck,
		RedisCheck:           sleepyCheck,
		RabbitMQCheck:        sleepyCheck,
		ObjectStorageCheck:   sleepyCheck,

		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	cfg := &Config{App: AppConfig{EnvName: "development"}}

	started := time.Now()
	httpStatus, checks, healthy := evaluateReadinessChecks(
		context.Background(), cfg, deps, nil, 2*time.Second,
	)
	elapsed := time.Since(started)

	require.Equal(t, fiber.StatusOK, httpStatus)
	require.True(t, healthy, "all checks pass must yield healthy=true")
	require.Len(t, checks, readyzDepCount, "every dep must produce a CheckResult")

	for _, name := range []string{"postgres", "postgres_replica", "redis", "rabbitmq", "object_storage"} {
		entry, ok := checks[name]
		require.True(t, ok, "missing check for %s", name)
		assert.Equal(t, "up", entry.Status, "expected up for %s", name)
	}

	assert.Less(t, elapsed, parallelCeiling,
		"evaluateReadinessChecks must fan out: expected <%s, got %s (sequential would be ~%s)",
		parallelCeiling, elapsed, 5*perCheckSleep)
}

// TestEvaluateReadinessChecks_AllDepsProbedUnderDegradation is a second-order
// regression guard: even when every dep is slow, the handler latency stays
// bounded by per-check timeout, not sum-of-timeouts. Wires 5 checks that
// each hang to the per-check timeout; asserts total handler latency does
// not exceed timeout + goroutine scheduling budget.
func TestEvaluateReadinessChecks_AllDepsProbedUnderDegradation(t *testing.T) {
	t.Parallel()

	perCheckTimeout := 200 * time.Millisecond
	// Outer ceiling: one timeout window plus generous scheduling slack.
	outerCeiling := perCheckTimeout + 500*time.Millisecond

	hungCheck := func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}

	deps := &HealthDependencies{
		PostgresCheck:        hungCheck,
		PostgresReplicaCheck: hungCheck,
		RedisCheck:           hungCheck,
		RabbitMQCheck:        hungCheck,
		ObjectStorageCheck:   hungCheck,

		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	cfg := &Config{App: AppConfig{EnvName: "development"}}

	started := time.Now()
	httpStatus, checks, healthy := evaluateReadinessChecks(
		context.Background(), cfg, deps, nil, perCheckTimeout,
	)
	elapsed := time.Since(started)

	require.Equal(t, fiber.StatusServiceUnavailable, httpStatus)
	require.False(t, healthy)
	require.Len(t, checks, readyzDepCount)

	for _, name := range []string{"postgres", "postgres_replica", "redis", "rabbitmq", "object_storage"} {
		entry, ok := checks[name]
		require.True(t, ok, "missing check for %s", name)
		assert.Equal(t, "down", entry.Status, "expected down for %s under timeout", name)
	}

	assert.Less(t, elapsed, outerCeiling,
		"handler latency must be bounded by max(per-check timeout), got %s ceiling %s",
		elapsed, outerCeiling)
}

// TestCategoriseProbeError_NilError returns empty string.
func TestCategoriseProbeError_NilError(t *testing.T) {
	t.Parallel()

	assert.Empty(t, categoriseProbeError(nil))
}

// TestCategoriseProbeError_CategoriesExhaustive covers the bounded token set.
// Each case constructs an error of a specific type and asserts the expected
// token is returned — and nothing else leaks through.
func TestCategoriseProbeError_CategoriesExhaustive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		want    string
		notWant []string // substrings that MUST NOT appear in the returned token
	}{
		{
			name: "deadline_exceeded_maps_to_timeout",
			err:  context.DeadlineExceeded,
			want: "timeout",
		},
		{
			name: "connection_refused_typed_error",
			err:  syscall.ECONNREFUSED,
			want: "connection refused",
		},
		{
			name: "dns_error_typed",
			err:  &net.DNSError{Err: "no such host", Name: "db.example.com"},
			want: "dns failure",
		},
		{
			name: "tls_handshake_substring_fallback",
			//nolint:err113 // test-local sentinel
			err:  errors.New("tls: handshake failed reading server hello"),
			want: "tls handshake failed",
		},
		{
			name: "unknown_error_falls_back_to_check_failed",
			//nolint:err113 // test-local sentinel
			err:  errors.New("pool exhausted: no free connections"),
			want: "check failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := categoriseProbeError(tc.err)
			assert.Equal(t, tc.want, got)

			for _, forbidden := range tc.notWant {
				assert.NotContains(t, got, forbidden, "forbidden substring leaked")
			}
		})
	}
}

// TestCategoriseProbeError_TLSAlertErrorTyped verifies the typed-error TLS
// detection path: a crypto/tls.AlertError value categorises as "tls handshake
// failed" without depending on substring matching.
func TestCategoriseProbeError_TLSAlertErrorTyped(t *testing.T) {
	t.Parallel()

	// tls.AlertError is just a uint8 under the hood; construct one directly.
	// Value is deliberately opaque — we only care that errors.As sees it.
	var alert tls.AlertError = 50

	got := categoriseProbeError(alert)
	assert.Equal(t, "tls handshake failed", got,
		"typed tls.AlertError must map to the TLS handshake token without substring matching")
}

// TestCategoriseProbeError_OrderingDNSBeatsDeadline pins the priority
// ordering. An error that wraps BOTH a net.DNSError and
// context.DeadlineExceeded must categorise as "dns failure" — DNS is the
// more-actionable signal for operators and wins ties.
func TestCategoriseProbeError_OrderingDNSBeatsDeadline(t *testing.T) {
	t.Parallel()

	dnsErr := &net.DNSError{Err: "no such host", Name: "db.example.com"}
	// Wrap both into a single chain: deadline at the outside, DNS at the
	// inside. errors.As must traverse the chain and find the DNS error first.
	wrapped := fmt.Errorf("%w: upstream returned %w", context.DeadlineExceeded, dnsErr)

	got := categoriseProbeError(wrapped)
	assert.Equal(t, "dns failure", got,
		"wrapped DNS + deadline must categorise as DNS (priority order is DNS → timeout → ...)")
}

// TestCategoriseProbeError_TLSHandshakeCredentialLeak ensures the TLS-handshake
// substring-match path cannot leak credentials even when the underlying error
// carries DSN fragments. The output must be exactly the bounded token.
func TestCategoriseProbeError_TLSHandshakeCredentialLeak(t *testing.T) {
	t.Parallel()

	//nolint:err113 // test-local sentinel
	err := errors.New("tls: handshake failed for user=admin host=db.internal password=secret123")

	got := categoriseProbeError(err)
	assert.Equal(t, "tls handshake failed", got)

	for _, forbidden := range []string{"admin", "secret123", "db.internal", "user=", "password="} {
		assert.NotContains(t, got, forbidden,
			"credential fragment %q must not leak through the TLS handshake token", forbidden)
	}
}

// TestCategoriseProbeError_CredentialLeakRegression is the core security
// assertion: an error whose message embeds credential-shaped tokens
// (password=, user=, DSN fragments) MUST NOT appear in the returned token.
// The categoriser returns opaque strings from a bounded set — none of which
// contain "password", "user=", "host=", or the literal credential values.
func TestCategoriseProbeError_CredentialLeakRegression(t *testing.T) {
	t.Parallel()

	// Simulate a pgx/libpq-style wrapped error that carries the DSN.
	const leakyMsg = "pq: host=db.internal user=matcher password=s3cr3t-abc123 dbname=matcher sslmode=disable connect failed"
	//nolint:err113 // test-local sentinel
	err := errors.New(leakyMsg)

	got := categoriseProbeError(err)

	// The bounded token set must not carry any part of the credential-bearing
	// error string. Hard-code the expected token AND assert against the
	// specific leak shapes to regression-guard future drift.
	assert.Equal(t, "check failed", got,
		"credential-bearing error must map to the opaque fallback token")

	for _, forbidden := range []string{"password", "s3cr3t", "abc123", "host=", "user=", "matcher", "db.internal"} {
		assert.NotContains(t, got, forbidden,
			"credential fragment %q leaked into probe error token", forbidden)
	}
}

// TestTLSPostureHelpers_NilCfgReturnsUnknown pins the nil-cfg short-circuit
// on every TLS posture helper. All five return (nil, "") for nil cfg so the
// response drops the tls field without emitting a misleading Reason.
func TestTLSPostureHelpers_NilCfgReturnsUnknown(t *testing.T) {
	t.Parallel()

	helpers := []struct {
		name string
		fn   func(*Config) (*bool, string)
	}{
		{"postgres", postgresTLSPosture},
		{"postgres_replica", postgresReplicaTLSPosture},
		{"redis", redisTLSPosture},
		{"rabbitmq", rabbitMQTLSPosture},
		{"object_storage", objectStorageTLSPosture},
	}

	for _, h := range helpers {
		t.Run(h.name, func(t *testing.T) {
			t.Parallel()

			tls, reason := h.fn(nil)
			assert.Nil(t, tls, "%s must return nil TLS pointer for nil cfg", h.name)
			assert.Empty(t, reason, "%s must return empty reason for nil cfg", h.name)
		})
	}
}

// TestReadinessHandler_TLSPostureUnknown_SurfacesReason verifies that when
// the TLS-posture helper cannot determine TLS (parse error on malformed
// conn string), the CheckResult carries a "TLS posture unknown" reason
// rather than silently dropping the tls field. Operators must be able to
// distinguish "TLS off" from "we could not figure it out".
func TestReadinessHandler_TLSPostureUnknown_SurfacesReason(t *testing.T) {
	t.Parallel()

	// Malformed IPv6 host → buildRedisURLForTLSCheck emits "rediss://[::1",
	// which url.Parse rejects → redisTLSPosture returns (nil, parse-error).
	cfg := &Config{
		App:   AppConfig{EnvName: "development", Mode: "local"},
		Redis: RedisConfig{Host: "[::1", TLS: true},
	}

	deps := &HealthDependencies{
		RedisCheck:    func(context.Context) error { return nil },
		RedisOptional: true,
	}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(cfg, nil, nil, deps, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))

	redis, ok := response.Checks["redis"]
	require.True(t, ok, "redis check must be present")
	assert.Equal(t, checkStatusUp, redis.Status, "probe itself succeeds; only tls posture is unknown")
	assert.Nil(t, redis.TLS, "tls field must be absent when posture cannot be determined")
	assert.Contains(t, redis.Reason, "TLS posture unknown",
		"response must carry a TLS-unknown explanation, not silently drop the field")
	assert.Contains(t, redis.Reason, "invalid redis connection configuration",
		"response reason must carry the bounded token from redisTLSPosture")
	assert.NotContains(t, redis.Reason, "[::1",
		"response reason must not leak raw parser input")
}

// TestReadinessHandler_SkippedDepCarriesReason verifies that a not-configured
// dep (no check func, no backing client) surfaces with status=skipped and
// a non-empty reason field, per the five-value contract.
func TestReadinessHandler_SkippedDepCarriesReason(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		PostgresCheck: func(context.Context) error { return nil },
		// everything else left nil → resolvers return not-available
		RedisOptional:           true,
		RabbitMQOptional:        true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}

	cfg := &Config{App: AppConfig{EnvName: "development"}}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(cfg, nil, nil, deps, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))

	redis := response.Checks["redis"]
	assert.Equal(t, "skipped", redis.Status)
	assert.NotEmpty(t, redis.Reason)
	assert.Empty(t, redis.Error, "skipped must not carry error field")
	assert.Zero(t, redis.LatencyMs, "skipped carries no latency")
}

func TestReadinessHandler_RequiredDepWithoutCheck_FailsClosed(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		// Required dependency intentionally unresolved.
		PostgresCheck: nil,

		RedisOptional:           true,
		RabbitMQOptional:        true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}

	cfg := &Config{App: AppConfig{EnvName: "development"}}

	app := fiber.New()
	app.Get("/readyz", readinessHandler(cfg, nil, nil, deps, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var response ReadinessResponse
	require.NoError(t, json.Unmarshal(body, &response))
	require.Equal(t, statusUnhealthy, response.Status)
	require.Equal(t, checkStatusDown, response.Checks["postgres"].Status)
	require.Equal(t, "check not configured", response.Checks["postgres"].Error)
}
