//go:build e2e

package journeys

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
)

// TestSystemplaneSettings_TenantRateLimitAffectsProtectedRoute exercises the
// end-to-end runtime-mutation story for matcher-scoped rate-limit keys by
// squeezing the admin rate limiter to 1 request per window, hitting
// /system/matcher twice, and expecting 200 → 429 before restoring the
// snapshot. "Tenant" in the name refers to the rate limiter identity: the
// lib-commons limiter keys buckets by tenant+IP (see rateLimitIdentityFunc),
// so two requests from the same test share an identity and race the same
// quota.
//
// Why the admin tier (rate_limit.admin_max) and not the global tier
// (rate_limit.max): after the protectedRouter refactor, the global rate
// limiter is scoped strictly to protected bounded-context routes and no
// longer leaks into /system/* via an app-global USE entry. The admin tier,
// installed by MountSystemplaneAPI on the /system prefix, is the only
// limiter that governs /system/matcher. Mutating rate_limit.admin_max is
// therefore the correct single-key knob for throttling /system/* without
// disturbing the business-plane quota.
//
// Prerequisite: docker-compose sets RATE_LIMIT_ENABLED=true so the
// lib-commons RateLimiter object is actually created at boot. With
// RATE_LIMIT_ENABLED=false, ratelimit.New returns a nil RateLimiter, the
// middleware collapses to a pass-through regardless of systemplane values,
// and this test cannot observe throttling.
//
// Counter-reset dependency: the setup PUTs and the readback Eventually
// increment the same per-identity Redis counter the critical GETs use,
// so the test flushes ratelimit:* keys RIGHT BEFORE the two critical
// GETs. Without this the counter is already above max=1 by the time the
// test expects a 200, and the first "should succeed" assertion fails
// with a confusing 429.
//
// Previously gated behind E2E_ENABLE_SETTINGS_RUNTIME_JOURNEY=1 because the
// restore path could strand throttle-tight values in shared test
// infrastructure if cleanup failed. With the error-surfacing cleanup added
// in ee1788b the failure mode becomes a loud t.Errorf instead of silent
// pollution, and the test is safe to run unconditionally — it is the best
// coverage we have of the runtime → HTTP middleware mutation path.
//
// Cleanup limitation: the v5 admin API exposes no DELETE verb, so keys that
// were ABSENT before the test (readSystemplaneKeyValue → not found) cannot
// be restored to absence on cleanup. In practice this never fires because
// rate_limit.enabled / rate_limit.admin_max / rate_limit.admin_expiry_sec
// are always registered — GET returns the seeded default with ok=true even
// if no runtime override was ever written. TestSystemplaneSettings_V4PathsRemoved
// below pins the removal of /v1/system/configs[...] surfaces that would
// otherwise be candidates for DELETE.
func TestSystemplaneSettings_TenantRateLimitAffectsProtectedRoute(t *testing.T) {
	cfg := e2e.GetConfig()
	require.NotNil(t, cfg)

	// Snapshot current values for restore. We mutate three keys to coerce
	// the admin rate limiter into a throttling-tight state. rate_limit.enabled
	// is the master switch that matcher's settingsBackedRateLimitHandler
	// consults at request time; it must be true for any throttling to apply.
	origEnabled, enabledFound, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.enabled")
	require.NoError(t, err)

	origAdminMax, adminMaxFound, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.admin_max")
	require.NoError(t, err)

	origAdminExpiry, adminExpiryFound, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.admin_expiry_sec")
	require.NoError(t, err)

	// Cleanup flush helper — reusable handle so we can reset the counter
	// before each restore PUT. Without these flushes the restore PUTs
	// themselves get throttled against the rate_limit.admin_max=1 we just
	// set, leaving pollution for the next test in the suite.
	cleanupFlusher := e2e.NewStackChecker(cfg)

	t.Cleanup(func() {
		cleanupCtx := context.Background()

		// Reset the rate-limit counter BEFORE any restore PUT, otherwise
		// the restore is itself throttled by the very constraint we are
		// trying to remove.
		if flushErr := cleanupFlusher.FlushRateLimitKeys(cleanupCtx); flushErr != nil {
			t.Logf("cleanup: flush ratelimit keys failed: %v (restore PUTs may be rate-limited)", flushErr)
		}

		// Restore admin_max FIRST so admin_max=1 stops applying as quickly
		// as possible. Any partial failure after this point still leaves a
		// sane quota instead of stranding rate_limit.admin_max=1 across tests.
		if adminMaxFound {
			if err := putSystemplaneValues(cfg.AppBaseURL, map[string]any{"rate_limit.admin_max": origAdminMax}); err != nil {
				t.Errorf("restore rate_limit.admin_max failed: %v (rate_limit.admin_max=1 may persist in systemplane — subsequent /system requests may throttle)", err)
			}
		} else {
			t.Logf("rate_limit.admin_max was absent before the test and stays set; v5 admin has no DELETE verb to revert")
		}

		// Wait for the admin_max=ORIG_ADMIN_MAX to propagate before the next PUT,
		// and flush again so the next PUTs start from a clean counter.
		time.Sleep(time.Second)

		if flushErr := cleanupFlusher.FlushRateLimitKeys(cleanupCtx); flushErr != nil {
			t.Logf("cleanup: second flush failed: %v", flushErr)
		}

		if adminExpiryFound {
			if err := putSystemplaneValues(cfg.AppBaseURL, map[string]any{"rate_limit.admin_expiry_sec": origAdminExpiry}); err != nil {
				t.Errorf("restore rate_limit.admin_expiry_sec failed: %v", err)
			}
		} else {
			t.Logf("rate_limit.admin_expiry_sec was absent before the test and stays set; v5 admin has no DELETE verb to revert")
		}

		// Restore the master switch LAST so throttling is disabled the moment
		// it needs to be — if the prior restores failed, leaving Enabled=true
		// with rate_limit.admin_max=1 would poison the suite.
		if enabledFound {
			if err := putSystemplaneValues(cfg.AppBaseURL, map[string]any{"rate_limit.enabled": origEnabled}); err != nil {
				t.Errorf("restore rate_limit.enabled failed: %v (rate limiting may stay enabled — other tests may be affected)", err)
			}
		} else {
			t.Logf("rate_limit.enabled was absent before the test and stays set; v5 admin has no DELETE verb to revert")
		}
	})

	// Sequencing note — each PUT/GET against /system/* increments the
	// per-identity rate-limit counter. Squeezing the throttling-tight max
	// last, with a Redis flush IMMEDIATELY before the critical GETs, is the
	// only way to guarantee the first critical GET sees counter ≤ max (allow)
	// and the second sees counter > max (deny). Setting max tight first would
	// throttle the readback that verifies propagation; interleaving the
	// flush between the tight PUT and the GETs gives us a clean zero-counter
	// starting point after propagation settles.
	const rateLimitMaxOneRequest = 1
	flusher := e2e.NewStackChecker(cfg)
	flushCtx := context.Background()

	// Step 1: enable rate limiting and fix the admin expiry window. admin_max
	// still high (docker-compose seed or whatever the prior state left), so
	// these PUTs and the subsequent readback polling will not themselves
	// throttle.
	require.NoError(t, putSystemplaneValues(cfg.AppBaseURL, map[string]any{
		"rate_limit.enabled":          true,
		"rate_limit.admin_expiry_sec": 60,
	}))

	// Step 2: confirm enabled/admin_expiry_sec propagated to the in-memory
	// cache before squeezing admin_max. The rate limiter middleware reads
	// from the SAME client the admin API reads from, so once admin GET sees
	// the new values, the middleware does too.
	require.Eventually(t, func() bool {
		enabled, ok, readErr := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.enabled")
		if readErr != nil || !ok || enabled != true {
			return false
		}

		expirySec, ok, readErr := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.admin_expiry_sec")
		if readErr != nil || !ok || expirySec != float64(60) {
			return false
		}

		return true
	}, 5*time.Second, 100*time.Millisecond, "rate_limit.enabled + admin_expiry_sec must propagate before squeezing admin_max")

	// Step 3: now squeeze admin_max. This PUT itself increments the counter
	// by the admin-tier middleware invocation, but admin_max is still high
	// at request time so the PUT succeeds.
	require.NoError(t, putSystemplaneValues(cfg.AppBaseURL, map[string]any{
		"rate_limit.admin_max": rateLimitMaxOneRequest,
	}))

	// Step 4: give LISTEN/NOTIFY time to propagate the new max into the
	// running middleware. We cannot admin-GET to verify here without
	// throttling ourselves against the value we just set, so we wait a
	// conservative interval. 1s is far longer than the observed LISTEN
	// propagation latency on a healthy local stack (sub-100ms) but keeps
	// the test stable under CI load.
	time.Sleep(time.Second)

	// Step 5: flush rate-limit counters so the two critical GETs start from
	// a known zero baseline. Without this, the counter is already above the
	// per-request budget from all the setup requests and the first
	// "expected 200" assertion fails with a spurious 429.
	require.NoError(
		t,
		flusher.FlushRateLimitKeys(flushCtx),
		"flush ratelimit:* keys before asserting throttle behavior",
	)

	// Hit a protected route twice — first should succeed (counter=1 ≤ max=1),
	// second should be throttled (counter=2 > max=1).
	headers := map[string]string{
		"Accept": "application/json",
	}

	listURL := cfg.AppBaseURL + "/system/" + systemplaneNamespace

	firstResp, firstBody, err := doSystemplaneRequest(http.MethodGet, listURL, nil, headers)
	require.NoError(t, err)
	defer firstResp.Body.Close() //nolint:errcheck // test helper
	assert.Equal(t, http.StatusOK, firstResp.StatusCode, string(firstBody))

	secondResp, secondBody, err := doSystemplaneRequest(http.MethodGet, listURL, nil, headers)
	require.NoError(t, err)
	defer secondResp.Body.Close() //nolint:errcheck // test helper
	assert.Equal(t, http.StatusTooManyRequests, secondResp.StatusCode, string(secondBody))
}

// TestSystemplaneSettings_V4PathsRemoved is the contract test for the
// v4-deprecation shim: every documented v4 admin API surface must return
// 410 Gone (not 404), with the canonical JSON body shape operators grep
// for in migration-lagging tooling. A regression that removes the shim
// middleware would silently break console migrations by returning 404
// instead of the actionable 410 + hint payload.
//
// The four paths pinned here (/v1/system/configs[...]) are the exact set
// called out in TEST-50's spec and also match the rows in
// v4_deprecation_middleware_test.go's positive case table, so a future
// drop of the shim fires both tests.
func TestSystemplaneSettings_V4PathsRemoved(t *testing.T) {
	cfg := e2e.GetConfig()
	require.NotNil(t, cfg)

	removedPaths := []string{
		"/v1/system/configs",
		"/v1/system/configs/schema",
		"/v1/system/configs/history",
		"/v1/system/configs/reload",
	}

	for _, path := range removedPaths {
		path := path

		t.Run(path, func(t *testing.T) {
			resp, body, err := doSystemplaneRequest(
				http.MethodGet,
				cfg.AppBaseURL+path,
				nil,
				map[string]string{"Accept": "application/json"},
			)
			require.NoError(t, err)
			defer resp.Body.Close() //nolint:errcheck // test helper

			assert.Equalf(t, http.StatusGone, resp.StatusCode,
				"%s must return 410 Gone (NOT 404) so console migrations discover the hint payload; body: %s",
				path, string(body))

			// Parse the canonical 410 body shape. A regression that turns
			// the shim into a generic 404 fallback would lose the hint
			// payload; parsing the structured response pins the contract.
			var decoded struct {
				Code    string `json:"code"`
				Title   string `json:"title"`
				Message string `json:"message"`
				Hint    string `json:"hint"`
				Removal string `json:"removal"`
			}

			require.NoError(t, json.Unmarshal(body, &decoded),
				"410 body must decode into the canonical shape; got: %s", string(body))

			assert.Equal(t, "GONE", decoded.Code)
			assert.Equal(t, "endpoint_removed", decoded.Title)
			assert.Contains(t, decoded.Message, "v4 admin API paths removed")
			assert.Contains(t, decoded.Hint, "/system/matcher",
				"hint must direct callers to the v5 admin path shape")
			assert.NotEmpty(t, decoded.Removal,
				"removal must carry the scheduled-removal window for ops")
		})
	}
}
