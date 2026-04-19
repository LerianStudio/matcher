//go:build e2e

package journeys

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
)

// TestSystemplaneSettings_TenantRateLimitAffectsProtectedRoute exercises the
// end-to-end runtime-mutation story for a matcher-scoped rate-limit key:
// snapshot the current value, PUT a throttling-tight value (max=1), hit
// the protected route twice and expect 200 → 429, then restore the snapshot.
//
// Previously gated behind E2E_ENABLE_SETTINGS_RUNTIME_JOURNEY=1 because the
// restore path could strand rate_limit.max=1 in shared test infrastructure
// if cleanup failed. With the error-surfacing cleanup added in ee1788b the
// failure mode becomes a loud t.Errorf instead of silent pollution, and the
// test is safe to run unconditionally — it is the best coverage we have of
// the runtime → HTTP middleware mutation path.
//
// Cleanup limitation: the v5 admin API exposes no DELETE verb, so keys that
// were ABSENT before the test (readSystemplaneKeyValue → not found) cannot
// be restored to absence on cleanup. In practice this never fires because
// rate_limit.max / rate_limit.expiry_sec are always registered — GET
// returns the seeded default with ok=true even if no runtime override was
// ever written. TestSystemplaneSettings_V4PathsRemoved below pins the
// removal of /v1/system/configs[...] surfaces that would otherwise be
// candidates for DELETE.
func TestSystemplaneSettings_TenantRateLimitAffectsProtectedRoute(t *testing.T) {
	cfg := e2e.GetConfig()
	require.NotNil(t, cfg)

	// Snapshot current values for restore.
	origMax, maxFound, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.max")
	require.NoError(t, err)

	origExpiry, expiryFound, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.expiry_sec")
	require.NoError(t, err)

	t.Cleanup(func() {
		if maxFound {
			if err := putSystemplaneValues(cfg.AppBaseURL, map[string]any{"rate_limit.max": origMax}); err != nil {
				t.Errorf("restore rate_limit.max failed: %v (rate_limit.max=1 may persist in systemplane — other tests may be affected)", err)
			}
		} else {
			// Absent originally. v5 admin has no DELETE verb; best we can
			// do is log the drift so the next operator running the suite
			// knows why the list response looks unusual.
			t.Logf("rate_limit.max was absent before the test and stays set; v5 admin has no DELETE verb to revert")
		}

		if expiryFound {
			if err := putSystemplaneValues(cfg.AppBaseURL, map[string]any{"rate_limit.expiry_sec": origExpiry}); err != nil {
				t.Errorf("restore rate_limit.expiry_sec failed: %v", err)
			}
		} else {
			t.Logf("rate_limit.expiry_sec was absent before the test and stays set; v5 admin has no DELETE verb to revert")
		}
	})

	require.NoError(t, putSystemplaneValues(cfg.AppBaseURL, map[string]any{
		"rate_limit.max":        1,
		"rate_limit.expiry_sec": 60,
	}))

	// Verify the key was set.
	val, found, err := readSystemplaneKeyValue(cfg.AppBaseURL, "rate_limit.max")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, float64(1), val)

	// Hit a protected route twice — second should be throttled.
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
