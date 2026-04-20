// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/gofiber/fiber/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"
	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// mountSystemplaneForCRUDTest builds a Fiber app with the v5 systemplane
// admin API wired up exactly as production does, minus the real auth
// service (see testMountOptions). Returns the app, the backing systemplane
// client so tests can Set values out-of-band, and the config the client
// was seeded from.
//
// The rate limiter uses miniredis and a 1M/sec admin-tier limit, which is
// effectively unthrottled for the handful of requests these tests fire.
// Keeping the rate-limit middleware in the chain (rather than nil'ing it)
// proves that CRUD still works when the admin-tier limiter is installed.
func mountSystemplaneForCRUDTest(t *testing.T, authEnabled bool) (*fiber.App, *Config) {
	t.Helper()

	server := miniredis.RunT(t)
	redisClient := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	redisConn := testutil.NewRedisClientWithMock(redisClient)
	rl := ratelimit.New(redisConn, ratelimit.WithFailOpen(false))

	app := fiber.New()
	t.Cleanup(func() { _ = app.Shutdown() })

	cfg := defaultConfig()
	// Generous buckets — these tests exercise CRUD, not throttling.
	cfg.RateLimit = RateLimitConfig{
		Enabled:        true,
		Max:            1_000_000,
		ExpirySec:      60,
		AdminMax:       1_000_000,
		AdminExpirySec: 60,
	}

	client := newStartedTestClient(t, cfg)

	extractor, err := auth.NewTenantExtractor(
		authEnabled, false, auth.DefaultTenantID, auth.DefaultTenantSlug, "test-secret", "test",
	)
	require.NoError(t, err)

	var authClient *authMiddleware.AuthClient
	if authEnabled {
		// Address is deliberately non-empty so the Authorize middleware
		// reaches the token-extraction branch (it short-circuits to
		// c.Next() when Address is empty regardless of Enabled). We never
		// actually reach the HTTP call in these tests because the 401
		// "Missing Token" path fires first.
		authClient = authMiddleware.NewAuthClient("http://auth.invalid", true, nil)
	} else {
		authClient = authMiddleware.NewAuthClient("", false, nil)
	}

	err = MountSystemplaneAPI(
		app,
		client,
		cfg,
		func() *Config { return cfg },
		newRuntimeSettingsResolver(client),
		authClient,
		extractor,
		func() *ratelimit.RateLimiter { return rl },
		nil,
	)
	require.NoError(t, err)

	return app, cfg
}

// readBody fully reads and returns resp.Body as a string, closing the body.
// Keeps the assertions below compact.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(body)
}

// TestMountSystemplaneAPI_AuthEnabled_MissingTokenReturns401 asserts the
// lib-auth Authorize middleware rejects an unauthenticated caller at the
// /system prefix before any systemplane handler executes. This is the
// outermost-layer auth guard; if it regresses, the admin API becomes open
// to anyone who can reach the Fiber app.
//
// The authClient has Address="http://auth.invalid" so the Authorize
// middleware does NOT short-circuit on the (Enabled=false || Address=="")
// fast-path. The 401 is returned by the accessToken-missing branch, not
// by a real call to the auth service — that branch fires before any HTTP
// traffic is generated, so the test is hermetic.
func TestMountSystemplaneAPI_AuthEnabled_MissingTokenReturns401(t *testing.T) {
	t.Parallel()

	app, _ := mountSystemplaneForCRUDTest(t, true)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/system/matcher", http.NoBody))
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"no Authorization header must be rejected at the middleware layer")
}

// TestMountSystemplaneAPI_AuthEnabled_EmptyBearerReturns401 pins the
// classic "Authorization: Bearer " shape with no token as also rejected.
// lib-auth's ExtractTokenFromHeader returns the empty string for this
// shape, triggering the same 401.
func TestMountSystemplaneAPI_AuthEnabled_EmptyBearerReturns401(t *testing.T) {
	t.Parallel()

	app, _ := mountSystemplaneForCRUDTest(t, true)

	req := httptest.NewRequest(http.MethodGet, "/system/matcher", http.NoBody)
	req.Header.Set("Authorization", "Bearer ")

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"empty bearer token must be rejected at the middleware layer")
}

// TestMountSystemplaneAPI_AuthDisabled_ListReturnsAllMatcherKeys asserts
// the GET /system/:namespace list endpoint returns every registered key
// in the matcher namespace with value + description. This is the default
// admin-console entry point — if it regresses, operators lose the
// discoverability needed to drive runtime changes.
func TestMountSystemplaneAPI_AuthDisabled_ListReturnsAllMatcherKeys(t *testing.T) {
	t.Parallel()

	app, _ := mountSystemplaneForCRUDTest(t, false)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/system/matcher", http.NoBody))
	require.NoError(t, err)

	body := readBody(t, resp)

	require.Equal(t, http.StatusOK, resp.StatusCode, "list must return 200: %s", body)

	var decoded struct {
		Namespace string `json:"namespace"`
		Entries   []struct {
			Key         string `json:"key"`
			Value       any    `json:"value"`
			Description string `json:"description,omitempty"`
		} `json:"entries"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &decoded))

	assert.Equal(t, systemplaneNamespace, decoded.Namespace)
	assert.NotEmpty(t, decoded.Entries, "matcher namespace must register at least one key")

	// Spot-check the entries include well-known matcher runtime keys so a
	// regression that dropped RegisterMatcherKeys from the mount path
	// fails here rather than in downstream integration tests.
	seen := make(map[string]struct{}, len(decoded.Entries))
	for _, e := range decoded.Entries {
		seen[e.Key] = struct{}{}
	}

	for _, required := range []string{
		"rate_limit.max",
		"webhook.timeout_sec",
		"deduplication.ttl_sec",
	} {
		_, ok := seen[required]
		assert.Truef(t, ok, "list response must include %q — missing keys in response: %v",
			required, body)
	}
}

// TestMountSystemplaneAPI_AuthDisabled_GetOneRegisteredKey asserts a
// GET /system/:namespace/:key on an existing registered key returns 200
// with the value, namespace, and key echoed back. This is the targeted
// read operators use to verify a single setting.
func TestMountSystemplaneAPI_AuthDisabled_GetOneRegisteredKey(t *testing.T) {
	t.Parallel()

	app, _ := mountSystemplaneForCRUDTest(t, false)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/system/matcher/rate_limit.max", http.NoBody))
	require.NoError(t, err)

	body := readBody(t, resp)

	require.Equal(t, http.StatusOK, resp.StatusCode, "get-one must return 200: %s", body)

	var decoded struct {
		Namespace string `json:"namespace"`
		Key       string `json:"key"`
		Value     any    `json:"value"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &decoded))
	assert.Equal(t, systemplaneNamespace, decoded.Namespace)
	assert.Equal(t, "rate_limit.max", decoded.Key)
	assert.NotNil(t, decoded.Value, "registered key must return a non-nil value")
}

// TestMountSystemplaneAPI_AuthDisabled_GetOneMissingKey asserts an
// unregistered key returns 404 rather than 200 with a nil value. This
// distinguishes "key you wanted does not exist" from "key exists with a
// nil value" for the console's error handling.
func TestMountSystemplaneAPI_AuthDisabled_GetOneMissingKey(t *testing.T) {
	t.Parallel()

	app, _ := mountSystemplaneForCRUDTest(t, false)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/system/matcher/no.such.key", http.NoBody))
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestMountSystemplaneAPI_AuthDisabled_PutThenGetRoundtrip asserts the
// full write → read path: PUT updates a registered key, subsequent GET
// reflects the new value. Exercises the two CRUD verbs that console
// runtime changes depend on (READ is covered by the list/get tests above).
func TestMountSystemplaneAPI_AuthDisabled_PutThenGetRoundtrip(t *testing.T) {
	t.Parallel()

	app, _ := mountSystemplaneForCRUDTest(t, false)

	// PUT a new value for a registered int key. The systemplane client
	// round-trips integers through JSON (they come back as float64) so
	// the GET assertion uses a numeric comparison tolerant of that.
	putBody := `{"value": 4242}`

	putReq := httptest.NewRequest(http.MethodPut,
		"/system/matcher/rate_limit.max",
		strings.NewReader(putBody))
	putReq.Header.Set("Content-Type", "application/json")

	putResp, err := app.Test(putReq)
	require.NoError(t, err)
	_ = putResp.Body.Close()

	require.Equal(t, http.StatusNoContent, putResp.StatusCode,
		"PUT must return 204 No Content on success")

	// GET must reflect the new value.
	getResp, err := app.Test(httptest.NewRequest(http.MethodGet,
		"/system/matcher/rate_limit.max", http.NoBody))
	require.NoError(t, err)

	body := readBody(t, getResp)

	require.Equal(t, http.StatusOK, getResp.StatusCode, "post-PUT GET must return 200: %s", body)

	var decoded struct {
		Namespace string  `json:"namespace"`
		Key       string  `json:"key"`
		Value     float64 `json:"value"` // JSON numbers decode as float64
	}

	require.NoError(t, json.Unmarshal([]byte(body), &decoded))
	assert.Equal(t, float64(4242), decoded.Value,
		"PUT value must be visible on subsequent GET")
}

// TestMountSystemplaneAPI_AuthDisabled_PutUnknownKeyRejected asserts PUT
// to an unregistered key returns 400, not 500 or silent success. This is
// the drift guard: if the key constant list in systemplane_keys.go is
// mismatched with the console's known keys, PUTs fail loudly.
func TestMountSystemplaneAPI_AuthDisabled_PutUnknownKeyRejected(t *testing.T) {
	t.Parallel()

	app, _ := mountSystemplaneForCRUDTest(t, false)

	putReq := httptest.NewRequest(http.MethodPut,
		"/system/matcher/totally.unregistered",
		strings.NewReader(`{"value": 1}`))
	putReq.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(putReq)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"PUT on unknown key must be rejected with 400, not 500")
}
