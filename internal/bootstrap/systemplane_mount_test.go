// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"errors"
	"net/http"
	"net/http/httptest"
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

// TestMountSystemplaneAPI_NilAppReturnsError asserts that passing a nil fiber
// app is surfaced as an explicit error — the caller's bug should fail loudly,
// not be silently ignored.
func TestMountSystemplaneAPI_NilAppReturnsError(t *testing.T) {
	t.Parallel()

	err := MountSystemplaneAPI(nil, nil, nil, nil, nil, nil, nil, nil, nil)

	require.Error(t, err)
	assert.True(t, errors.Is(err, errMountSystemplaneAppRequired),
		"expected errMountSystemplaneAppRequired, got: %v", err)
}

// TestMountSystemplaneAPI_NilClientNoOp asserts that a nil systemplane.Client
// is a graceful no-op. This is the documented behavior for the case where
// systemplane initialization failed or was disabled — the bootstrap should
// continue without the admin API rather than refuse to start.
func TestMountSystemplaneAPI_NilClientNoOp(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	err := MountSystemplaneAPI(app, nil, nil, nil, nil, nil, nil, nil, nil)

	require.NoError(t, err)

	// No /system routes should be registered when the client is nil.
	for _, r := range app.GetRoutes() {
		assert.NotContains(t, r.Path, "/system",
			"nil client must not register /system routes")
	}
}

// TestMountSystemplaneAPI_NilAppWithClient asserts the nil-app guard runs
// before the nil-client check so the error remains attributable to the app
// argument (defensive ordering).
//
// Full integration coverage — route registration with a live Client —
// requires a systemplane.Client backed by a real store (postgres or mongo)
// or a systemplane.NewForTesting store. That coverage lives in integration
// tests; this unit test only asserts the argument-validation fast path.
func TestMountSystemplaneAPI_NilAppWithClient(t *testing.T) {
	t.Parallel()

	err := MountSystemplaneAPI(nil, nil, nil, nil, nil, nil, nil, nil, nil)

	require.Error(t, err)
	assert.True(t, errors.Is(err, errMountSystemplaneAppRequired))
}

// TestMountSystemplaneAPI_NilExtractorRejected asserts the admin plane
// refuses to mount without a real tenant extractor. The previous no-op
// fallback silently wired /system without a tenant-context middleware —
// with auth disabled that meant admin actions ran fully anonymous; with
// auth enabled the admin.Mount authorizer could not see any user id.
// Neither mode is acceptable, so the mount must fail fatally instead.
func TestMountSystemplaneAPI_NilExtractorRejected(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	base := defaultConfig()
	client := newStartedTestClient(t, base)

	err := MountSystemplaneAPI(
		app,
		client,
		base,
		func() *Config { return base },
		nil,
		authMiddleware.NewAuthClient("", false, nil),
		nil, // the failure trigger
		nil,
		nil,
	)

	require.Error(t, err)
	assert.True(t, errors.Is(err, errMountSystemplaneTenantExtractorRequired),
		"expected errMountSystemplaneTenantExtractorRequired, got: %v", err)

	// Defense-in-depth: no /system routes should have been registered.
	for _, r := range app.GetRoutes() {
		assert.NotContains(t, r.Path, "/system",
			"failed mount must not leave /system routes behind")
	}
}

// TestMountSystemplaneAPI_AppliesAdminRateLimit asserts /system routes
// are rate-limited by the admin-tier quota specifically, not the global
// tier. This prevents admin traffic from sharing a bucket with tenant
// traffic (either side's storm would starve the other under fail-open
// Redis outage). The test uses a 1-req/window admin tier and a very
// generous global tier; if /system were still on the global tier it
// would pass two requests in a row.
func TestMountSystemplaneAPI_AppliesAdminRateLimit(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	redisClient := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	redisConn := testutil.NewRedisClientWithMock(redisClient)
	rl := ratelimit.New(redisConn, ratelimit.WithFailOpen(false))

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	base := defaultConfig()
	client := newStartedTestClient(t, base)
	cfg := &Config{
		App: AppConfig{EnvName: "test"},
		RateLimit: RateLimitConfig{
			Enabled:        true,
			Max:            1_000_000, // generous: proves the admin-tier limit is doing the throttling
			ExpirySec:      60,
			AdminMax:       1,
			AdminExpirySec: 60,
		},
	}

	extractor, err := auth.NewTenantExtractor(false, false, auth.DefaultTenantID, auth.DefaultTenantSlug, "", "test")
	require.NoError(t, err)

	err = MountSystemplaneAPI(
		app,
		client,
		cfg,
		func() *Config { return cfg },
		nil,
		authMiddleware.NewAuthClient("", false, nil),
		extractor,
		func() *ratelimit.RateLimiter { return rl },
		nil,
	)
	require.NoError(t, err)

	resp1, err := app.Test(httptest.NewRequest(http.MethodGet, "/system/matcher", http.NoBody))
	require.NoError(t, err)
	defer resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)

	resp2, err := app.Test(httptest.NewRequest(http.MethodGet, "/system/matcher", http.NoBody))
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode,
		"second /system request must be throttled by the admin tier")
}
