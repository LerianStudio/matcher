// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

// orderTracer records the order in which named middleware functions execute
// during a single request. Concurrent-safe because sync.Mutex guards the
// append; tests send one request at a time so contention is nil in practice.
type orderTracer struct {
	mu    sync.Mutex
	order []string
	count map[string]int
}

func newOrderTracer() *orderTracer {
	return &orderTracer{count: make(map[string]int)}
}

func (o *orderTracer) record(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.order = append(o.order, name)
	o.count[name]++
}

func (o *orderTracer) snapshot() ([]string, map[string]int) {
	o.mu.Lock()
	defer o.mu.Unlock()

	orderCopy := make([]string, len(o.order))
	copy(orderCopy, o.order)

	countCopy := make(map[string]int, len(o.count))
	for k, v := range o.count {
		countCopy[k] = v
	}

	return orderCopy, countCopy
}

// middlewareProbe returns a fiber.Handler that records its own name on the
// tracer and then calls c.Next(). Used to wrap real middleware where we only
// care about order/count, not behavior.
func middlewareProbe(tracer *orderTracer, name string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tracer.record(name)

		return c.Next()
	}
}

// newTestProtectedRouter wires a protectedRouter with caller-provided auth
// and shared chains. The underlying fiber.App is returned so tests can send
// requests and assert on the response.
func newTestProtectedRouter(t *testing.T, authChain, sharedChain []fiber.Handler) (*fiber.App, *protectedRouter) {
	t.Helper()

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	pr := &protectedRouter{
		app:         app,
		authChain:   authChain,
		sharedChain: sharedChain,
		logger:      &libLog.NopLogger{},
		label:       "resource=\"test\" actions=[read]",
		recordErr:   func(_ error) {},
	}

	return app, pr
}

// TestProtectedRouter_MiddlewareRunsExactlyOncePerRequest is the invariant
// test that catches the regression this refactor exists to prevent. A single
// registered route, a single request, and every middleware in the composed
// chain must record exactly ONE invocation. If a future refactor reverts to
// the old app.Group("/") pattern, every middleware will count 2+ (the
// previous app also had multiple Protected invocations globally) and this
// test fires loudly.
func TestProtectedRouter_MiddlewareRunsExactlyOncePerRequest(t *testing.T) {
	t.Parallel()

	tracer := newOrderTracer()

	authChain := []fiber.Handler{
		middlewareProbe(tracer, "validateTenantClaims"),
		middlewareProbe(tracer, "Authorize(read)"),
		middlewareProbe(tracer, "ExtractTenant"),
	}
	sharedChain := []fiber.Handler{
		middlewareProbe(tracer, "WhenEnabled(tenantDB)"),
		middlewareProbe(tracer, "idempotency"),
		middlewareProbe(tracer, "globalRateLimit"),
	}

	app, pr := newTestProtectedRouter(t, authChain, sharedChain)

	var handlerHits atomic.Int32

	pr.Get("/v1/widgets", func(c *fiber.Ctx) error {
		handlerHits.Add(1)
		tracer.record("user-handler")

		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/widgets", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck // test helper

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(1), handlerHits.Load(), "user handler must be called once")

	order, count := tracer.snapshot()

	// Exact count = 1 for every middleware. If Fiber ever silently
	// installs the handlers twice (e.g. via a future .Use reintroduction),
	// these assertions fire.
	expectedNames := []string{
		"validateTenantClaims",
		"Authorize(read)",
		"ExtractTenant",
		"WhenEnabled(tenantDB)",
		"idempotency",
		"globalRateLimit",
		"user-handler",
	}
	for _, name := range expectedNames {
		assert.Equalf(t, 1, count[name], "%q must run exactly once; got %d", name, count[name])
	}

	// Exact order: auth chain → shared chain → user handler.
	assert.Equal(t, expectedNames, order, "middleware execution order must match contract")
}

// TestProtectedRouter_MultipleRoutesShareComposedChainOnce pins the multi-route
// case. Registering several verbs on the SAME protectedRouter instance must
// not compound: each route only sees its own handlers + the shared chain +
// its own user handlers. This rules out a family of subtle bugs where
// successive .Get()/.Post() calls mutate the router's internal slice and end
// up double-installing the shared chain on later registrations.
func TestProtectedRouter_MultipleRoutesShareComposedChainOnce(t *testing.T) {
	t.Parallel()

	tracer := newOrderTracer()

	authChain := []fiber.Handler{middlewareProbe(tracer, "auth")}
	sharedChain := []fiber.Handler{middlewareProbe(tracer, "shared")}

	app, pr := newTestProtectedRouter(t, authChain, sharedChain)

	pr.Get("/a", func(c *fiber.Ctx) error {
		tracer.record("handler:a")
		return c.SendStatus(http.StatusOK)
	})
	pr.Get("/b", func(c *fiber.Ctx) error {
		tracer.record("handler:b")
		return c.SendStatus(http.StatusOK)
	})
	pr.Post("/c", func(c *fiber.Ctx) error {
		tracer.record("handler:c")
		return c.SendStatus(http.StatusOK)
	})

	// Hit /a and verify a single auth + shared + handler:a.
	respA, err := app.Test(httptest.NewRequest(http.MethodGet, "/a", http.NoBody))
	require.NoError(t, err)
	defer respA.Body.Close() //nolint:errcheck // test helper

	assert.Equal(t, http.StatusOK, respA.StatusCode)

	orderA, countA := tracer.snapshot()
	assert.Equal(t, []string{"auth", "shared", "handler:a"}, orderA)
	assert.Equal(t, 1, countA["auth"])
	assert.Equal(t, 1, countA["shared"])

	// Hit /b — the running tracer is shared, so counts accumulate. Just
	// assert the delta: one more of each.
	respB, err := app.Test(httptest.NewRequest(http.MethodGet, "/b", http.NoBody))
	require.NoError(t, err)
	defer respB.Body.Close() //nolint:errcheck // test helper

	assert.Equal(t, http.StatusOK, respB.StatusCode)

	_, countAB := tracer.snapshot()
	assert.Equal(t, 2, countAB["auth"], "two requests: auth runs twice total (once per request)")
	assert.Equal(t, 2, countAB["shared"], "two requests: shared runs twice total (once per request)")
	assert.Equal(t, 1, countAB["handler:a"])
	assert.Equal(t, 1, countAB["handler:b"])

	// Hit /c POST.
	respC, err := app.Test(httptest.NewRequest(http.MethodPost, "/c", http.NoBody))
	require.NoError(t, err)
	defer respC.Body.Close() //nolint:errcheck // test helper

	assert.Equal(t, http.StatusOK, respC.StatusCode)

	_, countABC := tracer.snapshot()
	assert.Equal(t, 3, countABC["auth"])
	assert.Equal(t, 3, countABC["shared"])
	assert.Equal(t, 1, countABC["handler:c"])
}

// TestProtectedRouter_DenialShortCircuits pins the most security-critical
// property of the chain: a failing Authorize handler MUST return its error
// (4xx) to the client WITHOUT running any later middleware — specifically,
// not ExtractTenant, not the shared chain, not the user handler. If a future
// refactor accidentally orders the shared chain BEFORE Authorize, an
// unauthenticated request would consume rate-limit budget and poison
// idempotency lookups belonging to a legitimate tenant. This test makes
// that regression impossible to merge silently.
func TestProtectedRouter_DenialShortCircuits(t *testing.T) {
	t.Parallel()

	tracer := newOrderTracer()

	// Authorize(read) denies. validateTenantClaims runs first (records) and
	// then Authorize returns a 403 error — Fiber propagates that back up the
	// middleware chain, so anything registered AFTER Authorize MUST NOT run.
	authChain := []fiber.Handler{
		middlewareProbe(tracer, "validateTenantClaims"),
		func(c *fiber.Ctx) error {
			tracer.record("Authorize(read):denying")

			return fiber.NewError(http.StatusForbidden, "denied")
		},
		// If the Authorize denial does not short-circuit, ExtractTenant
		// records its name here and the test assertion below fires.
		middlewareProbe(tracer, "ExtractTenant"),
	}
	sharedChain := []fiber.Handler{
		middlewareProbe(tracer, "WhenEnabled(tenantDB)"),
		middlewareProbe(tracer, "idempotency"),
		middlewareProbe(tracer, "globalRateLimit"),
	}

	app, pr := newTestProtectedRouter(t, authChain, sharedChain)

	var handlerHits atomic.Int32

	pr.Get("/v1/forbidden", func(c *fiber.Ctx) error {
		handlerHits.Add(1)
		tracer.record("user-handler")

		return c.SendStatus(http.StatusOK)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/forbidden", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck // test helper

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, int32(0), handlerHits.Load(), "user handler must NOT run on Authorize denial")

	order, count := tracer.snapshot()

	// validateTenantClaims and Authorize(read) record before Authorize
	// returns its error.
	assert.Equal(t, []string{"validateTenantClaims", "Authorize(read):denying"}, order,
		"only pre-Authorize middleware runs on denial")

	// Everything after the denial must be count=0.
	for _, name := range []string{
		"ExtractTenant",
		"WhenEnabled(tenantDB)",
		"idempotency",
		"globalRateLimit",
		"user-handler",
	} {
		assert.Equalf(t, 0, count[name], "%q must NOT run after Authorize denial", name)
	}
}

// TestProtectedRouter_UnsupportedMethodsRecordError verifies the fiber.Router
// methods that cannot safely be expressed on a protectedRouter record an
// error via recordErr so RegistrationErr fails at startup, and that they
// return the router itself so chained calls do not nil-dereference.
func TestProtectedRouter_UnsupportedMethodsRecordError(t *testing.T) {
	t.Parallel()

	var recorded []error

	app := fiber.New()
	pr := &protectedRouter{
		app:       app,
		logger:    &libLog.NopLogger{},
		label:     `resource="x" actions=[read]`,
		recordErr: func(e error) { recorded = append(recorded, e) },
	}

	cases := []struct {
		name string
		fn   func()
	}{
		{"Use", func() { pr.Use(func(c *fiber.Ctx) error { return c.Next() }) }},
		{"Group", func() { pr.Group("/x") }},
		{"Route", func() { pr.Route("/x", func(_ fiber.Router) {}) }},
		{"Mount", func() { pr.Mount("/x", fiber.New()) }},
		{"Name", func() { pr.Name("tag") }},
		{"Static", func() { pr.Static("/x", "./x") }},
	}

	for _, tc := range cases {
		tc.fn()
	}

	require.Len(t, recorded, len(cases), "one error recorded per unsupported-method call")

	for i, tc := range cases {
		// Each recorded error wraps ErrProtectedRouterUnsupportedMethod so
		// callers can detect the class via errors.Is, and the context
		// identifies the specific method (".Use", ".Group", ...) and the
		// originating protected(...) invocation label.
		require.ErrorIs(t, recorded[i], ErrProtectedRouterUnsupportedMethod,
			"recorded error must wrap ErrProtectedRouterUnsupportedMethod")
		assert.Contains(t, recorded[i].Error(), "."+tc.name+" called",
			"error message must identify the unsupported method")
		assert.Contains(t, recorded[i].Error(), pr.label,
			"error message must identify the offending protected(...) invocation")
	}
}

// TestProtectedRouter_UnsupportedMethodsReturnSelf ensures the no-op
// router returned by the unsupported-method handlers is the SAME
// protectedRouter instance, so fluent chains like pr.Group("/").Get(...)
// still surface the Get error via recordErr instead of panicking on a nil
// result.
func TestProtectedRouter_UnsupportedMethodsReturnSelf(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	pr := &protectedRouter{
		app:       app,
		logger:    &libLog.NopLogger{},
		label:     `resource="x" actions=[read]`,
		recordErr: func(_ error) {},
	}

	assert.Equal(t, fiber.Router(pr), pr.Use())
	assert.Equal(t, fiber.Router(pr), pr.Group("/x"))
	assert.Equal(t, fiber.Router(pr), pr.Route("/x", func(_ fiber.Router) {}))
	assert.Equal(t, fiber.Router(pr), pr.Mount("/x", fiber.New()))
	assert.Equal(t, fiber.Router(pr), pr.Name("tag"))
	assert.Equal(t, fiber.Router(pr), pr.Static("/x", "./x"))
}

// TestProtectedRouter_VerbMethodsReturnSelf ensures a chained fluent call
// pattern like pr.Get("/a", ...).Get("/b", ...) works — Fiber's
// fiber.Router interface returns fiber.Router from every verb method, so
// breaking this would be subtle and easy to miss.
func TestProtectedRouter_VerbMethodsReturnSelf(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	pr := &protectedRouter{
		app:       app,
		authChain: []fiber.Handler{},
		sharedChain: []fiber.Handler{
			func(c *fiber.Ctx) error { return c.Next() },
		},
		logger:    &libLog.NopLogger{},
		label:     "test",
		recordErr: func(_ error) {},
	}

	noop := func(c *fiber.Ctx) error { return c.SendStatus(http.StatusOK) }

	chained := pr.Get("/a", noop).
		Head("/b", noop).
		Post("/c", noop).
		Put("/d", noop).
		Delete("/e", noop).
		Connect("/f", noop).
		Options("/g", noop).
		Trace("/h", noop).
		Patch("/i", noop).
		Add(http.MethodGet, "/j", noop).
		All("/k", noop)

	assert.Equal(t, fiber.Router(pr), chained, "chained verb calls must return the same router")
}

// TestProtectedRouter_ErrorSurface_FromRegistrationClosure is an
// integration-flavored test that wires the real closure from
// RegisterRoutes against a *Routes and verifies the no-op-router branch:
// when BuildProtectedAuthChain fails (empty actions), a stub
// protectedRouter is returned, downstream .Get/.Post record no-op, and
// RegistrationErr surfaces the failure after all registrations complete.
func TestProtectedRouter_ErrorSurface_FromRegistrationClosure(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "test"}}

	client := authMiddleware.NewAuthClient("", false, nil)
	extractor, err := auth.NewTenantExtractor(false, false, "11111111-1111-1111-1111-111111111111", "default", "", "test")
	require.NoError(t, err)

	routes, err := RegisterRoutes(app, cfg, nil, nil, nil, nil, &libLog.NopLogger{}, client, extractor, nil, nil, nil)
	require.NoError(t, err)

	// Provoke an ErrNoActions by passing zero actions.
	stub := routes.Protected("resource")
	require.NotNil(t, stub)

	// Chained Get must not panic — it returns the same stub router.
	stub.Get("/never-reachable", func(c *fiber.Ctx) error { return c.SendStatus(http.StatusOK) })

	require.Error(t, routes.RegistrationErr(),
		"ErrNoActions must surface via RegistrationErr")
	assert.Contains(t, routes.RegistrationErr().Error(), "protected route registration failed")
}
