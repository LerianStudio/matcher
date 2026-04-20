// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// ErrProtectedRouterUnsupportedMethod is the sentinel returned when a caller
// invokes a fiber.Router method on protectedRouter that is not a terminal
// HTTP verb (e.g. .Group, .Use, .Route, .Mount, .Name, .Static). Surfacing
// a sentinel keeps startup diagnostics wrappable by errors.Is and avoids the
// err113 lint ban on dynamic errors at call sites.
var ErrProtectedRouterUnsupportedMethod = errors.New("protectedRouter: unsupported method")

// protectedRouter is a thin fiber.Router implementation that prepends a
// fixed auth chain and shared chain to every registered route, then calls
// the corresponding app method directly (app.Get, app.Post, ...).
//
// This replaces the previous pattern where routes.Protected(resource, actions...)
// returned app.Group("/", handlers...). In Fiber v2, app.Group("/", handlers...)
// is implemented as app.register(methodUse, "/", ..., handlers...) — i.e. each
// handler becomes an app-level USE entry matching every path. Calling
// routes.Protected 114 times (once per bounded-context route registration)
// installed the shared chain 114 times on the app, so a single request ran
// every handler 114 times: 114 Redis key lookups for idempotency, 114 rate
// limit counter increments, and — when auth is enabled — 114 Authorize
// checks with 114 DIFFERENT (resource, action) pairs, effectively requiring
// every caller to hold every permission defined anywhere in the app.
//
// protectedRouter fixes this by registering each route directly on the app
// with the composed chain, so each middleware runs exactly once per matching
// request, scoped to the specific route.
//
// protectedRouter is intended as a TERMINAL registration surface: callers
// are expected to invoke the HTTP verb methods (.Get/.Post/.Put/.Patch/
// .Delete/.Head/.Options/.Trace/.Connect/.All/.Add) and nothing else. The
// router-shaping methods (.Use/.Group/.Route/.Mount/.Name/.Static) are
// unsupported because they would either silently re-introduce the
// app-global USE stacking bug (.Use / .Group at "/") or mutate the
// registration surface in ways the auth chain cannot honor (.Route nests
// a sub-router with its own handlers; .Mount installs another app's
// router onto the parent; .Name tags the last registered route; .Static
// installs a filesystem handler outside the protected chain). Calling any
// of these records a startup registration error so the server refuses to
// start with an invalid route graph, instead of panicking at request time.
type protectedRouter struct {
	app         *fiber.App
	authChain   []fiber.Handler
	sharedChain []fiber.Handler

	// recordErr is set by the constructing Routes so unsupported-method
	// calls surface via the same registrationErrs path that
	// ProtectedGroupWithActionsWithMiddleware failures use. Kept as a
	// callback so protectedRouter stays decoupled from the Routes struct
	// and testable in isolation.
	recordErr func(err error)

	// logger is used to record the unsupported-method calls at warn level
	// in addition to surfacing them through recordErr. Nil-safe.
	logger libLog.Logger

	// label is a human-readable identifier (resource + actions) included in
	// unsupported-method error messages so startup logs can point callers
	// at the exact protected(...) invocation that tried to use .Group etc.
	label string
}

// compose returns the full handler chain for a single route registration:
// auth chain (validateTenantClaims, Authorize per action, ExtractTenant) +
// shared chain (WhenEnabled(tenantDB), idempotency, globalRateLimit) +
// the user-supplied handlers. The result is a NEW slice — callers mutate
// at their peril, but Fiber's route registration copies the slice so
// ownership does not leak back across calls.
func (pr *protectedRouter) compose(userHandlers ...fiber.Handler) []fiber.Handler {
	total := len(pr.authChain) + len(pr.sharedChain) + len(userHandlers)
	chain := make([]fiber.Handler, 0, total)
	chain = append(chain, pr.authChain...)
	chain = append(chain, pr.sharedChain...)
	chain = append(chain, userHandlers...)

	return chain
}

// recordUnsupported surfaces a startup-time error describing an
// unsupported fiber.Router method call on protectedRouter. The error flows
// through the routes.registrationErrs channel, causing RegistrationErr()
// to fail non-nil after all modules finish registering — the server then
// refuses to start with a misconfigured route graph.
func (pr *protectedRouter) recordUnsupported(method string) {
	// The sentinel is wrapped with %w so callers can still use errors.Is
	// to detect this class of failure, while the wrapping context identifies
	// the specific method and the originating protected(...) invocation.
	err := fmt.Errorf(
		"%w: .%s called on %s — Protected returns a terminal "+
			"route-registration surface; register routes directly via "+
			".Get/.Head/.Post/.Put/.Delete/.Patch/.Options/.Trace/.Connect/.All/.Add",
		ErrProtectedRouterUnsupportedMethod, method, pr.label,
	)

	if pr.logger != nil {
		pr.logger.Log(context.Background(), libLog.LevelError,
			"unsupported protectedRouter method", libLog.String("method", method), libLog.String("label", pr.label))
	}

	if pr.recordErr != nil {
		pr.recordErr(err)
	}
}

// HTTP verb methods. Each composes the chain and delegates to app.

// Get registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Get(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Get(path, pr.compose(handlers...)...)

	return pr
}

// Head registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Head(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Head(path, pr.compose(handlers...)...)

	return pr
}

// Post registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Post(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Post(path, pr.compose(handlers...)...)

	return pr
}

// Put registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Put(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Put(path, pr.compose(handlers...)...)

	return pr
}

// Delete registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Delete(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Delete(path, pr.compose(handlers...)...)

	return pr
}

// Connect registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Connect(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Connect(path, pr.compose(handlers...)...)

	return pr
}

// Options registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Options(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Options(path, pr.compose(handlers...)...)

	return pr
}

// Trace registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Trace(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Trace(path, pr.compose(handlers...)...)

	return pr
}

// Patch registers a route with the composed chain on the underlying app.
func (pr *protectedRouter) Patch(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Patch(path, pr.compose(handlers...)...)

	return pr
}

// Add registers a route for an arbitrary method with the composed chain.
func (pr *protectedRouter) Add(method, path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.Add(method, path, pr.compose(handlers...)...)

	return pr
}

// All registers a route matching every HTTP method with the composed chain.
func (pr *protectedRouter) All(path string, handlers ...fiber.Handler) fiber.Router {
	pr.app.All(path, pr.compose(handlers...)...)

	return pr
}

// Unsupported router-shaping methods follow. Each records an error via
// recordErr and returns the receiver so chained calls do not nil-dereference.

// Use is not supported: it would install the composed chain as an app-global
// USE entry, re-introducing the 114x stacking bug this type exists to avoid.
func (pr *protectedRouter) Use(_ ...any) fiber.Router {
	pr.recordUnsupported("Use")

	return pr
}

// Group is not supported: app.Group("/", handlers...) is implemented as
// app.Use("/", handlers...) in Fiber v2, which would re-introduce the 114x
// stacking bug this type exists to avoid.
func (pr *protectedRouter) Group(_ string, _ ...fiber.Handler) fiber.Router {
	pr.recordUnsupported("Group")

	return pr
}

// Route is not supported: it nests a sub-router with its own handler chain
// that would either duplicate or bypass the composed auth chain, neither of
// which is safe for a protected route.
func (pr *protectedRouter) Route(_ string, _ func(router fiber.Router), _ ...string) fiber.Router {
	pr.recordUnsupported("Route")

	return pr
}

// Mount is not supported: it installs another app's router onto the parent,
// which would escape the protected chain entirely.
func (pr *protectedRouter) Mount(_ string, _ *fiber.App) fiber.Router {
	pr.recordUnsupported("Mount")

	return pr
}

// Name is not supported: Fiber's Name tags the LAST REGISTERED route on the
// underlying router, and protectedRouter does not expose a stable "last
// route" abstraction (each verb call registers on the parent app). Silently
// tagging the wrong route would be a subtle observability bug.
func (pr *protectedRouter) Name(_ string) fiber.Router {
	pr.recordUnsupported("Name")

	return pr
}

// Static is not supported: it installs a filesystem handler that bypasses
// the composed auth/shared chains. Serving static assets behind an RBAC
// check must be done via an explicit Get route with a handler that reads
// the file system.
func (pr *protectedRouter) Static(_, _ string, _ ...fiber.Static) fiber.Router {
	pr.recordUnsupported("Static")

	return pr
}
