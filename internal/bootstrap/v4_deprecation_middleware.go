// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"strings"

	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// v4SystemplanePathPrefix identifies the deprecated v4 admin API surface
// rooted at /v1/system/configs[...] and /v1/system/settings[...]. The v5
// migration removed every handler behind these prefixes in favour of the
// canonical lib-commons layout under /system/:namespace[/:key].
const v4SystemplanePathPrefix = "/v1/system/"

// v4DeprecationRemovalTarget is the cutover message operators see in the
// 410 response body, giving them an explicit window to migrate. Update in
// lockstep with docs/PROJECT_RULES.md once the shim is scheduled for
// removal. Intentionally stored as a string rather than a time so the
// response body renders deterministically in contract tests.
const v4DeprecationRemovalTarget = "scheduled for removal four weeks after 2026-04-18 cutover"

// v4DeprecationHint directs clients to the canonical v5 admin API. The
// copy mirrors the exact route shape so console migrations can search for
// it mechanically.
const v4DeprecationHint = "Use PUT /system/matcher/:key (and GET /system/matcher[/:key])"

// v4DeprecationResponseBody is the canonical JSON shape returned for every
// 410 Gone response from the shim.
type v4DeprecationResponseBody struct {
	Code    string `json:"code"`
	Title   string `json:"title"`
	Message string `json:"message"`
	Hint    string `json:"hint"`
	Removal string `json:"removal"`
}

// v4DeprecationShim returns a Fiber middleware that short-circuits every
// request targeting the removed v4 admin API with a 410 Gone response.
// The shim is intentionally short-lived: v5 lands as a hard cutover with
// no deprecation window on the server side; the shim exists only to give
// operators a clear, discoverable error while they migrate stale tooling
// (health-checks, CI smoke tests, console pins) off the v4 paths. Remove
// the middleware once the removal target passes.
//
// Every intercepted call emits a WARN log with the caller's remote IP
// and User-Agent so ops can identify who is still hitting the deprecated
// prefix. The warn is intentionally lightweight — callers that hit this
// path have already accepted their routing is broken; we just want a
// record to drive follow-up conversations.
func v4DeprecationShim(logger libLog.Logger) fiber.Handler {
	body := v4DeprecationResponseBody{
		Code:    "GONE",
		Title:   "endpoint_removed",
		Message: "v4 admin API paths removed. " + v4DeprecationHint,
		Hint:    v4DeprecationHint,
		Removal: v4DeprecationRemovalTarget,
	}

	return func(fiberCtx *fiber.Ctx) error {
		path := fiberCtx.Path()

		if !isV4SystemplanePath(path) {
			return fiberCtx.Next()
		}

		if logger != nil {
			reqCtx := fiberCtx.UserContext()

			logger.Log(
				reqCtx,
				libLog.LevelWarn,
				"v4 admin API hit after removal; returning 410 Gone",
				libLog.String("http.path", path),
				libLog.String("http.method", fiberCtx.Method()),
				libLog.String("client.remote_ip", fiberCtx.IP()),
				libLog.String("client.user_agent", string(fiberCtx.Request().Header.UserAgent())),
			)
		}

		//nolint:wrapcheck // Fiber's JSON is the terminal response boundary.
		return fiberCtx.Status(fiber.StatusGone).JSON(body)
	}
}

// isV4SystemplanePath is true for any /v1/system/configs[...] or
// /v1/system/settings[...] URL. Kept separate from the handler so the
// contract test can assert coverage of the documented path set without
// spinning up a Fiber app.
func isV4SystemplanePath(path string) bool {
	if !strings.HasPrefix(path, v4SystemplanePathPrefix) {
		return false
	}

	remainder := strings.TrimPrefix(path, v4SystemplanePathPrefix)

	switch {
	case remainder == "configs",
		remainder == "settings",
		strings.HasPrefix(remainder, "configs/"),
		strings.HasPrefix(remainder, "settings/"):
		return true
	default:
		return false
	}
}
