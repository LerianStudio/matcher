// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"errors"
	"net/http"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// errorMapping maps domain sentinel errors to HTTP status codes and
// machine-readable error codes. The slice is scanned in order so more
// specific errors should appear before general ones.
var errorMapping = []struct {
	target error
	status int
	code   string
}{
	{domain.ErrKeyUnknown, http.StatusBadRequest, "system_key_unknown"},
	{domain.ErrValueInvalid, http.StatusBadRequest, "system_value_invalid"},
	{domain.ErrKeyNotMutable, http.StatusBadRequest, "system_key_not_mutable"},
	{domain.ErrScopeInvalid, http.StatusBadRequest, "system_scope_invalid"},
	{domain.ErrRevisionMismatch, http.StatusConflict, "system_revision_mismatch"},
	{domain.ErrPermissionDenied, http.StatusForbidden, "system_permission_denied"},
	{domain.ErrReloadFailed, http.StatusInternalServerError, "system_reload_failed"},
	{domain.ErrSupervisorStopped, http.StatusServiceUnavailable, "system_unavailable"},
}

// writeError maps a domain error to an HTTP error response. It scans the
// errorMapping table for a matching sentinel; unrecognized errors produce a
// generic 500 response that does not leak implementation details.
//
// For security-sensitive errors (permission denied, server errors), fixed
// messages are used instead of err.Error() to prevent leaking internal
// authorization model or wrapped error context.
func writeError(fiberCtx *fiber.Ctx, err error) error {
	for _, mapping := range errorMapping {
		if errors.Is(err, mapping.target) {
			msg := err.Error()

			// Use fixed messages for security-sensitive and server error codes.
			if mapping.status == http.StatusForbidden {
				msg = "permission denied"
			} else if mapping.status >= http.StatusInternalServerError {
				msg = mapping.code
			}

			return fiberCtx.Status(mapping.status).JSON(ErrorResponse{
				Code:    mapping.code,
				Message: msg,
			})
		}
	}

	return fiberCtx.Status(http.StatusInternalServerError).JSON(ErrorResponse{
		Code:    "system_internal_error",
		Message: "internal server error",
	})
}
