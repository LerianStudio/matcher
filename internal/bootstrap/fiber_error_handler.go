// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
)

func customErrorHandlerWithEnv(logger libLog.Logger, envName string) fiber.ErrorHandler {
	isProduction := IsProductionEnvironment(envName)

	return func(fiberCtx *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError

		var fe *fiber.Error
		if errors.As(err, &fe) {
			code = fe.Code
		}

		if logger != nil {
			reqCtx := fiberCtx.UserContext()
			if reqCtx == nil {
				reqCtx = context.Background()
			}

			if isProduction {
				// In production, log sanitized error details to avoid leaking PII
				logger.Log(reqCtx, libLog.LevelError, fmt.Sprintf(
					"HTTP error: status=%d path=%s method=%s",
					code,
					fiberCtx.Path(),
					fiberCtx.Method(),
				))
			} else {
				// In non-production, sanitize error message to prevent secret leakage
				sanitizedErr := sanitizeErrorForLogging(err)
				logger.Log(reqCtx, libLog.LevelError, fmt.Sprintf("HTTP error: status=%d error=%s path=%s method=%s", code, sanitizedErr, fiberCtx.Path(), fiberCtx.Method()))
			}
		}

		title := "internal_error"
		message := "internal server error"

		if code < fiber.StatusInternalServerError {
			title = clientErrorMessageForStatus(code)
			message = title
		}

		return sharedhttp.RespondError(fiberCtx, code, title, message)
	}
}

// sanitizeErrorForLogging redacts potential secrets from error messages.
// Matches common patterns for passwords, tokens, keys, and connection strings.
// Matching is case-insensitive so that "Password=", "PASSWORD=", and "password="
// are all caught by a single canonical (lower-case) pattern.
func sanitizeErrorForLogging(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()
	msgLower := strings.ToLower(msg)

	// Patterns that may contain secrets (canonical lower-case form).
	patterns := []struct {
		pattern     string
		replacement string
	}{
		{"password=", "password=***REDACTED***"},
		{"secret=", "secret=***REDACTED***"},
		{"token=", "token=***REDACTED***"},
		{"api_key=", "api_key=***REDACTED***"},
		{"apikey=", "apikey=***REDACTED***"},
		{"bearer ", "Bearer ***REDACTED***"},
		{"basic ", "Basic ***REDACTED***"},
	}

	for _, pat := range patterns {
		// Repeatedly search-and-replace until no more occurrences remain.
		// Track offset to avoid infinite loop when replacement contains pattern.
		offset := 0

		for {
			idx := strings.Index(msgLower[offset:], pat.pattern)
			if idx == -1 {
				break
			}
			// Adjust idx to be relative to the full string
			idx += offset
			// Find end of value (space, quote, or end of string) using original msg
			endIdx := findValueEnd(msg, idx+len(pat.pattern))
			// Replace the slice in both msg and msgLower so future searches stay aligned
			msg = msg[:idx] + pat.replacement + msg[endIdx:]
			msgLower = msgLower[:idx] + strings.ToLower(pat.replacement) + msgLower[endIdx:]
			// Move offset past the replacement to avoid re-matching
			offset = idx + len(pat.replacement)
		}
	}

	return msg
}

// findValueEnd finds the end of a secret value in an error message.
func findValueEnd(msg string, start int) int {
	for i := start; i < len(msg); i++ {
		switch msg[i] {
		case ' ', '"', '\'', '\n', '\r', '\t', ';', '&':
			return i
		}
	}

	return len(msg)
}

func clientErrorMessageForStatus(code int) string {
	switch code {
	case fiber.StatusBadRequest:
		return "invalid_request"
	case fiber.StatusUnauthorized:
		return "unauthorized"
	case fiber.StatusForbidden:
		return "forbidden"
	case fiber.StatusNotFound:
		return "not_found"
	case fiber.StatusRequestEntityTooLarge:
		return "request_entity_too_large"
	default:
		return "request_failed"
	}
}

func sanitizeHeaderID(headerID string) string {
	trimmed := strings.TrimSpace(headerID)

	if trimmed == "" {
		return uuid.NewString()
	}

	if len(trimmed) > maxHeaderIDLength {
		return truncateHeaderID(trimmed)
	}

	for _, char := range trimmed {
		if !isSafeHeaderChar(char) {
			sanitized := strings.Map(func(r rune) rune {
				if !isSafeHeaderChar(r) {
					return -1
				}

				return r
			}, trimmed)

			if strings.TrimSpace(sanitized) == "" {
				return uuid.NewString()
			}

			if len(sanitized) > maxHeaderIDLength {
				return truncateHeaderID(sanitized)
			}

			return sanitized
		}
	}

	return trimmed
}

// isSafeHeaderChar returns true if the rune is safe for use in header IDs.
// Filters out non-printable characters and control characters that could
// be used for log injection attacks (\r, \n, \t, ;, |).
func isSafeHeaderChar(r rune) bool {
	if !unicode.IsPrint(r) {
		return false
	}

	switch r {
	case '\r', '\n', '\t', ';', '|':
		return false
	default:
		return true
	}
}

func truncateHeaderID(value string) string {
	runes := []rune(value)
	if len(runes) > maxHeaderIDLength {
		return string(runes[:maxHeaderIDLength])
	}

	return value
}
