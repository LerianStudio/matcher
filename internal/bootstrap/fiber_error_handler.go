// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	matcherAuth "github.com/LerianStudio/matcher/internal/auth"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	matchererrors "github.com/LerianStudio/matcher/pkg"
	"github.com/LerianStudio/matcher/pkg/constant"
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

		return respondFallbackError(fiberCtx, err)
	}
}

//nolint:wrapcheck // HTTP transport response is the terminal error boundary.
func respondFallbackError(fiberCtx *fiber.Ctx, err error) error {
	if apiError, ok := authFallbackError(err); ok {
		return sharedhttp.RespondProductError(fiberCtx, apiError)
	}

	return sharedhttp.RespondProductError(fiberCtx, sharedhttp.ValidateFallbackError(err))
}

func authFallbackError(err error) (matchererrors.APIError, bool) {
	var fiberErr *fiber.Error
	if !errors.As(err, &fiberErr) || !isSafeAuthFallbackMessage(fiberErr.Message) {
		return nil, false
	}

	var definition matchererrors.Definition

	switch fiberErr.Code {
	case fiber.StatusUnauthorized:
		definition = matchererrors.Definition{Code: constant.CodeUnauthorized, Title: http.StatusText(http.StatusUnauthorized), HTTPStatus: http.StatusUnauthorized}
	case fiber.StatusForbidden:
		definition = matchererrors.Definition{Code: constant.CodeForbidden, Title: http.StatusText(http.StatusForbidden), HTTPStatus: http.StatusForbidden}
	case fiber.StatusInternalServerError:
		definition = matchererrors.Definition{Code: constant.CodeInternalServerError, Title: http.StatusText(http.StatusInternalServerError), HTTPStatus: http.StatusInternalServerError}
	default:
		return nil, false
	}

	return matchererrors.NewError(definition, fiberErr.Message, nil, err), true
}

func isSafeAuthFallbackMessage(message string) bool {
	switch message {
	case matcherAuth.ErrMissingToken.Error(), matcherAuth.ErrInvalidToken.Error(),
		"tenant claim required",
		"authentication service unavailable",
		"tenant extractor not initialized",
		"auth client not initialized":
		return true
	default:
		return false
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

func sanitizeHeaderID(headerID string) string {
	trimmed := strings.TrimSpace(headerID)

	if trimmed == "" {
		return uuid.NewString()
	}

	// Always sanitize unsafe characters first, before any truncation.
	// This prevents control characters (\r, \n, ;) from surviving inside
	// the first maxHeaderIDLength runes of an overlong input.
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
