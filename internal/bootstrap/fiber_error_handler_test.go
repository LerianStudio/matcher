//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

// --- customErrorHandlerWithEnv ---

func TestCustomErrorHandlerWithEnv_InternalError_Production(t *testing.T) {
	t.Parallel()

	handler := customErrorHandlerWithEnv(&libLog.NopLogger{}, "production")

	app := fiber.New(fiber.Config{ErrorHandler: handler})
	app.Get("/fail", func(_ *fiber.Ctx) error {
		return errors.New("unexpected failure")
	})

	req := httptest.NewRequest("GET", "/fail", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestCustomErrorHandlerWithEnv_InternalError_Development(t *testing.T) {
	t.Parallel()

	handler := customErrorHandlerWithEnv(&libLog.NopLogger{}, "development")

	app := fiber.New(fiber.Config{ErrorHandler: handler})
	app.Get("/fail", func(_ *fiber.Ctx) error {
		return errors.New("something broke")
	})

	req := httptest.NewRequest("GET", "/fail", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestCustomErrorHandlerWithEnv_ClientError(t *testing.T) {
	t.Parallel()

	handler := customErrorHandlerWithEnv(&libLog.NopLogger{}, "development")

	app := fiber.New(fiber.Config{ErrorHandler: handler})
	app.Get("/notfound", func(_ *fiber.Ctx) error {
		return fiber.NewError(fiber.StatusNotFound, "page not found")
	})

	req := httptest.NewRequest("GET", "/notfound", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestCustomErrorHandlerWithEnv_NilLogger(t *testing.T) {
	t.Parallel()

	handler := customErrorHandlerWithEnv(nil, "development")

	app := fiber.New(fiber.Config{ErrorHandler: handler})
	app.Get("/fail", func(_ *fiber.Ctx) error {
		return errors.New("no logger")
	})

	req := httptest.NewRequest("GET", "/fail", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	// Must not panic with nil logger.
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestCustomErrorHandlerWithEnv_FiberError_BadRequest(t *testing.T) {
	t.Parallel()

	handler := customErrorHandlerWithEnv(&libLog.NopLogger{}, "development")

	app := fiber.New(fiber.Config{ErrorHandler: handler})
	app.Get("/bad", func(_ *fiber.Ctx) error {
		return fiber.NewError(fiber.StatusBadRequest, "invalid input")
	})

	req := httptest.NewRequest("GET", "/bad", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "invalid_request")
}

func TestCustomErrorHandlerWithEnv_FiberError_Unauthorized(t *testing.T) {
	t.Parallel()

	handler := customErrorHandlerWithEnv(&libLog.NopLogger{}, "development")

	app := fiber.New(fiber.Config{ErrorHandler: handler})
	app.Get("/auth", func(_ *fiber.Ctx) error {
		return fiber.NewError(fiber.StatusUnauthorized, "no token")
	})

	req := httptest.NewRequest("GET", "/auth", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
}

// --- sanitizeErrorForLogging ---

func TestSanitizeErrorForLogging_NilError(t *testing.T) {
	t.Parallel()

	result := sanitizeErrorForLogging(nil)

	assert.Equal(t, "", result)
}

func TestSanitizeErrorForLogging_NoSecrets(t *testing.T) {
	t.Parallel()

	err := errors.New("connection refused to port 5432")
	result := sanitizeErrorForLogging(err)

	assert.Equal(t, "connection refused to port 5432", result)
}

func TestSanitizeErrorForLogging_RedactsPassword(t *testing.T) {
	t.Parallel()

	err := errors.New("DSN: host=localhost password=mysecret dbname=test")
	result := sanitizeErrorForLogging(err)

	assert.NotContains(t, result, "mysecret")
	assert.Contains(t, result, "***REDACTED***")
}

func TestSanitizeErrorForLogging_RedactsToken(t *testing.T) {
	t.Parallel()

	err := errors.New("auth failed: token=abc123def456")
	result := sanitizeErrorForLogging(err)

	assert.NotContains(t, result, "abc123def456")
	assert.Contains(t, result, "***REDACTED***")
}

func TestSanitizeErrorForLogging_RedactsBearer(t *testing.T) {
	t.Parallel()

	err := errors.New("Authorization: Bearer eyJhbGciOi...")
	result := sanitizeErrorForLogging(err)

	assert.NotContains(t, result, "eyJhbGciOi...")
	assert.Contains(t, result, "***REDACTED***")
}

func TestSanitizeErrorForLogging_CaseInsensitive(t *testing.T) {
	t.Parallel()

	err := errors.New("PASSWORD=hunter2 Secret=topsecret")
	result := sanitizeErrorForLogging(err)

	assert.NotContains(t, result, "hunter2")
	assert.NotContains(t, result, "topsecret")
}

// --- findValueEnd ---

func TestFindValueEnd_AtSpace(t *testing.T) {
	t.Parallel()

	msg := "password=secret next_field"
	end := findValueEnd(msg, len("password="))

	assert.Equal(t, len("password=secret"), end)
}

func TestFindValueEnd_AtEndOfString(t *testing.T) {
	t.Parallel()

	msg := "password=secret"
	end := findValueEnd(msg, len("password="))

	assert.Equal(t, len(msg), end)
}

func TestFindValueEnd_AtQuote(t *testing.T) {
	t.Parallel()

	msg := `password=secret"more`
	end := findValueEnd(msg, len("password="))

	assert.Equal(t, len("password=secret"), end)
}

func TestFindValueEnd_AtSemicolon(t *testing.T) {
	t.Parallel()

	msg := "password=secret;next"
	end := findValueEnd(msg, len("password="))

	assert.Equal(t, len("password=secret"), end)
}

// --- clientErrorMessageForStatus ---

func TestClientErrorMessageForStatus_KnownCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     int
		expected string
	}{
		{name: "bad_request", code: fiber.StatusBadRequest, expected: "invalid_request"},
		{name: "unauthorized", code: fiber.StatusUnauthorized, expected: "unauthorized"},
		{name: "forbidden", code: fiber.StatusForbidden, expected: "forbidden"},
		{name: "not_found", code: fiber.StatusNotFound, expected: "not_found"},
		{name: "request_too_large", code: fiber.StatusRequestEntityTooLarge, expected: "request_entity_too_large"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := clientErrorMessageForStatus(tt.code)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClientErrorMessageForStatus_UnknownCode(t *testing.T) {
	t.Parallel()

	result := clientErrorMessageForStatus(fiber.StatusConflict)

	assert.Equal(t, "request_failed", result)
}

// --- sanitizeHeaderID ---

func TestSanitizeHeaderID_EmptyGeneratesUUID(t *testing.T) {
	t.Parallel()

	result := sanitizeHeaderID("")
	assert.NotEmpty(t, result, "empty input must generate a UUID")
	assert.True(t, len(result) > 0)
}

func TestSanitizeHeaderID_WhitespaceOnlyGeneratesUUID(t *testing.T) {
	t.Parallel()

	result := sanitizeHeaderID("   ")
	assert.NotEmpty(t, result)
}

func TestSanitizeHeaderID_ValidPassthrough(t *testing.T) {
	t.Parallel()

	result := sanitizeHeaderID("req-abc-123")
	assert.Equal(t, "req-abc-123", result)
}

func TestSanitizeHeaderID_TruncatesLongInput(t *testing.T) {
	t.Parallel()

	long := make([]byte, maxHeaderIDLength+50)
	for i := range long {
		long[i] = 'a'
	}

	result := sanitizeHeaderID(string(long))
	assert.Len(t, result, maxHeaderIDLength)
}

func TestSanitizeHeaderID_RemovesUnsafeChars(t *testing.T) {
	t.Parallel()

	result := sanitizeHeaderID("header\nwith\nnewlines")
	assert.NotContains(t, result, "\n")
}

// --- isSafeHeaderChar ---

func TestIsSafeHeaderChar_Printable(t *testing.T) {
	t.Parallel()

	assert.True(t, isSafeHeaderChar('a'))
	assert.True(t, isSafeHeaderChar('Z'))
	assert.True(t, isSafeHeaderChar('0'))
	assert.True(t, isSafeHeaderChar('-'))
	assert.True(t, isSafeHeaderChar('_'))
}

func TestIsSafeHeaderChar_Unsafe(t *testing.T) {
	t.Parallel()

	assert.False(t, isSafeHeaderChar('\r'))
	assert.False(t, isSafeHeaderChar('\n'))
	assert.False(t, isSafeHeaderChar('\t'))
	assert.False(t, isSafeHeaderChar(';'))
	assert.False(t, isSafeHeaderChar('|'))
	assert.False(t, isSafeHeaderChar('\x00'))
}

// --- truncateHeaderID ---

func TestTruncateHeaderID_ShortInput(t *testing.T) {
	t.Parallel()

	result := truncateHeaderID("short")
	assert.Equal(t, "short", result)
}

func TestTruncateHeaderID_ExactLength(t *testing.T) {
	t.Parallel()

	exact := make([]byte, maxHeaderIDLength)
	for i := range exact {
		exact[i] = 'x'
	}

	result := truncateHeaderID(string(exact))
	assert.Len(t, result, maxHeaderIDLength)
}

func TestTruncateHeaderID_OverLength(t *testing.T) {
	t.Parallel()

	over := make([]byte, maxHeaderIDLength+20)
	for i := range over {
		over[i] = 'y'
	}

	result := truncateHeaderID(string(over))
	assert.Len(t, result, maxHeaderIDLength)
}
