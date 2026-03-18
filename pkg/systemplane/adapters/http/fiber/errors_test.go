//go:build unit

// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

func testErrorApp(t *testing.T, err error) *http.Response {
	t.Helper()

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	app.Get("/test", func(c *fiber.Ctx) error {
		return writeError(c, err)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	resp, testErr := app.Test(req)
	require.NoError(t, testErr)

	return resp
}

func TestWriteError_KeyUnknown(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, fmt.Errorf("key %q: %w", "bad.key", domain.ErrKeyUnknown))

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_key_unknown", errResp.Code)
	assert.Contains(t, errResp.Message, "bad.key")
}

func TestWriteError_ValueInvalid(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, domain.ErrValueInvalid)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_value_invalid", errResp.Code)
}

func TestWriteError_KeyNotMutable(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, domain.ErrKeyNotMutable)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_key_not_mutable", errResp.Code)
}

func TestWriteError_ScopeInvalid(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, domain.ErrScopeInvalid)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_scope_invalid", errResp.Code)
}

func TestWriteError_RevisionMismatch(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, domain.ErrRevisionMismatch)

	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_revision_mismatch", errResp.Code)
}

func TestWriteError_PermissionDenied(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, domain.ErrPermissionDenied)

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_permission_denied", errResp.Code)
}

func TestWriteError_ReloadFailed(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, fmt.Errorf("%w: something broke", domain.ErrReloadFailed))

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_reload_failed", errResp.Code)
}

func TestWriteError_SupervisorStopped(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, domain.ErrSupervisorStopped)

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_unavailable", errResp.Code)
}

func TestWriteError_UnknownError(t *testing.T) {
	t.Parallel()

	resp := testErrorApp(t, errors.New("something completely unknown"))

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_internal_error", errResp.Code)
	assert.Equal(t, "internal server error", errResp.Message)
}

func TestWriteError_WrappedError(t *testing.T) {
	t.Parallel()

	// errors.Is traverses the chain, so a wrapped ErrKeyUnknown should still match
	wrapped := fmt.Errorf("layer 1: %w", fmt.Errorf("layer 2: %w", domain.ErrKeyUnknown))
	resp := testErrorApp(t, wrapped)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_key_unknown", errResp.Code)
}
