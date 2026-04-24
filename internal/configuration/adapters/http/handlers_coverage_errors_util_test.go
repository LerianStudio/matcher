// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
)

// ─── boolDefault tests ────────────────────────────────────────

func TestBoolDefault_NilReturnDefault(t *testing.T) {
	t.Parallel()

	assert.True(t, boolDefault(nil, true))
	assert.False(t, boolDefault(nil, false))
}

func TestBoolDefault_NonNilReturnValue(t *testing.T) {
	t.Parallel()

	trueVal := true
	falseVal := false

	assert.True(t, boolDefault(&trueVal, false))
	assert.False(t, boolDefault(&falseVal, true))
}

// ─── handleContextVerificationError branch tests ──────────────

func TestHandleContextVerificationError_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "ErrMissingContextID returns 400",
			err:            libHTTP.ErrMissingContextID,
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "invalid_context_id",
		},
		{
			name:           "ErrInvalidContextID returns 400",
			err:            libHTTP.ErrInvalidContextID,
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "invalid_context_id",
		},
		{
			name:           "ErrTenantIDNotFound returns 401",
			err:            libHTTP.ErrTenantIDNotFound,
			expectedStatus: fiber.StatusUnauthorized,
			expectedCode:   "unauthorized",
		},
		{
			name:           "ErrInvalidTenantID returns 401",
			err:            libHTTP.ErrInvalidTenantID,
			expectedStatus: fiber.StatusUnauthorized,
			expectedCode:   "unauthorized",
		},
		{
			name:           "ErrContextNotFound returns 404",
			err:            libHTTP.ErrContextNotFound,
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "configuration_context_not_found",
		},
		{
			name:           "libHTTP.ErrContextNotActive returns 403",
			err:            libHTTP.ErrContextNotActive,
			expectedStatus: fiber.StatusForbidden,
			expectedCode:   "context_not_active",
		},
		{
			name:           "libHTTP.ErrContextNotOwned returns 403",
			err:            libHTTP.ErrContextNotOwned,
			expectedStatus: fiber.StatusForbidden,
			expectedCode:   "forbidden",
		},
		{
			name:           "libHTTP.ErrContextAccessDenied returns 403",
			err:            libHTTP.ErrContextAccessDenied,
			expectedStatus: fiber.StatusForbidden,
			expectedCode:   "forbidden",
		},
		{
			name:           "generic error returns 500",
			err:            errors.New("random infrastructure failure"),
			expectedStatus: fiber.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracer, _ := newTestTracer(t)
			_, span := tracer.Start(context.Background(), "test")

			defer span.End()

			app := fiber.New()
			app.Get("/test", func(c *fiber.Ctx) error {
				return (&Handler{}).handleContextVerificationError(c.UserContext(), c, span, &libLog.NopLogger{}, tt.err)
			})

			resp := performRequest(t, app, http.MethodGet, "/test", nil)
			defer resp.Body.Close()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedCode != "" {
				var payload map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
				assert.Equal(t, expectedConfigurationCode(tt.expectedCode), payload["code"])
				assert.Equal(t, http.StatusText(tt.expectedStatus), payload["title"])
			}
		})
	}
}

// ─── handleOwnershipVerificationError tests ───────────────────

func TestHandleOwnershipVerificationError_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "ErrContextNotFound returns 404",
			err:            libHTTP.ErrContextNotFound,
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "configuration_field_map_not_found",
		},
		{
			name:           "libHTTP.ErrContextNotOwned returns 404",
			err:            libHTTP.ErrContextNotOwned,
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "configuration_field_map_not_found",
		},
		{
			name:           "libHTTP.ErrContextAccessDenied returns 403",
			err:            libHTTP.ErrContextAccessDenied,
			expectedStatus: fiber.StatusForbidden,
			expectedCode:   "forbidden",
		},
		{
			name:           "generic error returns 500",
			err:            errors.New("database connection lost"),
			expectedStatus: fiber.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracer, _ := newTestTracer(t)
			_, span := tracer.Start(context.Background(), "test")

			defer span.End()

			app := fiber.New()
			app.Get("/test", func(c *fiber.Ctx) error {
				return (&Handler{}).handleOwnershipVerificationError(c.UserContext(), c, span, &libLog.NopLogger{}, tt.err, "configuration_field_map_not_found", "field map not found")
			})

			resp := performRequest(t, app, http.MethodGet, "/test", nil)
			defer resp.Body.Close()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedCode != "" {
				var payload map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
				assert.Equal(t, expectedConfigurationCode(tt.expectedCode), payload["code"])
				assert.Equal(t, http.StatusText(tt.expectedStatus), payload["title"])
			}
		})
	}
}

// ─── ensureSourceAccess tests ─────────────────────────────────

func TestEnsureSourceAccess_Success(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)

	_, span := tracer.Start(ctx, "test")
	defer span.End()

	app := fiber.New()
	app.Get("/test/:contextId/:sourceId", func(c *fiber.Ctx) error {
		return fixture.handler.ensureSourceAccess(ctx, c, span, &libLog.NopLogger{}, contextEntity.ID, sourceEntity.ID)
	})

	resp := performRequest(t, app, http.MethodGet,
		"/test/"+contextEntity.ID.String()+"/"+sourceEntity.ID.String(), nil)
	defer resp.Body.Close()

	// nil return means success (no error response written)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestEnsureSourceAccess_NotFound(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	missingSourceID := uuid.New()

	_, span := tracer.Start(ctx, "test")
	defer span.End()

	// ensureSourceAccess writes the 404 response to the Fiber context and returns nil
	// (the Fiber JSON write returns nil on success). The pattern in handlers is:
	//   if err := handler.ensureSourceAccess(...); err != nil { return err }
	// Here we test that ensureSourceAccess returns nil even on source-not-found,
	// and that the 404 response body was written to the context.
	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		err := fixture.handler.ensureSourceAccess(ctx, c, span, &libLog.NopLogger{}, contextEntity.ID, missingSourceID)
		// writeNotFound writes the 404 JSON response and returns nil from c.Status().JSON()
		// So err is nil here. The handler should return nil (or the err value) to let
		// Fiber send the already-written 404 response.
		return err
	})

	resp := performRequest(t, app, http.MethodGet, "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireNotFoundResponse(t, resp, "source not found")
}
