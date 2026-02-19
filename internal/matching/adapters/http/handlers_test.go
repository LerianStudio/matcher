//go:build unit

package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"

	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
)

func TestStartHandlerSpan(t *testing.T) {
	t.Parallel()

	t.Run("creates_span_with_tracer_from_context", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		var resultCtx context.Context
		var resultSpan trace.Span

		app.Get("/", func(c *fiber.Ctx) error {
			resultCtx, resultSpan, _ = startHandlerSpan(c, "test_span")
			defer resultSpan.End()
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotNil(t, resultCtx)
		assert.NotNil(t, resultSpan)
	})

	t.Run("uses_default_tracer_when_not_in_context", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		app := newFiberTestApp(ctx)

		var resultSpan trace.Span

		app.Get("/", func(c *fiber.Ctx) error {
			_, resultSpan, _ = startHandlerSpan(c, "default_tracer_span")
			defer resultSpan.End()
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotNil(t, resultSpan)
	})

	t.Run("returns_logger_when_present_in_context", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		mockLog := &mockLogger{}
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		ctx = libCommons.ContextWithLogger(ctx, mockLog)

		app := newFiberTestApp(ctx)

		var resultLogger libLog.Logger

		app.Get("/", func(c *fiber.Ctx) error {
			_, span, logger := startHandlerSpan(c, "with_logger")
			resultLogger = logger
			defer span.End()
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotNil(t, resultLogger)
	})
}

func TestBadRequest(t *testing.T) {
	t.Parallel()

	t.Run("returns_400_with_error_response", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			return badRequest(spanCtx, c, span, &libLog.NopLogger{}, "validation failed", errTestBoom)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp sharedhttp.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		assert.Equal(t, 400, errResp.Code)
		assert.Equal(t, "invalid_request", errResp.Title)
		assert.Equal(t, "validation failed", errResp.Message)
	})

	t.Run("logs_error_when_logger_provided", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		mockLog := &mockLogger{}
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		ctx = libCommons.ContextWithLogger(ctx, mockLog)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			return badRequest(spanCtx, c, span, mockLog, "test message", errTestBoom)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.True(t, mockLog.errorCalled)
	})

	t.Run("works_with_nop_logger", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			return badRequest(spanCtx, c, span, &libLog.NopLogger{}, "test", errTestBoom)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})
}

func TestWriteServiceError(t *testing.T) {
	t.Parallel()

	t.Run("returns_500_internal_server_error", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			return writeServiceError(spanCtx, c, span, &libLog.NopLogger{}, "database connection failed", errTestDatabaseError)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("logs_error_when_logger_provided", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		mockLog := &mockLogger{}
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			return writeServiceError(spanCtx, c, span, mockLog, "service failure", errTestBoom)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.True(t, mockLog.errorCalled)
	})
}

func TestWriteNotFound(t *testing.T) {
	t.Parallel()

	t.Run("returns_404_with_message", func(t *testing.T) {
		t.Parallel()

		app := newFiberTestApp(context.Background())

		app.Get("/", func(c *fiber.Ctx) error {
			return writeNotFound(c, "resource not found")
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)

		var errResp sharedhttp.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		assert.Equal(t, 404, errResp.Code)
		assert.Equal(t, "not_found", errResp.Title)
		assert.Equal(t, "resource not found", errResp.Message)
	})
}

func TestForbidden(t *testing.T) {
	t.Parallel()

	t.Run("returns_403_with_access_denied_message", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			return forbidden(spanCtx, c, span, &libLog.NopLogger{}, errTestBoom)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusForbidden, resp.StatusCode)

		var errResp sharedhttp.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		assert.Equal(t, 403, errResp.Code)
		assert.Equal(t, "forbidden", errResp.Title)
		assert.Equal(t, "access denied", errResp.Message)
	})

	t.Run("creates_error_when_err_is_nil", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			return forbidden(spanCtx, c, span, &libLog.NopLogger{}, nil)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, fiber.StatusForbidden, resp.StatusCode)
	})

	t.Run("logs_warning_when_logger_provided", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		mockLog := &mockLogger{}
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			return forbidden(spanCtx, c, span, mockLog, errTestBoom)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.True(t, mockLog.warnCalled)
	})
}

func TestHandleContextVerificationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedTitle  string
		shouldReturn   bool
	}{
		{
			name:         "nil_error_returns_false",
			err:          nil,
			shouldReturn: false,
		},
		{
			name:           "missing_context_id_returns_bad_request",
			err:            sharedhttp.ErrMissingContextID,
			expectedStatus: fiber.StatusBadRequest,
			expectedTitle:  "invalid_request",
			shouldReturn:   true,
		},
		{
			name:           "invalid_context_id_returns_bad_request",
			err:            sharedhttp.ErrInvalidContextID,
			expectedStatus: fiber.StatusBadRequest,
			expectedTitle:  "invalid_request",
			shouldReturn:   true,
		},
		{
			name:           "tenant_id_not_found_returns_unauthorized",
			err:            sharedhttp.ErrTenantIDNotFound,
			expectedStatus: fiber.StatusUnauthorized,
			expectedTitle:  "unauthorized",
			shouldReturn:   true,
		},
		{
			name:           "invalid_tenant_id_returns_unauthorized",
			err:            sharedhttp.ErrInvalidTenantID,
			expectedStatus: fiber.StatusUnauthorized,
			expectedTitle:  "unauthorized",
			shouldReturn:   true,
		},
		{
			name:           "context_not_found_returns_not_found",
			err:            sharedhttp.ErrContextNotFound,
			expectedStatus: fiber.StatusNotFound,
			expectedTitle:  "not_found",
			shouldReturn:   true,
		},
		{
			name:           "context_not_active_returns_forbidden",
			err:            sharedhttp.ErrContextNotActive,
			expectedStatus: fiber.StatusForbidden,
			expectedTitle:  "context_not_active",
			shouldReturn:   true,
		},
		{
			name:           "context_not_owned_returns_forbidden",
			err:            sharedhttp.ErrContextNotOwned,
			expectedStatus: fiber.StatusForbidden,
			expectedTitle:  "forbidden",
			shouldReturn:   true,
		},
		{
			name:           "context_access_denied_returns_forbidden",
			err:            sharedhttp.ErrContextAccessDenied,
			expectedStatus: fiber.StatusForbidden,
			expectedTitle:  "forbidden",
			shouldReturn:   true,
		},
		{
			name:           "context_lookup_failed_returns_internal_server_error",
			err:            sharedhttp.ErrContextLookupFailed,
			expectedStatus: fiber.StatusInternalServerError,
			expectedTitle:  "internal_server_error",
			shouldReturn:   true,
		},
		{
			name:           "unknown_error_returns_internal_server_error",
			err:            errors.New("unexpected error"),
			expectedStatus: fiber.StatusInternalServerError,
			expectedTitle:  "internal_server_error",
			shouldReturn:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracer := noop.NewTracerProvider().Tracer("test")
			ctx := libCommons.ContextWithTracer(context.Background(), tracer)
			app := newFiberTestApp(ctx)

			var shouldReturn bool

			app.Get("/", func(c *fiber.Ctx) error {
				spanCtx, span := tracer.Start(c.UserContext(), "test")
				c.SetUserContext(spanCtx)
				defer span.End()

				var handlerErr error
				shouldReturn, handlerErr = handleContextVerificationError(spanCtx, c, span, &libLog.NopLogger{}, tt.err)
				if shouldReturn {
					return handlerErr
				}
				return c.SendStatus(fiber.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.shouldReturn, shouldReturn)

			if tt.shouldReturn {
				assert.Equal(t, tt.expectedStatus, resp.StatusCode)

				var errResp sharedhttp.ErrorResponse
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
				assert.Equal(t, tt.expectedTitle, errResp.Title)
			} else {
				assert.Equal(t, fiber.StatusOK, resp.StatusCode)
			}
		})
	}
}

func TestHandleContextQueryVerificationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedTitle  string
		shouldReturn   bool
	}{
		{
			name:         "nil_error_returns_false",
			err:          nil,
			shouldReturn: false,
		},
		{
			name:           "missing_context_id_returns_bad_request",
			err:            sharedhttp.ErrMissingContextID,
			expectedStatus: fiber.StatusBadRequest,
			expectedTitle:  "invalid_request",
			shouldReturn:   true,
		},
		{
			name:           "invalid_context_id_returns_bad_request",
			err:            sharedhttp.ErrInvalidContextID,
			expectedStatus: fiber.StatusBadRequest,
			expectedTitle:  "invalid_request",
			shouldReturn:   true,
		},
		{
			name:           "context_not_active_returns_forbidden",
			err:            sharedhttp.ErrContextNotActive,
			expectedStatus: fiber.StatusForbidden,
			expectedTitle:  "context_not_active",
			shouldReturn:   true,
		},
		{
			name:           "tenant_id_not_found_returns_unauthorized",
			err:            sharedhttp.ErrTenantIDNotFound,
			expectedStatus: fiber.StatusUnauthorized,
			expectedTitle:  "unauthorized",
			shouldReturn:   true,
		},
		{
			name:           "invalid_tenant_id_returns_unauthorized",
			err:            sharedhttp.ErrInvalidTenantID,
			expectedStatus: fiber.StatusUnauthorized,
			expectedTitle:  "unauthorized",
			shouldReturn:   true,
		},
		{
			name:           "context_not_found_returns_not_found",
			err:            sharedhttp.ErrContextNotFound,
			expectedStatus: fiber.StatusNotFound,
			expectedTitle:  "not_found",
			shouldReturn:   true,
		},
		{
			name:           "context_not_owned_returns_forbidden",
			err:            sharedhttp.ErrContextNotOwned,
			expectedStatus: fiber.StatusForbidden,
			expectedTitle:  "forbidden",
			shouldReturn:   true,
		},
		{
			name:           "context_access_denied_returns_forbidden",
			err:            sharedhttp.ErrContextAccessDenied,
			expectedStatus: fiber.StatusForbidden,
			expectedTitle:  "forbidden",
			shouldReturn:   true,
		},
		{
			name:           "lookup_failed_returns_internal_server_error",
			err:            sharedhttp.ErrLookupFailed,
			expectedStatus: fiber.StatusInternalServerError,
			expectedTitle:  "internal_server_error",
			shouldReturn:   true,
		},
		{
			name:           "context_lookup_failed_returns_internal_server_error",
			err:            sharedhttp.ErrContextLookupFailed,
			expectedStatus: fiber.StatusInternalServerError,
			expectedTitle:  "internal_server_error",
			shouldReturn:   true,
		},
		{
			name:           "unknown_error_returns_internal_server_error",
			err:            errors.New("unexpected query error"),
			expectedStatus: fiber.StatusInternalServerError,
			expectedTitle:  "internal_server_error",
			shouldReturn:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracer := noop.NewTracerProvider().Tracer("test")
			ctx := libCommons.ContextWithTracer(context.Background(), tracer)
			app := newFiberTestApp(ctx)

			var shouldReturn bool

			app.Get("/", func(c *fiber.Ctx) error {
				spanCtx, span := tracer.Start(c.UserContext(), "test")
				c.SetUserContext(spanCtx)
				defer span.End()

				var handlerErr error
				shouldReturn, handlerErr = handleContextQueryVerificationError(spanCtx, c, span, &libLog.NopLogger{}, tt.err)
				if shouldReturn {
					return handlerErr
				}
				return c.SendStatus(fiber.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.shouldReturn, shouldReturn)

			if tt.shouldReturn {
				assert.Equal(t, tt.expectedStatus, resp.StatusCode)

				var errResp sharedhttp.ErrorResponse
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
				assert.Equal(t, tt.expectedTitle, errResp.Title)
			} else {
				assert.Equal(t, fiber.StatusOK, resp.StatusCode)
			}
		})
	}
}

func TestHandleContextVerificationError_WithLogger(t *testing.T) {
	t.Parallel()

	t.Run("logs_warning_for_context_not_active", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		mockLog := &mockLogger{}
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			shouldRet, handlerErr := handleContextVerificationError(spanCtx, c, span, mockLog, sharedhttp.ErrContextNotActive)
			if shouldRet {
				return handlerErr
			}
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.True(t, mockLog.warnCalled)
	})
}

func TestHandleContextQueryVerificationError_WithLogger(t *testing.T) {
	t.Parallel()

	t.Run("logs_warning_for_context_not_active", func(t *testing.T) {
		t.Parallel()

		tracer := noop.NewTracerProvider().Tracer("test")
		mockLog := &mockLogger{}
		ctx := libCommons.ContextWithTracer(context.Background(), tracer)
		app := newFiberTestApp(ctx)

		app.Get("/", func(c *fiber.Ctx) error {
			spanCtx, span := tracer.Start(c.UserContext(), "test")
			c.SetUserContext(spanCtx)
			defer span.End()

			shouldRet, handlerErr := handleContextQueryVerificationError(spanCtx, c, span, mockLog, sharedhttp.ErrContextNotActive)
			if shouldRet {
				return handlerErr
			}
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.True(t, mockLog.warnCalled)
	})
}

func TestParseUUIDParam_EmptyParam(t *testing.T) {
	t.Parallel()

	app := newFiberTestApp(context.Background())
	app.Get("/test/:id", func(c *fiber.Ctx) error {
		id, err := parseUUIDParam(c, "missing")
		if err != nil {
			return c.Status(400).SendString(err.Error())
		}
		return c.SendString(id.String())
	})

	req := httptest.NewRequest(http.MethodGet, "/test/some-value", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestDefaultTracerFallback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	app := newFiberTestApp(ctx)

	app.Get("/", func(c *fiber.Ctx) error {
		_, span, _ := startHandlerSpan(c, "fallback_test")
		defer span.End()

		defaultTracer := otel.Tracer("commons.default")
		assert.NotNil(t, defaultTracer)

		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestRequestDTOs(t *testing.T) {
	t.Parallel()

	t.Run("RunMatchRequest_validates_mode_enum", func(t *testing.T) {
		t.Parallel()

		req := RunMatchRequest{Mode: "DRY_RUN"}
		assert.Equal(t, "DRY_RUN", req.Mode)

		req2 := RunMatchRequest{Mode: "COMMIT"}
		assert.Equal(t, "COMMIT", req2.Mode)
	})

	t.Run("UnmatchRequest_requires_reason", func(t *testing.T) {
		t.Parallel()

		req := UnmatchRequest{Reason: "incorrect match - amounts do not match"}
		assert.NotEmpty(t, req.Reason)
	})

	t.Run("CreateManualMatchRequest_requires_transaction_ids", func(t *testing.T) {
		t.Parallel()

		req := CreateManualMatchRequest{
			TransactionIDs: []string{
				"550e8400-e29b-41d4-a716-446655440000",
				"550e8400-e29b-41d4-a716-446655440001",
			},
			Notes: "Manual match for Q4 reconciliation",
		}
		assert.Len(t, req.TransactionIDs, 2)
		assert.NotEmpty(t, req.Notes)
	})

	t.Run("CreateAdjustmentRequest_has_all_fields", func(t *testing.T) {
		t.Parallel()

		req := CreateAdjustmentRequest{
			MatchGroupID:  "550e8400-e29b-41d4-a716-446655440000",
			TransactionID: "550e8400-e29b-41d4-a716-446655440001",
			Type:          "BANK_FEE",
			Direction:     "DEBIT",
			Amount:        "10.50",
			Currency:      "USD",
			Description:   "Bank wire fee adjustment",
			Reason:        "Variance due to bank processing fee",
		}
		assert.Equal(t, "BANK_FEE", req.Type)
		assert.Equal(t, "DEBIT", req.Direction)
		assert.Equal(t, "10.50", req.Amount)
		assert.Equal(t, "USD", req.Currency)
	})
}

func TestResponseDTOs(t *testing.T) {
	t.Parallel()

	t.Run("RunMatchResponse_has_required_fields", func(t *testing.T) {
		t.Parallel()

		resp := RunMatchResponse{
			Status: "PROCESSING",
		}
		assert.Equal(t, "PROCESSING", resp.Status)
	})

	t.Run("ManualMatchResponse_contains_match_group", func(t *testing.T) {
		t.Parallel()

		resp := ManualMatchResponse{
			MatchGroup: nil,
		}
		assert.Nil(t, resp.MatchGroup)
	})

	t.Run("AdjustmentResponse_contains_adjustment", func(t *testing.T) {
		t.Parallel()

		resp := AdjustmentResponse{
			Adjustment: nil,
		}
		assert.Nil(t, resp.Adjustment)
	})
}

type mockLogger struct {
	errorCalled bool
	warnCalled  bool
}

func (m *mockLogger) Log(_ context.Context, level libLog.Level, _ string, _ ...libLog.Field) {
	switch level {
	case libLog.LevelError:
		m.errorCalled = true
	case libLog.LevelWarn:
		m.warnCalled = true
	}
}

//nolint:ireturn
func (m *mockLogger) With(_ ...libLog.Field) libLog.Logger { return m }

//nolint:ireturn
func (m *mockLogger) WithGroup(_ string) libLog.Logger { return m }
func (m *mockLogger) Enabled(_ libLog.Level) bool      { return true }
func (m *mockLogger) Sync(_ context.Context) error     { return nil }
