//go:build unit

package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

func TestStartHandlerSpan_StartsSpan(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	tracer := noop.NewTracerProvider().Tracer("test")

	var (
		resultCtx  context.Context
		resultSpan trace.Span
	)

	app.Get("/", func(c *fiber.Ctx) error {
		c.SetUserContext(libCommons.ContextWithTracer(c.UserContext(), tracer))
		resultCtx, resultSpan, _ = StartHandlerSpan(c, "handler.shared.test")
		resultSpan.End()

		return c.SendStatus(fiber.StatusNoContent)
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.NotNil(t, resultCtx)
	require.NotNil(t, resultSpan)
}

func TestStartHandlerSpanWithFallback_AllowsNilFiberContext(t *testing.T) {
	t.Parallel()

	ctx, span, logger := StartHandlerSpanWithFallback(nil, "handler.shared.nil", "test.fallback")
	defer span.End()

	require.NotNil(t, ctx)
	require.NotNil(t, span)
	require.Nil(t, logger)
}

func TestLogSpanError_AllowsNilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		ctx, span, _ := StartHandlerSpan(c, "handler.shared.log")
		defer span.End()

		require.NotPanics(t, func() {
			LogSpanError(ctx, span, nil, false, "test message", assertErr("boom"))
		})

		return c.SendStatus(fiber.StatusNoContent)
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	assertStatus(t, resp, fiber.StatusNoContent)
}

func TestBadRequest_WritesStandardResponse(t *testing.T) {
	t.Parallel()

	resp := callSharedErrorHelper(t, func(ctx context.Context, c *fiber.Ctx, span trace.Span, logger libLog.Logger) error {
		return BadRequest(ctx, c, span, logger, false, "validation failed", assertErr("boom"))
	})
	defer resp.Body.Close()

	assertStatus(t, resp, fiber.StatusBadRequest)
	assertErrorBody(t, resp, defInvalidRequest.Code, "validation failed")
}

func TestInternalError_WritesStandardResponse(t *testing.T) {
	t.Parallel()

	resp := callSharedErrorHelper(t, func(ctx context.Context, c *fiber.Ctx, span trace.Span, logger libLog.Logger) error {
		return InternalError(ctx, c, span, logger, false, "database failed", assertErr("boom"))
	})
	defer resp.Body.Close()

	assertStatus(t, resp, fiber.StatusInternalServerError)
	assertErrorBody(t, resp, defInternalServerError.Code, "an unexpected error occurred")
}

func callSharedErrorHelper(
	t *testing.T,
	handler func(context.Context, *fiber.Ctx, trace.Span, libLog.Logger) error,
) *http.Response {
	t.Helper()

	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		ctx, span, logger := StartHandlerSpan(c, "handler.shared.error")
		defer span.End()

		return handler(ctx, c, span, logger)
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/", http.NoBody))
	require.NoError(t, err)

	return resp
}

func assertStatus(t *testing.T, resp *http.Response, expected int) {
	t.Helper()
	require.Equal(t, expected, resp.StatusCode)
}

func assertErrorBody(t *testing.T, resp *http.Response, expectedCode, expectedMessage string) {
	t.Helper()

	var body ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, expectedCode, body.Code)
	require.Equal(t, expectedMessage, body.Message)
}

func assertErr(message string) error {
	return fiber.NewError(fiber.StatusInternalServerError, message)
}
