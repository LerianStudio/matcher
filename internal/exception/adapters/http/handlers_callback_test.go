// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"

	"github.com/LerianStudio/matcher/internal/exception/services/command"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
)

func TestHandleCallbackError_RateLimitExceeded(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		return (&Handlers{}).handleCallbackError(spanCtx, c, span, nil, command.ErrCallbackRateLimitExceeded)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusTooManyRequests, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "MTCH-0506", body["code"])
	assert.Equal(t, http.StatusText(http.StatusTooManyRequests), body["title"])
}

func TestHandleCallbackError_ValidationErrors(t *testing.T) {
	t.Parallel()

	validationErrors := []error{
		command.ErrCallbackExternalSystem,
		command.ErrCallbackExternalIssueID,
		command.ErrCallbackStatusRequired,
		command.ErrCallbackAssigneeRequired,
		command.ErrCallbackOpenNotValidTarget,
		command.ErrCallbackStatusUnsupported,
		command.ErrExceptionIDRequired,
	}

	for _, targetErr := range validationErrors {
		t.Run(targetErr.Error(), func(t *testing.T) {
			t.Parallel()

			resp := executeCallbackErrorHandler(t, targetErr)
			defer resp.Body.Close()

			assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		})
	}
}

func TestHandleCallbackError_NotFound(t *testing.T) {
	t.Parallel()

	resp := executeCallbackErrorHandler(t, errors.New("find exception: sql: no rows in result set"))
	defer resp.Body.Close()

	// Wrapped sql.ErrNoRows won't match with errors.Is since it's a plain string wrap.
	// Direct sentinel errors will match:
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestHandleCallbackError_NotFound_SqlNoRows(t *testing.T) {
	t.Parallel()

	// Use fmt.Errorf with %w to properly wrap sql.ErrNoRows so errors.Is matches.
	wrappedErr := fmt.Errorf("find exception: %w", sql.ErrNoRows)
	resp := executeCallbackErrorHandler(t, wrappedErr)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestHandleCallbackError_InternalError(t *testing.T) {
	t.Parallel()

	resp := executeCallbackErrorHandler(t, errTest)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestProcessCallback_MissingIdempotencyKey(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	handlers := newCallbackTestHandlers(t)
	app.Post("/v1/exceptions/:exceptionId/callback", handlers.ProcessCallback)

	body := `{
		"externalSystem": "JIRA",
		"externalIssueId": "RECON-1234",
		"status": "ASSIGNED",
		"assignee": "analyst@example.com"
	}`

	exceptionID := uuid.New()
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/"+exceptionID.String()+"/callback",
		strings.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	// Deliberately NOT setting X-Idempotency-Key

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"X-Idempotency-Key header is required",
	)
}

func TestProcessCallback_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	handlers := newCallbackTestHandlers(t)
	app.Post("/v1/exceptions/:exceptionId/callback", handlers.ProcessCallback)

	body := `{
		"externalSystem": "JIRA",
		"externalIssueId": "RECON-1234",
		"status": "ASSIGNED",
		"assignee": "analyst@example.com"
	}`

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/not-a-uuid/callback",
		strings.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Idempotency-Key", "test-key-123")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid exception id",
	)
}

func TestProcessCallback_InvalidDueAtFormat(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	handlers := newCallbackTestHandlers(t)
	app.Post("/v1/exceptions/:exceptionId/callback", handlers.ProcessCallback)

	body := `{
		"externalSystem": "JIRA",
		"externalIssueId": "RECON-1234",
		"status": "ASSIGNED",
		"assignee": "analyst@example.com",
		"dueAt": "not-a-date"
	}`

	exceptionID := uuid.New()
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/"+exceptionID.String()+"/callback",
		strings.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Idempotency-Key", "test-key-123")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid dueAt format, expected RFC3339",
	)
}

func TestProcessCallback_InvalidUpdatedAtFormat(t *testing.T) {
	t.Parallel()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	handlers := newCallbackTestHandlers(t)
	app.Post("/v1/exceptions/:exceptionId/callback", handlers.ProcessCallback)

	body := `{
		"externalSystem": "JIRA",
		"externalIssueId": "RECON-1234",
		"status": "ASSIGNED",
		"assignee": "analyst@example.com",
		"updatedAt": "bad-date-format"
	}`

	exceptionID := uuid.New()
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/exceptions/"+exceptionID.String()+"/callback",
		strings.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Idempotency-Key", "test-key-123")

	resp, err := app.Test(request)
	require.NoError(t, err)

	defer resp.Body.Close()

	requireErrorResponse(
		t,
		resp,
		fiber.StatusBadRequest,
		400,
		"invalid_request",
		"invalid updatedAt format, expected RFC3339",
	)
}

func TestNewHandlers_NilCallbackUseCase(t *testing.T) {
	t.Parallel()
	t.Skip("merged into single ExceptionUseCase; no separate callback UC argument")
}

// executeCallbackErrorHandler runs handleCallbackError through a fiber test app.
func executeCallbackErrorHandler(
	t *testing.T,
	err error,
) *http.Response {
	t.Helper()

	tracer := noop.NewTracerProvider().Tracer("test")
	ctx := libCommons.ContextWithTracer(context.Background(), tracer)

	app := newFiberTestApp(ctx)
	app.Get("/", func(c *fiber.Ctx) error {
		spanCtx, span := tracer.Start(c.UserContext(), "test")
		c.SetUserContext(spanCtx)

		defer span.End()

		return (&Handlers{}).handleCallbackError(spanCtx, c, span, nil, err)
	})

	request := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, reqErr := app.Test(request)
	require.NoError(t, reqErr)

	return resp
}

// newCallbackTestHandlers creates Handlers with a zero-value CallbackUseCase
// suitable for testing handler-level parsing before the use case is invoked.
func newCallbackTestHandlers(t *testing.T) *Handlers {
	t.Helper()

	exceptionProvider := &stubExceptionProvider{exists: true}
	disputeProvider := &stubDisputeProvider{exists: true}

	handlers, err := NewHandlers(&command.ExceptionUseCase{}, &query.UseCase{}, &stubCommentRepo{}, exceptionProvider, disputeProvider, false)
	require.NoError(t, err)

	return handlers
}
