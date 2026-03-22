//go:build unit

package http

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

func TestHandlers_CloneContext(t *testing.T) {
	t.Parallel()

	t.Run("clone context success", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		fixture.seedSource(t, contextEntity.ID)
		fixture.seedMatchRule(t, contextEntity.ID, 1)

		app.Post("/api/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

		payload := dto.CloneContextRequest{
			Name: "Cloned Context",
		}

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/clone",
			contextEntity.ID.String(),
		)

		resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusCreated, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.clone")

		var body dto.CloneContextResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		assert.NotEmpty(t, body.Context.ID)
	})

	t.Run("clone context invalid payload", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		app.Post("/api/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/clone",
			contextEntity.ID.String(),
		)

		resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.clone")
		requireErrorMessage(t, resp, "invalid_request")
	})

	t.Run("clone context not found", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		_ = fixture.seedContext(t, tenantID) // seed so tenant is valid

		app.Post("/api/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

		payload := dto.CloneContextRequest{
			Name: "Cloned Context",
		}

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/clone",
			uuid.NewString(),
		)

		resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.clone")
	})

	t.Run("clone context invalid context id", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		app.Post("/api/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

		payload := dto.CloneContextRequest{
			Name: "Cloned",
		}

		resp := performRequest(
			t,
			app,
			http.MethodPost,
			"/api/v1/contexts/not-a-uuid/clone",
			mustJSON(t, payload),
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.clone")
	})
}

func TestHandlers_ListContextsFilterByType(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	fixture.seedContext(t, tenantID)
	app.Get("/api/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(
		t,
		app,
		http.MethodGet,
		"/api/v1/contexts?type="+string(value_objects.ContextTypeOneToOne),
		nil,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	items, ok := payload["items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1)
}

func TestHandlers_ListContextsFilterByInvalidType(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Get("/api/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/api/v1/contexts?type=INVALID", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.list")
}

func TestHandlers_ListContextsFilterByStatus(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	fixture.seedContext(t, tenantID)
	app.Get("/api/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/api/v1/contexts?status=DRAFT", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Contains(t, payload, "items")
}

func TestHandlers_ListContextsFilterByInvalidStatus(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Get("/api/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/api/v1/contexts?status=BOGUS", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.list")
}

func TestHandlers_UpdateContextInvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Patch("/api/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
	requireErrorMessage(t, resp, "invalid_request")
}

func TestHandlers_UpdateContextInvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Patch("/api/v1/contexts/:contextId", fixture.handler.UpdateContext)

	payload := mustJSON(t, map[string]any{"name": "Updated"})

	resp := performRequest(t, app, http.MethodPatch, "/api/v1/contexts/not-a-uuid", payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}

func TestHandlers_DeleteContextInvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Delete("/api/v1/contexts/:contextId", fixture.handler.DeleteContext)

	resp := performRequest(t, app, http.MethodDelete, "/api/v1/contexts/not-a-uuid", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.delete")
}

func TestHandlers_ListContextsEmpty(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Get("/api/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/api/v1/contexts", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	items, ok := payload["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
	assert.Equal(t, false, payload["hasMore"])
}
