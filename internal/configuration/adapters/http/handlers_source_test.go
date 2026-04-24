// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

func TestHandlers_UpdateSourceNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Patch(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		fixture.handler.UpdateSource,
	)

	payload := mustJSON(t, entities.UpdateReconciliationSourceInput{
		Name: stringPointer("Updated Source"),
	})

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
	requireNotFoundResponse(t, resp, "source not found")
}

func TestHandlers_UpdateSourceInvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	app.Patch(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		fixture.handler.UpdateSource,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		sourceEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
	requireErrorMessage(t, resp, "invalid_request")
}

func TestHandlers_UpdateSourceInvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Patch(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		fixture.handler.UpdateSource,
	)

	payload := mustJSON(t, entities.UpdateReconciliationSourceInput{
		Name: stringPointer("Updated"),
	})

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
}

func TestHandlers_DeleteSourceNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Delete(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		fixture.handler.DeleteSource,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.delete")
	requireNotFoundResponse(t, resp, "source not found")
}

func TestHandlers_DeleteSourceInvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Delete(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		fixture.handler.DeleteSource,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.delete")
}

func TestHandlers_DeleteSourceConflictHasFieldMap(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Delete(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		fixture.handler.DeleteSource,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		sourceEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.delete")
	requireConflictResponse(t, resp, "has_field_map", "cannot delete source: has an associated field map that must be removed first")
}

func TestHandlers_ListSourcesFilterByType(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedSource(t, contextEntity.ID)
	app.Get("/api/v1/contexts/:contextId/sources", fixture.handler.ListSources)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources",
		contextEntity.ID.String(),
	)

	resp := performRequest(
		t,
		app,
		http.MethodGet,
		requestPath+"?type="+string(value_objects.SourceTypeLedger),
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

func TestHandlers_ListSourcesFilterByInvalidType(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Get("/api/v1/contexts/:contextId/sources", fixture.handler.ListSources)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath+"?type=BOGUS", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.list")
}

func TestHandlers_ListSourcesEmpty(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Get("/api/v1/contexts/:contextId/sources", fixture.handler.ListSources)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	items, ok := payload["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
	assert.Equal(t, false, payload["hasMore"])
}

func TestHandlers_ListSourcesInvalidPagination(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Get("/api/v1/contexts/:contextId/sources", fixture.handler.ListSources)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath+"?limit=bad", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.list")
	requireErrorMessage(t, resp, "invalid_request")
}

func TestHandlers_CreateSourceInvalidContextID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Post("/api/v1/contexts/:contextId/sources", fixture.handler.CreateSource)

	payload := mustJSON(t, entities.CreateReconciliationSourceInput{
		Name: "Source A",
		Type: value_objects.SourceTypeLedger,
	})

	resp := performRequest(
		t,
		app,
		http.MethodPost,
		"/api/v1/contexts/not-a-uuid/sources",
		payload,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.create")
}
