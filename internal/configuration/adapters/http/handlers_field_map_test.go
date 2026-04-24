// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"

	"github.com/LerianStudio/matcher/internal/auth"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestHandlers_UpdateFieldMapNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Patch("/api/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	payload := mustJSON(t, shared.UpdateFieldMapInput{
		Mapping: map[string]any{"field": "updated"},
	})

	resp := performRequest(
		t,
		app,
		http.MethodPatch,
		"/api/v1/field-maps/"+uuid.NewString(),
		payload,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
}

func TestHandlers_UpdateFieldMapInvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Patch("/api/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	resp := performRequest(
		t,
		app,
		http.MethodPatch,
		"/api/v1/field-maps/"+fieldMap.ID.String(),
		[]byte("{invalid"),
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
	requireErrorMessage(t, resp, "invalid_request")
}

func TestHandlers_DeleteFieldMapNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Delete("/api/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(
		t,
		app,
		http.MethodDelete,
		"/api/v1/field-maps/"+uuid.NewString(),
		nil,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
}

func TestHandlers_DeleteFieldMapInvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Delete("/api/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(
		t,
		app,
		http.MethodDelete,
		"/api/v1/field-maps/not-a-uuid",
		nil,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
}

func TestHandlers_DeleteFieldMapSuccess(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Delete("/api/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(
		t,
		app,
		http.MethodDelete,
		"/api/v1/field-maps/"+fieldMap.ID.String(),
		nil,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNoContent, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
}

func TestHandlers_CreateFieldMapInvalidContextID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Post(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		fixture.handler.CreateFieldMap,
	)

	payload := mustJSON(t, shared.CreateFieldMapInput{
		Mapping: map[string]any{"field": "value"},
	})

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		"not-a-uuid",
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.create")
}

func TestHandlers_CreateFieldMapSuccess(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)

	app.Post(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		fixture.handler.CreateFieldMap,
	)

	payload := mustJSON(t, shared.CreateFieldMapInput{
		Mapping: map[string]any{"field": "value"},
	})

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		contextEntity.ID.String(),
		sourceEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusCreated, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.create")

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "value", body["mapping"].(map[string]any)["field"])
}

func TestHandlers_GetFieldMapBySourceInvalidContextID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Get(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		fixture.handler.GetFieldMapBySource,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		"not-a-uuid",
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.get_by_source")
}

func TestHandlers_GetFieldMapBySourceSuccess(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Get(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		fixture.handler.GetFieldMapBySource,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		contextEntity.ID.String(),
		sourceEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.get_by_source")

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "value", body["mapping"].(map[string]any)["field"])
}

func TestHandlers_UpdateFieldMapUnauthorized(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	// Create context WITHOUT a valid tenant → will cause unauthorized
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Patch("/api/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	payload := mustJSON(t, shared.UpdateFieldMapInput{
		Mapping: map[string]any{"field": "updated"},
	})

	resp := performRequest(
		t,
		app,
		http.MethodPatch,
		"/api/v1/field-maps/"+uuid.NewString(),
		payload,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
	requireUnauthorizedResponse(t, resp)
}

func TestHandlers_DeleteFieldMapUnauthorized(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Delete("/api/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(
		t,
		app,
		http.MethodDelete,
		"/api/v1/field-maps/"+uuid.NewString(),
		nil,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
	requireUnauthorizedResponse(t, resp)
}

func TestHandlers_CreateFieldMapInvalidSourceUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		fixture.handler.CreateFieldMap,
	)

	payload := mustJSON(t, shared.CreateFieldMapInput{
		Mapping: map[string]any{"field": "value"},
	})

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.create")
}
