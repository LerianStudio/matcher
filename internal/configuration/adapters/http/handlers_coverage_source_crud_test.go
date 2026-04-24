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
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ─── UpdateSource error path tests ────────────────────────────

func TestUpdateSource_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Patch("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.UpdateSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	payload := mustJSON(t, entities.UpdateReconciliationSourceInput{
		Name: stringPointer("Updated"),
	})

	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
}

func TestUpdateSource_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	app.Patch("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.UpdateSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		sourceEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
}

func TestUpdateSource_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Patch("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.UpdateSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	payload := mustJSON(t, entities.UpdateReconciliationSourceInput{
		Name: stringPointer("Updated"),
	})

	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
	requireNotFoundResponse(t, resp, "source not found")
}

// ─── DeleteSource error path tests ────────────────────────────

func TestDeleteSource_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Delete("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.DeleteSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.delete")
}

func TestDeleteSource_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Delete("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.DeleteSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.delete")
	requireNotFoundResponse(t, resp, "source not found")
}

// ─── UpdateMatchRule invalid payload test ─────────────────────

func TestUpdateMatchRule_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	rule := fixture.seedMatchRule(t, contextEntity.ID, 1)
	app.Patch("/v1/contexts/:contextId/rules/:ruleId", fixture.handler.UpdateMatchRule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/rules/:ruleId",
		contextEntity.ID.String(),
		rule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.update")
}

// ─── CreateFieldMap source invalid UUID test ──────────────────

func TestCreateFieldMap_InvalidSourceUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/sources/:sourceId/field-maps", fixture.handler.CreateFieldMap)

	payload := shared.CreateFieldMapInput{
		Mapping: map[string]any{"field": "value"},
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId/field-maps",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.create")
}

// ─── ReorderMatchRules invalid payload ────────────────────────

func TestReorderMatchRules_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/rules/reorder", fixture.handler.ReorderMatchRules)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/rules/reorder",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.reorder")
}

// ─── ListContexts nil result test ─────────────────────────────

func TestListContexts_NilResult(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Don't seed any contexts
	app.Get("/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/v1/contexts", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	items, ok := payload["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
}

// ─── ListSources nil result test ──────────────────────────────

func TestListSources_NilResult(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Get("/v1/contexts/:contextId/sources", fixture.handler.ListSources)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var respPayload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&respPayload))
	items, ok := respPayload["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
}
