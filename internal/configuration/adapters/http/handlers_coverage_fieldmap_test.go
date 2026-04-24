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
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ─── CloneContext handler tests ───────────────────────────────

func TestCloneContext_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedSource(t, contextEntity.ID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)
	schedule := fixture.seedFeeSchedule(t, tenantID)
	fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "cloned-fee-rule", fee.MatchingSideAny, 1)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	payload := dto.CloneContextRequest{
		Name: "Cloned Context",
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusCreated, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")

	var response dto.CloneContextResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "Cloned Context", response.Context.Name)
	assert.Equal(t, 1, response.SourcesCloned)
	assert.Equal(t, 1, response.RulesCloned)
	assert.Equal(t, 1, response.FeeRulesCloned)
}

func TestCloneContext_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")
}

func TestCloneContext_ContextNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	payload := dto.CloneContextRequest{
		Name: "Cloned Context",
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")
	requireNotFoundResponse(t, resp, "context not found")
}

func TestCloneContext_EmptyName(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	// send name that is empty after validation - the validator requires min=1
	// but passing an empty string to the command triggers ErrCloneNameRequired
	// Let's pass valid JSON but with empty name to trigger validation
	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		contextEntity.ID.String(),
	)

	// Note: the validator will catch empty name (min=1), so we get 400 from libHTTP.ParseBodyAndValidate
	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, map[string]string{"name": ""}))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")
}

func TestCloneContext_WithBoolDefaults(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedSource(t, contextEntity.ID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	falseVal := false
	payload := dto.CloneContextRequest{
		Name:           "Cloned NoSources",
		IncludeSources: &falseVal,
		IncludeRules:   &falseVal,
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusCreated, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")

	var response dto.CloneContextResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "Cloned NoSources", response.Context.Name)
	assert.Equal(t, 0, response.SourcesCloned)
	assert.Equal(t, 0, response.RulesCloned)
	assert.Equal(t, 0, response.FeeRulesCloned)
}

// ─── UpdateFieldMap error path tests ──────────────────────────

func TestUpdateFieldMap_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Patch("/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	payload := mustJSON(t, shared.UpdateFieldMapInput{
		Mapping: map[string]any{"field": "updated"},
	})

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/field-maps/"+uuid.NewString(), payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
	requireNotFoundResponse(t, resp, "field map not found")
}

func TestUpdateFieldMap_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed a field map to pass the UUID parsing
	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Patch("/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/field-maps/"+fieldMap.ID.String(), []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
}

func TestUpdateFieldMap_UnauthorizedTenant(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	// Set up context with invalid tenant ID
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Patch("/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	payload := mustJSON(t, shared.UpdateFieldMapInput{
		Mapping: map[string]any{"field": "updated"},
	})

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/field-maps/"+uuid.NewString(), payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
}

func TestUpdateFieldMap_OwnershipDenied(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	differentTenantID := uuid.New()
	ctx := newRequestContext(tracer, differentTenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed with original tenant
	originalTenantID := uuid.New()
	contextEntity := fixture.seedContext(t, originalTenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Patch("/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	payload := mustJSON(t, shared.UpdateFieldMapInput{
		Mapping: map[string]any{"field": "updated"},
	})

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/field-maps/"+fieldMap.ID.String(), payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
	requireResourceNotFoundResponse(t, resp, "field map not found")
}

// ─── DeleteFieldMap error path tests ──────────────────────────

func TestDeleteFieldMap_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Delete("/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/field-maps/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
	requireNotFoundResponse(t, resp, "field map not found")
}

func TestDeleteFieldMap_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Delete("/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/field-maps/not-a-uuid", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
}

func TestDeleteFieldMap_UnauthorizedTenant(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Delete("/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/field-maps/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
}

func TestDeleteFieldMap_OwnershipDenied(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	differentTenantID := uuid.New()
	ctx := newRequestContext(tracer, differentTenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	originalTenantID := uuid.New()
	contextEntity := fixture.seedContext(t, originalTenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Delete("/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/field-maps/"+fieldMap.ID.String(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
	requireResourceNotFoundResponse(t, resp, "field map not found")
}
