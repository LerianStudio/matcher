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

	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

// ─── UpdateMatchRule priority conflict test ───────────────────

func TestUpdateMatchRule_PriorityConflict(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)
	ruleToUpdate := fixture.seedMatchRule(t, contextEntity.ID, 2)

	app.Patch("/v1/contexts/:contextId/rules/:ruleId", fixture.handler.UpdateMatchRule)

	// Try to update rule 2 to have priority 1 (conflict)
	payload := entities.UpdateMatchRuleInput{Priority: intPointer(1)}
	requestPath := replacePathParams(
		"/v1/contexts/:contextId/rules/:ruleId",
		contextEntity.ID.String(),
		ruleToUpdate.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.update")
	requireConflictResponse(t, resp, "priority_conflict", entities.ErrRulePriorityConflict.Error())
}

// ─── UpdateContext invalid payload test ───────────────────────

func TestUpdateContext_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Patch("/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}

func TestCreateContext_DeprecatedRateID_ReturnsExplicitBadRequest(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Post("/v1/config/contexts", fixture.handler.CreateContext)

	payload := []byte(`{"name":"Context","type":"1:1","interval":"daily","rateId":"550e8400-e29b-41d4-a716-446655440000"}`)
	resp := performRequest(t, app, http.MethodPost, "/v1/config/contexts", payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	var responsePayload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&responsePayload))
	require.Equal(t, expectedConfigurationCode("invalid_request"), responsePayload["code"])
	require.Equal(t, http.StatusText(http.StatusBadRequest), responsePayload["title"])
	assert.Contains(t, responsePayload["message"], dto.ErrDeprecatedRateID.Error())
	requireSpanName(t, recorder, "handler.context.create")
}

func TestUpdateContext_DeprecatedRateID_ReturnsExplicitBadRequest(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)
	contextEntity := fixture.seedContext(t, tenantID)

	app.Patch("/v1/config/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/config/contexts/:contextId",
		contextEntity.ID.String(),
	)
	payload := []byte(`{"status":"ACTIVE","rateId":"550e8400-e29b-41d4-a716-446655440000"}`)
	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	var responsePayload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&responsePayload))
	require.Equal(t, expectedConfigurationCode("invalid_request"), responsePayload["code"])
	require.Equal(t, http.StatusText(http.StatusBadRequest), responsePayload["title"])
	assert.Contains(t, responsePayload["message"], dto.ErrDeprecatedRateID.Error())
	requireSpanName(t, recorder, "handler.context.update")
}

func TestUpdateContext_InvalidStateTransition_Returns409(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed a DRAFT context; PAUSED is only reachable from ACTIVE, so
	// requesting status=PAUSED on a DRAFT triggers ErrInvalidStateTransition.
	contextEntity := fixture.seedContext(t, tenantID)

	app.Patch("/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	paused := value_objects.ContextStatusPaused
	payload := entities.UpdateReconciliationContextInput{Status: &paused}

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}

func TestUpdateContext_ArchivedContextCannotBeModified_Returns409(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed a context and force its status to ARCHIVED directly in the repo
	// so we can test the guard without running through the archival transition.
	contextEntity := fixture.seedContext(t, tenantID)
	contextEntity.Status = value_objects.ContextStatusArchived
	fixture.contextRepo.items[contextEntity.ID] = contextEntity

	app.Patch("/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	newName := "Updated Name"
	payload := entities.UpdateReconciliationContextInput{Name: &newName}

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}

func TestUpdateContext_SameStatus_IsNoOp_Returns200(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed a context and force ACTIVE status to exercise a valid same-status no-op.
	contextEntity := fixture.seedContext(t, tenantID)
	contextEntity.Status = value_objects.ContextStatusActive
	fixture.contextRepo.items[contextEntity.ID] = contextEntity

	app.Patch("/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	activeStatus := "ACTIVE"
	payload := dto.UpdateContextRequest{Status: &activeStatus}

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}
