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

func TestHandlers_UpdateMatchRulePriorityConflict(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)
	ruleToUpdate := fixture.seedMatchRule(t, contextEntity.ID, 2)

	app.Patch(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		fixture.handler.UpdateMatchRule,
	)

	payload := mustJSON(t, entities.UpdateMatchRuleInput{
		Priority: intPointer(1), // conflicts with existing rule at priority 1
	})

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		contextEntity.ID.String(),
		ruleToUpdate.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.update")
	requireConflictResponse(t, resp, "priority_conflict", entities.ErrRulePriorityConflict.Error())
}

func TestHandlers_UpdateMatchRuleInvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	matchRule := fixture.seedMatchRule(t, contextEntity.ID, 1)

	app.Patch(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		fixture.handler.UpdateMatchRule,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		contextEntity.ID.String(),
		matchRule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.update")
	requireErrorMessage(t, resp, "invalid_request")
}

func TestHandlers_ReorderMatchRulesInvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Post(
		"/api/v1/contexts/:contextId/rules/reorder",
		fixture.handler.ReorderMatchRules,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/reorder",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.reorder")
	requireErrorMessage(t, resp, "invalid_request")
}

func TestHandlers_ReorderMatchRulesInvalidContextID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Post(
		"/api/v1/contexts/:contextId/rules/reorder",
		fixture.handler.ReorderMatchRules,
	)

	payload := mustJSON(t, ReorderRequest{RuleIDs: []uuid.UUID{uuid.New()}})

	resp := performRequest(
		t,
		app,
		http.MethodPost,
		"/api/v1/contexts/not-a-uuid/rules/reorder",
		payload,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.reorder")
}

func TestHandlers_ListMatchRulesFilterByType(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)
	app.Get("/api/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules",
		contextEntity.ID.String(),
	)

	resp := performRequest(
		t,
		app,
		http.MethodGet,
		requestPath+"?type="+string(value_objects.RuleTypeExact),
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

func TestHandlers_ListMatchRulesFilterByInvalidType(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Get("/api/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath+"?type=BOGUS", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.list")
}

func TestHandlers_ListMatchRulesEmpty(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Get("/api/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules",
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

func TestHandlers_ListMatchRulesInvalidPagination(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Get("/api/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath+"?limit=bad", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.list")
	requireErrorMessage(t, resp, "invalid_request")
}

func TestHandlers_CreateMatchRuleInvalidContextID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Post("/api/v1/contexts/:contextId/rules", fixture.handler.CreateMatchRule)

	payload := mustJSON(t, entities.CreateMatchRuleInput{
		Priority: 1,
		Type:     value_objects.RuleTypeExact,
		Config:   map[string]any{"matchCurrency": true},
	})

	resp := performRequest(
		t,
		app,
		http.MethodPost,
		"/api/v1/contexts/not-a-uuid/rules",
		payload,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.create")
}

func TestHandlers_DeleteMatchRuleInvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Delete(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		fixture.handler.DeleteMatchRule,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.delete")
}

func TestHandlers_UpdateMatchRuleInvalidRuleUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Patch(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		fixture.handler.UpdateMatchRule,
	)

	payload := mustJSON(t, entities.UpdateMatchRuleInput{Priority: intPointer(2)})

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.update")
}

func TestHandlers_ReorderMatchRulesMultiple(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	ruleA := fixture.seedMatchRule(t, contextEntity.ID, 1)
	ruleB := fixture.seedMatchRule(t, contextEntity.ID, 2)
	ruleC := fixture.seedMatchRule(t, contextEntity.ID, 3)

	// Reverse the order: C, B, A
	payload := ReorderRequest{RuleIDs: []uuid.UUID{ruleC.ID, ruleB.ID, ruleA.ID}}

	app.Post("/api/v1/contexts/:contextId/rules/reorder", fixture.handler.ReorderMatchRules)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/reorder",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNoContent, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.reorder")

	// Verify the new priorities
	updatedA, err := fixture.matchRuleRepo.FindByID(
		requestContext,
		contextEntity.ID,
		ruleA.ID,
	)
	require.NoError(t, err)

	updatedB, err := fixture.matchRuleRepo.FindByID(
		requestContext,
		contextEntity.ID,
		ruleB.ID,
	)
	require.NoError(t, err)

	updatedC, err := fixture.matchRuleRepo.FindByID(
		requestContext,
		contextEntity.ID,
		ruleC.ID,
	)
	require.NoError(t, err)

	assert.Equal(t, 3, updatedA.Priority)
	assert.Equal(t, 2, updatedB.Priority)
	assert.Equal(t, 1, updatedC.Priority)
}

func TestHandlers_ListMatchRulesInvalidContextID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Get("/api/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

	resp := performRequest(
		t,
		app,
		http.MethodGet,
		"/api/v1/contexts/not-a-uuid/rules",
		nil,
	)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.list")
}

func TestHandlers_GetMatchRuleInvalidContextID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Get("/api/v1/contexts/:contextId/rules/:ruleId", fixture.handler.GetMatchRule)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/:ruleId",
		"not-a-uuid",
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.get")
}
