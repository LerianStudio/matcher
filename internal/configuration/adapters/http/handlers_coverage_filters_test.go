// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// ─── ListContexts type/status filter tests ────────────────────

func TestListContexts_TypeFilter(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	fixture.seedContext(t, tenantID)
	app.Get("/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/v1/contexts?type=1:1", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListContexts_InvalidType(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Get("/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/v1/contexts?type=INVALID_TYPE", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.list")
}

func TestListContexts_StatusFilter(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	fixture.seedContext(t, tenantID)
	app.Get("/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/v1/contexts?status=ACTIVE", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListContexts_InvalidStatus(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Get("/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/v1/contexts?status=INVALID_STATUS", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.list")
}

// ─── ListSources type filter tests ────────────────────────────

func TestListSources_TypeFilter(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedSource(t, contextEntity.ID)
	app.Get("/v1/contexts/:contextId/sources", fixture.handler.ListSources)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath+"?type=LEDGER", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListSources_InvalidType(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
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

	resp := performRequest(t, app, http.MethodGet, requestPath+"?type=INVALID_SOURCE_TYPE", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.list")
}

// ─── ListMatchRules type filter tests ─────────────────────────

func TestListMatchRules_TypeFilter(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)
	app.Get("/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/rules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath+"?type=EXACT", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestListMatchRules_InvalidType(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Get("/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/rules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath+"?type=INVALID_RULE_TYPE", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.list")
}
