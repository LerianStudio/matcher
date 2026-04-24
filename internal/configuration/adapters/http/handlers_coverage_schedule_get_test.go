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
)

// ─── GetSchedule ──────────────────────────────────────────

func TestGetSchedule_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedSchedule(t, contextEntity.ID)

	app.Get("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.GetSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		schedule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.get")

	var response dto.ScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, schedule.ID.String(), response.ID)
}

func TestGetSchedule_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Get("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.GetSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.get")
	requireNotFoundResponse(t, resp, "schedule not found")
}

func TestGetSchedule_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Get("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.GetSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.get")
}

func TestGetSchedule_WrongContext(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	otherContextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedSchedule(t, otherContextEntity.ID)

	app.Get("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.GetSchedule)

	// Use contextEntity but schedule belongs to otherContextEntity
	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		schedule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.get")
	requireNotFoundResponse(t, resp, "schedule not found")
}
