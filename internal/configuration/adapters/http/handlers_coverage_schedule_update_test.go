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
)

// ─── UpdateSchedule ──────────────────────────────────────────

func TestUpdateSchedule_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedSchedule(t, contextEntity.ID)

	app.Patch("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.UpdateSchedule)

	newCron := "30 6 * * *"
	payload := entities.UpdateScheduleInput{
		CronExpression: &newCron,
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		schedule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.update")

	var response dto.ScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "30 6 * * *", response.CronExpression)
}

func TestUpdateSchedule_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Patch("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.UpdateSchedule)

	newCron := "30 6 * * *"
	payload := entities.UpdateScheduleInput{
		CronExpression: &newCron,
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.update")
	requireNotFoundResponse(t, resp, "schedule not found")
}

func TestUpdateSchedule_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Patch("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.UpdateSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, map[string]string{}))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.update")
}

func TestUpdateSchedule_WrongContext(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	otherContextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedSchedule(t, otherContextEntity.ID)

	app.Patch("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.UpdateSchedule)

	newCron := "30 6 * * *"
	payload := entities.UpdateScheduleInput{
		CronExpression: &newCron,
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		schedule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.update")
}

func TestUpdateSchedule_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedSchedule(t, contextEntity.ID)

	app.Patch("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.UpdateSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		schedule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.update")
}

func TestUpdateSchedule_InvalidCron(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedSchedule(t, contextEntity.ID)

	app.Patch("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.UpdateSchedule)

	invalidCron := "not valid cron"
	payload := entities.UpdateScheduleInput{
		CronExpression: &invalidCron,
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		schedule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.update")
}

// ─── DeleteSchedule ──────────────────────────────────────────

func TestDeleteSchedule_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedSchedule(t, contextEntity.ID)

	app.Delete("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.DeleteSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		schedule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNoContent, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.delete")
}

func TestDeleteSchedule_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Delete("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.DeleteSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.delete")
	requireNotFoundResponse(t, resp, "schedule not found")
}

func TestDeleteSchedule_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Delete("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.DeleteSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.delete")
}

func TestDeleteSchedule_WrongContext(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	otherContextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedSchedule(t, otherContextEntity.ID)

	app.Delete("/v1/contexts/:contextId/schedules/:scheduleId", fixture.handler.DeleteSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules/:scheduleId",
		contextEntity.ID.String(),
		schedule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.delete")
}
