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
)

func TestFeeSchedule_UpdateSuccess(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Patch("/v1/fee-schedules/:scheduleId", fixture.handler.UpdateFeeSchedule)

	updatedName := "Updated Schedule"
	payload := dto.UpdateFeeScheduleRequest{
		Name: &updatedName,
	}

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/fee-schedules/"+schedule.ID.String(), mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.update")

	var response dto.FeeScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "Updated Schedule", response.Name)
}

func TestFeeSchedule_UpdateNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Patch("/v1/fee-schedules/:scheduleId", fixture.handler.UpdateFeeSchedule)

	updatedName := "Updated"
	payload := dto.UpdateFeeScheduleRequest{
		Name: &updatedName,
	}

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/fee-schedules/"+uuid.NewString(), mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.update")
	requireNotFoundResponse(t, resp, "fee schedule not found")
}

func TestFeeSchedule_UpdateInvalidPayload(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Patch("/v1/fee-schedules/:scheduleId", fixture.handler.UpdateFeeSchedule)

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/fee-schedules/"+schedule.ID.String(), []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.update")
}

func TestFeeSchedule_UpdateInvalidUUID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Patch("/v1/fee-schedules/:scheduleId", fixture.handler.UpdateFeeSchedule)

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/fee-schedules/not-a-uuid", mustJSON(t, map[string]string{}))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.update")
}

func TestFeeSchedule_UpdateUnauthorized(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Patch("/v1/fee-schedules/:scheduleId", fixture.handler.UpdateFeeSchedule)

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/fee-schedules/"+uuid.NewString(), mustJSON(t, map[string]string{}))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.update")
}
