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

func TestFeeSchedule_DeleteSuccess(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Delete("/v1/fee-schedules/:scheduleId", fixture.handler.DeleteFeeSchedule)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/fee-schedules/"+schedule.ID.String(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNoContent, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.delete")
}

func TestFeeSchedule_DeleteNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Delete("/v1/fee-schedules/:scheduleId", fixture.handler.DeleteFeeSchedule)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/fee-schedules/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.delete")
	requireNotFoundResponse(t, resp, "fee schedule not found")
}

func TestFeeSchedule_SimulateSuccess(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Post("/v1/fee-schedules/:scheduleId/simulate", fixture.handler.SimulateFeeSchedule)

	payload := dto.SimulateFeeRequest{
		GrossAmount: "100.00",
		Currency:    "USD",
	}

	resp := performRequest(t, app, http.MethodPost,
		"/v1/fee-schedules/"+schedule.ID.String()+"/simulate", mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.simulate")

	var response dto.SimulateFeeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "100", response.GrossAmount)
	assert.Equal(t, "USD", response.Currency)
}

func TestFeeSchedule_SimulateNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Post("/v1/fee-schedules/:scheduleId/simulate", fixture.handler.SimulateFeeSchedule)

	payload := dto.SimulateFeeRequest{
		GrossAmount: "100.00",
		Currency:    "USD",
	}

	resp := performRequest(t, app, http.MethodPost,
		"/v1/fee-schedules/"+uuid.NewString()+"/simulate", mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.simulate")
	requireNotFoundResponse(t, resp, "fee schedule not found")
}

func TestFeeSchedule_SimulateInvalidPayload(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Post("/v1/fee-schedules/:scheduleId/simulate", fixture.handler.SimulateFeeSchedule)

	resp := performRequest(t, app, http.MethodPost,
		"/v1/fee-schedules/"+schedule.ID.String()+"/simulate", []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.simulate")
}

func TestFeeSchedule_SimulateInvalidGrossAmount(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Post("/v1/fee-schedules/:scheduleId/simulate", fixture.handler.SimulateFeeSchedule)

	payload := dto.SimulateFeeRequest{
		GrossAmount: "not_a_number",
		Currency:    "USD",
	}

	resp := performRequest(t, app, http.MethodPost,
		"/v1/fee-schedules/"+schedule.ID.String()+"/simulate", mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.simulate")
}

func TestFeeSchedule_SimulateCurrencyMismatch(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Post("/v1/fee-schedules/:scheduleId/simulate", fixture.handler.SimulateFeeSchedule)

	payload := dto.SimulateFeeRequest{
		GrossAmount: "100.00",
		Currency:    "EUR", // Mismatch: schedule is USD
	}

	resp := performRequest(t, app, http.MethodPost,
		"/v1/fee-schedules/"+schedule.ID.String()+"/simulate", mustJSON(t, payload))
	defer resp.Body.Close()

	// Currency mismatch is a client error
	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.simulate")
}

func TestFeeSchedule_ListUnauthorized(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Get("/v1/fee-schedules", fixture.handler.ListFeeSchedules)

	resp := performRequest(t, app, http.MethodGet, "/v1/fee-schedules", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.list")
}

func TestFeeSchedule_GetUnauthorized(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Get("/v1/fee-schedules/:scheduleId", fixture.handler.GetFeeSchedule)

	resp := performRequest(t, app, http.MethodGet,
		"/v1/fee-schedules/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.get")
}

func TestFeeSchedule_DeleteUnauthorized(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Delete("/v1/fee-schedules/:scheduleId", fixture.handler.DeleteFeeSchedule)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/fee-schedules/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.delete")
}

func TestFeeSchedule_SimulateUnauthorized(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Post("/v1/fee-schedules/:scheduleId/simulate", fixture.handler.SimulateFeeSchedule)

	payload := dto.SimulateFeeRequest{
		GrossAmount: "100.00",
		Currency:    "USD",
	}

	resp := performRequest(t, app, http.MethodPost,
		"/v1/fee-schedules/"+uuid.NewString()+"/simulate", mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.simulate")
}

func TestFeeSchedule_ListEmpty(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Get("/v1/fee-schedules", fixture.handler.ListFeeSchedules)

	resp := performRequest(t, app, http.MethodGet, "/v1/fee-schedules", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.list")

	var response []dto.FeeScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Empty(t, response)
}
