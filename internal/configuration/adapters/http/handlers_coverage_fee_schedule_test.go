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
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// ─── Fee schedule handler tests with real repo ────────────────

func newFeeScheduleHandlerFixture(t *testing.T) *feeScheduleHandlerFixture {
	t.Helper()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(
		contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo,
		command.WithFeeScheduleRepository(feeRepo),
	)
	require.NoError(t, err)

	queryUseCase, err := query.NewUseCase(
		contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo,
	)
	require.NoError(t, err)

	handler, err := NewHandler(
		commandUseCase,
		queryUseCase,
		contextRepo,
		sourceRepo,
		matchRuleRepo,
		fieldMapRepo,
		feeRuleRepo,
		feeRepo,
		scheduleRepo,
		false,
	)
	require.NoError(t, err)

	return &feeScheduleHandlerFixture{
		handler: handler,
		feeRepo: feeRepo,
	}
}

type feeScheduleHandlerFixture struct {
	handler *Handler
	feeRepo *feeScheduleRepository
}

func (f *feeScheduleHandlerFixture) seedFeeSchedule(
	t *testing.T,
	tenantID uuid.UUID,
) *fee.FeeSchedule {
	t.Helper()

	now := time.Now().UTC()
	schedule := &fee.FeeSchedule{
		ID:               uuid.New(),
		TenantID:         tenantID,
		Name:             "Test Schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:        uuid.New(),
				Name:      "interchange",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromFloat(1.50)},
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	stored, err := f.feeRepo.Create(context.Background(), schedule)
	require.NoError(t, err)

	return stored
}

func TestFeeSchedule_CreateSuccess(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Post("/v1/fee-schedules", fixture.handler.CreateFeeSchedule)

	payload := dto.CreateFeeScheduleRequest{
		Name:             "New Schedule",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []dto.CreateFeeScheduleItemRequest{
			{
				Name:          "interchange",
				Priority:      1,
				StructureType: "FLAT",
				Structure:     map[string]any{"amount": "1.50"},
			},
		},
	}

	resp := performRequest(t, app, http.MethodPost, "/v1/fee-schedules", mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusCreated, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.create")

	var response dto.FeeScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "New Schedule", response.Name)
	assert.Equal(t, "USD", response.Currency)
}

func TestFeeSchedule_CreateUnauthorized(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Post("/v1/fee-schedules", fixture.handler.CreateFeeSchedule)

	payload := dto.CreateFeeScheduleRequest{
		Name:             "New Schedule",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []dto.CreateFeeScheduleItemRequest{
			{
				Name:          "interchange",
				Priority:      1,
				StructureType: "FLAT",
				Structure:     map[string]any{"amount": "1.50"},
			},
		},
	}

	resp := performRequest(t, app, http.MethodPost, "/v1/fee-schedules", mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.create")
}

func TestFeeSchedule_CreateInvalidItems(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Post("/v1/fee-schedules", fixture.handler.CreateFeeSchedule)

	payload := dto.CreateFeeScheduleRequest{
		Name:             "Bad Schedule",
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []dto.CreateFeeScheduleItemRequest{
			{
				Name:          "bad item",
				Priority:      1,
				StructureType: "UNKNOWN_TYPE",
				Structure:     map[string]any{},
			},
		},
	}

	resp := performRequest(t, app, http.MethodPost, "/v1/fee-schedules", mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.create")
}

func TestFeeSchedule_ListSuccess(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	fixture.seedFeeSchedule(t, tenantID)

	app.Get("/v1/fee-schedules", fixture.handler.ListFeeSchedules)

	resp := performRequest(t, app, http.MethodGet, "/v1/fee-schedules", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.list")

	var response []dto.FeeScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Len(t, response, 1)
}

func TestFeeSchedule_ListWithInvalidLimit(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, _ := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Get("/v1/fee-schedules", fixture.handler.ListFeeSchedules)

	// Test invalid limit defaults to 100
	resp := performRequest(t, app, http.MethodGet, "/v1/fee-schedules?limit=bad", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestFeeSchedule_GetSuccess(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Get("/v1/fee-schedules/:scheduleId", fixture.handler.GetFeeSchedule)

	resp := performRequest(t, app, http.MethodGet,
		"/v1/fee-schedules/"+schedule.ID.String(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.get")

	var response dto.FeeScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, schedule.ID.String(), response.ID)
}

func TestFeeSchedule_GetNotFound(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	tracer, recorder := newTestTracer(t)
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newFeeScheduleHandlerFixture(t)

	app.Get("/v1/fee-schedules/:scheduleId", fixture.handler.GetFeeSchedule)

	resp := performRequest(t, app, http.MethodGet,
		"/v1/fee-schedules/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_schedule.get")
	requireNotFoundResponse(t, resp, "fee schedule not found")
}

