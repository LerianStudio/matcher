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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ─── Schedule handler tests ──────────────────────────────────

func newScheduleFixture(t *testing.T) *handlerFixtureWithSchedule {
	t.Helper()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(
		contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo,
		command.WithScheduleRepository(scheduleRepo),
	)
	require.NoError(t, err)

	queryUseCase, err := query.NewUseCase(
		contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo,
		query.WithScheduleRepository(scheduleRepo),
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
		feeScheduleRepo,
		scheduleRepo,
		false,
	)
	require.NoError(t, err)

	return &handlerFixtureWithSchedule{
		handler:       handler,
		contextRepo:   contextRepo,
		sourceRepo:    sourceRepo,
		fieldMapRepo:  fieldMapRepo,
		matchRuleRepo: matchRuleRepo,
		scheduleRepo:  scheduleRepo,
	}
}

type handlerFixtureWithSchedule struct {
	handler       *Handler
	contextRepo   *contextRepository
	sourceRepo    *sourceRepository
	fieldMapRepo  *fieldMapRepository
	matchRuleRepo *matchRuleRepository
	scheduleRepo  *scheduleRepository
}

func (f *handlerFixtureWithSchedule) seedContext(
	t *testing.T,
	tenantID uuid.UUID,
) *entities.ReconciliationContext {
	t.Helper()

	input := entities.CreateReconciliationContextInput{
		Name:     "Test Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
	}

	contextEntity, err := entities.NewReconciliationContext(context.Background(), tenantID, input)
	require.NoError(t, err)

	stored, err := f.contextRepo.Create(context.Background(), contextEntity)
	require.NoError(t, err)

	return stored
}

func (f *handlerFixtureWithSchedule) seedSchedule(
	t *testing.T,
	contextID uuid.UUID,
) *entities.ReconciliationSchedule {
	t.Helper()

	now := time.Now().UTC()
	schedule := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      contextID,
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	stored, err := f.scheduleRepo.Create(context.Background(), schedule)
	require.NoError(t, err)

	return stored
}

// ─── CreateSchedule ──────────────────────────────────────────

func TestCreateSchedule_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/schedules", fixture.handler.CreateSchedule)

	payload := entities.CreateScheduleInput{
		CronExpression: "0 0 * * *",
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusCreated, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.create")

	var response dto.ScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "0 0 * * *", response.CronExpression)
	assert.True(t, response.Enabled)
}

func TestCreateSchedule_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/schedules", fixture.handler.CreateSchedule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.create")
}

func TestCreateSchedule_InvalidCron(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/schedules", fixture.handler.CreateSchedule)

	payload := entities.CreateScheduleInput{
		CronExpression: "not a cron expression",
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.create")
}

func TestCreateSchedule_ContextNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	app.Post("/v1/contexts/:contextId/schedules", fixture.handler.CreateSchedule)

	payload := entities.CreateScheduleInput{
		CronExpression: "0 0 * * *",
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules",
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	// Context not found triggers ErrContextNotFound → 404 from ParseAndVerifyTenantScopedID
	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.create")
}

// ─── ListSchedules ──────────────────────────────────────────

func TestListSchedules_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedSchedule(t, contextEntity.ID)

	app.Get("/v1/contexts/:contextId/schedules", fixture.handler.ListSchedules)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.list")

	var response []dto.ScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Len(t, response, 1)
}

func TestListSchedules_Empty(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newScheduleFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Get("/v1/contexts/:contextId/schedules", fixture.handler.ListSchedules)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/schedules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.schedule.list")

	var response []dto.ScheduleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Empty(t, response)
}

