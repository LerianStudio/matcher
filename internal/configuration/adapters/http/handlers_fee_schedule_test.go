//go:build unit

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func newFeeScheduleTestContext(tenantID uuid.UUID) context.Context {
	ctx := context.Background()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracer := tp.Tracer("test")
	ctx = libCommons.ContextWithTracer(ctx, tracer)

	if tenantID != uuid.Nil {
		ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	}

	return ctx
}

func newFeeScheduleTestApp(ctx context.Context) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.SetUserContext(ctx)

		return c.Next()
	})

	return app
}

func TestCreateFeeSchedule_Handler(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := newFeeScheduleTestContext(tenantID)
	app := newFeeScheduleTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Post("/v1/config/fee-schedules", fixture.handler.CreateFeeSchedule)

	payload := dto.CreateFeeScheduleRequest{
		Name:             "Test Schedule",
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

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/config/fee-schedules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	// Without a fee schedule repo wired in the fixture, the command service returns a nil repo error (internal)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"expected 500 when fee schedule repo is nil, got %d", resp.StatusCode)
}

func TestListFeeSchedules_Handler(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := newFeeScheduleTestContext(tenantID)
	app := newFeeScheduleTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Get("/v1/config/fee-schedules", fixture.handler.ListFeeSchedules)

	req := httptest.NewRequest(http.MethodGet, "/v1/config/fee-schedules", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)

	// Without fee schedule repo, this returns an internal server error
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"expected 500 when fee schedule repo is nil, got %d", resp.StatusCode)
}

func TestGetFeeSchedule_Handler_InvalidID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := newFeeScheduleTestContext(tenantID)
	app := newFeeScheduleTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Get("/v1/config/fee-schedules/:scheduleId", fixture.handler.GetFeeSchedule)

	req := httptest.NewRequest(http.MethodGet, "/v1/config/fee-schedules/invalid-uuid", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDeleteFeeSchedule_Handler_InvalidID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := newFeeScheduleTestContext(tenantID)
	app := newFeeScheduleTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Delete("/v1/config/fee-schedules/:scheduleId", fixture.handler.DeleteFeeSchedule)

	req := httptest.NewRequest(http.MethodDelete, "/v1/config/fee-schedules/invalid-uuid", nil)

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSimulateFeeSchedule_Handler_InvalidID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := newFeeScheduleTestContext(tenantID)
	app := newFeeScheduleTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Post("/v1/config/fee-schedules/:scheduleId/simulate", fixture.handler.SimulateFeeSchedule)

	payload := dto.SimulateFeeRequest{
		GrossAmount: "100.00",
		Currency:    "USD",
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/config/fee-schedules/invalid-uuid/simulate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestFeeScheduleToResponse_WithHandler(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	scheduleID := uuid.New()
	tenantID := uuid.New()
	itemID := uuid.New()

	schedule := &fee.FeeSchedule{
		ID:               scheduleID,
		TenantID:         tenantID,
		Name:             "Test Schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:        itemID,
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

	response := dto.FeeScheduleToResponse(schedule)

	assert.Equal(t, scheduleID.String(), response.ID)
	assert.Equal(t, tenantID.String(), response.TenantID)
	assert.Equal(t, "Test Schedule", response.Name)
	assert.Equal(t, "USD", response.Currency)
	assert.Equal(t, "PARALLEL", response.ApplicationOrder)
	assert.Equal(t, 2, response.RoundingScale)
	assert.Equal(t, "HALF_UP", response.RoundingMode)
	assert.Len(t, response.Items, 1)
	assert.Equal(t, itemID.String(), response.Items[0].ID)
	assert.Equal(t, "interchange", response.Items[0].Name)
	assert.Equal(t, "FLAT", response.Items[0].StructureType)
}

func TestIsFeeScheduleClientError(t *testing.T) {
	t.Parallel()

	assert.True(t, isFeeScheduleClientError(fee.ErrScheduleNameRequired))
	assert.True(t, isFeeScheduleClientError(fee.ErrInvalidApplicationOrder))
	assert.True(t, isFeeScheduleClientError(fee.ErrCurrencyMismatch))
	assert.False(t, isFeeScheduleClientError(context.Canceled))
}
