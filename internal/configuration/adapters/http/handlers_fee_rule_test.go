//go:build unit

package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestMapFeeRuleError_NotFound(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return mapFeeRuleError(c, fee.ErrFeeRuleNotFound)
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireNotFoundResponse(t, resp, "fee rule not found")
}

func TestMapFeeRuleError_DuplicatePriority(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return mapFeeRuleError(c, command.ErrDuplicateFeeRulePriority)
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireConflictResponse(
		t,
		resp,
		"duplicate_priority",
		command.ErrDuplicateFeeRulePriority.Error(),
	)
}

func TestMapFeeRuleError_DuplicateName(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return mapFeeRuleError(c, command.ErrDuplicateFeeRuleName)
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireConflictResponse(
		t,
		resp,
		"duplicate_name",
		command.ErrDuplicateFeeRuleName.Error(),
	)
}

func TestMapFeeRuleError_InvalidSide(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return mapFeeRuleError(c, fee.ErrInvalidMatchingSide)
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireBadRequestResponse(t, resp, "invalid_request", fee.ErrInvalidMatchingSide.Error())
}

func TestMapFeeRuleError_MissingFeeSchedule(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return mapFeeRuleError(c, fee.ErrFeeScheduleNotFound)
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireNotFoundResponse(t, resp, "fee schedule not found")
}

func TestMapFeeRuleError_WrappedClientErrors(t *testing.T) {
	t.Parallel()

	clientErrors := []struct {
		name string
		err  error
	}{
		{"name required", fee.ErrFeeRuleNameRequired},
		{"name too long", fee.ErrFeeRuleNameTooLong},
		{"schedule id required", fee.ErrFeeRuleScheduleIDRequired},
		{"context id required", fee.ErrFeeRuleContextIDRequired},
		{"priority negative", fee.ErrFeeRulePriorityNegative},
		{"too many predicates", fee.ErrFeeRuleTooManyPredicates},
		{"invalid predicate operator", fee.ErrInvalidPredicateOperator},
		{"predicate field required", fee.ErrPredicateFieldRequired},
		{"predicate value required", fee.ErrPredicateValueRequired},
		{"predicate values required", fee.ErrPredicateValuesRequired},
	}

	for _, tc := range clientErrors {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			wrapped := fmt.Errorf("validation: %w", tc.err)
			app := fiber.New()
			app.Get("/test", func(c *fiber.Ctx) error {
				return mapFeeRuleError(c, wrapped)
			})

			resp := performRequest(t, app, "GET", "/test", nil)
			defer resp.Body.Close()

			assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode,
				"expected 400 for client error: %s", tc.name)
		})
	}
}

func TestMapFeeRuleError_UnknownError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return mapFeeRuleError(c, errors.New("unexpected database failure"))
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestIsFeeRuleClientError(t *testing.T) {
	t.Parallel()

	t.Run("all client errors return true", func(t *testing.T) {
		t.Parallel()

		clientErrors := []error{
			fee.ErrFeeRuleNameRequired,
			fee.ErrFeeRuleNameTooLong,
			fee.ErrFeeRuleScheduleIDRequired,
			fee.ErrFeeRuleContextIDRequired,
			fee.ErrFeeRulePriorityNegative,
			fee.ErrFeeRuleTooManyPredicates,
			fee.ErrInvalidMatchingSide,
			fee.ErrInvalidPredicateOperator,
			fee.ErrPredicateFieldRequired,
			fee.ErrPredicateValueRequired,
			fee.ErrPredicateValuesRequired,
		}

		for _, err := range clientErrors {
			assert.True(t, isFeeRuleClientError(err),
				"expected true for %v", err)
		}
	})

	t.Run("wrapped client errors return true", func(t *testing.T) {
		t.Parallel()

		wrapped := fmt.Errorf("fee rule side: %w", fee.ErrInvalidMatchingSide)
		assert.True(t, isFeeRuleClientError(wrapped))
	})

	t.Run("server errors return false", func(t *testing.T) {
		t.Parallel()

		serverErrors := []error{
			errors.New("database connection lost"),
			context.Canceled,
			context.DeadlineExceeded,
			fee.ErrFeeRuleNotFound,
			command.ErrDuplicateFeeRulePriority,
			command.ErrDuplicateFeeRuleName,
		}

		for _, err := range serverErrors {
			assert.False(t, isFeeRuleClientError(err),
				"expected false for %v", err)
		}
	})
}

func TestMapFeeRuleError_WrappedNotFound(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		wrapped := fmt.Errorf("get fee rule: %w", fee.ErrFeeRuleNotFound)
		return mapFeeRuleError(c, wrapped)
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func TestMapFeeRuleError_WrappedDuplicatePriority(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		wrapped := fmt.Errorf("create fee rule: %w", command.ErrDuplicateFeeRulePriority)
		return mapFeeRuleError(c, wrapped)
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusConflict, resp.StatusCode)
}

func TestMapFeeRuleError_WrappedDuplicateName(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		wrapped := fmt.Errorf("create fee rule: %w", command.ErrDuplicateFeeRuleName)
		return mapFeeRuleError(c, wrapped)
	})

	resp := performRequest(t, app, "GET", "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusConflict, resp.StatusCode)
}

// TestHandlers_FeeRuleEndpoints_InvalidUUID verifies that fee rule endpoints
// that parse UUID path params return 400 for malformed IDs.
func TestHandlers_FeeRuleEndpoints_InvalidUUID(t *testing.T) {
	t.Parallel()

	t.Run("get fee rule invalid uuid", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := newTestTenantID()
		reqCtx := newRequestContext(tracer, tenantID)
		app := newTestApp(reqCtx)
		fixture := newHandlerFixture(t)

		app.Get("/v1/fee-rules/:feeRuleId", fixture.handler.GetFeeRule)

		resp := performRequest(t, app, "GET", "/v1/fee-rules/not-a-uuid", nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fee_rule.get")
	})

	t.Run("update fee rule invalid uuid", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := newTestTenantID()
		reqCtx := newRequestContext(tracer, tenantID)
		app := newTestApp(reqCtx)
		fixture := newHandlerFixture(t)

		app.Patch("/v1/fee-rules/:feeRuleId", fixture.handler.UpdateFeeRule)

		payload := mustJSON(t, map[string]any{"name": "updated"})

		resp := performRequest(t, app, "PATCH", "/v1/fee-rules/not-a-uuid", payload)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fee_rule.update")
	})

	t.Run("delete fee rule invalid uuid", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := newTestTenantID()
		reqCtx := newRequestContext(tracer, tenantID)
		app := newTestApp(reqCtx)
		fixture := newHandlerFixture(t)

		app.Delete("/v1/fee-rules/:feeRuleId", fixture.handler.DeleteFeeRule)

		resp := performRequest(t, app, "DELETE", "/v1/fee-rules/not-a-uuid", nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fee_rule.delete")
	})
}

// TestHandlers_FeeRuleEndpoints_InvalidTenant verifies that fee rule endpoints
// return 401 when the tenant ID in the context is not a valid UUID.
func TestHandlers_FeeRuleEndpoints_InvalidTenant(t *testing.T) {
	t.Parallel()

	t.Run("get fee rule invalid tenant", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
		ctx = libCommons.ContextWithTracer(ctx, tracer)
		app := newTestApp(ctx)
		fixture := newHandlerFixture(t)

		app.Get("/v1/fee-rules/:feeRuleId", fixture.handler.GetFeeRule)

		resp := performRequest(t, app, "GET", "/v1/fee-rules/"+uuid.NewString(), nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fee_rule.get")
		requireUnauthorizedResponse(t, resp)
	})

	t.Run("update fee rule invalid tenant", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
		ctx = libCommons.ContextWithTracer(ctx, tracer)
		app := newTestApp(ctx)
		fixture := newHandlerFixture(t)

		app.Patch("/v1/fee-rules/:feeRuleId", fixture.handler.UpdateFeeRule)

		payload := mustJSON(t, map[string]any{"name": "updated"})

		resp := performRequest(t, app, "PATCH", "/v1/fee-rules/"+uuid.NewString(), payload)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fee_rule.update")
		requireUnauthorizedResponse(t, resp)
	})

	t.Run("delete fee rule invalid tenant", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
		ctx = libCommons.ContextWithTracer(ctx, tracer)
		app := newTestApp(ctx)
		fixture := newHandlerFixture(t)

		app.Delete("/v1/fee-rules/:feeRuleId", fixture.handler.DeleteFeeRule)

		resp := performRequest(t, app, "DELETE", "/v1/fee-rules/"+uuid.NewString(), nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fee_rule.delete")
		requireUnauthorizedResponse(t, resp)
	})
}

// newTestTenantID is a helper to avoid repetition in fee rule tests.
func newTestTenantID() uuid.UUID {
	return uuid.New()
}

func TestCreateFeeRule_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedFeeSchedule(t, tenantID)

	app.Post("/v1/config/contexts/:contextId/fee-rules", fixture.handler.CreateFeeRule)

	payload := mustJSON(t, map[string]any{
		"side":          "ANY",
		"feeScheduleId": schedule.ID.String(),
		"name":          "catch-all",
		"priority":      0,
		"predicates":    []any{},
	})

	requestPath := replacePathParams("/v1/config/contexts/:contextId/fee-rules", contextEntity.ID.String())
	resp := performRequest(t, app, http.MethodPost, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusCreated, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.create")

	var body dto.FeeRuleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, contextEntity.ID.String(), body.ContextID)
	assert.Equal(t, schedule.ID.String(), body.FeeScheduleID)
	assert.Equal(t, "catch-all", body.Name)
	assert.Equal(t, "ANY", body.Side)
	assert.Len(t, fixture.feeRuleRepo.items, 1)
}

func TestCreateFeeRule_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Post("/v1/config/contexts/:contextId/fee-rules", fixture.handler.CreateFeeRule)

	requestPath := replacePathParams("/v1/config/contexts/:contextId/fee-rules", contextEntity.ID.String())
	resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.create")
}

func TestCreateFeeRule_InvalidFeeScheduleID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Post("/v1/config/contexts/:contextId/fee-rules", fixture.handler.CreateFeeRule)

	payload := mustJSON(t, map[string]any{
		"side":          "ANY",
		"feeScheduleId": "not-a-uuid",
		"name":          "catch-all",
		"priority":      0,
		"predicates":    []any{},
	})

	requestPath := replacePathParams("/v1/config/contexts/:contextId/fee-rules", contextEntity.ID.String())
	resp := performRequest(t, app, http.MethodPost, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.create")
}

func TestListFeeRules_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedFeeSchedule(t, tenantID)
	fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "left-rule", fee.MatchingSideLeft, 1)
	fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "right-rule", fee.MatchingSideRight, 2)

	app.Get("/v1/config/contexts/:contextId/fee-rules", fixture.handler.ListFeeRules)

	requestPath := replacePathParams("/v1/config/contexts/:contextId/fee-rules", contextEntity.ID.String())
	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.list")

	var body []dto.FeeRuleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Len(t, body, 2)
}

func TestGetFeeRule_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedFeeSchedule(t, tenantID)
	rule := fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "lookup-rule", fee.MatchingSideAny, 1)

	app.Get("/v1/fee-rules/:feeRuleId", fixture.handler.GetFeeRule)

	resp := performRequest(t, app, http.MethodGet, "/v1/fee-rules/"+rule.ID.String(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.get")

	var body dto.FeeRuleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, rule.ID.String(), body.ID)
	assert.Equal(t, contextEntity.ID.String(), body.ContextID)
}

func TestGetFeeRule_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	// Override FindByID to return (nil, nil), exercising the nil-result guard in the handler.
	fixture.feeRuleRepo.findByIDOverride = func(_ context.Context, _ uuid.UUID) (*fee.FeeRule, error) {
		return nil, nil
	}

	app.Get("/v1/fee-rules/:feeRuleId", fixture.handler.GetFeeRule)

	resp := performRequest(t, app, http.MethodGet, "/v1/fee-rules/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.get")
	requireNotFoundResponse(t, resp, "fee rule not found")
}

func TestUpdateFeeRule_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedFeeSchedule(t, tenantID)
	rule := fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "before-update", fee.MatchingSideAny, 1)

	app.Patch("/v1/fee-rules/:feeRuleId", fixture.handler.UpdateFeeRule)

	payload := mustJSON(t, map[string]any{
		"name":     "after-update",
		"priority": 2,
	})

	resp := performRequest(t, app, http.MethodPatch, "/v1/fee-rules/"+rule.ID.String(), payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.update")

	var body dto.FeeRuleResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "after-update", body.Name)
	assert.Equal(t, 2, body.Priority)
	assert.Equal(t, "after-update", fixture.feeRuleRepo.items[rule.ID].Name)
}

func TestUpdateFeeRule_TooManyPredicates(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedFeeSchedule(t, tenantID)
	rule := fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "before-update", fee.MatchingSideAny, 1)

	app.Patch("/v1/fee-rules/:feeRuleId", fixture.handler.UpdateFeeRule)

	predicates := make([]map[string]any, 0, 51)
	for i := 0; i < 51; i++ {
		predicates = append(predicates, map[string]any{
			"field":    "institution",
			"operator": "EXISTS",
		})
	}

	payload := mustJSON(t, map[string]any{"predicates": predicates})
	resp := performRequest(t, app, http.MethodPatch, "/v1/fee-rules/"+rule.ID.String(), payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.update")
}

func TestDeleteFeeRule_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := newTestTenantID()
	reqCtx := newRequestContext(tracer, tenantID)
	app := newTestApp(reqCtx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	schedule := fixture.seedFeeSchedule(t, tenantID)
	rule := fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "delete-me", fee.MatchingSideAny, 1)

	app.Delete("/v1/fee-rules/:feeRuleId", fixture.handler.DeleteFeeRule)

	resp := performRequest(t, app, http.MethodDelete, "/v1/fee-rules/"+rule.ID.String(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNoContent, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fee_rule.delete")
	_, exists := fixture.feeRuleRepo.items[rule.ID]
	assert.False(t, exists)
}

func TestFeeRuleEndpoints_ForbiddenForContextOutsideTenant(t *testing.T) {
	t.Parallel()

	ownerTenantID := uuid.New()
	requestTenantID := uuid.New()
	require.NotEqual(t, ownerTenantID, requestTenantID)

	tracer, _ := newTestTracer(t)
	ctx := newRequestContext(tracer, requestTenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, ownerTenantID)
	schedule := fixture.seedFeeSchedule(t, ownerTenantID)
	rule := fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "foreign-rule", fee.MatchingSideAny, 1)

	app.Get("/v1/fee-rules/:feeRuleId", fixture.handler.GetFeeRule)
	app.Patch("/v1/fee-rules/:feeRuleId", fixture.handler.UpdateFeeRule)
	app.Delete("/v1/fee-rules/:feeRuleId", fixture.handler.DeleteFeeRule)

	tests := []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{name: "get", method: http.MethodGet, path: "/v1/fee-rules/" + rule.ID.String()},
		{name: "update", method: http.MethodPatch, path: "/v1/fee-rules/" + rule.ID.String(), body: mustJSON(t, map[string]any{"name": "blocked"})},
		{name: "delete", method: http.MethodDelete, path: "/v1/fee-rules/" + rule.ID.String()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := performRequest(t, app, tt.method, tt.path, tt.body)
			defer resp.Body.Close()
			require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		})
	}
}
