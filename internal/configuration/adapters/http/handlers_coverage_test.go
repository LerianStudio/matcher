//go:build unit

package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/shopspring/decimal"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// ─── boolDefault tests ────────────────────────────────────────

func TestBoolDefault_NilReturnDefault(t *testing.T) {
	t.Parallel()

	assert.True(t, boolDefault(nil, true))
	assert.False(t, boolDefault(nil, false))
}

func TestBoolDefault_NonNilReturnValue(t *testing.T) {
	t.Parallel()

	trueVal := true
	falseVal := false

	assert.True(t, boolDefault(&trueVal, false))
	assert.False(t, boolDefault(&falseVal, true))
}

// ─── handleContextVerificationError branch tests ──────────────

func TestHandleContextVerificationError_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "ErrMissingContextID returns 400",
			err:            libHTTP.ErrMissingContextID,
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "invalid_context_id",
		},
		{
			name:           "ErrInvalidContextID returns 400",
			err:            libHTTP.ErrInvalidContextID,
			expectedStatus: fiber.StatusBadRequest,
			expectedCode:   "invalid_context_id",
		},
		{
			name:           "ErrTenantIDNotFound returns 401",
			err:            libHTTP.ErrTenantIDNotFound,
			expectedStatus: fiber.StatusUnauthorized,
			expectedCode:   "unauthorized",
		},
		{
			name:           "ErrInvalidTenantID returns 401",
			err:            libHTTP.ErrInvalidTenantID,
			expectedStatus: fiber.StatusUnauthorized,
			expectedCode:   "unauthorized",
		},
		{
			name:           "ErrContextNotFound returns 404",
			err:            libHTTP.ErrContextNotFound,
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "configuration_context_not_found",
		},
		{
			name:           "libHTTP.ErrContextNotActive returns 403",
			err:            libHTTP.ErrContextNotActive,
			expectedStatus: fiber.StatusForbidden,
			expectedCode:   "context_not_active",
		},
		{
			name:           "libHTTP.ErrContextNotOwned returns 403",
			err:            libHTTP.ErrContextNotOwned,
			expectedStatus: fiber.StatusForbidden,
			expectedCode:   "forbidden",
		},
		{
			name:           "libHTTP.ErrContextAccessDenied returns 403",
			err:            libHTTP.ErrContextAccessDenied,
			expectedStatus: fiber.StatusForbidden,
			expectedCode:   "forbidden",
		},
		{
			name:           "generic error returns 500",
			err:            errors.New("random infrastructure failure"),
			expectedStatus: fiber.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracer, _ := newTestTracer(t)
			_, span := tracer.Start(context.Background(), "test")

			defer span.End()

			app := fiber.New()
			app.Get("/test", func(c *fiber.Ctx) error {
				return (&Handler{}).handleContextVerificationError(c.UserContext(), c, span, &libLog.NopLogger{}, tt.err)
			})

			resp := performRequest(t, app, http.MethodGet, "/test", nil)
			defer resp.Body.Close()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedCode != "" {
				var payload map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
				assert.Equal(t, expectedConfigurationCode(tt.expectedCode), payload["code"])
				assert.Equal(t, http.StatusText(tt.expectedStatus), payload["title"])
			}
		})
	}
}

// ─── handleOwnershipVerificationError tests ───────────────────

func TestHandleOwnershipVerificationError_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedCode   string
	}{
		{
			name:           "ErrContextNotFound returns 404",
			err:            libHTTP.ErrContextNotFound,
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "configuration_field_map_not_found",
		},
		{
			name:           "libHTTP.ErrContextNotOwned returns 404",
			err:            libHTTP.ErrContextNotOwned,
			expectedStatus: fiber.StatusNotFound,
			expectedCode:   "configuration_field_map_not_found",
		},
		{
			name:           "libHTTP.ErrContextAccessDenied returns 403",
			err:            libHTTP.ErrContextAccessDenied,
			expectedStatus: fiber.StatusForbidden,
			expectedCode:   "forbidden",
		},
		{
			name:           "generic error returns 500",
			err:            errors.New("database connection lost"),
			expectedStatus: fiber.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracer, _ := newTestTracer(t)
			_, span := tracer.Start(context.Background(), "test")

			defer span.End()

			app := fiber.New()
			app.Get("/test", func(c *fiber.Ctx) error {
				return (&Handler{}).handleOwnershipVerificationError(c.UserContext(), c, span, &libLog.NopLogger{}, tt.err, "configuration_field_map_not_found", "field map not found")
			})

			resp := performRequest(t, app, http.MethodGet, "/test", nil)
			defer resp.Body.Close()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedCode != "" {
				var payload map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
				assert.Equal(t, expectedConfigurationCode(tt.expectedCode), payload["code"])
				assert.Equal(t, http.StatusText(tt.expectedStatus), payload["title"])
			}
		})
	}
}

// ─── ensureSourceAccess tests ─────────────────────────────────

func TestEnsureSourceAccess_Success(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)

	_, span := tracer.Start(ctx, "test")
	defer span.End()

	app := fiber.New()
	app.Get("/test/:contextId/:sourceId", func(c *fiber.Ctx) error {
		return fixture.handler.ensureSourceAccess(ctx, c, span, &libLog.NopLogger{}, contextEntity.ID, sourceEntity.ID)
	})

	resp := performRequest(t, app, http.MethodGet,
		"/test/"+contextEntity.ID.String()+"/"+sourceEntity.ID.String(), nil)
	defer resp.Body.Close()

	// nil return means success (no error response written)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestEnsureSourceAccess_NotFound(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	missingSourceID := uuid.New()

	_, span := tracer.Start(ctx, "test")
	defer span.End()

	// ensureSourceAccess writes the 404 response to the Fiber context and returns nil
	// (the Fiber JSON write returns nil on success). The pattern in handlers is:
	//   if err := handler.ensureSourceAccess(...); err != nil { return err }
	// Here we test that ensureSourceAccess returns nil even on source-not-found,
	// and that the 404 response body was written to the context.
	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		err := fixture.handler.ensureSourceAccess(ctx, c, span, &libLog.NopLogger{}, contextEntity.ID, missingSourceID)
		// writeNotFound writes the 404 JSON response and returns nil from c.Status().JSON()
		// So err is nil here. The handler should return nil (or the err value) to let
		// Fiber send the already-written 404 response.
		return err
	})

	resp := performRequest(t, app, http.MethodGet, "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireNotFoundResponse(t, resp, "source not found")
}

// ─── CloneContext handler tests ───────────────────────────────

func TestCloneContext_Success(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedSource(t, contextEntity.ID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)
	schedule := fixture.seedFeeSchedule(t, tenantID)
	fixture.seedFeeRule(t, contextEntity.ID, schedule.ID, "cloned-fee-rule", fee.MatchingSideAny, 1)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	payload := dto.CloneContextRequest{
		Name: "Cloned Context",
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusCreated, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")

	var response dto.CloneContextResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "Cloned Context", response.Context.Name)
	assert.Equal(t, 1, response.SourcesCloned)
	assert.Equal(t, 1, response.RulesCloned)
	assert.Equal(t, 1, response.FeeRulesCloned)
}

func TestCloneContext_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")
}

func TestCloneContext_ContextNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	payload := dto.CloneContextRequest{
		Name: "Cloned Context",
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")
	requireNotFoundResponse(t, resp, "context not found")
}

func TestCloneContext_EmptyName(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	// send name that is empty after validation - the validator requires min=1
	// but passing an empty string to the command triggers ErrCloneNameRequired
	// Let's pass valid JSON but with empty name to trigger validation
	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		contextEntity.ID.String(),
	)

	// Note: the validator will catch empty name (min=1), so we get 400 from libHTTP.ParseBodyAndValidate
	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, map[string]string{"name": ""}))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")
}

func TestCloneContext_WithBoolDefaults(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedSource(t, contextEntity.ID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)

	app.Post("/v1/contexts/:contextId/clone", fixture.handler.CloneContext)

	falseVal := false
	payload := dto.CloneContextRequest{
		Name:           "Cloned NoSources",
		IncludeSources: &falseVal,
		IncludeRules:   &falseVal,
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/clone",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusCreated, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.clone")

	var response dto.CloneContextResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, "Cloned NoSources", response.Context.Name)
	assert.Equal(t, 0, response.SourcesCloned)
	assert.Equal(t, 0, response.RulesCloned)
	assert.Equal(t, 0, response.FeeRulesCloned)
}

// ─── UpdateFieldMap error path tests ──────────────────────────

func TestUpdateFieldMap_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Patch("/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	payload := mustJSON(t, entities.UpdateFieldMapInput{
		Mapping: map[string]any{"field": "updated"},
	})

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/field-maps/"+uuid.NewString(), payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
	requireNotFoundResponse(t, resp, "field map not found")
}

func TestUpdateFieldMap_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed a field map to pass the UUID parsing
	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Patch("/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/field-maps/"+fieldMap.ID.String(), []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
}

func TestUpdateFieldMap_UnauthorizedTenant(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	// Set up context with invalid tenant ID
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Patch("/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	payload := mustJSON(t, entities.UpdateFieldMapInput{
		Mapping: map[string]any{"field": "updated"},
	})

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/field-maps/"+uuid.NewString(), payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
}

func TestUpdateFieldMap_OwnershipDenied(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	differentTenantID := uuid.New()
	ctx := newRequestContext(tracer, differentTenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed with original tenant
	originalTenantID := uuid.New()
	contextEntity := fixture.seedContext(t, originalTenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Patch("/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

	payload := mustJSON(t, entities.UpdateFieldMapInput{
		Mapping: map[string]any{"field": "updated"},
	})

	resp := performRequest(t, app, http.MethodPatch,
		"/v1/field-maps/"+fieldMap.ID.String(), payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.update")
	requireResourceNotFoundResponse(t, resp, "field map not found")
}

// ─── DeleteFieldMap error path tests ──────────────────────────

func TestDeleteFieldMap_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Delete("/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/field-maps/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
	requireNotFoundResponse(t, resp, "field map not found")
}

func TestDeleteFieldMap_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Delete("/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/field-maps/not-a-uuid", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
}

func TestDeleteFieldMap_UnauthorizedTenant(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")
	ctx = libCommons.ContextWithTracer(ctx, tracer)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Delete("/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/field-maps/"+uuid.NewString(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
}

func TestDeleteFieldMap_OwnershipDenied(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	differentTenantID := uuid.New()
	ctx := newRequestContext(tracer, differentTenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	originalTenantID := uuid.New()
	contextEntity := fixture.seedContext(t, originalTenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

	app.Delete("/v1/field-maps/:fieldMapId", fixture.handler.DeleteFieldMap)

	resp := performRequest(t, app, http.MethodDelete,
		"/v1/field-maps/"+fieldMap.ID.String(), nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.delete")
	requireResourceNotFoundResponse(t, resp, "field map not found")
}

// ─── NewHandler tests ─────────────────────────────────────────

func TestNewHandler_NilCommandUseCase(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(nil, queryUseCase, false)
	require.ErrorIs(t, err, ErrNilCommandUseCase)
}

func TestNewHandler_NilQueryUseCase(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, nil, false)
	require.ErrorIs(t, err, ErrNilQueryUseCase)
}

// ─── safeClientMessage tests ──────────────────────────────────

func TestSafeClientMessage_NilError(t *testing.T) {
	t.Parallel()

	result := safeClientMessage("default message", nil)
	assert.Equal(t, "default message", result)
}

func TestSafeClientMessage_UnsafeError(t *testing.T) {
	t.Parallel()

	result := safeClientMessage("default message", errors.New("internal problem"))
	assert.Equal(t, "default message", result)
}

func TestSafeClientMessage_SafeError(t *testing.T) {
	t.Parallel()

	result := safeClientMessage("default message", entities.ErrContextNameRequired)
	assert.Equal(t, entities.ErrContextNameRequired.Error(), result)
}

// ─── writeServiceError tests ──────────────────────────────────

func TestWriteServiceError_SafeClientError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return writeServiceError(c, entities.ErrContextNameRequired)
	})

	resp := performRequest(t, app, http.MethodGet, "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestWriteServiceError_InternalError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return writeServiceError(c, errors.New("some infrastructure error"))
	})

	resp := performRequest(t, app, http.MethodGet, "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

// ─── isClientSafeError tests ──────────────────────────────────

func TestIsClientSafeError_Comprehensive(t *testing.T) {
	t.Parallel()

	safeErrors := []error{
		dto.ErrDeprecatedRateID,
		entities.ErrNilReconciliationContext,
		entities.ErrContextNameRequired,
		entities.ErrContextNameTooLong,
		entities.ErrContextTypeInvalid,
		entities.ErrContextStatusInvalid,
		entities.ErrContextIntervalRequired,
		entities.ErrContextTenantRequired,
		entities.ErrSourceNameRequired,
		entities.ErrSourceNameTooLong,
		entities.ErrSourceTypeInvalid,
		entities.ErrSourceContextRequired,
		entities.ErrFieldMapNil,
		entities.ErrFieldMapContextRequired,
		entities.ErrFieldMapSourceRequired,
		entities.ErrFieldMapMappingRequired,
		entities.ErrFieldMapMappingValueEmpty,
		entities.ErrMatchRuleNil,
		entities.ErrRuleContextRequired,
		entities.ErrRulePriorityInvalid,
		entities.ErrRuleTypeInvalid,
		entities.ErrRuleConfigRequired,
		entities.ErrRuleConfigMissingRequiredKeys,
		entities.ErrRulePriorityConflict,
	}

	for _, safeErr := range safeErrors {
		assert.True(t, isClientSafeError(safeErr), "expected %v to be client safe", safeErr)
	}

	assert.False(t, isClientSafeError(errors.New("random error")))
	assert.False(t, isClientSafeError(sql.ErrNoRows))
}

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

// ─── Schedule handler tests ──────────────────────────────────

func newScheduleFixture(t *testing.T) *handlerFixtureWithSchedule {
	t.Helper()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
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

	handler, err := NewHandler(commandUseCase, queryUseCase, false)
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
		Type:     value_objects.ContextTypeOneToOne,
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

// ─── UpdateMatchRule priority conflict test ───────────────────

func TestUpdateMatchRule_PriorityConflict(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)
	ruleToUpdate := fixture.seedMatchRule(t, contextEntity.ID, 2)

	app.Patch("/v1/contexts/:contextId/rules/:ruleId", fixture.handler.UpdateMatchRule)

	// Try to update rule 2 to have priority 1 (conflict)
	payload := entities.UpdateMatchRuleInput{Priority: intPointer(1)}
	requestPath := replacePathParams(
		"/v1/contexts/:contextId/rules/:ruleId",
		contextEntity.ID.String(),
		ruleToUpdate.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.update")
	requireConflictResponse(t, resp, "priority_conflict", entities.ErrRulePriorityConflict.Error())
}

// ─── UpdateContext invalid payload test ───────────────────────

func TestUpdateContext_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Patch("/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}

func TestCreateContext_DeprecatedRateID_ReturnsExplicitBadRequest(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	app.Post("/v1/config/contexts", fixture.handler.CreateContext)

	payload := []byte(`{"name":"Context","type":"1:1","interval":"daily","rateId":"550e8400-e29b-41d4-a716-446655440000"}`)
	resp := performRequest(t, app, http.MethodPost, "/v1/config/contexts", payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	var responsePayload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&responsePayload))
	require.Equal(t, expectedConfigurationCode("invalid_request"), responsePayload["code"])
	require.Equal(t, http.StatusText(http.StatusBadRequest), responsePayload["title"])
	assert.Contains(t, responsePayload["message"], dto.ErrDeprecatedRateID.Error())
	requireSpanName(t, recorder, "handler.context.create")
}

func TestUpdateContext_DeprecatedRateID_ReturnsExplicitBadRequest(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)
	contextEntity := fixture.seedContext(t, tenantID)

	app.Patch("/v1/config/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/config/contexts/:contextId",
		contextEntity.ID.String(),
	)
	payload := []byte(`{"status":"ACTIVE","rateId":"550e8400-e29b-41d4-a716-446655440000"}`)
	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	var responsePayload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&responsePayload))
	require.Equal(t, expectedConfigurationCode("invalid_request"), responsePayload["code"])
	require.Equal(t, http.StatusText(http.StatusBadRequest), responsePayload["title"])
	assert.Contains(t, responsePayload["message"], dto.ErrDeprecatedRateID.Error())
	requireSpanName(t, recorder, "handler.context.update")
}

func TestUpdateContext_InvalidStateTransition_Returns409(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed a DRAFT context; PAUSED is only reachable from ACTIVE, so
	// requesting status=PAUSED on a DRAFT triggers ErrInvalidStateTransition.
	contextEntity := fixture.seedContext(t, tenantID)

	app.Patch("/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	paused := value_objects.ContextStatusPaused
	payload := entities.UpdateReconciliationContextInput{Status: &paused}

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}

func TestUpdateContext_ArchivedContextCannotBeModified_Returns409(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed a context and force its status to ARCHIVED directly in the repo
	// so we can test the guard without running through the archival transition.
	contextEntity := fixture.seedContext(t, tenantID)
	contextEntity.Status = value_objects.ContextStatusArchived
	fixture.contextRepo.items[contextEntity.ID] = contextEntity

	app.Patch("/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	newName := "Updated Name"
	payload := entities.UpdateReconciliationContextInput{Name: &newName}

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}

func TestUpdateContext_SameStatus_IsNoOp_Returns200(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Seed a context and force ACTIVE status to exercise a valid same-status no-op.
	contextEntity := fixture.seedContext(t, tenantID)
	contextEntity.Status = value_objects.ContextStatusActive
	fixture.contextRepo.items[contextEntity.ID] = contextEntity

	app.Patch("/v1/contexts/:contextId", fixture.handler.UpdateContext)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId",
		contextEntity.ID.String(),
	)

	activeStatus := "ACTIVE"
	payload := dto.UpdateContextRequest{Status: &activeStatus}

	resp := performRequest(t, app, http.MethodPatch, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.update")
}

// ─── UpdateSource error path tests ────────────────────────────

func TestUpdateSource_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Patch("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.UpdateSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	payload := mustJSON(t, entities.UpdateReconciliationSourceInput{
		Name: stringPointer("Updated"),
	})

	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
}

func TestUpdateSource_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)
	app.Patch("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.UpdateSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		sourceEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
}

func TestUpdateSource_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Patch("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.UpdateSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	payload := mustJSON(t, entities.UpdateReconciliationSourceInput{
		Name: stringPointer("Updated"),
	})

	resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.update")
	requireNotFoundResponse(t, resp, "source not found")
}

// ─── DeleteSource error path tests ────────────────────────────

func TestDeleteSource_InvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Delete("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.DeleteSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.delete")
}

func TestDeleteSource_NotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	app.Delete("/v1/contexts/:contextId/sources/:sourceId", fixture.handler.DeleteSource)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.delete")
	requireNotFoundResponse(t, resp, "source not found")
}

// ─── UpdateMatchRule invalid payload test ─────────────────────

func TestUpdateMatchRule_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	rule := fixture.seedMatchRule(t, contextEntity.ID, 1)
	app.Patch("/v1/contexts/:contextId/rules/:ruleId", fixture.handler.UpdateMatchRule)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/rules/:ruleId",
		contextEntity.ID.String(),
		rule.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPatch, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.update")
}

// ─── CreateFieldMap source invalid UUID test ──────────────────

func TestCreateFieldMap_InvalidSourceUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/sources/:sourceId/field-maps", fixture.handler.CreateFieldMap)

	payload := entities.CreateFieldMapInput{
		Mapping: map[string]any{"field": "value"},
	}

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/sources/:sourceId/field-maps",
		contextEntity.ID.String(),
		"not-a-uuid",
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.create")
}

// ─── ReorderMatchRules invalid payload ────────────────────────

func TestReorderMatchRules_InvalidPayload(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Post("/v1/contexts/:contextId/rules/reorder", fixture.handler.ReorderMatchRules)

	requestPath := replacePathParams(
		"/v1/contexts/:contextId/rules/reorder",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.reorder")
}

// ─── ListContexts nil result test ─────────────────────────────

func TestListContexts_NilResult(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	tenantID := uuid.New()
	ctx := newRequestContext(tracer, tenantID)
	app := newTestApp(ctx)
	fixture := newHandlerFixture(t)

	// Don't seed any contexts
	app.Get("/v1/contexts", fixture.handler.ListContexts)

	resp := performRequest(t, app, http.MethodGet, "/v1/contexts", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	items, ok := payload["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
}

// ─── ListSources nil result test ──────────────────────────────

func TestListSources_NilResult(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
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

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var respPayload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&respPayload))
	items, ok := respPayload["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, items)
}

// ─── Fee schedule handler tests with real repo ────────────────

func newFeeScheduleHandlerFixture(t *testing.T) *feeScheduleHandlerFixture {
	t.Helper()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRepo := newFeeScheduleRepository()

	commandUseCase, err := command.NewUseCase(
		contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo,
		command.WithFeeScheduleRepository(feeRepo),
	)
	require.NoError(t, err)

	queryUseCase, err := query.NewUseCase(
		contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo,
		query.WithFeeScheduleRepository(feeRepo),
	)
	require.NoError(t, err)

	handler, err := NewHandler(commandUseCase, queryUseCase, false)
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
