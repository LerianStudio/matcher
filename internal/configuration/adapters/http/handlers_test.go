//go:build unit

package http

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

type handlerFixture struct {
	handler         *Handler
	contextRepo     *contextRepository
	sourceRepo      *sourceRepository
	fieldMapRepo    *fieldMapRepository
	matchRuleRepo   *matchRuleRepository
	feeRuleRepo     *feeRuleRepository
	feeScheduleRepo *feeScheduleRepository
}

func newHandlerFixture(t *testing.T) *handlerFixture {
	t.Helper()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()

	commandUseCase, err := command.NewUseCase(
		contextRepo,
		sourceRepo,
		fieldMapRepo,
		matchRuleRepo,
		command.WithFeeRuleRepository(feeRuleRepo),
		command.WithFeeScheduleRepository(feeScheduleRepo),
	)
	require.NoError(t, err)

	queryUseCase, err := query.NewUseCase(
		contextRepo,
		sourceRepo,
		fieldMapRepo,
		matchRuleRepo,
		query.WithFeeRuleRepository(feeRuleRepo),
		query.WithFeeScheduleRepository(feeScheduleRepo),
	)
	require.NoError(t, err)

	handler, err := NewHandler(commandUseCase, queryUseCase, false)
	require.NoError(t, err)

	return &handlerFixture{
		handler:         handler,
		contextRepo:     contextRepo,
		sourceRepo:      sourceRepo,
		fieldMapRepo:    fieldMapRepo,
		matchRuleRepo:   matchRuleRepo,
		feeRuleRepo:     feeRuleRepo,
		feeScheduleRepo: feeScheduleRepo,
	}
}

func (fixture *handlerFixture) seedContext(
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

	stored, err := fixture.contextRepo.Create(context.Background(), contextEntity)
	require.NoError(t, err)

	return stored
}

func (fixture *handlerFixture) seedSource(
	t *testing.T,
	contextID uuid.UUID,
) *entities.ReconciliationSource {
	t.Helper()

	input := entities.CreateReconciliationSourceInput{
		Name: "Test Source",
		Type: value_objects.SourceTypeLedger,
		Side: fee.MatchingSideLeft,
	}
	sourceEntity, err := entities.NewReconciliationSource(context.Background(), contextID, input)
	require.NoError(t, err)

	stored, err := fixture.sourceRepo.Create(context.Background(), sourceEntity)
	require.NoError(t, err)

	return stored
}

func (fixture *handlerFixture) seedFieldMap(
	t *testing.T,
	contextID, sourceID uuid.UUID,
) *entities.FieldMap {
	t.Helper()

	ctx := context.Background()
	input := entities.CreateFieldMapInput{
		Mapping: map[string]any{"field": "value"},
	}
	fieldMap, err := entities.NewFieldMap(ctx, contextID, sourceID, input)
	require.NoError(t, err)

	stored, err := fixture.fieldMapRepo.Create(ctx, fieldMap)
	require.NoError(t, err)

	return stored
}

func (fixture *handlerFixture) seedFeeSchedule(
	t *testing.T,
	tenantID uuid.UUID,
) *fee.FeeSchedule {
	t.Helper()

	schedule, err := fee.NewFeeSchedule(context.Background(), fee.NewFeeScheduleInput{
		TenantID:         tenantID,
		Name:             "Test Fee Schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItemInput{{
			Name:      "Processing",
			Priority:  1,
			Structure: fee.FlatFee{},
		}},
	})
	require.NoError(t, err)

	stored, err := fixture.feeScheduleRepo.Create(context.Background(), schedule)
	require.NoError(t, err)

	return stored
}

func (fixture *handlerFixture) seedFeeRule(
	t *testing.T,
	contextID, feeScheduleID uuid.UUID,
	name string,
	side fee.MatchingSide,
	priority int,
) *fee.FeeRule {
	t.Helper()

	rule, err := fee.NewFeeRule(context.Background(), contextID, feeScheduleID, side, name, priority, nil)
	require.NoError(t, err)
	require.NoError(t, fixture.feeRuleRepo.Create(context.Background(), rule))

	return rule
}

func (fixture *handlerFixture) seedMatchRule(
	t *testing.T,
	contextID uuid.UUID,
	priority int,
) *entities.MatchRule {
	t.Helper()

	input := entities.CreateMatchRuleInput{
		Priority: priority,
		Type:     value_objects.RuleTypeExact,
		Config:   map[string]any{"matchCurrency": true},
	}
	matchRule, err := entities.NewMatchRule(context.Background(), contextID, input)
	require.NoError(t, err)

	stored, err := fixture.matchRuleRepo.Create(context.Background(), matchRule)
	require.NoError(t, err)

	return stored
}

func newTestTracer(t *testing.T) (trace.Tracer, *tracetest.SpanRecorder) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	tracer := provider.Tracer("test")

	return tracer, recorder
}

func newRequestContext(tracer trace.Tracer, tenantID uuid.UUID) context.Context {
	ctx := context.Background()
	ctx = libCommons.ContextWithTracer(ctx, tracer)

	if tenantID != uuid.Nil {
		ctx = context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
	}

	return ctx
}

func newTestApp(ctx context.Context) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.SetUserContext(ctx)

		return c.Next()
	})

	return app
}

func performRequest(t *testing.T, app *fiber.App, method, path string, body []byte) *http.Response {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader(body))

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func mustJSON(t *testing.T, payload any) []byte {
	t.Helper()

	data, err := json.Marshal(payload)
	require.NoError(t, err)

	return data
}

func stringPointer(value string) *string {
	return &value
}

func intPointer(value int) *int {
	return &value
}

func requireSpanName(t *testing.T, recorder *tracetest.SpanRecorder, name string) {
	t.Helper()

	spans := recorder.Ended()

	for _, span := range spans {
		if span.Name() == name {
			return
		}
	}

	require.Failf(t, "span not found", "expected span %q", name)
}

func requireErrorMessage(t *testing.T, response *http.Response, expectedMessage string) {
	t.Helper()

	var payload map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
	require.Equal(t, float64(400), payload["code"], "expected code to be 400")
	require.Equal(t, "invalid_request", payload["title"], "expected title to be 'invalid_request'")
	require.NotEmpty(t, payload["message"], "expected message to not be empty")

	if expectedMessage != "invalid_request" {
		require.Equal(t, expectedMessage, payload["message"])
	}
}

func requireUnauthorizedResponse(t *testing.T, response *http.Response) {
	t.Helper()

	var payload map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
	require.Equal(t, float64(401), payload["code"])
	require.Equal(t, "unauthorized", payload["title"])
	require.Equal(t, "unauthorized", payload["message"])
}

func requireNotFoundResponse(t *testing.T, response *http.Response, expectedMessage string) {
	t.Helper()

	var payload map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
	require.Equal(t, float64(404), payload["code"])
	require.Equal(t, "not_found", payload["title"])
	require.Equal(t, expectedMessage, payload["message"])
}

func requireConflictResponse(
	t *testing.T,
	response *http.Response,
	expectedTitle, expectedMessage string,
) {
	t.Helper()

	var payload map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
	require.Equal(t, float64(409), payload["code"])
	require.Equal(t, expectedTitle, payload["title"])
	require.Equal(t, expectedMessage, payload["message"])
}

type contextHandlerTestCase struct {
	name           string
	method         string
	path           string
	payload        any
	registerRoute  func(*fiber.App, *Handler)
	setupFixture   func(*handlerFixture) *entities.ReconciliationContext
	assertResponse func(*testing.T, *http.Response, *entities.ReconciliationContext)
	expectedStatus int
	expectedSpan   string
}

func runContextHandlerTest(t *testing.T, tenantID uuid.UUID, testCase contextHandlerTestCase) {
	t.Helper()
	tracer, recorder := newTestTracer(t)
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	requestPath := testCase.path

	var seededContext *entities.ReconciliationContext
	if testCase.setupFixture != nil {
		seededContext = testCase.setupFixture(fixture)
		if seededContext != nil {
			requestPath = replacePathParams(testCase.path, seededContext.ID.String())
		}
	}

	testCase.registerRoute(app, fixture.handler)

	var body []byte
	if testCase.payload != nil {
		body = mustJSON(t, testCase.payload)
	}

	resp := performRequest(t, app, testCase.method, requestPath, body)
	defer resp.Body.Close()

	require.Equal(t, testCase.expectedStatus, resp.StatusCode)
	requireSpanName(t, recorder, testCase.expectedSpan)

	if testCase.assertResponse != nil {
		testCase.assertResponse(t, resp, seededContext)
	}
}

type sourceHandlerTestCase struct {
	name           string
	method         string
	path           string
	payload        any
	registerRoute  func(*fiber.App, *Handler)
	setupFixture   func(*handlerFixture) (uuid.UUID, uuid.UUID)
	assertResponse func(*testing.T, *http.Response)
	expectedStatus int
	expectedSpan   string
}

func runSourceHandlerTest(t *testing.T, tenantID uuid.UUID, testCase sourceHandlerTestCase) {
	t.Helper()
	tracer, recorder := newTestTracer(t)
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextID, sourceID := testCase.setupFixture(fixture)
	requestPath := replacePathParams(testCase.path, contextID.String(), sourceID.String())

	testCase.registerRoute(app, fixture.handler)

	var body []byte
	if testCase.payload != nil {
		body = mustJSON(t, testCase.payload)
	}

	resp := performRequest(t, app, testCase.method, requestPath, body)
	defer resp.Body.Close()

	require.Equal(t, testCase.expectedStatus, resp.StatusCode)
	requireSpanName(t, recorder, testCase.expectedSpan)

	if testCase.assertResponse != nil {
		testCase.assertResponse(t, resp)
	}
}

type fieldMapHandlerTestCase struct {
	name           string
	method         string
	path           string
	payload        any
	registerRoute  func(*fiber.App, *Handler)
	setupFixture   func(*handlerFixture) []string
	assertResponse func(*testing.T, *http.Response)
	expectedStatus int
	expectedSpan   string
}

func runFieldMapHandlerTest(t *testing.T, tenantID uuid.UUID, testCase fieldMapHandlerTestCase) {
	t.Helper()
	tracer, recorder := newTestTracer(t)
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	ids := testCase.setupFixture(fixture)
	requestPath := replacePathParams(testCase.path, ids...)

	testCase.registerRoute(app, fixture.handler)

	var body []byte
	if testCase.payload != nil {
		body = mustJSON(t, testCase.payload)
	}

	resp := performRequest(t, app, testCase.method, requestPath, body)
	defer resp.Body.Close()

	require.Equal(t, testCase.expectedStatus, resp.StatusCode)
	requireSpanName(t, recorder, testCase.expectedSpan)

	if testCase.assertResponse != nil {
		testCase.assertResponse(t, resp)
	}
}

type matchRuleHandlerTestCase struct {
	name           string
	method         string
	path           string
	payload        any
	registerRoute  func(*fiber.App, *Handler)
	setupFixture   func(*handlerFixture) []string
	assertResponse func(*testing.T, *http.Response)
	expectedStatus int
	expectedSpan   string
}

func runMatchRuleHandlerTest(t *testing.T, tenantID uuid.UUID, testCase matchRuleHandlerTestCase) {
	t.Helper()
	tracer, recorder := newTestTracer(t)
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	ids := testCase.setupFixture(fixture)
	requestPath := replacePathParams(testCase.path, ids...)

	testCase.registerRoute(app, fixture.handler)

	var body []byte
	if testCase.payload != nil {
		body = mustJSON(t, testCase.payload)
	}

	resp := performRequest(t, app, testCase.method, requestPath, body)
	defer resp.Body.Close()

	require.Equal(t, testCase.expectedStatus, resp.StatusCode)
	requireSpanName(t, recorder, testCase.expectedSpan)

	if testCase.assertResponse != nil {
		testCase.assertResponse(t, resp)
	}
}

func setupMatchRuleErrorTest(
	t *testing.T,
) (uuid.UUID, *fiber.App, *handlerFixture, *tracetest.SpanRecorder) {
	t.Helper()
	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)

	fixture := newHandlerFixture(t)

	return tenantID, app, fixture, recorder
}

func getContextHandlerTestCases(t *testing.T, tenantID uuid.UUID) []contextHandlerTestCase {
	t.Helper()

	return []contextHandlerTestCase{
		{
			name:   "create context",
			method: http.MethodPost,
			path:   "/v1/contexts",
			payload: entities.CreateReconciliationContextInput{
				Name:     "Context A",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "daily",
			},
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Post("/v1/contexts", handler.CreateContext)
			},
			assertResponse: func(t *testing.T, response *http.Response, _ *entities.ReconciliationContext) {
				t.Helper()

				var payload entities.ReconciliationContext
				require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
				require.Equal(t, "Context A", payload.Name)
				require.NotEqual(t, uuid.Nil, payload.ID)
			},
			expectedStatus: fiber.StatusCreated,
			expectedSpan:   "handler.context.create",
		},
		{
			name:   "list contexts",
			method: http.MethodGet,
			path:   "/v1/contexts",
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Get("/v1/contexts", handler.ListContexts)
			},
			setupFixture: func(fixture *handlerFixture) *entities.ReconciliationContext {
				return fixture.seedContext(t, tenantID)
			},
			assertResponse: func(t *testing.T, response *http.Response, seededContext *entities.ReconciliationContext) {
				t.Helper()

				var payload map[string]any
				require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
				items, ok := payload["items"].([]any)
				require.True(t, ok)
				require.Len(t, items, 1)
				item, ok := items[0].(map[string]any)
				require.True(t, ok)
				require.Equal(t, seededContext.ID.String(), item["id"])
				require.InDelta(t, float64(20), payload["limit"], 0.01)
			},
			expectedStatus: fiber.StatusOK,
			expectedSpan:   "handler.context.list",
		},
		{
			name:   "get context",
			method: http.MethodGet,
			path:   "/v1/contexts/:contextId",
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Get("/v1/contexts/:contextId", handler.GetContext)
			},
			setupFixture: func(fixture *handlerFixture) *entities.ReconciliationContext {
				return fixture.seedContext(t, tenantID)
			},
			assertResponse: func(t *testing.T, response *http.Response, seededContext *entities.ReconciliationContext) {
				t.Helper()

				var payload entities.ReconciliationContext
				require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
				require.Equal(t, seededContext.ID, payload.ID)
				require.Equal(t, seededContext.Name, payload.Name)
			},
			expectedStatus: fiber.StatusOK,
			expectedSpan:   "handler.context.get",
		},
		{
			name:   "update context",
			method: http.MethodPatch,
			path:   "/v1/contexts/:contextId",
			payload: entities.UpdateReconciliationContextInput{
				Name: stringPointer("Updated"),
			},
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Patch("/v1/contexts/:contextId", handler.UpdateContext)
			},
			setupFixture: func(fixture *handlerFixture) *entities.ReconciliationContext {
				return fixture.seedContext(t, tenantID)
			},
			assertResponse: func(t *testing.T, response *http.Response, _ *entities.ReconciliationContext) {
				t.Helper()

				var payload entities.ReconciliationContext
				require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
				require.Equal(t, "Updated", payload.Name)
			},
			expectedStatus: fiber.StatusOK,
			expectedSpan:   "handler.context.update",
		},
		{
			name:   "delete context",
			method: http.MethodDelete,
			path:   "/v1/contexts/:contextId",
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Delete("/v1/contexts/:contextId", handler.DeleteContext)
			},
			setupFixture: func(fixture *handlerFixture) *entities.ReconciliationContext {
				return fixture.seedContext(t, tenantID)
			},
			expectedStatus: fiber.StatusNoContent,
			expectedSpan:   "handler.context.delete",
		},
	}
}

func TestHandlers_ContextHandlersTracing(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	testCases := getContextHandlerTestCases(t, tenantID)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runContextHandlerTest(t, tenantID, tc)
		})
	}
}

func TestHandlers_GetContextInvalidUUID(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	requestContext := newRequestContext(tracer, uuid.New())
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	app.Get("/api/v1/contexts/:contextId", fixture.handler.GetContext)

	resp := performRequest(t, app, http.MethodGet, "/api/v1/contexts/not-a-uuid", nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.context.get")
	requireErrorMessage(t, resp, "invalid context id")
}

func TestHandlers_ContextErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("create context invalid payload", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		requestContext := newRequestContext(tracer, uuid.New())
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		app.Post("/api/v1/contexts", fixture.handler.CreateContext)

		resp := performRequest(
			t,
			app,
			http.MethodPost,
			"/api/v1/contexts",
			[]byte("{invalid"),
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.create")
		requireErrorMessage(t, resp, "invalid_request")
	})

	t.Run("list contexts invalid pagination", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		requestContext := newRequestContext(tracer, uuid.New())
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		app.Get("/api/v1/contexts", fixture.handler.ListContexts)

		resp := performRequest(t, app, http.MethodGet, "/api/v1/contexts?limit=bad", nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.list")
		requireErrorMessage(t, resp, "invalid_request")
	})

	t.Run("get context not found", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		requestContext := newRequestContext(tracer, uuid.New())
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		app.Get("/api/v1/contexts/:contextId", fixture.handler.GetContext)

		resp := performRequest(
			t,
			app,
			http.MethodGet,
			"/api/v1/contexts/"+uuid.NewString(),
			nil,
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.get")
		requireNotFoundResponse(t, resp, "context not found")
	})

	t.Run("update context not found", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		requestContext := newRequestContext(tracer, uuid.New())
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		app.Patch("/api/v1/contexts/:contextId", fixture.handler.UpdateContext)

		payload := mustJSON(
			t,
			entities.UpdateReconciliationContextInput{Name: stringPointer("Updated")},
		)

		resp := performRequest(
			t,
			app,
			http.MethodPatch,
			"/api/v1/contexts/"+uuid.NewString(),
			payload,
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.update")
		requireNotFoundResponse(t, resp, "context not found")
	})

	t.Run("delete context not found", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		requestContext := newRequestContext(tracer, uuid.New())
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		app.Delete("/api/v1/contexts/:contextId", fixture.handler.DeleteContext)

		resp := performRequest(
			t,
			app,
			http.MethodDelete,
			"/api/v1/contexts/"+uuid.NewString(),
			nil,
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.delete")
		requireNotFoundResponse(t, resp, "context not found")
	})

	t.Run("invalid tenant id", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "invalid-tenant")
		ctx = libCommons.ContextWithTracer(ctx, tracer)
		app := newTestApp(ctx)
		fixture := newHandlerFixture(t)

		app.Post("/api/v1/contexts", fixture.handler.CreateContext)

		payload := mustJSON(t, entities.CreateReconciliationContextInput{
			Name:     "Context A",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "daily",
		})

		resp := performRequest(t, app, http.MethodPost, "/api/v1/contexts", payload)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.create")
		requireUnauthorizedResponse(t, resp)
	})
}

func TestHandlers_InvalidCursor(t *testing.T) {
	t.Parallel()

	t.Run("list contexts invalid cursor", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		requestContext := newRequestContext(tracer, uuid.New())
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		app.Get("/api/v1/contexts", fixture.handler.ListContexts)

		resp := performRequest(
			t,
			app,
			http.MethodGet,
			"/api/v1/contexts?cursor=invalid-cursor!!!",
			nil,
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.context.list")
		requireErrorMessage(t, resp, "invalid pagination")
	})

	t.Run("list sources invalid cursor", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		app.Get("/api/v1/contexts/:contextId/sources", fixture.handler.ListSources)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/sources",
			contextEntity.ID.String(),
		)

		resp := performRequest(
			t,
			app,
			http.MethodGet,
			requestPath+"?cursor=invalid-cursor!!!",
			nil,
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.source.list")
		requireErrorMessage(t, resp, "invalid pagination")
	})

	t.Run("list match rules invalid cursor", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		app.Get("/api/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/rules",
			contextEntity.ID.String(),
		)

		resp := performRequest(
			t,
			app,
			http.MethodGet,
			requestPath+"?cursor=invalid-cursor!!!",
			nil,
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.matchrule.list")
		requireErrorMessage(t, resp, "invalid pagination")
	})
}

func TestHandlers_ListPaginationMetadata(t *testing.T) {
	t.Parallel()

	t.Run("list contexts includes pagination metadata", func(t *testing.T) {
		t.Parallel()

		tracer, _ := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		fixture.seedContext(t, tenantID)
		app.Get("/api/v1/contexts", fixture.handler.ListContexts)

		resp := performRequest(t, app, http.MethodGet, "/api/v1/contexts", nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var payload map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		require.Contains(t, payload, "items")
		require.Contains(t, payload, "limit")
		require.Contains(t, payload, "hasMore")
		require.InDelta(t, float64(20), payload["limit"], 0.01)
		require.Equal(t, false, payload["hasMore"])
	})

	t.Run("list sources includes pagination metadata", func(t *testing.T) {
		t.Parallel()

		tracer, _ := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		fixture.seedSource(t, contextEntity.ID)
		app.Get("/api/v1/contexts/:contextId/sources", fixture.handler.ListSources)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/sources",
			contextEntity.ID.String(),
		)

		resp := performRequest(t, app, http.MethodGet, requestPath, nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var payload map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		require.Contains(t, payload, "items")
		require.Contains(t, payload, "limit")
		require.Contains(t, payload, "hasMore")
		require.InDelta(t, float64(20), payload["limit"], 0.01)
		require.Equal(t, false, payload["hasMore"])
	})

	t.Run("list match rules includes pagination metadata", func(t *testing.T) {
		t.Parallel()

		tracer, _ := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		fixture.seedMatchRule(t, contextEntity.ID, 1)
		app.Get("/api/v1/contexts/:contextId/rules", fixture.handler.ListMatchRules)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/rules",
			contextEntity.ID.String(),
		)

		resp := performRequest(t, app, http.MethodGet, requestPath, nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var payload map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
		require.Contains(t, payload, "items")
		require.Contains(t, payload, "limit")
		require.Contains(t, payload, "hasMore")
		require.InDelta(t, float64(20), payload["limit"], 0.01)
		require.Equal(t, false, payload["hasMore"])
	})
}

func makeCreateSourceTestCase(t *testing.T, tenantID uuid.UUID) sourceHandlerTestCase {
	t.Helper()

	return sourceHandlerTestCase{
		name:   "create source",
		method: http.MethodPost,
		path:   "/api/v1/contexts/:contextId/sources",
		payload: entities.CreateReconciliationSourceInput{
			Name: "Source A",
			Type: value_objects.SourceTypeLedger,
			Side: fee.MatchingSideLeft,
		},
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Post("/api/v1/contexts/:contextId/sources", handler.CreateSource)
		},
		setupFixture: func(fixture *handlerFixture) (uuid.UUID, uuid.UUID) {
			contextEntity := fixture.seedContext(t, tenantID)
			return contextEntity.ID, uuid.Nil
		},
		assertResponse: func(t *testing.T, response *http.Response) {
			t.Helper()

			var payload entities.ReconciliationSource

			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			require.Equal(t, "Source A", payload.Name)
			require.NotEqual(t, uuid.Nil, payload.ID)
		},
		expectedStatus: fiber.StatusCreated,
		expectedSpan:   "handler.source.create",
	}
}

func makeListSourcesTestCase(t *testing.T, tenantID uuid.UUID) sourceHandlerTestCase {
	t.Helper()

	return sourceHandlerTestCase{
		name:   "list sources",
		method: http.MethodGet,
		path:   "/api/v1/contexts/:contextId/sources",
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Get("/api/v1/contexts/:contextId/sources", handler.ListSources)
		},
		setupFixture: func(fixture *handlerFixture) (uuid.UUID, uuid.UUID) {
			contextEntity := fixture.seedContext(t, tenantID)
			fixture.seedSource(t, contextEntity.ID)

			return contextEntity.ID, uuid.Nil
		},
		assertResponse: func(t *testing.T, response *http.Response) {
			t.Helper()

			var payload map[string]any
			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			items, ok := payload["items"].([]any)
			require.True(t, ok)
			require.Len(t, items, 1)
			item, ok := items[0].(map[string]any)
			require.True(t, ok)
			require.Equal(t, "Test Source", item["name"])
			require.InDelta(t, float64(20), payload["limit"], 0.01)
		},
		expectedStatus: fiber.StatusOK,
		expectedSpan:   "handler.source.list",
	}
}

func makeGetSourceTestCase(t *testing.T, tenantID uuid.UUID) sourceHandlerTestCase {
	t.Helper()

	return sourceHandlerTestCase{
		name:   "get source",
		method: http.MethodGet,
		path:   "/api/v1/contexts/:contextId/sources/:sourceId",
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Get("/api/v1/contexts/:contextId/sources/:sourceId", handler.GetSource)
		},
		setupFixture: func(fixture *handlerFixture) (uuid.UUID, uuid.UUID) {
			contextEntity := fixture.seedContext(t, tenantID)
			sourceEntity := fixture.seedSource(t, contextEntity.ID)

			return contextEntity.ID, sourceEntity.ID
		},
		assertResponse: func(t *testing.T, response *http.Response) {
			t.Helper()

			var payload entities.ReconciliationSource

			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			require.Equal(t, "Test Source", payload.Name)
		},
		expectedStatus: fiber.StatusOK,
		expectedSpan:   "handler.source.get",
	}
}

func makeUpdateSourceTestCase(t *testing.T, tenantID uuid.UUID) sourceHandlerTestCase {
	t.Helper()

	return sourceHandlerTestCase{
		name:   "update source",
		method: http.MethodPatch,
		path:   "/api/v1/contexts/:contextId/sources/:sourceId",
		payload: entities.UpdateReconciliationSourceInput{
			Name: stringPointer("Updated Source"),
		},
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Patch("/api/v1/contexts/:contextId/sources/:sourceId", handler.UpdateSource)
		},
		setupFixture: func(fixture *handlerFixture) (uuid.UUID, uuid.UUID) {
			contextEntity := fixture.seedContext(t, tenantID)
			sourceEntity := fixture.seedSource(t, contextEntity.ID)

			return contextEntity.ID, sourceEntity.ID
		},
		assertResponse: func(t *testing.T, response *http.Response) {
			t.Helper()

			var payload entities.ReconciliationSource

			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			require.Equal(t, "Updated Source", payload.Name)
		},
		expectedStatus: fiber.StatusOK,
		expectedSpan:   "handler.source.update",
	}
}

func makeDeleteSourceTestCase(t *testing.T, tenantID uuid.UUID) sourceHandlerTestCase {
	t.Helper()

	return sourceHandlerTestCase{
		name:   "delete source",
		method: http.MethodDelete,
		path:   "/api/v1/contexts/:contextId/sources/:sourceId",
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Delete("/api/v1/contexts/:contextId/sources/:sourceId", handler.DeleteSource)
		},
		setupFixture: func(fixture *handlerFixture) (uuid.UUID, uuid.UUID) {
			contextEntity := fixture.seedContext(t, tenantID)
			sourceEntity := fixture.seedSource(t, contextEntity.ID)

			return contextEntity.ID, sourceEntity.ID
		},
		expectedStatus: fiber.StatusNoContent,
		expectedSpan:   "handler.source.delete",
	}
}

func getSourceHandlerTestCases(t *testing.T, tenantID uuid.UUID) []sourceHandlerTestCase {
	t.Helper()

	return []sourceHandlerTestCase{
		makeCreateSourceTestCase(t, tenantID),
		makeListSourcesTestCase(t, tenantID),
		makeGetSourceTestCase(t, tenantID),
		makeUpdateSourceTestCase(t, tenantID),
		makeDeleteSourceTestCase(t, tenantID),
	}
}

func TestHandlers_SourceHandlersTracing(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	testCases := getSourceHandlerTestCases(t, tenantID)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSourceHandlerTest(t, tenantID, tc)
		})
	}
}

func TestHandlers_GetSourceNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	app.Get("/api/v1/contexts/:contextId/sources/:sourceId", fixture.handler.GetSource)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId",
		contextEntity.ID.String(),
		uuid.NewString(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.source.get")
	requireNotFoundResponse(t, resp, "source not found")
}

func TestHandlers_SourceErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("create source invalid payload", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		app.Post("/api/v1/contexts/:contextId/sources", fixture.handler.CreateSource)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/sources",
			contextEntity.ID.String(),
		)

		resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.source.create")
		requireErrorMessage(t, resp, "invalid_request")
	})

	t.Run("get source invalid uuid", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		app.Get("/api/v1/contexts/:contextId/sources/:sourceId", fixture.handler.GetSource)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/sources/:sourceId",
			contextEntity.ID.String(),
			"not-a-uuid",
		)

		resp := performRequest(t, app, http.MethodGet, requestPath, nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.source.get")
		requireErrorMessage(t, resp, "invalid_request")
	})
}

func TestHandlers_FieldMapHandlersTracing(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	testCases := []fieldMapHandlerTestCase{
		{
			name:   "create field map",
			method: http.MethodPost,
			path:   "/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
			payload: entities.CreateFieldMapInput{
				Mapping: map[string]any{"field": "value"},
			},
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Post(
					"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
					handler.CreateFieldMap,
				)
			},
			setupFixture: func(fixture *handlerFixture) []string {
				contextEntity := fixture.seedContext(t, tenantID)
				sourceEntity := fixture.seedSource(t, contextEntity.ID)

				return []string{contextEntity.ID.String(), sourceEntity.ID.String()}
			},
			assertResponse: func(t *testing.T, response *http.Response) {
				t.Helper()

				var payload entities.FieldMap
				require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
				require.Equal(t, "value", payload.Mapping["field"])
			},
			expectedStatus: fiber.StatusCreated,
			expectedSpan:   "handler.fieldmap.create",
		},
		{
			name:   "get field map",
			method: http.MethodGet,
			path:   "/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Get(
					"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
					handler.GetFieldMapBySource,
				)
			},
			setupFixture: func(fixture *handlerFixture) []string {
				contextEntity := fixture.seedContext(t, tenantID)
				sourceEntity := fixture.seedSource(t, contextEntity.ID)
				fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

				return []string{contextEntity.ID.String(), sourceEntity.ID.String()}
			},
			assertResponse: func(t *testing.T, response *http.Response) {
				t.Helper()

				var payload entities.FieldMap
				require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
				require.Equal(t, "value", payload.Mapping["field"])
			},
			expectedStatus: fiber.StatusOK,
			expectedSpan:   "handler.fieldmap.get_by_source",
		},
		{
			name:   "update field map",
			method: http.MethodPatch,
			path:   "/api/v1/field-maps/:fieldMapId",
			payload: entities.UpdateFieldMapInput{
				Mapping: map[string]any{"field": "updated"},
			},
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Patch("/api/v1/field-maps/:fieldMapId", handler.UpdateFieldMap)
			},
			setupFixture: func(fixture *handlerFixture) []string {
				contextEntity := fixture.seedContext(t, tenantID)
				sourceEntity := fixture.seedSource(t, contextEntity.ID)
				fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

				return []string{fieldMap.ID.String()}
			},
			assertResponse: func(t *testing.T, response *http.Response) {
				t.Helper()

				var payload entities.FieldMap
				require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
				require.Equal(t, "updated", payload.Mapping["field"])
			},
			expectedStatus: fiber.StatusOK,
			expectedSpan:   "handler.fieldmap.update",
		},
		{
			name:   "delete field map",
			method: http.MethodDelete,
			path:   "/api/v1/field-maps/:fieldMapId",
			registerRoute: func(app *fiber.App, handler *Handler) {
				app.Delete("/api/v1/field-maps/:fieldMapId", handler.DeleteFieldMap)
			},
			setupFixture: func(fixture *handlerFixture) []string {
				contextEntity := fixture.seedContext(t, tenantID)
				sourceEntity := fixture.seedSource(t, contextEntity.ID)
				fieldMap := fixture.seedFieldMap(t, contextEntity.ID, sourceEntity.ID)

				return []string{fieldMap.ID.String()}
			},
			expectedStatus: fiber.StatusNoContent,
			expectedSpan:   "handler.fieldmap.delete",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runFieldMapHandlerTest(t, tenantID, tc)
		})
	}
}

func TestHandlers_GetFieldMapNotFound(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	sourceEntity := fixture.seedSource(t, contextEntity.ID)

	app.Get(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		fixture.handler.GetFieldMapBySource,
	)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
		contextEntity.ID.String(),
		sourceEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodGet, requestPath, nil)
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	requireSpanName(t, recorder, "handler.fieldmap.get_by_source")
	requireNotFoundResponse(t, resp, "field map not found")
}

func TestHandlers_FieldMapErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("create field map invalid payload", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		sourceEntity := fixture.seedSource(t, contextEntity.ID)
		app.Post(
			"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
			fixture.handler.CreateFieldMap,
		)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
			contextEntity.ID.String(),
			sourceEntity.ID.String(),
		)

		resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fieldmap.create")
		requireErrorMessage(t, resp, "invalid_request")
	})

	t.Run("get field map invalid uuid", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		app.Get(
			"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
			fixture.handler.GetFieldMapBySource,
		)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/sources/:sourceId/field-maps",
			contextEntity.ID.String(),
			"not-a-uuid",
		)

		resp := performRequest(t, app, http.MethodGet, requestPath, nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fieldmap.get_by_source")
		requireErrorMessage(t, resp, "invalid_request")
	})

	t.Run("update field map invalid uuid", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		app.Patch("/api/v1/field-maps/:fieldMapId", fixture.handler.UpdateFieldMap)

		payload := mustJSON(
			t,
			entities.UpdateFieldMapInput{Mapping: map[string]any{"field": "updated"}},
		)

		resp := performRequest(
			t,
			app,
			http.MethodPatch,
			"/api/v1/field-maps/not-a-uuid",
			payload,
		)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.fieldmap.update")
		requireErrorMessage(t, resp, "invalid_request")
	})
}

func makeCreateMatchRuleTestCase(t *testing.T, tenantID uuid.UUID) matchRuleHandlerTestCase {
	t.Helper()

	return matchRuleHandlerTestCase{
		name:   "create match rule",
		method: http.MethodPost,
		path:   "/api/v1/contexts/:contextId/rules",
		payload: entities.CreateMatchRuleInput{
			Priority: 1,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchCurrency": true},
		},
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Post("/api/v1/contexts/:contextId/rules", handler.CreateMatchRule)
		},
		setupFixture: func(fixture *handlerFixture) []string {
			contextEntity := fixture.seedContext(t, tenantID)
			return []string{contextEntity.ID.String()}
		},
		assertResponse: func(t *testing.T, response *http.Response) {
			t.Helper()

			var payload entities.MatchRule

			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			require.Equal(t, 1, payload.Priority)
		},
		expectedStatus: fiber.StatusCreated,
		expectedSpan:   "handler.matchrule.create",
	}
}

func makeListMatchRulesTestCase(t *testing.T, tenantID uuid.UUID) matchRuleHandlerTestCase {
	t.Helper()

	return matchRuleHandlerTestCase{
		name:   "list match rules",
		method: http.MethodGet,
		path:   "/api/v1/contexts/:contextId/rules",
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Get("/api/v1/contexts/:contextId/rules", handler.ListMatchRules)
		},
		setupFixture: func(fixture *handlerFixture) []string {
			contextEntity := fixture.seedContext(t, tenantID)
			fixture.seedMatchRule(t, contextEntity.ID, 1)

			return []string{contextEntity.ID.String()}
		},
		assertResponse: func(t *testing.T, response *http.Response) {
			t.Helper()

			var payload map[string]any
			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			items, ok := payload["items"].([]any)
			require.True(t, ok)
			require.Len(t, items, 1)
			item, ok := items[0].(map[string]any)
			require.True(t, ok)
			require.InDelta(t, float64(1), item["priority"], 0.01)
			require.InDelta(t, float64(20), payload["limit"], 0.01)
		},
		expectedStatus: fiber.StatusOK,
		expectedSpan:   "handler.matchrule.list",
	}
}

func makeGetMatchRuleTestCase(t *testing.T, tenantID uuid.UUID) matchRuleHandlerTestCase {
	t.Helper()

	return matchRuleHandlerTestCase{
		name:   "get match rule",
		method: http.MethodGet,
		path:   "/api/v1/contexts/:contextId/rules/:ruleId",
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Get("/api/v1/contexts/:contextId/rules/:ruleId", handler.GetMatchRule)
		},
		setupFixture: func(fixture *handlerFixture) []string {
			contextEntity := fixture.seedContext(t, tenantID)
			matchRule := fixture.seedMatchRule(t, contextEntity.ID, 1)

			return []string{contextEntity.ID.String(), matchRule.ID.String()}
		},
		assertResponse: func(t *testing.T, response *http.Response) {
			t.Helper()

			var payload entities.MatchRule

			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			require.Equal(t, 1, payload.Priority)
		},
		expectedStatus: fiber.StatusOK,
		expectedSpan:   "handler.matchrule.get",
	}
}

func makeUpdateMatchRuleTestCase(t *testing.T, tenantID uuid.UUID) matchRuleHandlerTestCase {
	t.Helper()

	return matchRuleHandlerTestCase{
		name:   "update match rule",
		method: http.MethodPatch,
		path:   "/api/v1/contexts/:contextId/rules/:ruleId",
		payload: entities.UpdateMatchRuleInput{
			Priority: intPointer(2),
		},
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Patch("/api/v1/contexts/:contextId/rules/:ruleId", handler.UpdateMatchRule)
		},
		setupFixture: func(fixture *handlerFixture) []string {
			contextEntity := fixture.seedContext(t, tenantID)
			matchRule := fixture.seedMatchRule(t, contextEntity.ID, 1)

			return []string{contextEntity.ID.String(), matchRule.ID.String()}
		},
		assertResponse: func(t *testing.T, response *http.Response) {
			t.Helper()

			var payload entities.MatchRule

			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			require.Equal(t, 2, payload.Priority)
		},
		expectedStatus: fiber.StatusOK,
		expectedSpan:   "handler.matchrule.update",
	}
}

func makeDeleteMatchRuleTestCase(t *testing.T, tenantID uuid.UUID) matchRuleHandlerTestCase {
	t.Helper()

	return matchRuleHandlerTestCase{
		name:   "delete match rule",
		method: http.MethodDelete,
		path:   "/api/v1/contexts/:contextId/rules/:ruleId",
		registerRoute: func(app *fiber.App, handler *Handler) {
			app.Delete("/api/v1/contexts/:contextId/rules/:ruleId", handler.DeleteMatchRule)
		},
		setupFixture: func(fixture *handlerFixture) []string {
			contextEntity := fixture.seedContext(t, tenantID)
			matchRule := fixture.seedMatchRule(t, contextEntity.ID, 1)

			return []string{contextEntity.ID.String(), matchRule.ID.String()}
		},
		expectedStatus: fiber.StatusNoContent,
		expectedSpan:   "handler.matchrule.delete",
	}
}

func getMatchRuleHandlerTestCases(t *testing.T, tenantID uuid.UUID) []matchRuleHandlerTestCase {
	t.Helper()

	return []matchRuleHandlerTestCase{
		makeCreateMatchRuleTestCase(t, tenantID),
		makeListMatchRulesTestCase(t, tenantID),
		makeGetMatchRuleTestCase(t, tenantID),
		makeUpdateMatchRuleTestCase(t, tenantID),
		makeDeleteMatchRuleTestCase(t, tenantID),
	}
}

func TestHandlers_MatchRuleHandlersTracing(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()

	testCases := getMatchRuleHandlerTestCases(t, tenantID)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runMatchRuleHandlerTest(t, tenantID, tc)
		})
	}
}

func TestHandlers_MatchRuleErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("create match rule invalid payload", func(t *testing.T) {
		t.Parallel()
		tenantID, app, fixture, recorder := setupMatchRuleErrorTest(t)
		contextEntity := fixture.seedContext(t, tenantID)
		app.Post("/api/v1/contexts/:contextId/rules", fixture.handler.CreateMatchRule)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/rules",
			contextEntity.ID.String(),
		)

		resp := performRequest(t, app, http.MethodPost, requestPath, []byte("{invalid"))
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.matchrule.create")
		requireErrorMessage(t, resp, "invalid_request")
	})

	t.Run("get match rule invalid uuid", func(t *testing.T) {
		t.Parallel()
		tenantID, app, fixture, recorder := setupMatchRuleErrorTest(t)
		contextEntity := fixture.seedContext(t, tenantID)
		app.Get("/api/v1/contexts/:contextId/rules/:ruleId", fixture.handler.GetMatchRule)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/rules/:ruleId",
			contextEntity.ID.String(),
			"not-a-uuid",
		)

		resp := performRequest(t, app, http.MethodGet, requestPath, nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
		requireSpanName(t, recorder, "handler.matchrule.get")
		requireErrorMessage(t, resp, "invalid_request")
	})

	t.Run("get match rule not found", func(t *testing.T) {
		t.Parallel()
		tenantID, app, fixture, recorder := setupMatchRuleErrorTest(t)
		contextEntity := fixture.seedContext(t, tenantID)
		app.Get("/api/v1/contexts/:contextId/rules/:ruleId", fixture.handler.GetMatchRule)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/rules/:ruleId",
			contextEntity.ID.String(),
			uuid.NewString(),
		)

		resp := performRequest(t, app, http.MethodGet, requestPath, nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		requireSpanName(t, recorder, "handler.matchrule.get")
		requireNotFoundResponse(t, resp, "match rule not found")
	})

	t.Run("update match rule not found", func(t *testing.T) {
		t.Parallel()
		tenantID, app, fixture, recorder := setupMatchRuleErrorTest(t)
		contextEntity := fixture.seedContext(t, tenantID)
		app.Patch(
			"/api/v1/contexts/:contextId/rules/:ruleId",
			fixture.handler.UpdateMatchRule,
		)

		payload := mustJSON(t, entities.UpdateMatchRuleInput{Priority: intPointer(2)})

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/rules/:ruleId",
			contextEntity.ID.String(),
			uuid.NewString(),
		)

		resp := performRequest(t, app, http.MethodPatch, requestPath, payload)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		requireSpanName(t, recorder, "handler.matchrule.update")
		requireNotFoundResponse(t, resp, "match rule not found")
	})

	t.Run("delete match rule not found", func(t *testing.T) {
		t.Parallel()
		tenantID, app, fixture, recorder := setupMatchRuleErrorTest(t)
		contextEntity := fixture.seedContext(t, tenantID)
		app.Delete(
			"/api/v1/contexts/:contextId/rules/:ruleId",
			fixture.handler.DeleteMatchRule,
		)

		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/rules/:ruleId",
			contextEntity.ID.String(),
			uuid.NewString(),
		)

		resp := performRequest(t, app, http.MethodDelete, requestPath, nil)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		requireSpanName(t, recorder, "handler.matchrule.delete")
		requireNotFoundResponse(t, resp, "match rule not found")
	})

	t.Run("reorder match rules not found", func(t *testing.T) {
		t.Parallel()

		tracer, recorder := newTestTracer(t)
		tenantID := uuid.New()
		requestContext := newRequestContext(tracer, tenantID)
		app := newTestApp(requestContext)
		fixture := newHandlerFixture(t)

		contextEntity := fixture.seedContext(t, tenantID)
		app.Post(
			"/api/v1/contexts/:contextId/rules/reorder",
			fixture.handler.ReorderMatchRules,
		)

		payload := ReorderRequest{RuleIDs: []uuid.UUID{uuid.New()}}
		requestPath := replacePathParams(
			"/api/v1/contexts/:contextId/rules/reorder",
			contextEntity.ID.String(),
		)

		resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
		requireSpanName(t, recorder, "handler.matchrule.reorder")
		requireNotFoundResponse(t, resp, "match rule not found")
	})
}

func TestHandlers_ReorderMatchRulesTracing(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	firstRule := fixture.seedMatchRule(t, contextEntity.ID, 1)
	secondRule := fixture.seedMatchRule(t, contextEntity.ID, 2)

	payload := ReorderRequest{RuleIDs: []uuid.UUID{secondRule.ID, firstRule.ID}}

	app.Post("/api/v1/contexts/:contextId/rules/reorder", fixture.handler.ReorderMatchRules)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/reorder",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNoContent, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.reorder")

	updatedFirst, err := fixture.matchRuleRepo.FindByID(
		context.Background(),
		contextEntity.ID,
		firstRule.ID,
	)
	require.NoError(t, err)
	updatedSecond, err := fixture.matchRuleRepo.FindByID(
		context.Background(),
		contextEntity.ID,
		secondRule.ID,
	)
	require.NoError(t, err)
	require.Equal(t, 2, updatedFirst.Priority)
	require.Equal(t, 1, updatedSecond.Priority)
}

func TestHandlers_CreateMatchRuleConflict(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)
	fixture.seedMatchRule(t, contextEntity.ID, 1)

	payload := entities.CreateMatchRuleInput{
		Priority: 1,
		Type:     value_objects.RuleTypeExact,
		Config:   map[string]any{"matchCurrency": true},
	}

	app.Post("/api/v1/contexts/:contextId/rules", fixture.handler.CreateMatchRule)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusConflict, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.create")
	requireConflictResponse(t, resp, "priority_conflict", entities.ErrRulePriorityConflict.Error())
}

func TestHandlers_ReorderMatchRulesEmpty(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	tenantID := uuid.New()
	requestContext := newRequestContext(tracer, tenantID)
	app := newTestApp(requestContext)
	fixture := newHandlerFixture(t)

	contextEntity := fixture.seedContext(t, tenantID)

	payload := ReorderRequest{RuleIDs: []uuid.UUID{}}

	app.Post("/api/v1/contexts/:contextId/rules/reorder", fixture.handler.ReorderMatchRules)

	requestPath := replacePathParams(
		"/api/v1/contexts/:contextId/rules/reorder",
		contextEntity.ID.String(),
	)

	resp := performRequest(t, app, http.MethodPost, requestPath, mustJSON(t, payload))
	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	requireSpanName(t, recorder, "handler.matchrule.reorder")
	requireErrorMessage(t, resp, "invalid_request")
}

type contextRepository struct {
	items map[uuid.UUID]*entities.ReconciliationContext
}

func newContextRepository() *contextRepository {
	return &contextRepository{items: make(map[uuid.UUID]*entities.ReconciliationContext)}
}

func (repo *contextRepository) Create(
	_ context.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	repo.items[entity.ID] = entity
	return entity, nil
}

func (repo *contextRepository) FindByID(
	_ context.Context,
	identifier uuid.UUID,
) (*entities.ReconciliationContext, error) {
	contextEntity, ok := repo.items[identifier]
	if !ok {
		return nil, sql.ErrNoRows
	}

	return contextEntity, nil
}

func (repo *contextRepository) FindByName(
	_ context.Context,
	_ string,
) (*entities.ReconciliationContext, error) {
	return nil, sql.ErrNoRows
}

func (repo *contextRepository) FindAll(
	_ context.Context,
	cursor string,
	_ int,
	contextType *value_objects.ContextType,
	status *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	if cursor != "" {
		if _, err := libHTTP.DecodeUUIDCursor(cursor); err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("decode cursor: %w", err)
		}
	}

	results := make([]*entities.ReconciliationContext, 0)

	for _, contextEntity := range repo.items {
		if contextType != nil && contextEntity.Type != *contextType {
			continue
		}

		if status != nil && contextEntity.Status != *status {
			continue
		}

		results = append(results, contextEntity)
	}

	return results, libHTTP.CursorPagination{}, nil
}

func (repo *contextRepository) Update(
	_ context.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if _, ok := repo.items[entity.ID]; !ok {
		return nil, sql.ErrNoRows
	}

	repo.items[entity.ID] = entity

	return entity, nil
}

func (repo *contextRepository) Delete(_ context.Context, identifier uuid.UUID) error {
	_, ok := repo.items[identifier]

	if !ok {
		return sql.ErrNoRows
	}

	delete(repo.items, identifier)

	return nil
}

func (repo *contextRepository) Count(_ context.Context) (int64, error) {
	return int64(len(repo.items)), nil
}

type sourceRepository struct {
	items map[uuid.UUID]*entities.ReconciliationSource
}

func newSourceRepository() *sourceRepository {
	return &sourceRepository{items: make(map[uuid.UUID]*entities.ReconciliationSource)}
}

func (repo *sourceRepository) Create(
	_ context.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	repo.items[entity.ID] = entity
	return entity, nil
}

func (repo *sourceRepository) FindByID(
	_ context.Context,
	contextID, identifier uuid.UUID,
) (*entities.ReconciliationSource, error) {
	sourceEntity, ok := repo.items[identifier]
	if !ok || sourceEntity.ContextID != contextID {
		return nil, sql.ErrNoRows
	}

	return sourceEntity, nil
}

func (repo *sourceRepository) FindByContextID(
	_ context.Context,
	contextID uuid.UUID,
	cursor string,
	_ int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if cursor != "" {
		if _, err := libHTTP.DecodeUUIDCursor(cursor); err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("decode cursor: %w", err)
		}
	}

	results := make([]*entities.ReconciliationSource, 0)

	for _, sourceEntity := range repo.items {
		if sourceEntity.ContextID == contextID {
			results = append(results, sourceEntity)
		}
	}

	return results, libHTTP.CursorPagination{}, nil
}

func (repo *sourceRepository) FindByContextIDAndType(
	_ context.Context,
	contextID uuid.UUID,
	sourceType value_objects.SourceType,
	cursor string,
	_ int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if cursor != "" {
		if _, err := libHTTP.DecodeUUIDCursor(cursor); err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("decode cursor: %w", err)
		}
	}

	results := make([]*entities.ReconciliationSource, 0)

	for _, sourceEntity := range repo.items {
		if sourceEntity.ContextID == contextID && sourceEntity.Type == sourceType {
			results = append(results, sourceEntity)
		}
	}

	return results, libHTTP.CursorPagination{}, nil
}

func (repo *sourceRepository) Update(
	_ context.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if _, ok := repo.items[entity.ID]; !ok {
		return nil, sql.ErrNoRows
	}

	repo.items[entity.ID] = entity

	return entity, nil
}

func (repo *sourceRepository) Delete(_ context.Context, contextID, identifier uuid.UUID) error {
	sourceEntity, ok := repo.items[identifier]

	if !ok || sourceEntity.ContextID != contextID {
		return sql.ErrNoRows
	}

	delete(repo.items, identifier)

	return nil
}

type fieldMapRepository struct {
	items    map[uuid.UUID]*entities.FieldMap
	bySource map[uuid.UUID]uuid.UUID
}

func newFieldMapRepository() *fieldMapRepository {
	return &fieldMapRepository{
		items:    make(map[uuid.UUID]*entities.FieldMap),
		bySource: make(map[uuid.UUID]uuid.UUID),
	}
}

func (repo *fieldMapRepository) Create(
	_ context.Context,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	repo.items[entity.ID] = entity
	repo.bySource[entity.SourceID] = entity.ID

	return entity, nil
}

func (repo *fieldMapRepository) FindByID(
	_ context.Context,
	identifier uuid.UUID,
) (*entities.FieldMap, error) {
	fieldMapEntity, ok := repo.items[identifier]
	if !ok {
		return nil, sql.ErrNoRows
	}

	return fieldMapEntity, nil
}

func (repo *fieldMapRepository) FindBySourceID(
	_ context.Context,
	sourceID uuid.UUID,
) (*entities.FieldMap, error) {
	fieldMapID, ok := repo.bySource[sourceID]
	if !ok {
		return nil, sql.ErrNoRows
	}

	return repo.items[fieldMapID], nil
}

func (repo *fieldMapRepository) Update(
	_ context.Context,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	if _, ok := repo.items[entity.ID]; !ok {
		return nil, sql.ErrNoRows
	}

	repo.items[entity.ID] = entity
	repo.bySource[entity.SourceID] = entity.ID

	return entity, nil
}

func (repo *fieldMapRepository) ExistsBySourceIDs(
	_ context.Context,
	sourceIDs []uuid.UUID,
) (map[uuid.UUID]bool, error) {
	result := make(map[uuid.UUID]bool, len(sourceIDs))

	for _, sourceID := range sourceIDs {
		if _, ok := repo.bySource[sourceID]; ok {
			result[sourceID] = true
		}
	}

	return result, nil
}

func (repo *fieldMapRepository) Delete(_ context.Context, identifier uuid.UUID) error {
	fieldMapEntity, ok := repo.items[identifier]

	if !ok {
		return sql.ErrNoRows
	}

	delete(repo.bySource, fieldMapEntity.SourceID)
	delete(repo.items, identifier)

	return nil
}

type matchRuleRepository struct {
	items map[uuid.UUID]*entities.MatchRule
}

func newMatchRuleRepository() *matchRuleRepository {
	return &matchRuleRepository{items: make(map[uuid.UUID]*entities.MatchRule)}
}

func (repo *matchRuleRepository) Create(
	_ context.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	repo.items[entity.ID] = entity
	return entity, nil
}

func (repo *matchRuleRepository) FindByID(
	_ context.Context,
	contextID, identifier uuid.UUID,
) (*entities.MatchRule, error) {
	matchRule, ok := repo.items[identifier]
	if !ok || matchRule.ContextID != contextID {
		return nil, sql.ErrNoRows
	}

	return matchRule, nil
}

func (repo *matchRuleRepository) FindByContextID(
	_ context.Context,
	contextID uuid.UUID,
	cursor string,
	_ int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if cursor != "" {
		if _, err := libHTTP.DecodeUUIDCursor(cursor); err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("decode cursor: %w", err)
		}
	}

	results := repo.matchRulesByContext(contextID)

	sort.Slice(results, func(i, j int) bool {
		return results[i].Priority < results[j].Priority
	})

	return results, libHTTP.CursorPagination{}, nil
}

func (repo *matchRuleRepository) FindByContextIDAndType(
	_ context.Context,
	contextID uuid.UUID,
	ruleType value_objects.RuleType,
	cursor string,
	_ int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if cursor != "" {
		if _, err := libHTTP.DecodeUUIDCursor(cursor); err != nil {
			return nil, libHTTP.CursorPagination{}, fmt.Errorf("decode cursor: %w", err)
		}
	}

	results := make(entities.MatchRules, 0)

	for _, matchRule := range repo.items {
		if matchRule.ContextID == contextID && matchRule.Type == ruleType {
			results = append(results, matchRule)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Priority < results[j].Priority
	})

	return results, libHTTP.CursorPagination{}, nil
}

func (repo *matchRuleRepository) FindByPriority(
	_ context.Context,
	contextID uuid.UUID,
	priority int,
) (*entities.MatchRule, error) {
	for _, matchRule := range repo.items {
		if matchRule.ContextID == contextID && matchRule.Priority == priority {
			return matchRule, nil
		}
	}

	return nil, sql.ErrNoRows
}

func (repo *matchRuleRepository) Update(
	_ context.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if _, ok := repo.items[entity.ID]; !ok {
		return nil, sql.ErrNoRows
	}

	repo.items[entity.ID] = entity

	return entity, nil
}

func (repo *matchRuleRepository) Delete(_ context.Context, contextID, identifier uuid.UUID) error {
	matchRule, ok := repo.items[identifier]

	if !ok || matchRule.ContextID != contextID {
		return sql.ErrNoRows
	}

	delete(repo.items, identifier)

	return nil
}

func (repo *matchRuleRepository) ReorderPriorities(
	_ context.Context,
	contextID uuid.UUID,
	ruleIDs []uuid.UUID,
) error {
	for index, ruleID := range ruleIDs {
		matchRule, ok := repo.items[ruleID]

		if !ok || matchRule.ContextID != contextID {
			return sql.ErrNoRows
		}

		matchRule.Priority = index + 1
		repo.items[ruleID] = matchRule
	}

	return nil
}

func (repo *matchRuleRepository) matchRulesByContext(contextID uuid.UUID) entities.MatchRules {
	results := make(entities.MatchRules, 0)

	for _, matchRule := range repo.items {
		if matchRule.ContextID == contextID {
			results = append(results, matchRule)
		}
	}

	return results
}

func replacePathParams(path string, identifiers ...string) string {
	if !strings.Contains(path, ":") {
		return path
	}

	segments := strings.Split(path, "/")
	identifierIndex := 0

	for index, segment := range segments {
		if strings.HasPrefix(segment, ":") && identifierIndex < len(identifiers) {
			segments[index] = identifiers[identifierIndex]
			identifierIndex++
		}
	}

	return strings.Join(segments, "/")
}

func TestLogSpanError_WithNilLogger(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	_, span := tracer.Start(context.Background(), "test")

	defer span.End()

	require.NotPanics(t, func() {
		logSpanError(context.Background(), span, nil, "test message", errors.New("test error"))
	})
}

func TestLogSpanError_WithLogger(t *testing.T) {
	t.Parallel()

	tracer, _ := newTestTracer(t)
	_, span := tracer.Start(context.Background(), "test")

	defer span.End()

	mock := &testutil.TestLogger{}
	logSpanError(context.Background(), span, mock, "test message", errors.New("test error"))
	require.True(t, mock.ErrorCalled)
}

var (
	_ repositories.ContextRepository   = (*contextRepository)(nil)
	_ repositories.SourceRepository    = (*sourceRepository)(nil)
	_ repositories.FieldMapRepository  = (*fieldMapRepository)(nil)
	_ repositories.MatchRuleRepository = (*matchRuleRepository)(nil)
)
