// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
	governanceQuery "github.com/LerianStudio/matcher/internal/governance/services/query"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/pkg/constant"
)

// newTestHandler constructs a Handler wired to a query use case backed by
// the provided mock repository. Centralises the repo → queryUC → handler
// chain so individual tests can keep treating the mock as the read-path
// boundary without rewriting every call site.
func newTestHandler(t *testing.T, repo *mocks.MockAuditLogRepository, production bool) (*Handler, error) {
	t.Helper()

	queryUC, err := governanceQuery.NewUseCase(repo)
	require.NoError(t, err)

	return NewHandler(queryUC, production)
}

var (
	errTestDatabaseConnectionFailed = errors.New("database connection failed")
	errTestDatabaseTimeout          = errors.New("database timeout")
)

const invalidDateInput = "not-a-date"

func newFiberTestApp(ctx context.Context) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.SetUserContext(ctx)
		return c.Next()
	})

	return app
}

func TestNewHandler(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		repo := mocks.NewMockAuditLogRepository(ctrl)
		queryUC, err := governanceQuery.NewUseCase(repo)
		require.NoError(t, err)

		handler, err := NewHandler(queryUC, false)
		require.NoError(t, err)
		require.NotNil(t, handler)
	})

	t.Run("nil query use case", func(t *testing.T) {
		t.Parallel()

		handler, err := NewHandler(nil, false)
		require.ErrorIs(t, err, ErrQueryUseCaseRequired)
		require.Nil(t, handler)
	})
}

func TestRegisterRoutes(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		repo := mocks.NewMockAuditLogRepository(ctrl)
		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		app := fiber.New()
		protectedCalled := false

		protected := func(resource string, actions ...string) fiber.Router {
			protectedCalled = true

			require.Equal(t, "governance", resource)
			require.Equal(t, []string{"audit:read"}, actions)

			return app
		}

		err = RegisterRoutes(protected, handler)
		require.NoError(t, err)
		require.True(t, protectedCalled)
	})

	t.Run("nil protected helper", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		repo := mocks.NewMockAuditLogRepository(ctrl)
		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		err = RegisterRoutes(nil, handler)
		require.ErrorIs(t, err, ErrProtectedRouteHelperRequired)
	})

	t.Run("nil handler", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		protected := func(resource string, actions ...string) fiber.Router {
			return app
		}

		err := RegisterRoutes(protected, nil)
		require.ErrorIs(t, err, ErrHandlerRequired)
	})
}

func testGetAuditLogSuccess(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	auditLogID := uuid.New()
	tenantID := uuid.New()
	entityID := uuid.New()
	actorID := "user@example.com"

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   tenantID,
		EntityType: "reconciliation_context",
		EntityID:   entityID,
		Action:     "CREATE",
		ActorID:    &actorID,
		Changes:    []byte(`{"name": "Test Context"}`),
		CreatedAt:  time.Now().UTC(),
	}

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().GetByID(gomock.Any(), auditLogID).Return(auditLog, nil)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContextWithTenant(tenantID)
	resp := testGetAuditLogRequest(ctx, t, handler, auditLogID.String())

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var response dto.AuditLogResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	verifyAuditLogResponse(t, response, auditLogID, tenantID, entityID, actorID)
}

func testGetAuditLogMissingID(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockAuditLogRepository(ctrl)
	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContext()
	resp := testGetAuditLogRequest(ctx, t, handler, "")

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
}

func testGetAuditLogInvalidUUID(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockAuditLogRepository(ctrl)
	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContext()
	resp := testGetAuditLogRequest(ctx, t, handler, "not-a-uuid")

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, "invalid audit log id", errResp.Message)
}

func testGetAuditLogNotFoundError(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	auditLogID := uuid.New()

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().GetByID(gomock.Any(), auditLogID).Return(nil, governanceErrors.ErrAuditLogNotFound)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContext()
	resp := testGetAuditLogRequest(ctx, t, handler, auditLogID.String())

	defer resp.Body.Close()

	verifyErrorResponse(t, resp, fiber.StatusNotFound, "audit log not found")
}

func testGetAuditLogNotFoundNil(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	auditLogID := uuid.New()

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().GetByID(gomock.Any(), auditLogID).Return(nil, nil)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContext()
	resp := testGetAuditLogRequest(ctx, t, handler, auditLogID.String())

	defer resp.Body.Close()

	verifyErrorResponse(t, resp, fiber.StatusNotFound, "audit log not found")
}

func testGetAuditLogInternalError(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	auditLogID := uuid.New()

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().GetByID(gomock.Any(), auditLogID).Return(nil, errTestDatabaseConnectionFailed)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContext()
	resp := testGetAuditLogRequest(ctx, t, handler, auditLogID.String())

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestGetAuditLog(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		testGetAuditLogSuccess(t)
	})

	t.Run("missing id", func(t *testing.T) {
		t.Parallel()
		testGetAuditLogMissingID(t)
	})

	t.Run("invalid uuid", func(t *testing.T) {
		t.Parallel()
		testGetAuditLogInvalidUUID(t)
	})

	t.Run("not found - error", func(t *testing.T) {
		t.Parallel()
		testGetAuditLogNotFoundError(t)
	})

	t.Run("not found - nil result", func(t *testing.T) {
		t.Parallel()
		testGetAuditLogNotFoundNil(t)
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()
		testGetAuditLogInternalError(t)
	})
}

func createTestContext() context.Context {
	return libCommons.ContextWithTracer(
		context.Background(),
		noop.NewTracerProvider().Tracer("test"),
	)
}

func createTestContextWithTenant(tenantID uuid.UUID) context.Context {
	ctx := createTestContext()
	return context.WithValue(ctx, auth.TenantIDKey, tenantID.String())
}

func testGetAuditLogRequest(
	ctx context.Context,
	t *testing.T,
	handler *Handler,
	auditLogID string,
) *http.Response {
	t.Helper()

	app := newFiberTestApp(ctx)
	app.Get("/v1/governance/audit-logs/:id", handler.GetAuditLog)

	url := "/v1/governance/audit-logs/"
	if auditLogID != "" {
		url += auditLogID
	}

	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func verifyAuditLogResponse(
	t *testing.T,
	response dto.AuditLogResponse,
	auditLogID, tenantID, entityID uuid.UUID,
	actorID string,
) {
	t.Helper()

	require.Equal(t, auditLogID.String(), response.ID)
	require.Equal(t, tenantID.String(), response.TenantID)
	require.Equal(t, "reconciliation_context", response.EntityType)
	require.Equal(t, entityID.String(), response.EntityID)
	require.Equal(t, "CREATE", response.Action)
	require.NotNil(t, response.ActorID)
	require.Equal(t, actorID, *response.ActorID)
}

func verifyErrorResponse(
	t *testing.T,
	resp *http.Response,
	expectedStatus int,
	expectedMessage string,
) {
	t.Helper()

	require.Equal(t, expectedStatus, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, expectedGovernanceCode(expectedStatus, expectedMessage), errResp.Code)
	require.Equal(t, http.StatusText(expectedStatus), errResp.Title)
	require.Equal(t, expectedMessage, errResp.Message)
}

func expectedGovernanceCode(expectedStatus int, expectedMessage string) string {
	switch expectedMessage {
	case "audit log not found":
		return constant.CodeGovernanceAuditLogNotFound
	case "actor mapping not found":
		return constant.CodeGovernanceActorMappingNotFound
	case "archive not found":
		return constant.CodeGovernanceArchiveNotFound
	}

	switch expectedStatus {
	case fiber.StatusBadRequest:
		return constant.CodeInvalidRequest
	case fiber.StatusUnauthorized:
		return constant.CodeUnauthorized
	case fiber.StatusForbidden:
		return constant.CodeForbidden
	case fiber.StatusNotFound:
		return constant.CodeNotFound
	default:
		return constant.CodeInternalServerError
	}
}

func testListAuditLogsByEntitySuccess(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	tenantID := uuid.New()
	entityID := uuid.New()
	entityType := "reconciliation_context"

	logs := createTestAuditLogs(tenantID, entityID, entityType, 2)

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().
		ListByEntity(gomock.Any(), entityType, entityID, (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
		Return(logs, "", nil)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContextWithTenant(tenantID)
	resp := testListAuditLogsByEntityRequest(ctx, t, handler, entityType, entityID, "")

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var response dto.ListAuditLogsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	require.Len(t, response.Items, 2)
	require.Equal(t, constants.DefaultPaginationLimit, response.Limit)
	require.Empty(t, response.NextCursor)
}

func testListAuditLogsByEntityLimitCapped(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	tenantID := uuid.New()
	entityID := uuid.New()
	entityType := "source"

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().
		ListByEntity(gomock.Any(), entityType, entityID, (*sharedhttp.TimestampCursor)(nil), constants.MaximumPaginationLimit).
		Return([]*entities.AuditLog{}, "", nil)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContextWithTenant(tenantID)
	resp := testListAuditLogsByEntityRequest(ctx, t, handler, entityType, entityID, "limit=500")

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var response dto.ListAuditLogsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	require.Equal(t, constants.MaximumPaginationLimit, response.Limit)
}

func testListAuditLogsByEntityInvalidID(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	tenantID := uuid.New()

	repo := mocks.NewMockAuditLogRepository(ctrl)
	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContextWithTenant(tenantID)
	app := newFiberTestApp(ctx)
	app.Get(
		"/v1/governance/entities/:entityType/:entityId/audit-logs",
		handler.ListAuditLogsByEntity,
	)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/governance/entities/reconciliation_context/not-a-uuid/audit-logs",
		http.NoBody,
	)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	verifyErrorResponse(t, resp, fiber.StatusBadRequest, "invalid entity id")
}

func testListAuditLogsByEntityInvalidTenant(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	entityID := uuid.New()
	entityType := "reconciliation_context"

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().
		ListByEntity(gomock.Any(), entityType, entityID, (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
		Return([]*entities.AuditLog{}, "", nil)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContext()
	ctx = context.WithValue(ctx, auth.TenantIDKey, "not-a-uuid")
	resp := testListAuditLogsByEntityRequest(ctx, t, handler, entityType, entityID, "")

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func testListAuditLogsByEntityInternalError(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	tenantID := uuid.New()
	entityID := uuid.New()
	entityType := "reconciliation_context"

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().
		ListByEntity(gomock.Any(), entityType, entityID, (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
		Return(nil, "", errTestDatabaseTimeout)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContextWithTenant(tenantID)
	resp := testListAuditLogsByEntityRequest(ctx, t, handler, entityType, entityID, "")

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func testListAuditLogsByEntityInvalidCursor(t *testing.T) {
	t.Helper()
	ctrl := gomock.NewController(t)
	tenantID := uuid.New()
	entityID := uuid.New()
	entityType := "reconciliation_context"

	repo := mocks.NewMockAuditLogRepository(ctrl)
	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContextWithTenant(tenantID)
	resp := testListAuditLogsByEntityRequest(
		ctx,
		t,
		handler,
		entityType,
		entityID,
		"cursor=not-a-valid-cursor",
	)

	defer resp.Body.Close()

	verifyErrorResponse(t, resp, fiber.StatusBadRequest, "invalid pagination parameters")
}

func TestListAuditLogsByEntity(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsByEntitySuccess(t)
	})

	t.Run("success with cursor pagination", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsWithCursorPagination(t)
	})

	t.Run("limit capped at max", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsByEntityLimitCapped(t)
	})

	t.Run("missing entity type", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsMissingParam(t, "", uuid.New(), fiber.StatusNotFound)
	})

	t.Run("missing entity id", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsMissingParam(t, "reconciliation_context", uuid.Nil, fiber.StatusNotFound)
	})

	t.Run("invalid entity id", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsByEntityInvalidID(t)
	})

	t.Run("uses default tenant id when not set", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsWithTenantScenario(t, uuid.Nil, fiber.StatusOK)
	})

	t.Run("invalid tenant id proceeds with request", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsByEntityInvalidTenant(t)
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsByEntityInternalError(t)
	})

	t.Run("empty result", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsWithTenantScenario(t, uuid.New(), fiber.StatusOK)
	})

	t.Run("invalid cursor format for list audit logs", func(t *testing.T) {
		t.Parallel()
		testListAuditLogsByEntityInvalidCursor(t)
	})
}

func createTestAuditLogs(
	tenantID, entityID uuid.UUID,
	entityType string,
	count int,
) []*entities.AuditLog {
	logs := make([]*entities.AuditLog, count)
	actions := []string{"CREATE", "UPDATE", "DELETE"}

	for i := 0; i < count; i++ {
		logs[i] = &entities.AuditLog{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: entityType,
			EntityID:   entityID,
			Action:     actions[i%len(actions)],
			Changes:    []byte(`{}`),
			CreatedAt:  time.Now().UTC(),
		}
	}

	return logs
}

func testListAuditLogsByEntityRequest(
	ctx context.Context,
	t *testing.T,
	handler *Handler,
	entityType string,
	entityID uuid.UUID,
	queryParams string,
) *http.Response {
	t.Helper()

	app := newFiberTestApp(ctx)
	app.Get(
		"/v1/governance/entities/:entityType/:entityId/audit-logs",
		handler.ListAuditLogsByEntity,
	)

	url := "/v1/governance/entities/"
	if entityType != "" {
		url += entityType + "/"
	} else {
		url += "/"
	}

	if entityID != uuid.Nil {
		url += entityID.String()
	}

	url += "/audit-logs"

	if queryParams != "" {
		url += "?" + queryParams
	}

	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func testListAuditLogsWithCursorPagination(t *testing.T) {
	t.Helper()

	ctrl := gomock.NewController(t)

	tenantID := uuid.New()
	entityID := uuid.New()
	entityType := "match_rule"
	cursorTime := time.Now().UTC()
	cursorID := uuid.New()
	nextCursorTime := time.Now().UTC().Add(-time.Hour)
	nextCursorID := uuid.New()

	logs := createTestAuditLogs(tenantID, entityID, entityType, 1)
	logs[0].ID = nextCursorID
	logs[0].CreatedAt = nextCursorTime

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().ListByEntity(gomock.Any(), entityType, entityID, gomock.Any(), 10).DoAndReturn(
		func(_ context.Context, _ string, _ uuid.UUID, cursor *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
			require.NotNil(t, cursor)
			require.Equal(t, cursorID, cursor.ID)

			encodedCursor, encodeErr := sharedhttp.EncodeTimestampCursor(nextCursorTime, nextCursorID)
			require.NoError(t, encodeErr)

			return logs, encodedCursor, nil
		})

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContextWithTenant(tenantID)

	cursorParam, err := sharedhttp.EncodeTimestampCursor(cursorTime, cursorID)
	require.NoError(t, err)
	resp := testListAuditLogsByEntityRequest(
		ctx,
		t,
		handler,
		entityType,
		entityID,
		"limit=10&cursor="+cursorParam,
	)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	var response dto.ListAuditLogsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	require.Len(t, response.Items, 1)
	require.Equal(t, 10, response.Limit)
	expectedCursor, err := sharedhttp.EncodeTimestampCursor(nextCursorTime, nextCursorID)
	require.NoError(t, err)
	require.Equal(t, expectedCursor, response.NextCursor)
}

func testListAuditLogsMissingParam(
	t *testing.T,
	entityType string,
	entityID uuid.UUID,
	expectedStatus int,
) {
	t.Helper()

	ctrl := gomock.NewController(t)
	tenantID := uuid.New()

	repo := mocks.NewMockAuditLogRepository(ctrl)
	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	ctx := createTestContextWithTenant(tenantID)
	resp := testListAuditLogsByEntityRequest(ctx, t, handler, entityType, entityID, "")

	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func testListAuditLogsWithTenantScenario(t *testing.T, tenantID uuid.UUID, expectedStatus int) {
	t.Helper()

	ctrl := gomock.NewController(t)
	entityID := uuid.New()
	entityType := "reconciliation_context"

	repo := mocks.NewMockAuditLogRepository(ctrl)
	repo.EXPECT().
		ListByEntity(gomock.Any(), entityType, entityID, (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
		Return([]*entities.AuditLog{}, "", nil)

	handler, err := newTestHandler(t, repo, false)
	require.NoError(t, err)

	var ctx context.Context
	if tenantID == uuid.Nil {
		ctx = createTestContext()
	} else {
		ctx = createTestContextWithTenant(tenantID)
	}

	resp := testListAuditLogsByEntityRequest(ctx, t, handler, entityType, entityID, "")

	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)

	if expectedStatus == fiber.StatusOK {
		var response dto.ListAuditLogsResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Empty(t, response.Items)
	}
}

func TestParseDateTo(t *testing.T) {
	t.Parallel()

	t.Run("RFC3339 format unchanged", func(t *testing.T) {
		t.Parallel()

		input := "2025-01-20T15:30:00Z"
		result, err := parseDateTo(input)
		require.NoError(t, err)

		expected, _ := time.Parse(time.RFC3339, input)
		require.Equal(t, expected, result)
	})

	t.Run("date only format adjusted to end of day", func(t *testing.T) {
		t.Parallel()

		input := "2025-01-20"
		result, err := parseDateTo(input)
		require.NoError(t, err)

		expected := time.Date(2025, 1, 20, 23, 59, 59, 999999999, time.UTC)
		require.Equal(t, expected, result)
	})

	t.Run("invalid format", func(t *testing.T) {
		t.Parallel()

		_, err := parseDateTo("not-a-date")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidDateFormat)
	})
}

func TestParseDate(t *testing.T) {
	t.Parallel()

	t.Run("RFC3339 format", func(t *testing.T) {
		t.Parallel()

		input := "2025-01-20T15:30:00Z"
		result, err := parseDate(input)
		require.NoError(t, err)

		expected, _ := time.Parse(time.RFC3339, input)
		require.Equal(t, expected, result)
	})

	t.Run("date only format", func(t *testing.T) {
		t.Parallel()

		input := "2025-01-20"
		result, err := parseDate(input)
		require.NoError(t, err)

		expected, _ := time.Parse(time.DateOnly, input)
		require.Equal(t, expected, result)
	})

	t.Run("invalid format", func(t *testing.T) {
		t.Parallel()

		_, err := parseDate(invalidDateInput)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidDateFormat)
	})
}

func testListAuditLogsRequest(
	ctx context.Context,
	t *testing.T,
	handler *Handler,
	queryParams string,
) *http.Response {
	t.Helper()

	app := newFiberTestApp(ctx)
	app.Get("/v1/governance/audit-logs", handler.ListAuditLogs)

	url := "/v1/governance/audit-logs"
	if queryParams != "" {
		url += "?" + queryParams
	}

	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func TestListAuditLogs(t *testing.T) {
	t.Parallel()

	t.Run("success without filters", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		entityID := uuid.New()

		logs := createTestAuditLogs(tenantID, entityID, "reconciliation_context", 2)

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			Return(logs, "", nil)

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response dto.ListAuditLogsResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Len(t, response.Items, 2)
		require.Equal(t, constants.DefaultPaginationLimit, response.Limit)
		require.Empty(t, response.NextCursor)
		require.False(t, response.HasMore)
	})

	t.Run("success with actor filter", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		entityID := uuid.New()
		actorFilter := "user@example.com"

		logs := createTestAuditLogs(tenantID, entityID, "reconciliation_context", 1)
		logs[0].ActorID = &actorFilter

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			DoAndReturn(
				func(_ context.Context, filter entities.AuditLogFilter, _ *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
					require.NotNil(t, filter.Actor)
					require.Equal(t, actorFilter, *filter.Actor)
					return logs, "", nil
				})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "actor="+actorFilter)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("success with action filter", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			DoAndReturn(
				func(_ context.Context, filter entities.AuditLogFilter, _ *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
					require.NotNil(t, filter.Action)
					require.Equal(t, "CREATE", *filter.Action)
					return []*entities.AuditLog{}, "", nil
				})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "action=CREATE")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("success with entity_type filter", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			DoAndReturn(
				func(_ context.Context, filter entities.AuditLogFilter, _ *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
					require.NotNil(t, filter.EntityType)
					require.Equal(t, "match_rule", *filter.EntityType)
					return []*entities.AuditLog{}, "", nil
				})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "entity_type=match_rule")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("success with date_from filter RFC3339", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			DoAndReturn(
				func(_ context.Context, filter entities.AuditLogFilter, _ *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
					require.NotNil(t, filter.DateFrom)
					expected, _ := time.Parse(time.RFC3339, "2025-01-15T00:00:00Z")
					require.Equal(t, expected, *filter.DateFrom)
					return []*entities.AuditLog{}, "", nil
				})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "date_from=2025-01-15T00:00:00Z")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("success with date_from filter DateOnly", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			DoAndReturn(
				func(_ context.Context, filter entities.AuditLogFilter, _ *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
					require.NotNil(t, filter.DateFrom)
					expected, _ := time.Parse(time.DateOnly, "2025-01-15")
					require.Equal(t, expected, *filter.DateFrom)
					return []*entities.AuditLog{}, "", nil
				})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "date_from=2025-01-15")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("success with date_to filter RFC3339", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			DoAndReturn(
				func(_ context.Context, filter entities.AuditLogFilter, _ *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
					require.NotNil(t, filter.DateTo)
					expected, _ := time.Parse(time.RFC3339, "2025-01-20T23:59:59Z")
					require.Equal(t, expected, *filter.DateTo)
					return []*entities.AuditLog{}, "", nil
				})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "date_to=2025-01-20T23:59:59Z")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("success with date_to filter DateOnly", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			DoAndReturn(
				func(_ context.Context, filter entities.AuditLogFilter, _ *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
					require.NotNil(t, filter.DateTo)
					expected := time.Date(2025, 1, 20, 23, 59, 59, 999999999, time.UTC)
					require.Equal(t, expected, *filter.DateTo)
					return []*entities.AuditLog{}, "", nil
				})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "date_to=2025-01-20")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("success with all filters", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			DoAndReturn(
				func(_ context.Context, filter entities.AuditLogFilter, _ *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
					require.NotNil(t, filter.Actor)
					require.Equal(t, "admin@example.com", *filter.Actor)
					require.NotNil(t, filter.Action)
					require.Equal(t, "UPDATE", *filter.Action)
					require.NotNil(t, filter.EntityType)
					require.Equal(t, "source", *filter.EntityType)
					require.NotNil(t, filter.DateFrom)
					require.NotNil(t, filter.DateTo)
					return []*entities.AuditLog{}, "", nil
				})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(
			ctx,
			t,
			handler,
			"actor=admin@example.com&action=UPDATE&entity_type=source&date_from=2025-01-01&date_to=2025-01-31",
		)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("success with pagination", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()
		entityID := uuid.New()
		cursorTime := time.Now().UTC()
		cursorID := uuid.New()
		nextCursorTime := time.Now().UTC().Add(-time.Hour)
		nextCursorID := uuid.New()

		logs := createTestAuditLogs(tenantID, entityID, "reconciliation_context", 1)
		logs[0].ID = nextCursorID
		logs[0].CreatedAt = nextCursorTime

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any(), 10).DoAndReturn(
			func(_ context.Context, _ entities.AuditLogFilter, cursor *sharedhttp.TimestampCursor, _ int) ([]*entities.AuditLog, string, error) {
				require.NotNil(t, cursor)
				require.Equal(t, cursorID, cursor.ID)

				encodedCursor, encodeErr := sharedhttp.EncodeTimestampCursor(nextCursorTime, nextCursorID)
				require.NoError(t, encodeErr)

				return logs, encodedCursor, nil
			})

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)

		cursorParam, err := sharedhttp.EncodeTimestampCursor(cursorTime, cursorID)
		require.NoError(t, err)
		resp := testListAuditLogsRequest(ctx, t, handler, "limit=10&cursor="+cursorParam)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response dto.ListAuditLogsResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Len(t, response.Items, 1)
		require.Equal(t, 10, response.Limit)
		require.NotEmpty(t, response.NextCursor)
		require.True(t, response.HasMore)
	})

	t.Run("limit capped at max", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.MaximumPaginationLimit).
			Return([]*entities.AuditLog{}, "", nil)

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "limit=500")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response dto.ListAuditLogsResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Equal(t, constants.MaximumPaginationLimit, response.Limit)
	})

	t.Run("invalid date_from format", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "date_from=not-a-date")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		require.Contains(t, errResp.Message, "date_from")
	})

	t.Run("invalid date_to format", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "date_to=invalid-date")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		require.Contains(t, errResp.Message, "date_to")
	})

	t.Run("invalid cursor format", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "cursor=invalid-cursor")

		defer resp.Body.Close()

		verifyErrorResponse(t, resp, fiber.StatusBadRequest, "invalid pagination parameters")
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			Return(nil, "", errTestDatabaseConnectionFailed)

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("empty result", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), (*sharedhttp.TimestampCursor)(nil), constants.DefaultPaginationLimit).
			Return([]*entities.AuditLog{}, "", nil)

		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		resp := testListAuditLogsRequest(ctx, t, handler, "")

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response dto.ListAuditLogsResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.Empty(t, response.Items)
		require.False(t, response.HasMore)
	})

	t.Run("actor exceeds max length", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		longActor := strings.Repeat("a", 256)
		resp := testListAuditLogsRequest(ctx, t, handler, "actor="+longActor)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		require.Contains(t, errResp.Message, "actor")
	})

	t.Run("action exceeds max length", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		longAction := strings.Repeat("x", 51)
		resp := testListAuditLogsRequest(ctx, t, handler, "action="+longAction)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		require.Contains(t, errResp.Message, "action")
	})

	t.Run("entity_type exceeds max length", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		tenantID := uuid.New()

		repo := mocks.NewMockAuditLogRepository(ctrl)
		handler, err := newTestHandler(t, repo, false)
		require.NoError(t, err)

		ctx := createTestContextWithTenant(tenantID)
		longEntityType := strings.Repeat("z", 51)
		resp := testListAuditLogsRequest(ctx, t, handler, "entity_type="+longEntityType)

		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		require.Contains(t, errResp.Message, "entity_type")
	})
}

func TestStartHandlerSpanWithNilTracer(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		ctx, span, _ := startHandlerSpan(c, "test_span")
		defer span.End()

		require.NotNil(t, ctx)
		require.NotNil(t, span)

		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestBadRequestWithNilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		tracer := noop.NewTracerProvider().Tracer("test")
		_, span := tracer.Start(c.UserContext(), "test")
		defer span.End()

		return (&Handler{}).badRequest(c.UserContext(), c, span, &libLog.NopLogger{}, "test error", errors.New("test"))
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, http.StatusText(http.StatusBadRequest), errResp.Title)
	require.Equal(t, "test error", errResp.Message)
}

func TestWriteServiceErrorWithNilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		tracer := noop.NewTracerProvider().Tracer("test")
		_, span := tracer.Start(c.UserContext(), "test")
		defer span.End()

		return (&Handler{}).writeServiceError(c.UserContext(), c, span, &libLog.NopLogger{}, "test error", errors.New("test"))
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, http.StatusText(http.StatusInternalServerError), errResp.Title)
	require.Equal(t, "an unexpected error occurred", errResp.Message)
}

func TestWriteNotFoundWithNilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		tracer := noop.NewTracerProvider().Tracer("test")
		_, span := tracer.Start(c.UserContext(), "test")
		defer span.End()

		return (&Handler{}).writeNotFound(c.UserContext(), c, span, &libLog.NopLogger{}, "not_found", "not found", errors.New("test"))
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, fiber.StatusNotFound, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	require.Equal(t, http.StatusText(http.StatusNotFound), errResp.Title)
	require.Equal(t, "not found", errResp.Message)
}
