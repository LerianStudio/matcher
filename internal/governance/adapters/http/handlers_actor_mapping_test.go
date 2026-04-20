//go:build unit

package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/governance/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
	"github.com/LerianStudio/matcher/internal/governance/services/query"
)

var errTestActorMappingRepoFailed = errors.New("actor mapping repo failed")

func newTestActorMappingHandler(
	t *testing.T,
	repo *mocks.MockActorMappingRepository,
) *ActorMappingHandler {
	t.Helper()

	cmdUC, err := command.NewActorMappingUseCase(repo)
	require.NoError(t, err)

	queryUC, err := query.NewActorMappingQueryUseCase(repo)
	require.NoError(t, err)

	handler, err := NewActorMappingHandler(cmdUC, queryUC, false)
	require.NoError(t, err)

	return handler
}

func TestNewActorMappingHandler(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		handler := newTestActorMappingHandler(t, repo)
		require.NotNil(t, handler)
	})

	t.Run("nil command use case", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		queryUC, err := query.NewActorMappingQueryUseCase(repo)
		require.NoError(t, err)

		handler, err := NewActorMappingHandler(nil, queryUC, false)
		require.ErrorIs(t, err, ErrActorMappingCommandUCRequired)
		require.Nil(t, handler)
	})

	t.Run("nil query use case", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		cmdUC, err := command.NewActorMappingUseCase(repo)
		require.NoError(t, err)

		handler, err := NewActorMappingHandler(cmdUC, nil, false)
		require.ErrorIs(t, err, ErrActorMappingQueryUCRequired)
		require.Nil(t, handler)
	})
}

func TestUpsertActorMappingHandler(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		now := time.Now().UTC()
		displayName := "John Doe"
		email := "john@example.com"

		mapping := &entities.ActorMapping{
			ActorID:     "actor-123",
			DisplayName: &displayName,
			Email:       &email,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(mapping, nil)

		handler := newTestActorMappingHandler(t, repo)

		body := dto.UpsertActorMappingRequest{
			DisplayName: &displayName,
			Email:       &email,
		}

		resp := testUpsertActorMappingRequest(t, handler, "actor-123", body)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response dto.ActorMappingResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		assert.Equal(t, "actor-123", response.ActorID)
		require.NotNil(t, response.DisplayName)
		assert.Equal(t, "John Doe", *response.DisplayName)
		require.NotNil(t, response.Email)
		assert.Equal(t, "john@example.com", *response.Email)
	})

	t.Run("success preserves existing email when omitted in request", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		now := time.Now().UTC()
		updatedName := "Updated Name"
		existingEmail := "existing@example.com"

		mapping := &entities.ActorMapping{
			ActorID:     "actor-123",
			DisplayName: &updatedName,
			Email:       &existingEmail,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(mapping, nil)

		handler := newTestActorMappingHandler(t, repo)

		body := dto.UpsertActorMappingRequest{DisplayName: &updatedName}

		resp := testUpsertActorMappingRequest(t, handler, "actor-123", body)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response dto.ActorMappingResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		require.NotNil(t, response.DisplayName)
		assert.Equal(t, updatedName, *response.DisplayName)
		require.NotNil(t, response.Email)
		assert.Equal(t, existingEmail, *response.Email)
	})

	t.Run("missing actor id", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		handler := newTestActorMappingHandler(t, repo)

		resp := testUpsertActorMappingRequest(t, handler, "", dto.UpsertActorMappingRequest{})
		defer resp.Body.Close()

		// Empty path param results in 404 from Fiber
		require.Equal(t, fiber.StatusNotFound, resp.StatusCode)
	})

	t.Run("empty body rejected - at least one field required", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		handler := newTestActorMappingHandler(t, repo)

		resp := testUpsertActorMappingRequest(t, handler, "actor-123", dto.UpsertActorMappingRequest{})
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})

	t.Run("repository error on upsert", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, errTestActorMappingRepoFailed)

		handler := newTestActorMappingHandler(t, repo)

		displayName := "John Doe"
		body := dto.UpsertActorMappingRequest{DisplayName: &displayName}

		resp := testUpsertActorMappingRequest(t, handler, "actor-123", body)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("nil persisted mapping returns internal server error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Upsert(gomock.Any(), gomock.Any()).Return(nil, nil)

		handler := newTestActorMappingHandler(t, repo)

		displayName := "John Doe"
		body := dto.UpsertActorMappingRequest{DisplayName: &displayName}

		resp := testUpsertActorMappingRequest(t, handler, "actor-123", body)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("entity validation error - actor id too long", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)

		handler := newTestActorMappingHandler(t, repo)

		longActorID := make([]byte, 256)
		for i := range longActorID {
			longActorID[i] = 'a'
		}

		displayName := "Test"
		body := dto.UpsertActorMappingRequest{DisplayName: &displayName}

		resp := testUpsertActorMappingRequest(t, handler, string(longActorID), body)
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
	})
}

func TestGetActorMappingHandler(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		now := time.Now().UTC()
		displayName := "Jane Doe"

		mapping := &entities.ActorMapping{
			ActorID:     "actor-456",
			DisplayName: &displayName,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().GetByActorID(gomock.Any(), "actor-456").Return(mapping, nil)

		handler := newTestActorMappingHandler(t, repo)

		resp := testGetActorMappingRequest(t, handler, "actor-456")
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)

		var response dto.ActorMappingResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
		assert.Equal(t, "actor-456", response.ActorID)
		require.NotNil(t, response.DisplayName)
		assert.Equal(t, "Jane Doe", *response.DisplayName)
	})

	t.Run("not found error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().GetByActorID(gomock.Any(), "nonexistent").Return(nil, governanceErrors.ErrActorMappingNotFound)

		handler := newTestActorMappingHandler(t, repo)

		resp := testGetActorMappingRequest(t, handler, "nonexistent")
		defer resp.Body.Close()

		verifyErrorResponse(t, resp, fiber.StatusNotFound, "actor mapping not found")
	})

	t.Run("nil result returns not found", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().GetByActorID(gomock.Any(), "ghost").Return(nil, nil)

		handler := newTestActorMappingHandler(t, repo)

		resp := testGetActorMappingRequest(t, handler, "ghost")
		defer resp.Body.Close()

		verifyErrorResponse(t, resp, fiber.StatusNotFound, "actor mapping not found")
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().GetByActorID(gomock.Any(), "actor-err").Return(nil, errTestActorMappingRepoFailed)

		handler := newTestActorMappingHandler(t, repo)

		resp := testGetActorMappingRequest(t, handler, "actor-err")
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})
}

func TestPseudonymizeActorHandler(t *testing.T) {
	t.Parallel()

	t.Run("success returns 204", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Pseudonymize(gomock.Any(), "actor-123").Return(nil)

		handler := newTestActorMappingHandler(t, repo)

		resp := testPseudonymizeActorRequest(t, handler, "actor-123")
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNoContent, resp.StatusCode)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Pseudonymize(gomock.Any(), "nonexistent").Return(governanceErrors.ErrActorMappingNotFound)

		handler := newTestActorMappingHandler(t, repo)

		resp := testPseudonymizeActorRequest(t, handler, "nonexistent")
		defer resp.Body.Close()

		verifyErrorResponse(t, resp, fiber.StatusNotFound, "actor mapping not found")
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Pseudonymize(gomock.Any(), "actor-err").Return(errTestActorMappingRepoFailed)

		handler := newTestActorMappingHandler(t, repo)

		resp := testPseudonymizeActorRequest(t, handler, "actor-err")
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})
}

func TestDeleteActorMappingHandler(t *testing.T) {
	t.Parallel()

	t.Run("success returns 204", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Delete(gomock.Any(), "actor-123").Return(nil)

		handler := newTestActorMappingHandler(t, repo)

		resp := testDeleteActorMappingRequest(t, handler, "actor-123")
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusNoContent, resp.StatusCode)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Delete(gomock.Any(), "nonexistent").Return(governanceErrors.ErrActorMappingNotFound)

		handler := newTestActorMappingHandler(t, repo)

		resp := testDeleteActorMappingRequest(t, handler, "nonexistent")
		defer resp.Body.Close()

		verifyErrorResponse(t, resp, fiber.StatusNotFound, "actor mapping not found")
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		repo.EXPECT().Delete(gomock.Any(), "actor-err").Return(errTestActorMappingRepoFailed)

		handler := newTestActorMappingHandler(t, repo)

		resp := testDeleteActorMappingRequest(t, handler, "actor-err")
		defer resp.Body.Close()

		require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	})
}

func TestActorMappingSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrActorMappingCommandUCRequired", ErrActorMappingCommandUCRequired, "actor mapping command use case is required"},
		{"ErrActorMappingQueryUCRequired", ErrActorMappingQueryUCRequired, "actor mapping query use case is required"},
		{"ErrMissingActorID", ErrMissingActorID, "actor id path parameter is required"},
		{"ErrActorMappingHandlerRequired", ErrActorMappingHandlerRequired, "actor mapping handler is required"},
		{"ErrAtLeastOneFieldRequired", ErrAtLeastOneFieldRequired, "at least one of display_name or email must be provided"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestRegisterActorMappingRoutes(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		handler := newTestActorMappingHandler(t, repo)

		app := fiber.New()
		protected := func(_ string, _ ...string) fiber.Router {
			return app.Group("/")
		}

		err := RegisterActorMappingRoutes(protected, handler)
		require.NoError(t, err)
	})

	t.Run("nil protected helper", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockActorMappingRepository(ctrl)
		handler := newTestActorMappingHandler(t, repo)

		err := RegisterActorMappingRoutes(nil, handler)
		require.ErrorIs(t, err, ErrProtectedRouteHelperRequired)
	})

	t.Run("nil handler", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		protected := func(_ string, _ ...string) fiber.Router {
			return app.Group("/")
		}

		err := RegisterActorMappingRoutes(protected, nil)
		require.ErrorIs(t, err, ErrActorMappingHandlerRequired)
	})
}

// --- Helpers ---

func testUpsertActorMappingRequest(
	t *testing.T,
	handler *ActorMappingHandler,
	actorID string,
	body dto.UpsertActorMappingRequest,
) *http.Response {
	t.Helper()

	ctx := createTestContext()
	app := newFiberTestApp(ctx)
	app.Put("/v1/governance/actor-mappings/:actorId", handler.UpsertActorMapping)

	jsonBody, err := json.Marshal(body)
	require.NoError(t, err)

	url := "/v1/governance/actor-mappings/"
	if actorID != "" {
		url += actorID
	}

	req := httptest.NewRequest(http.MethodPut, url, bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func testGetActorMappingRequest(
	t *testing.T,
	handler *ActorMappingHandler,
	actorID string,
) *http.Response {
	t.Helper()

	ctx := createTestContext()
	app := newFiberTestApp(ctx)
	app.Get("/v1/governance/actor-mappings/:actorId", handler.GetActorMapping)

	url := "/v1/governance/actor-mappings/" + actorID

	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func testPseudonymizeActorRequest(
	t *testing.T,
	handler *ActorMappingHandler,
	actorID string,
) *http.Response {
	t.Helper()

	ctx := createTestContext()
	app := newFiberTestApp(ctx)
	app.Post("/v1/governance/actor-mappings/:actorId/pseudonymize", handler.PseudonymizeActor)

	url := "/v1/governance/actor-mappings/" + actorID + "/pseudonymize"

	req := httptest.NewRequest(http.MethodPost, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}

func testDeleteActorMappingRequest(
	t *testing.T,
	handler *ActorMappingHandler,
	actorID string,
) *http.Response {
	t.Helper()

	ctx := createTestContext()
	app := newFiberTestApp(ctx)
	app.Delete("/v1/governance/actor-mappings/:actorId", handler.DeleteActorMapping)

	url := "/v1/governance/actor-mappings/" + actorID

	req := httptest.NewRequest(http.MethodDelete, url, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	return resp
}
