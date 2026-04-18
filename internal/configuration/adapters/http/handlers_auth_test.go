//go:build unit

// Package http provides HTTP handlers for the configuration service.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// errNotImplemented is returned by noop repositories for unimplemented methods.
var errNotImplemented = errors.New("not implemented")

const defaultPaginationOffset = 0

func TestConfigRoutes_AuthEnforced(t *testing.T) {
	t.Parallel()

	authServer := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			switch request.URL.Path {
			case "/health":
				writer.WriteHeader(http.StatusOK)
				_, _ = writer.Write([]byte("healthy"))
			case "/v1/authorize":
				writer.WriteHeader(http.StatusOK)
				_, _ = writer.Write([]byte(`{"authorized": false}`))
			default:
				writer.WriteHeader(http.StatusNotFound)
			}
		}),
	)
	defer authServer.Close()

	var loggerInterface libLog.Logger = libLog.NewNop()

	authClient := authMiddleware.NewAuthClient(authServer.URL, true, &loggerInterface)
	extractor, err := auth.NewTenantExtractor(
		true,
		true,
		auth.DefaultTenantID,
		auth.DefaultTenantSlug,
		"test-secret",
		"development",
	)
	require.NoError(t, err)

	app := fiber.New()
	protected := func(resource string, actions ...string) fiber.Router {
		group, err := auth.ProtectedGroupWithActionsWithMiddleware(app, authClient, extractor, resource, actions)
		require.NoError(t, err)

		return group
	}

	commandUseCase, err := command.NewUseCase(
		&noopContextRepo{},
		&noopSourceRepo{},
		&noopFieldMapRepo{},
		&noopMatchRuleRepo{},
	)
	require.NoError(t, err)
	queryUseCase, err := query.NewUseCase(
		&noopContextRepo{},
		&noopSourceRepo{},
		&noopFieldMapRepo{},
		&noopMatchRuleRepo{},
	)
	require.NoError(t, err)

	handler, err := NewHandler(commandUseCase, queryUseCase, false)
	require.NoError(t, err)
	err = RegisterRoutes(protected, handler)
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/v1/contexts", nil)

	response, err := app.Test(request)
	require.NoError(t, err)

	defer response.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, response.StatusCode)
}

func TestParsePagination_LimitClampsToDefault(t *testing.T) {
	t.Parallel()

	type paginationResponse struct {
		Limit  int `json:"limit"`
		Offset int `json:"offset"`
	}

	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		limit, offset, err := libHTTP.ParsePagination(c)
		if err != nil {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}

		return c.Status(http.StatusOK).JSON(fiber.Map{
			"limit":  limit,
			"offset": offset,
		})
	})

	// In lib-commons v4, ParseOpaqueCursorPagination silently clamps
	// limit <= 0 to DefaultLimit instead of returning an error.
	tests := []struct {
		name          string
		query         string
		expectedLimit int
	}{
		{"zero limit clamps to default", "/?limit=0", constants.DefaultPaginationLimit},
		{"negative limit clamps to default", "/?limit=-5", constants.DefaultPaginationLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			request := httptest.NewRequest(http.MethodGet, tt.query, nil)

			response, err := app.Test(request)
			require.NoError(t, err)

			defer response.Body.Close()

			assert.Equal(t, http.StatusOK, response.StatusCode)

			var payload paginationResponse
			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			assert.Equal(t, tt.expectedLimit, payload.Limit)
			assert.Equal(t, defaultPaginationOffset, payload.Offset)
		})
	}
}

func TestParsePagination_OffsetClampsToDefault(t *testing.T) {
	t.Parallel()

	type paginationResponse struct {
		Limit  int `json:"limit"`
		Offset int `json:"offset"`
	}

	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		limit, offset, err := libHTTP.ParsePagination(c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"limit":  limit,
			"offset": offset,
		})
	})

	// Contract: ParsePagination clamps offset < 0 to default offset.
	tests := []struct {
		name  string
		query string
	}{
		{"negative offset clamps to default", "/?offset=-1"},
		{"large negative offset clamps to default", "/?offset=-100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			request := httptest.NewRequest(http.MethodGet, tt.query, nil)

			response, err := app.Test(request)
			require.NoError(t, err)

			defer response.Body.Close()

			assert.Equal(t, http.StatusOK, response.StatusCode)

			var payload paginationResponse
			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			assert.Equal(t, constants.DefaultPaginationLimit, payload.Limit)
			assert.Equal(t, defaultPaginationOffset, payload.Offset)
		})
	}
}

type noopContextRepo struct{}

type noopSourceRepo struct{}

type noopFieldMapRepo struct{}

type noopMatchRuleRepo struct{}

func (noopContextRepo) Create(
	_ context.Context,
	_ *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	return nil, errNotImplemented
}

func (noopContextRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.ReconciliationContext, error) {
	return nil, errNotImplemented
}

func (noopContextRepo) FindByName(
	_ context.Context,
	_ string,
) (*entities.ReconciliationContext, error) {
	return nil, errNotImplemented
}

func (noopContextRepo) FindAll(
	_ context.Context,
	_ string,
	_ int,
	_ *value_objects.ContextType,
	_ *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errNotImplemented
}

func (noopContextRepo) Update(
	_ context.Context,
	_ *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	return nil, errNotImplemented
}

func (noopContextRepo) Delete(_ context.Context, _ uuid.UUID) error {
	return errNotImplemented
}

func (noopContextRepo) Count(_ context.Context) (int64, error) {
	return 0, errNotImplemented
}

func (noopSourceRepo) Create(
	_ context.Context,
	_ *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	return nil, errNotImplemented
}

func (noopSourceRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*entities.ReconciliationSource, error) {
	return nil, errNotImplemented
}

func (noopSourceRepo) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errNotImplemented
}

func (noopSourceRepo) FindByContextIDAndType(
	_ context.Context,
	_ uuid.UUID,
	_ value_objects.SourceType,
	_ string,
	_ int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errNotImplemented
}

func (noopSourceRepo) Update(
	_ context.Context,
	_ *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	return nil, errNotImplemented
}

func (noopSourceRepo) Delete(_ context.Context, _, _ uuid.UUID) error {
	return errNotImplemented
}

func (noopFieldMapRepo) Create(
	_ context.Context,
	_ *entities.FieldMap,
) (*entities.FieldMap, error) {
	return nil, errNotImplemented
}

func (noopFieldMapRepo) FindByID(_ context.Context, _ uuid.UUID) (*entities.FieldMap, error) {
	return nil, errNotImplemented
}

func (noopFieldMapRepo) FindBySourceID(_ context.Context, _ uuid.UUID) (*entities.FieldMap, error) {
	return nil, errNotImplemented
}

func (noopFieldMapRepo) Update(
	_ context.Context,
	_ *entities.FieldMap,
) (*entities.FieldMap, error) {
	return nil, errNotImplemented
}

func (noopFieldMapRepo) ExistsBySourceIDs(
	_ context.Context,
	sourceIDs []uuid.UUID,
) (map[uuid.UUID]bool, error) {
	return make(map[uuid.UUID]bool, len(sourceIDs)), nil
}

func (noopFieldMapRepo) Delete(_ context.Context, _ uuid.UUID) error {
	return errNotImplemented
}

func (noopMatchRuleRepo) Create(
	_ context.Context,
	_ *entities.MatchRule,
) (*entities.MatchRule, error) {
	return nil, errNotImplemented
}

func (noopMatchRuleRepo) FindByID(_ context.Context, _, _ uuid.UUID) (*entities.MatchRule, error) {
	return nil, errNotImplemented
}

func (noopMatchRuleRepo) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errNotImplemented
}

func (noopMatchRuleRepo) FindByContextIDAndType(
	_ context.Context,
	_ uuid.UUID,
	_ value_objects.RuleType,
	_ string,
	_ int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errNotImplemented
}

func (noopMatchRuleRepo) FindByPriority(
	_ context.Context,
	_ uuid.UUID,
	_ int,
) (*entities.MatchRule, error) {
	return nil, errNotImplemented
}

func (noopMatchRuleRepo) Update(
	_ context.Context,
	_ *entities.MatchRule,
) (*entities.MatchRule, error) {
	return nil, errNotImplemented
}

func (noopMatchRuleRepo) Delete(_ context.Context, _, _ uuid.UUID) error {
	return errNotImplemented
}

func (noopMatchRuleRepo) ReorderPriorities(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return errNotImplemented
}
