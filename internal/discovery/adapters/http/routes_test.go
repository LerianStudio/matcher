//go:build unit

package http

import (
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryQuery "github.com/LerianStudio/matcher/internal/discovery/services/query"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			"ErrProtectedRouteHelperRequired",
			ErrProtectedRouteHelperRequired,
			"protected route helper is required",
		},
		{"ErrHandlerRequired", ErrHandlerRequired, "discovery handler is required"},
		{"ErrHandlerNotInitialized", ErrHandlerNotInitialized, "discovery handler dependencies are required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestRegisterRoutesNilProtected(t *testing.T) {
	t.Parallel()

	handler := &Handler{}

	err := RegisterRoutes(nil, handler)

	require.ErrorIs(t, err, ErrProtectedRouteHelperRequired)
}

func TestRegisterRoutesNilHandler(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	protected := func(_ string, _ ...string) fiber.Router {
		return app.Group("/")
	}

	err := RegisterRoutes(protected, nil)

	require.ErrorIs(t, err, ErrHandlerRequired)
}

func TestRegisterRoutesUninitializedHandler(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	protected := func(_ string, _ ...string) fiber.Router {
		return app.Group("/")
	}

	err := RegisterRoutes(protected, &Handler{})

	require.ErrorIs(t, err, ErrHandlerNotInitialized)
}

func TestRegisterRoutesSuccess(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	type authCall struct {
		resource string
		action   string
	}

	var calls []authCall
	protected := func(resource string, actions ...string) fiber.Router {
		for _, action := range actions {
			calls = append(calls, authCall{resource: resource, action: action})
		}

		return app.Group("/")
	}
	handler := &Handler{command: &discoveryCommand.UseCase{}, query: &discoveryQuery.UseCase{}}

	err := RegisterRoutes(protected, handler)

	assert.NoError(t, err)
	expectedCalls := []authCall{
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryRead},
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryRead},
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryRead},
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryRead},
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryWrite},
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryWrite},
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryRead},
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryWrite},
		{resource: auth.ResourceDiscovery, action: auth.ActionDiscoveryWrite},
	}
	require.Equal(t, expectedCalls, calls)

	registeredRoutes := make(map[string]bool)
	for _, routesByMethod := range app.Stack() {
		for _, route := range routesByMethod {
			registeredRoutes[route.Method+" "+route.Path] = true
		}
	}

	for _, routeKey := range []string{
		http.MethodGet + " /v1/discovery/status",
		http.MethodGet + " /v1/discovery/connections",
		http.MethodGet + " /v1/discovery/connections/:connectionId",
		http.MethodGet + " /v1/discovery/connections/:connectionId/schema",
		http.MethodPost + " /v1/discovery/connections/:connectionId/test",
		http.MethodPost + " /v1/discovery/connections/:connectionId/extractions",
		http.MethodGet + " /v1/discovery/extractions/:extractionId",
		http.MethodPost + " /v1/discovery/extractions/:extractionId/poll",
		http.MethodPost + " /v1/discovery/refresh",
	} {
		assert.True(t, registeredRoutes[routeKey], "expected route %s to be registered", routeKey)
	}
}
