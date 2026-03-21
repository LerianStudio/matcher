//go:build unit

package http

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{"ErrHandlerRequired", ErrHandlerRequired, "handler is required"},
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
	protected := func(_, _ string) fiber.Router {
		return app.Group("/")
	}

	err := RegisterRoutes(protected, nil)

	require.ErrorIs(t, err, ErrHandlerRequired)
}

func TestRegisterRoutesSuccess(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	protected := func(_, _ string) fiber.Router {
		return app.Group("/")
	}
	handler := &Handler{}

	err := RegisterRoutes(protected, handler)

	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// HIGH-17: Route cutover — verify domain-native paths exist, legacy paths absent
// ---------------------------------------------------------------------------

func TestRegisterRoutes_DomainNativePathsExist_LegacyPathsAbsent(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()

	protected := func(_, _ string) fiber.Router {
		return app
	}
	handler := &Handler{}

	err := RegisterRoutes(protected, handler)
	require.NoError(t, err)

	routes := app.GetRoutes()
	routeSet := make(map[string]bool)

	for _, r := range routes {
		routeSet[r.Method+" "+r.Path] = true
	}

	// Domain-native paths MUST exist.
	assert.True(t, routeSet["POST /v1/contexts"], "POST /v1/contexts should be registered")
	assert.True(t, routeSet["GET /v1/contexts"], "GET /v1/contexts should be registered")
	assert.True(t, routeSet["GET /v1/fee-schedules"], "GET /v1/fee-schedules should be registered")
	assert.True(t, routeSet["POST /v1/fee-schedules"], "POST /v1/fee-schedules should be registered")
	assert.True(t, routeSet["PATCH /v1/field-maps/:fieldMapId"], "PATCH /v1/field-maps/:fieldMapId should be registered")

	// Legacy paths MUST NOT exist.
	assert.False(t, routeSet["POST /v1/config/contexts"], "legacy POST /v1/config/contexts should NOT be registered")
	assert.False(t, routeSet["GET /v1/config/contexts"], "legacy GET /v1/config/contexts should NOT be registered")
	assert.False(t, routeSet["GET /v1/config/fee-schedules"], "legacy GET /v1/config/fee-schedules should NOT be registered")
}
