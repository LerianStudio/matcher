//go:build unit

package http

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
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
	var calls []struct {
		resource string
		action   string
	}
	protected := func(resource, action string) fiber.Router {
		calls = append(calls, struct {
			resource string
			action   string
		}{resource: resource, action: action})

		return app.Group("/")
	}
	handler := &Handler{}

	err := RegisterRoutes(protected, handler)

	assert.NoError(t, err)
	require.Len(t, calls, 6)
	assert.Equal(t, auth.ResourceDiscovery, calls[0].resource)
	assert.Equal(t, auth.ActionDiscoveryRead, calls[0].action)
	assert.Equal(t, auth.ActionDiscoveryWrite, calls[4].action)
	assert.Equal(t, auth.ActionDiscoveryWrite, calls[5].action)
}
