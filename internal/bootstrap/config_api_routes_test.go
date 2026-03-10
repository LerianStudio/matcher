//go:build unit

package bootstrap

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
)

type protectedCall struct {
	resource string
	action   string
}

func TestRegisterConfigAPIRoutes_Success(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	app := fiber.New()
	protectedCalls := make([]protectedCall, 0)

	protected := func(resource, action string) fiber.Router {
		protectedCalls = append(protectedCalls, protectedCall{resource: resource, action: action})

		// Return a group that mimics auth middleware without enforcing it.
		return app.Group("")
	}

	err = RegisterConfigAPIRoutes(protected, handler)
	assert.NoError(t, err)

	require.Len(t, protectedCalls, 5)
	assert.Equal(t, protectedCall{resource: auth.ResourceSystem, action: auth.ActionConfigRead}, protectedCalls[0])
	assert.Equal(t, protectedCall{resource: auth.ResourceSystem, action: auth.ActionConfigRead}, protectedCalls[1])
	assert.Equal(t, protectedCall{resource: auth.ResourceSystem, action: auth.ActionConfigRead}, protectedCalls[2])
	assert.Equal(t, protectedCall{resource: auth.ResourceSystem, action: auth.ActionConfigWrite}, protectedCalls[3])
	assert.Equal(t, protectedCall{resource: auth.ResourceSystem, action: auth.ActionConfigWrite}, protectedCalls[4])
}

func TestRegisterConfigAPIRoutes_NilProtected_ReturnsError(t *testing.T) {
	t.Parallel()

	cm := newAPITestConfigManager(t)

	handler, err := NewConfigAPIHandler(cm, &libLog.NopLogger{})
	require.NoError(t, err)

	err = RegisterConfigAPIRoutes(nil, handler)
	assert.ErrorIs(t, err, ErrConfigAPIProtectedRequired)
}

func TestRegisterConfigAPIRoutes_NilHandler_ReturnsError(t *testing.T) {
	t.Parallel()

	protected := func(_, _ string) fiber.Router {
		return fiber.New().Group("")
	}

	err := RegisterConfigAPIRoutes(protected, nil)
	assert.ErrorIs(t, err, ErrConfigAPIHandlerRequired)
}

func TestRegisterConfigAPIRoutes_SentinelErrors(t *testing.T) {
	t.Parallel()

	// Verify sentinel error messages are stable — they may appear in logs or API responses.
	assert.Equal(t, "protected route helper is required for config API", ErrConfigAPIProtectedRequired.Error())
	assert.Equal(t, "config API handler is required", ErrConfigAPIHandlerRequired.Error())
}
