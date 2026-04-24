// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterRoutes_Errors(t *testing.T) {
	t.Parallel()

	dummyHandler := &Handlers{}
	dummyLimiter := func(c *fiber.Ctx) error { return c.Next() }
	dummyProtected := func(_ string, _ ...string) fiber.Router { return fiber.New().Group("/") }

	tests := []struct {
		name          string
		protected     func(resource string, actions ...string) fiber.Router
		handlers      *Handlers
		exportLimiter fiber.Handler
		expectedError error
	}{
		{
			name:          "nil protected route helper",
			protected:     nil,
			handlers:      dummyHandler,
			exportLimiter: dummyLimiter,
			expectedError: ErrProtectedRouteHelperRequired,
		},
		{
			name:          "nil handlers",
			protected:     dummyProtected,
			handlers:      nil,
			exportLimiter: dummyLimiter,
			expectedError: ErrHandlersRequired,
		},
		{
			name:          "nil export limiter",
			protected:     dummyProtected,
			handlers:      dummyHandler,
			exportLimiter: nil,
			expectedError: ErrExportLimiterRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := RegisterRoutes(tt.protected, tt.handlers, tt.exportLimiter)

			require.Error(t, err)
			assert.Equal(t, tt.expectedError, err)
		})
	}
}

func TestRegisterRoutes_Success(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	routesCalled := make(map[string]bool)

	protected := func(resource string, actions ...string) fiber.Router {
		for _, action := range actions {
			routesCalled[resource+":"+action] = true
		}

		return app.Group("/api")
	}

	handlers := &Handlers{}
	limiter := func(c *fiber.Ctx) error { return c.Next() }

	err := RegisterRoutes(protected, handlers, limiter)

	require.NoError(t, err)

	assert.True(t, routesCalled["reporting:dashboard:read"])
	assert.True(t, routesCalled["reporting:export:read"])
}

func TestErrorSentinels(t *testing.T) {
	t.Parallel()

	require.EqualError(t, ErrProtectedRouteHelperRequired, "protected route helper is required")
	require.EqualError(t, ErrHandlersRequired, "reporting handlers are required")
	require.EqualError(t, ErrExportLimiterRequired, "export rate limiter is required")
}
