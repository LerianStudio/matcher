//go:build unit

package http

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"
)

func TestRegisterRoutesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		protected func(string, ...string) fiber.Router
		handlers  *Handlers
		wantErr   error
	}{
		{
			name:      "nil protected route helper",
			protected: nil,
			handlers:  &Handlers{},
			wantErr:   ErrProtectedRouteHelperRequired,
		},
		{
			name:      "nil handlers",
			protected: func(_ string, _ ...string) fiber.Router { return nil },
			handlers:  nil,
			wantErr:   ErrHandlersRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := RegisterRoutes(tt.protected, tt.handlers)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestRegisterRoutesSuccess(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	protected := func(resource string, actions ...string) fiber.Router {
		return app.Group("/api")
	}

	handlers := &Handlers{}
	require.NoError(t, RegisterRoutes(protected, handlers))
}
