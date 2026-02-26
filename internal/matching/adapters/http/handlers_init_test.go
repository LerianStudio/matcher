//go:build unit

package http

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	matchingQuery "github.com/LerianStudio/matcher/internal/matching/services/query"
)

func TestNewHandler(t *testing.T) {
	t.Parallel()

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: uuid.New(), Active: true},
	}
	queryUC := newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{})

	tests := []struct {
		name           string
		commandUseCase *command.UseCase
		queryUseCase   *matchingQuery.UseCase
		ctxProvider    contextProvider
		expectedErr    error
	}{
		{
			name:           "nil command use case",
			commandUseCase: nil,
			queryUseCase:   queryUC,
			ctxProvider:    ctxProv,
			expectedErr:    ErrNilCommandUseCase,
		},
		{
			name:           "nil query use case",
			commandUseCase: &command.UseCase{},
			queryUseCase:   nil,
			ctxProvider:    ctxProv,
			expectedErr:    ErrNilQueryUseCase,
		},
		{
			name:           "nil context provider",
			commandUseCase: &command.UseCase{},
			queryUseCase:   queryUC,
			ctxProvider:    nil,
			expectedErr:    ErrNilContextProvider,
		},
		{
			name:           "success",
			commandUseCase: &command.UseCase{},
			queryUseCase:   queryUC,
			ctxProvider:    ctxProv,
			expectedErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, err := NewHandler(
				tt.commandUseCase,
				tt.queryUseCase,
				tt.ctxProvider,
			)

			if tt.expectedErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.expectedErr)
				assert.Nil(t, handler)
			} else {
				require.NoError(t, err)
				require.NotNil(t, handler)
			}
		})
	}
}

func TestNewHandler_Success_InitializesVerifier(t *testing.T) {
	t.Parallel()

	ctxProv := &stubContextProvider{
		info: &ports.ReconciliationContextInfo{ID: uuid.New(), Active: true},
	}
	queryUC := newQueryUseCase(t, &stubMatchRunRepo{}, &stubMatchGroupRepo{})

	handler, err := NewHandler(&command.UseCase{}, queryUC, ctxProv)

	require.NoError(t, err)
	require.NotNil(t, handler)
	assert.NotNil(t, handler.contextVerifier)
}

func TestParseUUIDParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		paramValue string
		expectErr  bool
		expectNil  bool
	}{
		{
			name:       "valid uuid",
			paramValue: "11111111-1111-1111-1111-111111111111",
			expectErr:  false,
			expectNil:  false,
		},
		{
			name:       "nil uuid",
			paramValue: "00000000-0000-0000-0000-000000000000",
			expectErr:  false,
			expectNil:  false,
		},
		{
			name:       "invalid uuid format",
			paramValue: "not-a-uuid",
			expectErr:  true,
			expectNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app := newFiberTestApp(context.Background())
			app.Get("/test/:id", func(c *fiber.Ctx) error {
				id, err := parseUUIDParam(c, "id")
				if err != nil {
					return c.Status(400).SendString(err.Error())
				}
				return c.SendString(id.String())
			})

			url := "/test/" + tt.paramValue

			req := httptest.NewRequest("GET", url, nil)
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			if tt.expectErr {
				assert.Equal(t, 400, resp.StatusCode)
			} else {
				assert.Equal(t, 200, resp.StatusCode)
			}
		})
	}
}

func TestParseOptionalUUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		expectNil bool
		expectErr bool
	}{
		{
			name:      "empty string returns nil",
			input:     "",
			expectNil: true,
			expectErr: false,
		},
		{
			name:      "valid uuid",
			input:     "11111111-1111-1111-1111-111111111111",
			expectNil: false,
			expectErr: false,
		},
		{
			name:      "invalid uuid format",
			input:     "not-a-uuid",
			expectNil: true,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseOptionalUUID(tt.input)

			if tt.expectErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				if tt.expectNil {
					assert.Nil(t, result)
				} else {
					assert.NotNil(t, result)
				}
			}
		})
	}
}

func TestHandlerSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrNilCommandUseCase", ErrNilCommandUseCase, "command use case is required"},
		{"ErrNilQueryUseCase", ErrNilQueryUseCase, "query use case is required"},
		{"ErrNilContextProvider", ErrNilContextProvider, "context provider is required"},
		{"ErrMatchRunResponseNil", ErrMatchRunResponseNil, "match run response is nil"},
		{"ErrInvalidSortOrder", ErrInvalidSortOrder, "invalid sort_order"},
		{"ErrInvalidSortBy", ErrInvalidSortBy, "invalid sort_by"},
		{"ErrReasonRequired", ErrReasonRequired, "reason is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestHandlerConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "desc", sortOrderDesc)
	assert.Equal(t, 2, minTransactionIDsForManualMatch)
}
