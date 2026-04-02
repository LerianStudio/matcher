//go:build unit

package matchererrors

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewError_SelectsTypedErrorByStatus(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		status     int
		assertType func(*testing.T, APIError)
	}{
		{name: "bad request", status: http.StatusBadRequest, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*ValidationError); require.True(t, ok) }},
		{name: "unauthorized", status: http.StatusUnauthorized, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*UnauthorizedError); require.True(t, ok) }},
		{name: "forbidden", status: http.StatusForbidden, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*ForbiddenError); require.True(t, ok) }},
		{name: "not found", status: http.StatusNotFound, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*NotFoundError); require.True(t, ok) }},
		{name: "conflict", status: http.StatusConflict, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*ConflictError); require.True(t, ok) }},
		{name: "unprocessable", status: http.StatusUnprocessableEntity, assertType: func(t *testing.T, apiError APIError) {
			_, ok := apiError.(*UnprocessableEntityError)
			require.True(t, ok)
		}},
		{name: "rate limit", status: http.StatusTooManyRequests, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*RateLimitError); require.True(t, ok) }},
		{name: "generic client error", status: http.StatusMethodNotAllowed, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*ClientError); require.True(t, ok) }},
		{name: "request entity too large", status: http.StatusRequestEntityTooLarge, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*ClientError); require.True(t, ok) }},
		{name: "gone", status: http.StatusGone, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*ClientError); require.True(t, ok) }},
		{name: "service unavailable", status: http.StatusServiceUnavailable, assertType: func(t *testing.T, apiError APIError) {
			_, ok := apiError.(*ServiceUnavailableError)
			require.True(t, ok)
		}},
		{name: "default internal", status: 599, assertType: func(t *testing.T, apiError APIError) { _, ok := apiError.(*InternalError); require.True(t, ok) }},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			apiError := NewError(Definition{
				Code:       "MTCH-9999",
				Title:      http.StatusText(testCase.status),
				HTTPStatus: testCase.status,
			}, "status specific message", map[string]any{"status": testCase.status}, nil)

			testCase.assertType(t, apiError)
			require.Equal(t, "MTCH-9999", apiError.ProductCode())
			require.Equal(t, testCase.status, apiError.HTTPStatusCode())
			require.Equal(t, "status specific message", apiError.ProductMessage())
			require.Equal(t, map[string]any{"status": testCase.status}, apiError.ProductDetails())
		})
	}
}

func TestBaseError_Unwrap(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	apiError := NewInternalError(Definition{
		Code:       "MTCH-9998",
		Title:      "Internal Server Error",
		HTTPStatus: http.StatusInternalServerError,
	}, "failed", nil, cause)

	require.ErrorIs(t, apiError, cause)
}

func TestBaseError_ErrorFallsBackToTitle(t *testing.T) {
	t.Parallel()

	apiError := NewInternalError(Definition{
		Code:       "MTCH-9997",
		Title:      http.StatusText(http.StatusInternalServerError),
		HTTPStatus: http.StatusInternalServerError,
	}, "", nil, nil)

	require.Equal(t, http.StatusText(http.StatusInternalServerError), apiError.Error())
}
