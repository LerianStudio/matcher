//go:build unit

package fetcher

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyResponse_Success_ReturnsBody(t *testing.T) {
	t.Parallel()

	body := []byte(`{"status":"ok"}`)

	result, statusCode, err := classifyResponse(http.StatusOK, body)

	require.NoError(t, err)
	assert.Equal(t, body, result)
	assert.Equal(t, http.StatusOK, statusCode)
}

func TestClassifyResponse_201Created_ReturnsBody(t *testing.T) {
	t.Parallel()

	body := []byte(`{"id":"new-resource"}`)

	result, statusCode, err := classifyResponse(http.StatusCreated, body)

	require.NoError(t, err)
	assert.Equal(t, body, result)
	assert.Equal(t, http.StatusCreated, statusCode)
}

func TestClassifyResponse_204NoContent_ReturnsEmptyBody(t *testing.T) {
	t.Parallel()

	result, statusCode, err := classifyResponse(http.StatusNoContent, nil)

	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, http.StatusNoContent, statusCode)
}

func TestClassifyResponse_404NotFound_ReturnsSpecificError(t *testing.T) {
	t.Parallel()

	result, statusCode, err := classifyResponse(http.StatusNotFound, nil)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, http.StatusNotFound, statusCode)
	assert.ErrorIs(t, err, ErrFetcherNotFound)
}

func TestClassifyResponse_RedirectStatuses_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"301 Moved Permanently", http.StatusMovedPermanently},
		{"302 Found", http.StatusFound},
		{"307 Temporary Redirect", http.StatusTemporaryRedirect},
		{"308 Permanent Redirect", http.StatusPermanentRedirect},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, sc, err := classifyResponse(tt.statusCode, nil)

			require.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, tt.statusCode, sc)
			assert.ErrorIs(t, err, ErrFetcherBadResponse)
			assert.Contains(t, err.Error(), "redirects are not allowed")
		})
	}
}

func TestClassifyResponse_ClientErrors_ReturnsBadResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"400 Bad Request", http.StatusBadRequest},
		{"401 Unauthorized", http.StatusUnauthorized},
		{"403 Forbidden", http.StatusForbidden},
		{"409 Conflict", http.StatusConflict},
		{"422 Unprocessable", http.StatusUnprocessableEntity},
		{"429 Too Many Requests", http.StatusTooManyRequests},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, sc, err := classifyResponse(tt.statusCode, nil)

			require.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, tt.statusCode, sc)
			assert.ErrorIs(t, err, ErrFetcherBadResponse)
		})
	}
}

func TestClassifyResponse_ServerErrors_ReturnsBadResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{"500 Internal Server Error", http.StatusInternalServerError},
		{"502 Bad Gateway", http.StatusBadGateway},
		{"503 Service Unavailable", http.StatusServiceUnavailable},
		{"504 Gateway Timeout", http.StatusGatewayTimeout},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, sc, err := classifyResponse(tt.statusCode, nil)

			require.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, tt.statusCode, sc)
			assert.ErrorIs(t, err, ErrFetcherBadResponse)
		})
	}
}

func TestClassifyResponse_300MultipleChoices_IsRedirectRange(t *testing.T) {
	t.Parallel()

	result, statusCode, err := classifyResponse(http.StatusMultipleChoices, nil)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, http.StatusMultipleChoices, statusCode)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "redirects are not allowed")
}

func TestClassifyResponse_399_IsRedirectRange(t *testing.T) {
	t.Parallel()

	result, statusCode, err := classifyResponse(399, nil)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 399, statusCode)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "redirects are not allowed")
}

func TestClassifyResponse_404_TakesPriorityOverGenericClientError(t *testing.T) {
	t.Parallel()

	result, statusCode, err := classifyResponse(http.StatusNotFound, []byte(`{"error":"not found"}`))

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, http.StatusNotFound, statusCode)
	assert.ErrorIs(t, err, ErrFetcherNotFound)
}
