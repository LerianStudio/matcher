//go:build unit

package fetcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- tryParseFetcherError tests ---

func TestTryParseFetcherError_EmptyBody_ReturnsNil(t *testing.T) {
	t.Parallel()

	got := tryParseFetcherError(nil)

	assert.Nil(t, got)
}

func TestTryParseFetcherError_ZeroLengthBody_ReturnsNil(t *testing.T) {
	t.Parallel()

	got := tryParseFetcherError([]byte{})

	assert.Nil(t, got)
}

func TestTryParseFetcherError_InvalidJSON_ReturnsNil(t *testing.T) {
	t.Parallel()

	got := tryParseFetcherError([]byte("not-json{"))

	assert.Nil(t, got)
}

func TestTryParseFetcherError_EmptyCodeAndMessage_ReturnsNil(t *testing.T) {
	t.Parallel()

	// Body is valid JSON but carries no useful detail.
	body := []byte(`{"title":"some title","entityType":"Connection"}`)

	got := tryParseFetcherError(body)

	assert.Nil(t, got)
}

func TestTryParseFetcherError_CodeOnly_ReturnsParsedBody(t *testing.T) {
	t.Parallel()

	body := []byte(`{"code":"INVALID_PATH_PARAMETER"}`)

	got := tryParseFetcherError(body)

	require.NotNil(t, got)
	assert.Equal(t, "INVALID_PATH_PARAMETER", got.Code)
	assert.Empty(t, got.Message)
	assert.Empty(t, got.Title)
	assert.Empty(t, got.EntityType)
}

func TestTryParseFetcherError_MessageOnly_ReturnsParsedBody(t *testing.T) {
	t.Parallel()

	body := []byte(`{"message":"connection not found"}`)

	got := tryParseFetcherError(body)

	require.NotNil(t, got)
	assert.Empty(t, got.Code)
	assert.Equal(t, "connection not found", got.Message)
}

func TestTryParseFetcherError_AllFields_ReturnsParsedBody(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"code":"CONNECTION_NOT_FOUND",
		"title":"Not Found",
		"message":"connection not found",
		"entityType":"Connection"
	}`)

	got := tryParseFetcherError(body)

	require.NotNil(t, got)
	assert.Equal(t, "CONNECTION_NOT_FOUND", got.Code)
	assert.Equal(t, "Not Found", got.Title)
	assert.Equal(t, "connection not found", got.Message)
	assert.Equal(t, "Connection", got.EntityType)
}

func TestTryParseFetcherError_EmptyJSONObject_ReturnsNil(t *testing.T) {
	t.Parallel()

	got := tryParseFetcherError([]byte(`{}`))

	assert.Nil(t, got)
}
