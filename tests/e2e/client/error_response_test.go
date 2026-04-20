//go:build unit

package client

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorResponse_DetailsRoundTrip(t *testing.T) {
	t.Parallel()

	resp := ErrorResponse{
		Code:    "MTCH-0001",
		Title:   "Bad Request",
		Message: "invalid request",
		Details: map[string]any{"field": "contextId"},
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ErrorResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, resp.Code, decoded.Code)
	assert.Equal(t, resp.Title, decoded.Title)
	assert.Equal(t, resp.Message, decoded.Message)
	assert.Equal(t, "contextId", decoded.Details["field"])
}
