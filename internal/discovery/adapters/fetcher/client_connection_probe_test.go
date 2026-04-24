// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestConnection_Success_Healthy(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/management/connections/conn-1/test", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		resp := fetcherTestResponse{
			Status:    "success",
			LatencyMs: 42,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	result, err := client.TestConnection(context.Background(), "conn-1")

	require.NoError(t, err)
	assert.Equal(t, "success", result.Status)
	assert.Equal(t, int64(42), result.LatencyMs)
	assert.Empty(t, result.Message)
}

func TestTestConnection_Success_Unhealthy(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherTestResponse{
			Status:    "error",
			Message:   "connection refused",
			LatencyMs: 0,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	result, err := client.TestConnection(context.Background(), "conn-2")

	require.NoError(t, err)
	assert.Equal(t, "error", result.Status)
	assert.Equal(t, "connection refused", result.Message)
}

func TestTestConnection_ReturnsStatusAndMessage(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherTestResponse{Status: "success", Message: "all good", LatencyMs: 12}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	result, err := client.TestConnection(context.Background(), "conn-1")

	require.NoError(t, err)
	assert.Equal(t, "success", result.Status)
	assert.Equal(t, "all good", result.Message)
	assert.Equal(t, int64(12), result.LatencyMs)
}

func TestTestConnection_NullPayloadRejected(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("null"))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	result, err := client.TestConnection(context.Background(), "conn-1")

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}
