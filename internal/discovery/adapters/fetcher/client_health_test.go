// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsHealthy_Healthy_StatusOK(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		resp := fetcherHealthResponse{Status: "ok"}
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	assert.True(t, client.IsHealthy(context.Background()))
}

func TestIsHealthy_Healthy_StatusHealthy(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		resp := fetcherHealthResponse{Status: "healthy"}
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	assert.True(t, client.IsHealthy(context.Background()))
}

func TestIsHealthy_Unhealthy_BadStatus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		resp := fetcherHealthResponse{Status: "degraded"}
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	assert.False(t, client.IsHealthy(context.Background()))
}

func TestIsHealthy_Unhealthy_NonOKStatusCode(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	assert.False(t, client.IsHealthy(context.Background()))
}

func TestIsHealthy_Unreachable(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http://127.0.0.1:1"
	cfg.AllowPrivateIPs = true

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)
	assert.False(t, client.IsHealthy(context.Background()))
}

func TestIsHealthy_InvalidJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json")) //nolint:errcheck // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	assert.False(t, client.IsHealthy(context.Background()))
}

func TestIsHealthy_OversizedBody(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("a"), maxResponseBodySize+1))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	assert.False(t, client.IsHealthy(context.Background()))
}
