// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestDoRequest_RetriesOn500(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempt := attempts.Add(1)
		if attempt <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		resp := fetcherConnectionListResponse{
			Items: []fetcherConnectionResponse{{ID: "conn-1", Type: "POSTGRESQL"}},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 0

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	require.Len(t, conns, 1)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestDoRequest_ExhaustsRetries(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 2
	cfg.RetryBaseDelay = 0

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.Contains(t, err.Error(), "exhausted retries")
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestDoRequest_NoRetryOn4xx(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`)) //nolint:errcheck // test helper
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 0

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Equal(t, int32(1), attempts.Load())
}

func TestDoRequest_NoRetryOn404(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 0

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	_, err = client.GetSchema(context.Background(), "nonexistent")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherNotFound)
	assert.Equal(t, int32(1), attempts.Load())
}

func TestDoRequest_CanceledContext(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 5
	cfg.RetryBaseDelay = 0

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.ListConnections(ctx, "")

	require.Error(t, err)
}

func TestDoRequest_TransportFailure_RetryableGet(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	transportErr := errors.New("dial tcp: connection refused")
	client := &HTTPFetcherClient{
		httpClient: &http.Client{Transport: roundTripperFunc(func(_ *http.Request) (*http.Response, error) {
			attempts.Add(1)
			return nil, transportErr
		})},
		baseURL: "http://fetcher.internal",
		cfg: HTTPClientConfig{
			RequestTimeout:  time.Second,
			MaxRetries:      2,
			RetryBaseDelay:  0,
			AllowPrivateIPs: true,
		},
	}

	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.Contains(t, err.Error(), "exhausted retries")
	assert.ErrorIs(t, err, ErrFetcherUnreachable)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestDoRequest_TransportFailure_NonRetryablePost(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	transportErr := errors.New("dial tcp: connection refused")
	client := &HTTPFetcherClient{
		httpClient: &http.Client{Transport: roundTripperFunc(func(_ *http.Request) (*http.Response, error) {
			attempts.Add(1)
			return nil, transportErr
		})},
		baseURL: "http://fetcher.internal",
		cfg: HTTPClientConfig{
			RequestTimeout:  time.Second,
			MaxRetries:      3,
			RetryBaseDelay:  0,
			AllowPrivateIPs: true,
		},
	}

	_, err := client.SubmitExtractionJob(context.Background(), sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{"db": {"transactions": {"id"}}},
		Metadata:     map[string]any{"source": "src-1"},
	})

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "exhausted retries")
	assert.ErrorIs(t, err, ErrFetcherUnreachable)
	assert.Equal(t, int32(1), attempts.Load())
}

func TestSubmitExtractionJob_DoesNotRetryOnServerError(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 0

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	_, err = client.SubmitExtractionJob(context.Background(), sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{"db": {"tx": {"id"}}},
		Metadata:     map[string]any{"source": "src-1"},
	})

	require.Error(t, err)
	assert.Equal(t, int32(1), attempts.Load())
}

func TestDoRequest_RejectsRedirectResponses(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://example.com/elsewhere")
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)

	_, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestDoPost_SetsContentTypeJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"jobId":"job-ct"}`)) //nolint:errcheck // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)

	input := sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{
			"db": {"t": {"id"}},
		},
		Metadata: map[string]any{"source": "src-1"},
	}

	_, err := client.SubmitExtractionJob(context.Background(), input)

	require.NoError(t, err)
}

func TestDoGet_DoesNotSetContentType(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Content-Type"))

		resp := fetcherConnectionListResponse{Items: []fetcherConnectionResponse{}}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	_, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
}
