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

// Compile-time interface check.
var _ sharedPorts.FetcherClient = (*HTTPFetcherClient)(nil)

// newTestClient creates an HTTPFetcherClient pointing at the given test server.
func newTestClient(t *testing.T, serverURL string) *HTTPFetcherClient {
	t.Helper()

	cfg := DefaultConfig()
	cfg.BaseURL = serverURL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 0 // no retries by default to keep tests fast

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	return client
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

// --- NewHTTPFetcherClient tests ---

func TestNewHTTPFetcherClient_ValidConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	client, err := NewHTTPFetcherClient(cfg)

	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewHTTPFetcherClient_InvalidConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = ""

	client, err := NewHTTPFetcherClient(cfg)

	require.Error(t, err)
	assert.Nil(t, client)
	assert.ErrorIs(t, err, ErrEmptyURL)
}

func TestNewHTTPFetcherClient_TrimsTrailingSlash(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http://localhost:4006/"

	client, err := NewHTTPFetcherClient(cfg)

	require.NoError(t, err)
	assert.Equal(t, "http://localhost:4006", client.baseURL)
}

func TestHTTPFetcherClient_NilReceiverSafety(t *testing.T) {
	t.Parallel()

	var client *HTTPFetcherClient

	assert.False(t, client.IsHealthy(context.Background()))

	_, err := client.ListConnections(context.Background(), "org-1")
	require.ErrorIs(t, err, ErrFetcherClientNil)

	_, err = client.GetSchema(context.Background(), "conn-1")
	require.ErrorIs(t, err, ErrFetcherClientNil)

	_, err = client.TestConnection(context.Background(), "conn-1")
	require.ErrorIs(t, err, ErrFetcherClientNil)

	_, err = client.SubmitExtractionJob(context.Background(), sharedPorts.ExtractionJobInput{})
	require.ErrorIs(t, err, ErrFetcherClientNil)

	_, err = client.GetExtractionJobStatus(context.Background(), "job-1")
	require.ErrorIs(t, err, ErrFetcherClientNil)
}

// --- ListConnections tests ---

func TestListConnections_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/management/connections", r.URL.Path)
		assert.Equal(t, "midaz", r.Header.Get("X-Product-Name"))

		resp := fetcherConnectionListResponse{
			Items: []fetcherConnectionResponse{
				{
					ID:           "conn-1",
					ConfigName:   "prod-db",
					Type:         "POSTGRESQL",
					Host:         "db.example.com",
					Port:         5432,
					DatabaseName: "production",
					ProductName:  "PostgreSQL 16",
					CreatedAt:    "2026-01-15T10:00:00Z",
					UpdatedAt:    "2026-01-16T12:00:00Z",
				},
				{
					ID:           "conn-2",
					ConfigName:   "staging-db",
					Type:         "MYSQL",
					Host:         "staging.example.com",
					Port:         3306,
					DatabaseName: "staging",
					ProductName:  "MySQL 8",
				},
			},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "midaz")

	require.NoError(t, err)
	require.Len(t, conns, 2)
	assert.Equal(t, "conn-1", conns[0].ID)
	assert.Equal(t, "POSTGRESQL", conns[0].DatabaseType)
	assert.Equal(t, 5432, conns[0].Port)
	assert.False(t, conns[0].CreatedAt.IsZero())
	assert.Equal(t, "conn-2", conns[1].ID)
	assert.Equal(t, "MYSQL", conns[1].DatabaseType)
}

func TestListConnections_EmptyList(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherConnectionListResponse{Items: []fetcherConnectionResponse{}}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	assert.Empty(t, conns)
}

func TestListConnections_NoProductName(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/management/connections", r.URL.Path)
		assert.Empty(t, r.Header.Get("X-Product-Name"))

		resp := fetcherConnectionListResponse{Items: []fetcherConnectionResponse{}}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	_, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
}

func TestListConnections_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
}

func TestListConnections_BadJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{invalid json}")) //nolint:errcheck // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.Contains(t, err.Error(), "decode connections response")
}

func TestListConnections_NullPayloadRejected(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("null"))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "null/empty payload")
}

// --- TestConnection tests ---

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

// --- SubmitExtractionJob tests ---

func TestSubmitExtractionJob_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/fetcher", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var reqBody fetcherExtractionSubmitRequest
		err := json.NewDecoder(r.Body).Decode(&reqBody)
		require.NoError(t, err)

		require.Contains(t, reqBody.DataRequest.MappedFields, "prod-db")
		require.Contains(t, reqBody.DataRequest.MappedFields["prod-db"], "transactions")
		assert.Equal(t, []string{"id", "amount"}, reqBody.DataRequest.MappedFields["prod-db"]["transactions"])
		assert.Equal(t, "src-1", reqBody.Metadata["source"])

		resp := fetcherExtractionSubmitResponse{JobID: "job-xyz"}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)

	input := sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{
			"prod-db": {
				"transactions": {"id", "amount"},
			},
		},
		Filters: map[string]map[string]map[string]any{
			"prod-db": {
				"transactions": {"currency": "USD"},
			},
		},
		Metadata: map[string]any{"source": "src-1"},
	}

	jobID, err := client.SubmitExtractionJob(context.Background(), input)

	require.NoError(t, err)
	assert.Equal(t, "job-xyz", jobID)
}

func TestSubmitExtractionJob_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)

	input := sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{},
		Metadata:     map[string]any{"source": "src-1"},
	}

	jobID, err := client.SubmitExtractionJob(context.Background(), input)

	require.Error(t, err)
	assert.Empty(t, jobID)
}

func TestSubmitExtractionJob_EmptyJobIDFailsClosed(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(fetcherExtractionSubmitResponse{}) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)

	input := sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{"db": {"transactions": {"id"}}},
		Metadata:     map[string]any{"source": "src-1"},
	}

	jobID, err := client.SubmitExtractionJob(context.Background(), input)

	require.Error(t, err)
	assert.Empty(t, jobID)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.ErrorIs(t, err, ErrFetcherJobIDEmpty)
}

func TestSubmitExtractionJob_NullPayloadRejected(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("null"))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	jobID, err := client.SubmitExtractionJob(context.Background(), sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{"db": {"transactions": {"id"}}},
		Metadata:     map[string]any{"source": "src-1"},
	})

	require.Error(t, err)
	assert.Empty(t, jobID)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

// --- Retry behavior tests ---

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
			Items: []fetcherConnectionResponse{
				{ID: "conn-1", Type: "POSTGRESQL"},
			},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 0 // no delay for tests

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	require.Len(t, conns, 1)
	assert.Equal(t, int32(3), attempts.Load()) // 2 failures + 1 success
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
	assert.Equal(t, int32(3), attempts.Load()) // initial + 2 retries = 3
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
	assert.Equal(t, int32(1), attempts.Load()) // no retries on 4xx
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
	assert.Equal(t, int32(1), attempts.Load()) // no retries on 404
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
	cancel() // cancel immediately

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

// --- Content-Type header tests ---

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
