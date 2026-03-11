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

// --- IsHealthy tests ---

func TestIsHealthy_Healthy_StatusOK(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)

		resp := fetcherHealthResponse{Status: "ok"}
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	healthy := client.IsHealthy(context.Background())

	assert.True(t, healthy)
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
	healthy := client.IsHealthy(context.Background())

	assert.True(t, healthy)
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
	healthy := client.IsHealthy(context.Background())

	assert.False(t, healthy)
}

func TestIsHealthy_Unhealthy_NonOKStatusCode(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	healthy := client.IsHealthy(context.Background())

	assert.False(t, healthy)
}

func TestIsHealthy_Unreachable(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http://127.0.0.1:1" // port 1 is extremely unlikely to be open

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	healthy := client.IsHealthy(context.Background())

	assert.False(t, healthy)
}

func TestIsHealthy_InvalidJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json")) //nolint:errcheck // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	healthy := client.IsHealthy(context.Background())

	assert.False(t, healthy)
}

// --- ListConnections tests ---

func TestListConnections_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/connections", r.URL.Path)
		assert.Equal(t, "org-123", r.URL.Query().Get("orgId"))

		resp := fetcherConnectionListResponse{
			Connections: []fetcherConnectionResponse{
				{
					ID:           "conn-1",
					ConfigName:   "prod-db",
					DatabaseType: "POSTGRESQL",
					Host:         "db.example.com",
					Port:         5432,
					DatabaseName: "production",
					ProductName:  "PostgreSQL 16",
					Status:       "AVAILABLE",
				},
				{
					ID:           "conn-2",
					ConfigName:   "staging-db",
					DatabaseType: "MYSQL",
					Host:         "staging.example.com",
					Port:         3306,
					DatabaseName: "staging",
					ProductName:  "MySQL 8",
					Status:       "UNREACHABLE",
				},
			},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "org-123")

	require.NoError(t, err)
	require.Len(t, conns, 2)
	assert.Equal(t, "conn-1", conns[0].ID)
	assert.Equal(t, "POSTGRESQL", conns[0].DatabaseType)
	assert.Equal(t, 5432, conns[0].Port)
	assert.Equal(t, "conn-2", conns[1].ID)
	assert.Equal(t, "MYSQL", conns[1].DatabaseType)
}

func TestListConnections_EmptyList(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherConnectionListResponse{Connections: []fetcherConnectionResponse{}}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	assert.Empty(t, conns)
}

func TestListConnections_NoOrgID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/connections", r.URL.Path)
		assert.Empty(t, r.URL.Query().Get("orgId"))

		resp := fetcherConnectionListResponse{Connections: []fetcherConnectionResponse{}}

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

// --- GetSchema tests ---

func TestGetSchema_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/connections/conn-abc/schema", r.URL.Path)

		resp := fetcherSchemaResponse{
			ConnectionID: "conn-abc",
			Tables: []fetcherTableResponse{
				{
					TableName: "transactions",
					Columns: []fetcherColumnResponse{
						{Name: "id", Type: "uuid", Nullable: false},
						{Name: "amount", Type: "decimal", Nullable: false},
						{Name: "note", Type: "text", Nullable: true},
					},
				},
			},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "conn-abc")

	require.NoError(t, err)
	assert.Equal(t, "conn-abc", schema.ConnectionID)
	assert.False(t, schema.DiscoveredAt.IsZero())
	require.Len(t, schema.Tables, 1)
	assert.Equal(t, "transactions", schema.Tables[0].TableName)
	require.Len(t, schema.Tables[0].Columns, 3)
	assert.Equal(t, "id", schema.Tables[0].Columns[0].Name)
	assert.False(t, schema.Tables[0].Columns[0].Nullable)
	assert.True(t, schema.Tables[0].Columns[2].Nullable)
}

func TestGetSchema_NotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "nonexistent")

	require.Error(t, err)
	assert.Nil(t, schema)
	assert.ErrorIs(t, err, ErrFetcherNotFound)
}

func TestGetSchema_EmptyTables(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherSchemaResponse{
			ConnectionID: "conn-empty",
			Tables:       []fetcherTableResponse{},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "conn-empty")

	require.NoError(t, err)
	assert.Empty(t, schema.Tables)
}

// --- TestConnection tests ---

func TestTestConnection_Success_Healthy(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/connections/conn-1/test", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		resp := fetcherTestResponse{
			ConnectionID: "conn-1",
			Healthy:      true,
			LatencyMs:    42,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	result, err := client.TestConnection(context.Background(), "conn-1")

	require.NoError(t, err)
	assert.Equal(t, "conn-1", result.ConnectionID)
	assert.True(t, result.Healthy)
	assert.Equal(t, int64(42), result.LatencyMs)
	assert.Empty(t, result.ErrorMessage)
}

func TestTestConnection_Success_Unhealthy(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherTestResponse{
			ConnectionID: "conn-2",
			Healthy:      false,
			LatencyMs:    0,
			ErrorMessage: "connection refused",
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	result, err := client.TestConnection(context.Background(), "conn-2")

	require.NoError(t, err)
	assert.False(t, result.Healthy)
	assert.Equal(t, "connection refused", result.ErrorMessage)
}

// --- SubmitExtractionJob tests ---

func TestSubmitExtractionJob_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/extractions", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var reqBody fetcherExtractionSubmitRequest
		err := json.NewDecoder(r.Body).Decode(&reqBody)
		require.NoError(t, err)

		assert.Equal(t, "conn-abc", reqBody.ConnectionID)
		require.Contains(t, reqBody.Tables, "transactions")
		assert.Equal(t, []string{"id", "amount"}, reqBody.Tables["transactions"].Columns)
		assert.Equal(t, "2026-01-01", reqBody.Tables["transactions"].StartDate)

		resp := fetcherExtractionSubmitResponse{JobID: "job-xyz"}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)

	input := sharedPorts.ExtractionJobInput{
		ConnectionID: "conn-abc",
		Tables: map[string]sharedPorts.ExtractionTableConfig{
			"transactions": {
				Columns:   []string{"id", "amount"},
				StartDate: "2026-01-01",
				EndDate:   "2026-01-31",
			},
		},
		Filters: map[string]interface{}{
			"currency": "USD",
		},
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
		ConnectionID: "conn-1",
		Tables:       map[string]sharedPorts.ExtractionTableConfig{},
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
		ConnectionID: "conn-1",
		Tables:       map[string]sharedPorts.ExtractionTableConfig{"transactions": {}},
	}

	jobID, err := client.SubmitExtractionJob(context.Background(), input)

	require.Error(t, err)
	assert.Empty(t, jobID)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.ErrorIs(t, err, ErrFetcherJobIDEmpty)
}

// --- GetExtractionJobStatus tests ---

func TestGetExtractionJobStatus_Success_Running(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/extractions/job-1", r.URL.Path)

		resp := fetcherExtractionStatusResponse{
			JobID:    "job-1",
			Status:   "RUNNING",
			Progress: 60,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-1")

	require.NoError(t, err)
	assert.Equal(t, "job-1", status.JobID)
	assert.Equal(t, "RUNNING", status.Status)
	assert.Equal(t, 60, status.Progress)
	assert.Empty(t, status.ResultPath)
}

func TestGetExtractionJobStatus_Success_Complete(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{
			JobID:      "job-2",
			Status:     "COMPLETE",
			Progress:   100,
			ResultPath: "/data/results/job-2.json",
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-2")

	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", status.Status)
	assert.Equal(t, 100, status.Progress)
	assert.Equal(t, "/data/results/job-2.json", status.ResultPath)
}

func TestGetExtractionJobStatus_Success_Failed(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{
			JobID:        "job-3",
			Status:       "FAILED",
			Progress:     25,
			ErrorMessage: "connection lost during extraction",
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-3")

	require.NoError(t, err)
	assert.Equal(t, "FAILED", status.Status)
	assert.Equal(t, "connection lost during extraction", status.ErrorMessage)
}

func TestGetExtractionJobStatus_NotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "nonexistent")

	require.Error(t, err)
	assert.Nil(t, status)
	assert.ErrorIs(t, err, ErrFetcherNotFound)
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
			Connections: []fetcherConnectionResponse{
				{ID: "conn-1", Status: "AVAILABLE"},
			},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.BaseURL = srv.URL
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
		ConnectionID: "conn-1",
		Tables:       map[string]sharedPorts.ExtractionTableConfig{"transactions": {}},
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
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 0

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	_, err = client.SubmitExtractionJob(context.Background(), sharedPorts.ExtractionJobInput{
		ConnectionID: "conn-1",
		Tables:       map[string]sharedPorts.ExtractionTableConfig{"tx": {}},
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
		ConnectionID: "conn-1",
		Tables: map[string]sharedPorts.ExtractionTableConfig{
			"t": {Columns: []string{"id"}},
		},
	}

	_, err := client.SubmitExtractionJob(context.Background(), input)

	require.NoError(t, err)
}

func TestDoGet_DoesNotSetContentType(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Content-Type"))

		resp := fetcherConnectionListResponse{Connections: []fetcherConnectionResponse{}}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	_, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
}
