//go:build e2e

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	client := NewClient("http://localhost:4018", "tenant-123", 30*time.Second)

	assert.NotNil(t, client)
	assert.Equal(t, "http://localhost:4018", client.baseURL)
	assert.Equal(t, "tenant-123", client.tenantID)
	assert.NotNil(t, client.httpClient)
}

func TestClient_SetTenantID(t *testing.T) {
	t.Parallel()

	client := NewClient("http://localhost:4018", "tenant-1", 30*time.Second)
	assert.Equal(t, "tenant-1", client.tenantID)

	client.SetTenantID("tenant-2")
	assert.Equal(t, "tenant-2", client.tenantID)
}

func TestClient_Do_Success(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/test-path", r.URL.Path)
		assert.Equal(t, "tenant-123", r.Header.Get("X-Tenant-ID"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	resp, err := client.Do(context.Background(), http.MethodGet, "/test-path", nil, "")

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestClient_Do_WithContentType(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	resp, err := client.Do(
		context.Background(),
		http.MethodPost,
		"/test",
		bytes.NewReader([]byte("{}")),
		"application/json",
	)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestClient_DoJSON_Success(t *testing.T) {
	t.Parallel()

	type response struct {
		Message string `json:"message"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response{Message: "hello"})
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	var resp response
	err := client.DoJSON(context.Background(), http.MethodGet, "/test", nil, &resp)

	require.NoError(t, err)
	assert.Equal(t, "hello", resp.Message)
}

func TestClient_DoJSON_WithRequestBody(t *testing.T) {
	t.Parallel()

	type request struct {
		Name string `json:"name"`
	}
	type response struct {
		ID string `json:"id"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req request
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "test-name", req.Name)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response{ID: "123"})
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	var resp response
	err := client.DoJSON(
		context.Background(),
		http.MethodPost,
		"/test",
		request{Name: "test-name"},
		&resp,
	)

	require.NoError(t, err)
	assert.Equal(t, "123", resp.ID)
}

func TestClient_DoJSON_APIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	err := client.DoJSON(context.Background(), http.MethodGet, "/test", nil, nil)

	require.Error(t, err)
	var apiErr *APIError
	assert.True(t, errors.As(err, &apiErr))
	assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
	assert.Contains(t, string(apiErr.Body), "bad request")
}

func TestClient_DoJSON_NilResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	err := client.DoJSON(context.Background(), http.MethodPost, "/test", nil, nil)

	require.NoError(t, err)
}

func TestClient_DoJSONWithOptions_UsesExplicitIdempotencyKey(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "fixed-key", r.Header.Get("X-Idempotency-Key"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	err := client.DoJSONWithOptions(
		context.Background(),
		http.MethodPost,
		"/test",
		nil,
		nil,
		RequestOptions{IdempotencyKey: "fixed-key"},
	)

	require.NoError(t, err)
}

func TestClient_DoMultipart_Success(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Content-Type"), "multipart/form-data")

		err := r.ParseMultipartForm(10 << 20)
		require.NoError(t, err)

		file, header, err := r.FormFile("file")
		require.NoError(t, err)
		defer file.Close()

		assert.Equal(t, "test.csv", header.Filename)

		content, err := io.ReadAll(file)
		require.NoError(t, err)
		assert.Equal(t, "test content", string(content))

		assert.Equal(t, "csv", r.FormValue("format"))

		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"id":"job-123"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	resp, body, err := client.DoMultipart(
		context.Background(),
		"/upload",
		"file",
		"test.csv",
		[]byte("test content"),
		map[string]string{"format": "csv"},
	)

	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.Contains(t, string(body), "job-123")
}

func TestClient_DoRaw_Success(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("raw data"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	resp, data, err := client.DoRaw(context.Background(), http.MethodGet, "/test", nil, "")

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "raw data", string(data))
}

func TestClient_DoRaw_APIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)
	resp, data, err := client.DoRaw(context.Background(), http.MethodGet, "/test", nil, "")

	require.Error(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "not found", string(data))

	var apiErr *APIError
	assert.True(t, errors.As(err, &apiErr))
	assert.True(t, apiErr.IsNotFound())
}

func TestAPIError_Error(t *testing.T) {
	t.Parallel()

	err := &APIError{
		StatusCode: 400,
		Body:       []byte("invalid request"),
	}

	assert.Contains(t, err.Error(), "400")
	assert.Contains(t, err.Error(), "invalid request")
}

func TestAPIError_IsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code     int
		expected bool
	}{
		{http.StatusNotFound, true},
		{http.StatusOK, false},
		{http.StatusBadRequest, false},
		{http.StatusInternalServerError, false},
	}

	for _, tt := range tests {
		err := &APIError{StatusCode: tt.code}
		assert.Equal(t, tt.expected, err.IsNotFound())
	}
}

func TestAPIError_IsBadRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code     int
		expected bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusNotFound, false},
		{http.StatusOK, false},
	}

	for _, tt := range tests {
		err := &APIError{StatusCode: tt.code}
		assert.Equal(t, tt.expected, err.IsBadRequest())
	}
}

func TestAPIError_IsConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code     int
		expected bool
	}{
		{http.StatusConflict, true},
		{http.StatusNotFound, false},
		{http.StatusOK, false},
	}

	for _, tt := range tests {
		err := &APIError{StatusCode: tt.code}
		assert.Equal(t, tt.expected, err.IsConflict())
	}
}

func TestClient_ContextCancellation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "tenant-123", 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Do(ctx, http.MethodGet, "/test", nil, "")
	assert.Error(t, err)
}
