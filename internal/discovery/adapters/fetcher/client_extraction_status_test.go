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

func TestGetExtractionJobStatus_Success_Running(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/fetcher/job-1", r.URL.Path)
		resp := fetcherExtractionStatusResponse{ID: "job-1", Status: "RUNNING"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-1")
	require.NoError(t, err)
	assert.Equal(t, "job-1", status.ID)
	assert.Equal(t, "RUNNING", status.Status)
	assert.Empty(t, status.ResultPath)
}

func TestGetExtractionJobStatus_Success_Complete(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{ID: "job-2", Status: "COMPLETE", ResultPath: "/data/results/job-2.json", ResultHmac: "abc123", RequestHash: "hash456"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-2")
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", status.Status)
	assert.Equal(t, "/data/results/job-2.json", status.ResultPath)
	assert.Equal(t, "abc123", status.ResultHmac)
	assert.Equal(t, "hash456", status.RequestHash)
}

func TestGetExtractionJobStatus_Success_Failed(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{ID: "job-3", Status: "FAILED", Metadata: map[string]any{"error": "connection lost during extraction"}}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-3")
	require.NoError(t, err)
	assert.Equal(t, "FAILED", status.Status)
	assert.Equal(t, "connection lost during extraction", status.Metadata["error"])
}

func TestGetExtractionJobStatus_CanceledNormalizedToCancelled(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{ID: "job-4", Status: "canceled"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-4")
	require.NoError(t, err)
	require.NotNil(t, status)
	assert.Equal(t, "CANCELLED", status.Status)
}

func TestGetExtractionJobStatus_MismatchedJobID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{ID: "job-other", Status: "RUNNING"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-5")
	require.Error(t, err)
	assert.Nil(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "job id mismatch")
}

func TestGetExtractionJobStatus_CompleteMissingResultPath(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{ID: "job-6", Status: "COMPLETE"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-6")
	require.Error(t, err)
	assert.Nil(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "missing result path")
}

func TestGetExtractionJobStatus_CompleteRejectsInvalidResultPath(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{ID: "job-6b", Status: "COMPLETE", ResultPath: "s3://bucket/output.csv"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-6b")
	require.Error(t, err)
	assert.Nil(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "result path")
}

func TestGetExtractionJobStatus_FailedWithoutMetadata_Succeeds(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{ID: "job-7", Status: "FAILED"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-7")
	require.NoError(t, err)
	assert.Equal(t, "FAILED", status.Status)
}

func TestGetExtractionJobStatus_NullPayloadRejected(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("null"))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-8")
	require.Error(t, err)
	assert.Nil(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
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
