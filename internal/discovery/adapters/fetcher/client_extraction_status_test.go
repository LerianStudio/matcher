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
		assert.Equal(t, "/api/v1/extractions/job-1", r.URL.Path)
		resp := fetcherExtractionStatusResponse{JobID: "job-1", Status: "RUNNING", Progress: 60}
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
		resp := fetcherExtractionStatusResponse{JobID: "job-2", Status: "COMPLETE", Progress: 100, ResultPath: "/data/results/job-2.json"}
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
		resp := fetcherExtractionStatusResponse{JobID: "job-3", Status: "FAILED", Progress: 25, ErrorMessage: "connection lost during extraction"}
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

func TestGetExtractionJobStatus_CanceledNormalizedToCancelled(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{JobID: "job-4", Status: "canceled", Progress: 100}
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
		resp := fetcherExtractionStatusResponse{JobID: "job-other", Status: "RUNNING", Progress: 25}
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
		resp := fetcherExtractionStatusResponse{JobID: "job-6", Status: "COMPLETE", Progress: 100}
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
		resp := fetcherExtractionStatusResponse{JobID: "job-6b", Status: "COMPLETE", Progress: 100, ResultPath: "s3://bucket/output.csv"}
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

func TestGetExtractionJobStatus_FailedMissingErrorMessage(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherExtractionStatusResponse{JobID: "job-7", Status: "FAILED", Progress: 100}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	status, err := client.GetExtractionJobStatus(context.Background(), "job-7")
	require.Error(t, err)
	assert.Nil(t, status)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "missing error message")
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
