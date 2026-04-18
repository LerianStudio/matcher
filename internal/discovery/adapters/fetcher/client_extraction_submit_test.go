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

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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

		require.Contains(t, reqBody.DataRequest.Filters, "prod-db")
		require.Contains(t, reqBody.DataRequest.Filters["prod-db"], "transactions")
		currencyFilter := reqBody.DataRequest.Filters["prod-db"]["transactions"]["currency"]
		assert.Equal(t, []any{"USD"}, currencyFilter.Eq)

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

func TestSubmitExtractionJob_202Accepted(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(fetcherExtractionSubmitResponse{JobID: "job-new"}) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	jobID, err := client.SubmitExtractionJob(context.Background(), sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{"db": {"tx": {"id"}}},
		Metadata:     map[string]any{"source": "src-1"},
	})

	require.NoError(t, err)
	assert.Equal(t, "job-new", jobID)
}

func TestSubmitExtractionJob_200Deduplicated(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(fetcherExtractionSubmitResponse{JobID: "job-dedup"}) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	jobID, err := client.SubmitExtractionJob(context.Background(), sharedPorts.ExtractionJobInput{
		MappedFields: map[string]map[string][]string{"db": {"tx": {"id"}}},
		Metadata:     map[string]any{"source": "src-1"},
	})

	require.NoError(t, err)
	assert.Equal(t, "job-dedup", jobID)
}
