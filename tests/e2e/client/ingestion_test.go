//go:build e2e

package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIngestionClient(t *testing.T) {
	t.Parallel()

	baseClient := NewClient("http://localhost:4018", "tenant-123", 30*time.Second)
	ingestionClient := NewIngestionClient(baseClient)

	assert.NotNil(t, ingestionClient)
	assert.Equal(t, baseClient, ingestionClient.client)
}

func TestIngestionClient_UploadFile(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/imports/contexts/ctx-123/sources/src-456/upload", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Contains(t, r.Header.Get("Content-Type"), "multipart/form-data")

		err := r.ParseMultipartForm(10 << 20)
		require.NoError(t, err)

		file, header, err := r.FormFile("file")
		require.NoError(t, err)
		defer file.Close()

		assert.Equal(t, "test.csv", header.Filename)

		content, err := io.ReadAll(file)
		require.NoError(t, err)
		assert.Equal(t, "id,amount\n1,100.00", string(content))

		assert.Equal(t, "csv", r.FormValue("format"))

		w.WriteHeader(http.StatusAccepted)
		err = json.NewEncoder(w).Encode(IngestionJob{
			ID:        "job-123",
			ContextID: "ctx-123",
			SourceID:  "src-456",
			Status:    "PENDING",
		})
		require.NoError(t, err)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.UploadFile(
		context.Background(),
		"ctx-123",
		"src-456",
		"test.csv",
		[]byte("id,amount\n1,100.00"),
		"csv",
	)

	require.NoError(t, err)
	assert.Equal(t, "job-123", result.ID)
	assert.Equal(t, "PENDING", result.Status)
}

func TestIngestionClient_UploadCSV(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseMultipartForm(10 << 20)
		require.NoError(t, err)
		assert.Equal(t, "csv", r.FormValue("format"))

		w.WriteHeader(http.StatusAccepted)
		err = json.NewEncoder(w).Encode(IngestionJob{ID: "job-123"})
		require.NoError(t, err)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.UploadCSV(
		context.Background(),
		"ctx-123",
		"src-456",
		"data.csv",
		[]byte("header\nvalue"),
	)

	require.NoError(t, err)
	assert.Equal(t, "job-123", result.ID)
}

func TestIngestionClient_UploadJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseMultipartForm(10 << 20)
		require.NoError(t, err)
		assert.Equal(t, "json", r.FormValue("format"))

		w.WriteHeader(http.StatusAccepted)
		err = json.NewEncoder(w).Encode(IngestionJob{ID: "job-123"})
		require.NoError(t, err)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.UploadJSON(
		context.Background(),
		"ctx-123",
		"src-456",
		"data.json",
		[]byte(`[{"id": 1}]`),
	)

	require.NoError(t, err)
	assert.Equal(t, "job-123", result.ID)
}

func TestIngestionClient_UploadFile_NonAcceptedStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid file"}`))
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.UploadFile(
		context.Background(),
		"ctx-123",
		"src-456",
		"bad.csv",
		[]byte("invalid"),
		"csv",
	)

	assert.Error(t, err)
	assert.Nil(t, result)

	var apiErr *APIError
	assert.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
}

func TestIngestionClient_GetJob(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/imports/contexts/ctx-123/jobs/job-456", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		err := json.NewEncoder(w).Encode(IngestionJob{
			ID:          "job-456",
			ContextID:   "ctx-123",
			SourceID:    "src-789",
			FileName:    "data.csv",
			Status:      "COMPLETED",
			TotalRows:   102,
			FailedRows:  2,
			StartedAt:   now,
			CompletedAt: now.Add(5 * time.Minute),
		})
		require.NoError(t, err)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetJob(context.Background(), "ctx-123", "job-456")

	require.NoError(t, err)
	assert.Equal(t, "job-456", result.ID)
	assert.Equal(t, "COMPLETED", result.Status)
	assert.Equal(t, 102, result.TotalRows)
	assert.Equal(t, 2, result.FailedRows)
}

func TestIngestionClient_ListJobsByContext(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/imports/contexts/ctx-123/jobs", r.URL.Path)

		resp := struct {
			Items []IngestionJob `json:"items"`
		}{
			Items: []IngestionJob{
				{ID: "job-1", Status: "COMPLETED"},
				{ID: "job-2", Status: "PROCESSING"},
				{ID: "job-3", Status: "FAILED"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListJobsByContext(context.Background(), "ctx-123")

	require.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Equal(t, "COMPLETED", result[0].Status)
	assert.Equal(t, "FAILED", result[2].Status)
}

func TestIngestionClient_ListTransactionsByJob(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/imports/contexts/ctx-123/jobs/job-456/transactions", r.URL.Path)

		resp := struct {
			Items      []Transaction `json:"items"`
			NextCursor string        `json:"nextCursor"`
			HasMore    bool          `json:"hasMore"`
		}{
			Items: []Transaction{
				{
					ID:         "tx-1",
					JobID:      "job-456",
					ExternalID: "ext-001",
					Amount:     "100.00",
					Currency:   "USD",
					Date:       now,
					Status:     "PENDING",
				},
				{
					ID:         "tx-2",
					JobID:      "job-456",
					ExternalID: "ext-002",
					Amount:     "200.00",
					Currency:   "USD",
					Date:       now,
					Status:     "MATCHED",
				},
			},
			NextCursor: "",
			HasMore:    false,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListTransactionsByJob(context.Background(), "ctx-123", "job-456")

	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "100.00", result[0].Amount)
	assert.Equal(t, "MATCHED", result[1].Status)
}

func TestIngestionClient_ListTransactionsByJob_Pagination(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	callCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		cursor := r.URL.Query().Get("cursor")

		var resp struct {
			Items      []Transaction `json:"items"`
			NextCursor string        `json:"nextCursor"`
			HasMore    bool          `json:"hasMore"`
		}

		if cursor == "" {
			resp.Items = []Transaction{
				{
					ID:         "tx-1",
					JobID:      "job-456",
					ExternalID: "ext-001",
					Amount:     "100.00",
					Currency:   "USD",
					Date:       now,
					Status:     "PENDING",
				},
				{
					ID:         "tx-2",
					JobID:      "job-456",
					ExternalID: "ext-002",
					Amount:     "200.00",
					Currency:   "USD",
					Date:       now,
					Status:     "PENDING",
				},
			}
			resp.NextCursor = "cursor-page-2"
			resp.HasMore = true
		} else if cursor == "cursor-page-2" {
			resp.Items = []Transaction{
				{ID: "tx-3", JobID: "job-456", ExternalID: "ext-003", Amount: "300.00", Currency: "USD", Date: now, Status: "MATCHED"},
			}
			resp.NextCursor = ""
			resp.HasMore = false
		}

		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListTransactionsByJob(context.Background(), "ctx-123", "job-456")

	require.NoError(t, err)
	assert.Equal(t, 2, callCount, "should make 2 API calls for pagination")
	assert.Len(t, result, 3, "should collect all 3 transactions from both pages")
	assert.Equal(t, "tx-1", result[0].ID)
	assert.Equal(t, "tx-2", result[1].ID)
	assert.Equal(t, "tx-3", result[2].ID)
}

func TestIngestionClient_ErrorHandling(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"job not found"}`))
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))

	_, err := client.GetJob(context.Background(), "ctx-123", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get job")
}

func TestIngestionClient_EmptyJobList(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Items []IngestionJob `json:"items"`
		}{
			Items: []IngestionJob{},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListJobsByContext(context.Background(), "ctx-123")

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestIngestionClient_EmptyTransactionList(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Items      []Transaction `json:"items"`
			NextCursor string        `json:"nextCursor"`
			HasMore    bool          `json:"hasMore"`
		}{
			Items:      []Transaction{},
			NextCursor: "",
			HasMore:    false,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewIngestionClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListTransactionsByJob(context.Background(), "ctx-123", "job-456")

	require.NoError(t, err)
	assert.Empty(t, result)
}
