//go:build e2e

package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMatchingClient(t *testing.T) {
	t.Parallel()

	baseClient := NewClient("http://localhost:4018", "tenant-123", 30*time.Second)
	matchingClient := NewMatchingClient(baseClient)

	assert.NotNil(t, matchingClient)
	assert.Equal(t, baseClient, matchingClient.client)
}

func TestMatchingClient_RunMatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/matching/contexts/ctx-123/run", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var req RunMatchRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "COMMIT", req.Mode)

		resp := RunMatchResponse{
			RunID:  "run-456",
			Status: "STARTED",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.RunMatch(context.Background(), "ctx-123", "COMMIT")

	require.NoError(t, err)
	assert.Equal(t, "run-456", result.RunID)
	assert.Equal(t, "STARTED", result.Status)
}

func TestMatchingClient_RunMatchCommit(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req RunMatchRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "COMMIT", req.Mode)

		resp := RunMatchResponse{RunID: "run-123", Status: "STARTED"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.RunMatchCommit(context.Background(), "ctx-123")

	require.NoError(t, err)
	assert.Equal(t, "run-123", result.RunID)
}

func TestMatchingClient_RunMatchDryRun(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req RunMatchRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "DRY_RUN", req.Mode)

		resp := RunMatchResponse{RunID: "run-123", Status: "STARTED"}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.RunMatchDryRun(context.Background(), "ctx-123")

	require.NoError(t, err)
	assert.Equal(t, "run-123", result.RunID)
}

func TestMatchingClient_GetMatchRun(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/matching/runs/run-123", r.URL.Path)
		assert.Equal(t, "ctx-456", r.URL.Query().Get("contextId"))
		assert.Equal(t, http.MethodGet, r.Method)

		resp := MatchRun{
			ID:        "run-123",
			ContextID: "ctx-456",
			Mode:      "COMMIT",
			Status:    "COMPLETED",
			Stats: map[string]int{
				"matched":   100,
				"unmatched": 10,
			},
			StartedAt:   now,
			CompletedAt: now.Add(5 * time.Minute),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetMatchRun(context.Background(), "ctx-456", "run-123")

	require.NoError(t, err)
	assert.Equal(t, "run-123", result.ID)
	assert.Equal(t, "COMPLETED", result.Status)
	assert.Equal(t, 100, result.Stats["matched"])
}

func TestMatchingClient_GetMatchRunResults(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/matching/runs/run-123/groups", r.URL.Path)
		assert.Equal(t, "ctx-456", r.URL.Query().Get("contextId"))

		resp := struct {
			Items []MatchGroup `json:"items"`
		}{
			Items: []MatchGroup{
				{
					ID:         "grp-1",
					RunID:      "run-123",
					Confidence: 0.95,
					Items: []MatchItem{
						{ID: "item-1", TransactionID: "tx-1", Amount: "100.00"},
						{ID: "item-2", TransactionID: "tx-2", Amount: "100.00"},
					},
					CreatedAt: now,
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetMatchRunResults(context.Background(), "ctx-456", "run-123")

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "grp-1", result[0].ID)
	assert.Equal(t, 0.95, result[0].Confidence)
	assert.Len(t, result[0].Items, 2)
}

func TestMatchingClient_ListMatchRuns(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/matching/contexts/ctx-123/runs", r.URL.Path)

		resp := struct {
			Items []MatchRun `json:"items"`
		}{
			Items: []MatchRun{
				{ID: "run-1", Status: "COMPLETED"},
				{ID: "run-2", Status: "RUNNING"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListMatchRuns(context.Background(), "ctx-123")

	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "run-1", result[0].ID)
	assert.Equal(t, "COMPLETED", result[0].Status)
}

func TestMatchingClient_ErrorHandling(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid context"}`))
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))

	_, err := client.RunMatch(context.Background(), "invalid", "COMMIT")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "run match")
}

func TestMatchingClient_NotFound(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"run not found"}`))
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))

	_, err := client.GetMatchRun(context.Background(), "ctx-123", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get match run")
}

func TestMatchingClient_EmptyResults(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Items []MatchGroup `json:"items"`
		}{
			Items: []MatchGroup{},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewMatchingClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetMatchRunResults(context.Background(), "ctx-123", "run-123")

	require.NoError(t, err)
	assert.Empty(t, result)
}
