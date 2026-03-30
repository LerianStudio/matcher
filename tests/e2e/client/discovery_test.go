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

func TestNewDiscoveryClient(t *testing.T) {
	t.Parallel()

	baseClient := NewClient("http://localhost:4018", "tenant-123", 30*time.Second)
	discoveryClient := NewDiscoveryClient(baseClient)

	assert.NotNil(t, discoveryClient)
	assert.Equal(t, baseClient, discoveryClient.client)
}

func TestDiscoveryClient_GetStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/status", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryStatusResponse{
			FetcherHealthy:  true,
			ConnectionCount: 3,
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetStatus(context.Background())

	require.NoError(t, err)
	assert.True(t, result.FetcherHealthy)
	assert.Equal(t, 3, result.ConnectionCount)
}

func TestDiscoveryClient_ListConnections(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/connections", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryConnectionListResponse{
			Connections: []DiscoveryConnectionResponse{
				{ID: "conn-1", ConfigName: "pg-main", DatabaseType: "postgres", Status: "active", SchemaDiscovered: true, LastSeenAt: now},
				{ID: "conn-2", ConfigName: "mysql-aux", DatabaseType: "mysql", Status: "active", SchemaDiscovered: false, LastSeenAt: now},
			},
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListConnections(context.Background())

	require.NoError(t, err)
	assert.Len(t, result.Connections, 2)
	assert.Equal(t, "conn-1", result.Connections[0].ID)
	assert.Equal(t, "postgres", result.Connections[0].DatabaseType)
	assert.True(t, result.Connections[0].SchemaDiscovered)
}

func TestDiscoveryClient_GetConnection(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/connections/conn-42", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryConnectionResponse{
			ID:               "conn-42",
			ConfigName:       "warehouse",
			DatabaseType:     "postgres",
			Status:           "active",
			SchemaDiscovered: true,
			LastSeenAt:       now,
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetConnection(context.Background(), "conn-42")

	require.NoError(t, err)
	assert.Equal(t, "conn-42", result.ID)
	assert.Equal(t, "warehouse", result.ConfigName)
}

func TestDiscoveryClient_GetConnectionSchema(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/connections/conn-1/schema", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryConnectionSchemaResponse{
			ConnectionID: "conn-1",
			Tables: []DiscoverySchemaTableResponse{
				{
					TableName: "payments",
					Columns: []DiscoverySchemaColumnResponse{
						{Name: "id", Type: "uuid", Nullable: false},
						{Name: "amount", Type: "numeric", Nullable: false},
						{Name: "description", Type: "text", Nullable: true},
					},
				},
			},
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetConnectionSchema(context.Background(), "conn-1")

	require.NoError(t, err)
	assert.Equal(t, "conn-1", result.ConnectionID)
	assert.Len(t, result.Tables, 1)
	assert.Equal(t, "payments", result.Tables[0].TableName)
	assert.Len(t, result.Tables[0].Columns, 3)
	assert.Equal(t, "id", result.Tables[0].Columns[0].Name)
	assert.False(t, result.Tables[0].Columns[0].Nullable)
	assert.True(t, result.Tables[0].Columns[2].Nullable)
}

func TestDiscoveryClient_TestConnection(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/connections/conn-1/test", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryTestConnectionResponse{
			ConnectionID: "conn-1",
			Healthy:      true,
			LatencyMs:    42,
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.TestConnection(context.Background(), "conn-1")

	require.NoError(t, err)
	assert.Equal(t, "conn-1", result.ConnectionID)
	assert.True(t, result.Healthy)
	assert.Equal(t, int64(42), result.LatencyMs)
	assert.Empty(t, result.ErrorMessage)
}

func TestDiscoveryClient_TestConnection_Unhealthy(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryTestConnectionResponse{
			ConnectionID: "conn-bad",
			Healthy:      false,
			LatencyMs:    0,
			ErrorMessage: "connection refused",
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.TestConnection(context.Background(), "conn-bad")

	require.NoError(t, err)
	assert.False(t, result.Healthy)
	assert.Equal(t, "connection refused", result.ErrorMessage)
}

func TestDiscoveryClient_StartExtraction(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/connections/conn-1/extractions", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		var reqBody DiscoveryStartExtractionRequest
		err := json.NewDecoder(r.Body).Decode(&reqBody)
		assert.NoError(t, err)
		assert.Contains(t, reqBody.Tables, "payments")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		err = json.NewEncoder(w).Encode(DiscoveryExtractionResponse{
			ID:           "ext-100",
			ConnectionID: "conn-1",
			Tables:       map[string]DiscoveryExtractionTableResponse{"payments": {Columns: []string{"id", "amount"}}},
			Status:       "PENDING",
			CreatedAt:    now,
			UpdatedAt:    now,
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.StartExtraction(context.Background(), "conn-1", DiscoveryStartExtractionRequest{
		Tables: map[string]DiscoveryExtractionTableRequest{
			"payments": {Columns: []string{"id", "amount"}},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, "ext-100", result.ID)
	assert.Equal(t, "conn-1", result.ConnectionID)
	assert.Equal(t, "PENDING", result.Status)
}

func TestDiscoveryClient_GetExtraction(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/extractions/ext-100", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryExtractionResponse{
			ID:           "ext-100",
			ConnectionID: "conn-1",
			Status:       "COMPLETED",
			CreatedAt:    now,
			UpdatedAt:    now,
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetExtraction(context.Background(), "ext-100")

	require.NoError(t, err)
	assert.Equal(t, "ext-100", result.ID)
	assert.Equal(t, "COMPLETED", result.Status)
}

func TestDiscoveryClient_PollExtraction(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/extractions/ext-100/poll", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryExtractionResponse{
			ID:           "ext-100",
			ConnectionID: "conn-1",
			Status:       "IN_PROGRESS",
			CreatedAt:    now,
			UpdatedAt:    now,
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.PollExtraction(context.Background(), "ext-100")

	require.NoError(t, err)
	assert.Equal(t, "ext-100", result.ID)
	assert.Equal(t, "IN_PROGRESS", result.Status)
}

func TestDiscoveryClient_RefreshDiscovery(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/discovery/refresh", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(DiscoveryRefreshResponse{
			ConnectionsSynced: 5,
		})
		assert.NoError(t, err)
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.RefreshDiscovery(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 5, result.ConnectionsSynced)
}

func TestDiscoveryClient_ErrorHandling(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"connection not found"}`))
	}))
	defer server.Close()

	client := NewDiscoveryClient(NewClient(server.URL, "tenant-123", 5*time.Second))

	_, err := client.GetConnection(context.Background(), "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get discovery connection")
}
