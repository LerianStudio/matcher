//go:build e2e

package mock

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMockFetcherServer_ListConnections_ReturnsItemsWrapper(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.AddConnection(MockConnection{ID: "conn-a", ConfigName: "alpha", Type: "postgresql", ProductName: "PostgreSQL 15"})
	server.AddConnection(MockConnection{ID: "conn-b", ConfigName: "beta", Type: "mysql", ProductName: "MySQL 8.0"})

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	// No orgId required — should return 200 with all connections.
	resp, err := http.Get(baseURL + "/v1/management/connections") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload struct {
		Items []struct {
			ID          string `json:"id"`
			ConfigName  string `json:"configName"`
			Type        string `json:"type"`
			ProductName string `json:"productName"`
		} `json:"items"`
		Page  int `json:"page"`
		Limit int `json:"limit"`
		Total int `json:"total"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Len(t, payload.Items, 2)
	require.Equal(t, 1, payload.Page)
	require.Equal(t, 2, payload.Total)
	require.Equal(t, "conn-a", payload.Items[0].ID)
	require.Equal(t, "postgresql", payload.Items[0].Type)
	require.Equal(t, "conn-b", payload.Items[1].ID)
}

func TestMockFetcherServer_ListConnections_FiltersXProductName(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.AddConnection(MockConnection{ID: "conn-a", ConfigName: "alpha", Type: "postgresql", ProductName: "PostgreSQL 15"})
	server.AddConnection(MockConnection{ID: "conn-b", ConfigName: "beta", Type: "mysql", ProductName: "MySQL 8.0"})

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	client := &http.Client{Timeout: 5 * time.Second}

	// "matcher" is the expected product name — should return ALL connections.
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/management/connections", http.NoBody) //nolint:noctx // test server request
	require.NoError(t, err)
	req.Header.Set("X-Product-Name", "matcher")

	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		Total int `json:"total"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Len(t, payload.Items, 2, "X-Product-Name=matcher should return all connections")
	require.Equal(t, 2, payload.Total)

	// An unrecognized product name should return an empty list.
	req2, err := http.NewRequest(http.MethodGet, baseURL+"/v1/management/connections", http.NoBody) //nolint:noctx // test server request
	require.NoError(t, err)
	req2.Header.Set("X-Product-Name", "unknown-product")

	resp2, err := client.Do(req2)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp2.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	var payload2 struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		Total int `json:"total"`
	}
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&payload2))
	require.Len(t, payload2.Items, 0, "unrecognized product name should return empty list")
	require.Equal(t, 0, payload2.Total)
}

func TestMockFetcherServer_ListConnections_ConnectionFieldsPresent(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.AddConnection(MockConnection{
		ID:           "conn-x",
		ConfigName:   "xdb",
		Type:         "postgresql",
		Host:         "db.example.com",
		Port:         5432,
		Schema:       "public",
		DatabaseName: "mydb",
		UserName:     "admin",
		ProductName:  "PostgreSQL 15",
		Metadata:     map[string]any{"region": "us-east-1"},
	})

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	resp, err := http.Get(baseURL + "/v1/management/connections") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload struct {
		Items []struct {
			ID           string         `json:"id"`
			ConfigName   string         `json:"configName"`
			Type         string         `json:"type"`
			Host         string         `json:"host"`
			Port         int            `json:"port"`
			Schema       string         `json:"schema"`
			DatabaseName string         `json:"databaseName"`
			UserName     string         `json:"userName"`
			ProductName  string         `json:"productName"`
			Metadata     map[string]any `json:"metadata"`
			CreatedAt    string         `json:"createdAt"`
			UpdatedAt    string         `json:"updatedAt"`
		} `json:"items"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Len(t, payload.Items, 1)

	conn := payload.Items[0]
	require.Equal(t, "conn-x", conn.ID)
	require.Equal(t, "xdb", conn.ConfigName)
	require.Equal(t, "postgresql", conn.Type)
	require.Equal(t, "db.example.com", conn.Host)
	require.Equal(t, 5432, conn.Port)
	require.Equal(t, "public", conn.Schema)
	require.Equal(t, "mydb", conn.DatabaseName)
	require.Equal(t, "admin", conn.UserName)
	require.Equal(t, "PostgreSQL 15", conn.ProductName)
	require.Equal(t, "us-east-1", conn.Metadata["region"])
	require.NotEmpty(t, conn.CreatedAt, "createdAt should be populated")
	require.NotEmpty(t, conn.UpdatedAt, "updatedAt should be populated")
}

func TestMockFetcherServer_GetSchema_NilSchemaReturnsNotFound(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.SetSchema("conn-1", nil)

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	resp, err := http.Get(baseURL + "/v1/management/connections/conn-1/schema") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestMockFetcherServer_GetSchema_ReturnsFlatFields(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.SetSchema("conn-pg", &MockSchema{
		ID:           "conn-pg",
		ConfigName:   "postgres-orders",
		DatabaseName: "orders_db",
		Type:         "postgresql",
		Tables: []MockTable{
			{Name: "orders", Fields: []string{"id", "amount", "currency", "created_at"}},
			{Name: "payments", Fields: []string{"id", "order_id", "amount"}},
		},
	})

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	resp, err := http.Get(baseURL + "/v1/management/connections/conn-pg/schema") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload struct {
		ID           string `json:"id"`
		ConfigName   string `json:"configName"`
		DatabaseName string `json:"databaseName"`
		Type         string `json:"type"`
		Tables       []struct {
			Name   string   `json:"name"`
			Fields []string `json:"fields"`
		} `json:"tables"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, "conn-pg", payload.ID)
	require.Equal(t, "postgres-orders", payload.ConfigName)
	require.Equal(t, "orders_db", payload.DatabaseName)
	require.Equal(t, "postgresql", payload.Type)
	require.Len(t, payload.Tables, 2)
	require.Equal(t, "orders", payload.Tables[0].Name)
	require.Equal(t, []string{"id", "amount", "currency", "created_at"}, payload.Tables[0].Fields)
	require.Equal(t, "payments", payload.Tables[1].Name)
	require.Equal(t, []string{"id", "order_id", "amount"}, payload.Tables[1].Fields)
}

func TestMockFetcherServer_TestConnection_NilResultReturnsNotFound(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.SetTestResult("conn-1", nil)

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/management/connections/conn-1/test", http.NoBody) //nolint:noctx // test server request
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestMockFetcherServer_TestConnection_ReturnsStatusAndMessage(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.SetTestResult("conn-ok", &MockTestResult{
		Status:    "success",
		Message:   "connection established",
		LatencyMs: 42,
	})
	server.SetTestResult("conn-fail", &MockTestResult{
		Status:    "error",
		Message:   "connection timed out",
		LatencyMs: 5000,
	})

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	client := &http.Client{Timeout: 5 * time.Second}

	// Test successful connection.
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/management/connections/conn-ok/test", http.NoBody) //nolint:noctx // test server request
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var okPayload struct {
		Status    string `json:"status"`
		Message   string `json:"message"`
		LatencyMs int64  `json:"latencyMs"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&okPayload))
	require.Equal(t, "success", okPayload.Status)
	require.Equal(t, "connection established", okPayload.Message)
	require.Equal(t, int64(42), okPayload.LatencyMs)

	// Test failed connection.
	req2, err := http.NewRequest(http.MethodPost, baseURL+"/v1/management/connections/conn-fail/test", http.NoBody) //nolint:noctx // test server request
	require.NoError(t, err)

	resp2, err := client.Do(req2)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp2.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	var failPayload struct {
		Status    string `json:"status"`
		Message   string `json:"message"`
		LatencyMs int64  `json:"latencyMs"`
	}
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&failPayload))
	require.Equal(t, "error", failPayload.Status)
	require.Equal(t, "connection timed out", failPayload.Message)
	require.Equal(t, int64(5000), failPayload.LatencyMs)
}

func TestMockFetcherServer_SubmitExtraction_Returns202WithNestedBody(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	reqBody := map[string]any{
		"dataRequest": map[string]any{
			"mappedFields": map[string]any{
				"orders": map[string]any{
					"orders": []string{"id", "amount", "currency"},
				},
			},
		},
		"metadata": map[string]any{
			"source": "conn-postgres-001",
		},
	}
	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/fetcher", bytes.NewReader(bodyBytes)) //nolint:noctx // test server request
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "submit extraction should return 202")

	var payload struct {
		JobID     string `json:"jobId"`
		Status    string `json:"status"`
		CreatedAt string `json:"createdAt"`
		Message   string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.NotEmpty(t, payload.JobID, "jobId should be present")
	require.Equal(t, "pending", payload.Status)
	require.NotEmpty(t, payload.CreatedAt, "createdAt should be populated")
	require.Equal(t, "extraction job accepted", payload.Message)
}

func TestMockFetcherServer_SubmitExtraction_MissingSourceReturns400(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	// Missing metadata.source.
	reqBody := map[string]any{
		"dataRequest": map[string]any{
			"mappedFields": map[string]any{},
		},
		"metadata": map[string]any{},
	}
	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/fetcher", bytes.NewReader(bodyBytes)) //nolint:noctx // test server request
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var payload struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Contains(t, payload.Error, "source")
}

func TestMockFetcherServer_GetExtractionStatus_LowercaseStatuses(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	now := time.Now().UTC()
	completed := now.Add(time.Minute)

	server.AddJob(MockExtractionJob{
		ID:          "job-001",
		Status:      "completed",
		ResultPath:  "/data/extractions/orders.csv",
		ResultHmac:  "abc123",
		RequestHash: "hash456",
		Metadata:    map[string]any{"source": "my-source"},
		CreatedAt:   now,
		CompletedAt: &completed,
	})
	server.AddJob(MockExtractionJob{
		ID:        "job-002",
		Status:    "pending",
		CreatedAt: now,
	})

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	// Test completed job.
	resp, err := http.Get(baseURL + "/v1/fetcher/job-001") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var completedPayload struct {
		ID          string         `json:"id"`
		Status      string         `json:"status"`
		ResultPath  string         `json:"resultPath"`
		ResultHmac  string         `json:"resultHmac"`
		RequestHash string         `json:"requestHash"`
		Metadata    map[string]any `json:"metadata"`
		CreatedAt   string         `json:"createdAt"`
		CompletedAt string         `json:"completedAt"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&completedPayload))
	require.Equal(t, "job-001", completedPayload.ID)
	require.Equal(t, "completed", completedPayload.Status)
	require.Equal(t, "/data/extractions/orders.csv", completedPayload.ResultPath)
	require.Equal(t, "abc123", completedPayload.ResultHmac)
	require.Equal(t, "hash456", completedPayload.RequestHash)
	require.Equal(t, "my-source", completedPayload.Metadata["source"])
	require.NotEmpty(t, completedPayload.CreatedAt)
	require.NotEmpty(t, completedPayload.CompletedAt)

	// Test pending job — no resultPath, no completedAt.
	resp2, err := http.Get(baseURL + "/v1/fetcher/job-002") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp2.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	var pendingPayload struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		ResultPath  string `json:"resultPath"`
		CompletedAt string `json:"completedAt"`
	}
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&pendingPayload))
	require.Equal(t, "job-002", pendingPayload.ID)
	require.Equal(t, "pending", pendingPayload.Status)
	require.Empty(t, pendingPayload.ResultPath, "pending job should not have resultPath")
	require.Empty(t, pendingPayload.CompletedAt, "pending job should not have completedAt")
}

func TestMockFetcherServer_GetExtractionStatus_NotFound(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	resp, err := http.Get(baseURL + "/v1/fetcher/nonexistent-job") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestMockFetcherServer_SetJobStatus_UpdatesStatus(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.AddJob(MockExtractionJob{
		ID:     "job-x",
		Status: "pending",
	})
	server.SetJobStatus("job-x", "completed")

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	resp, err := http.Get(baseURL + "/v1/fetcher/job-x") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		CompletedAt string `json:"completedAt"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, "job-x", payload.ID)
	require.Equal(t, "completed", payload.Status)
	require.NotEmpty(t, payload.CompletedAt, "completed job should have completedAt")
}

func TestMockFetcherServer_Health_HealthyAndUnhealthy(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	// Healthy by default.
	resp, err := http.Get(baseURL + "/health") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var okPayload struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&okPayload))
	require.Equal(t, "ok", okPayload.Status)

	// Set unhealthy.
	server.SetHealthy(false)
	resp2, err := http.Get(baseURL + "/health") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp2.Body.Close()
	})
	require.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode)

	var badPayload struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&badPayload))
	require.Equal(t, "unhealthy", badPayload.Status)
}

func TestMockFetcherServer_Reset_ClearsAllState(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.AddConnection(MockConnection{ID: "c1", Type: "postgresql", ProductName: "PG"})
	server.AddJob(MockExtractionJob{ID: "j1", Status: "pending"})

	server.Reset()

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	// Connections should be empty.
	resp, err := http.Get(baseURL + "/v1/management/connections") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})

	var payload struct {
		Items []any `json:"items"`
		Total int   `json:"total"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Len(t, payload.Items, 0)
	require.Equal(t, 0, payload.Total)

	// Job should not exist.
	resp2, err := http.Get(baseURL + "/v1/fetcher/j1") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp2.Body.Close()
	})
	require.Equal(t, http.StatusNotFound, resp2.StatusCode)
}
