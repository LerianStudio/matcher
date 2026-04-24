//go:build unit

//nolint:varnamelen // Governance client tests use compact handler fixtures.
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

func TestNewGovernanceClient(t *testing.T) {
	t.Parallel()

	baseClient := NewClient("http://localhost:4018", "tenant-123", 30*time.Second)
	governanceClient := NewGovernanceClient(baseClient)

	assert.NotNil(t, governanceClient)
	assert.Equal(t, baseClient, governanceClient.client)
}

func TestGovernanceClient_GetAuditLog(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/governance/audit-logs/log-123", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		resp := AuditLog{
			ID:         "log-123",
			EntityType: "CONTEXT",
			EntityID:   "ctx-456",
			Action:     "CREATE",
			ActorID:    "user@example.com",
			CreatedAt:  now,
		}
		err := json.NewEncoder(w).Encode(resp)
		require.NoError(t, err)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.GetAuditLog(context.Background(), "log-123")

	require.NoError(t, err)
	assert.Equal(t, "log-123", result.ID)
	assert.Equal(t, "CONTEXT", result.EntityType)
	assert.Equal(t, "CREATE", result.Action)
}

func TestGovernanceClient_ListAuditLogsByEntity(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/governance/entities/CONTEXT/ctx-123/audit-logs", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		resp := struct {
			Items []AuditLog `json:"items"`
		}{
			Items: []AuditLog{
				{ID: "log-1", Action: "CREATE"},
				{ID: "log-2", Action: "UPDATE"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListAuditLogsByEntity(context.Background(), "CONTEXT", "ctx-123")

	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "CREATE", result[0].Action)
	assert.Equal(t, "UPDATE", result[1].Action)
}

func TestGovernanceClient_ListAuditLogs_NoParams(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/governance/audit-logs", r.URL.Path)
		assert.Empty(t, r.URL.RawQuery)

		resp := struct {
			Items []AuditLog `json:"items"`
		}{
			Items: []AuditLog{
				{ID: "log-1"},
				{ID: "log-2"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListAuditLogs(context.Background(), nil)

	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestGovernanceClient_ListAuditLogs_WithParams(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.RawQuery, "action=CREATE")
		assert.Contains(t, r.URL.RawQuery, "entity_type=CONTEXT")

		resp := struct {
			Items []AuditLog `json:"items"`
		}{
			Items: []AuditLog{
				{ID: "log-1", Action: "CREATE", EntityType: "CONTEXT"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListAuditLogs(context.Background(), map[string]string{
		"action":      "CREATE",
		"entity_type": "CONTEXT",
	})

	require.NoError(t, err)
	assert.Len(t, result, 1)
}

func TestGovernanceClient_ListAuditLogsByAction(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.RawQuery, "action=DELETE")

		resp := struct {
			Items []AuditLog `json:"items"`
		}{
			Items: []AuditLog{
				{ID: "log-1", Action: "DELETE"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListAuditLogsByAction(context.Background(), "DELETE")

	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "DELETE", result[0].Action)
}

func TestGovernanceClient_ListAuditLogsByEntityType(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.RawQuery, "entity_type=SOURCE")

		resp := struct {
			Items []AuditLog `json:"items"`
		}{
			Items: []AuditLog{
				{ID: "log-1", EntityType: "SOURCE"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListAuditLogsByEntityType(context.Background(), "SOURCE")

	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "SOURCE", result[0].EntityType)
}

func TestGovernanceClient_ErrorHandling(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"code":"MTCH-0601","title":"Not Found","message":"not found"}`))
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))

	_, err := client.GetAuditLog(context.Background(), "nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "get audit log")
}

func TestGovernanceClient_ListArchives(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/governance/archives", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		resp := struct {
			Items []ArchiveMetadata `json:"items"`
		}{
			Items: []ArchiveMetadata{
				{ID: "arch-1", Status: "READY", StartDate: "2026-01-01", EndDate: "2026-01-31"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListArchives(context.Background())

	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "arch-1", result[0].ID)
	assert.Equal(t, "READY", result[0].Status)
}

func TestGovernanceClient_DownloadArchive(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/governance/archives/arch-123/download", r.URL.Path)
		assert.Equal(t, http.MethodGet, r.Method)

		resp := ArchiveDownloadResponse{
			DownloadURL: "https://storage.example.com/archive.zip",
			ExpiresAt:   "2026-01-31T00:00:00Z",
			Checksum:    "abc123",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.DownloadArchive(context.Background(), "arch-123")

	require.NoError(t, err)
	assert.Equal(t, "https://storage.example.com/archive.zip", result.DownloadURL)
	assert.Equal(t, "abc123", result.Checksum)
}

func TestGovernanceClient_DownloadArchive_NotFound(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"code":"MTCH-0603","title":"Not Found","message":"archive not found"}`))
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	_, err := client.DownloadArchive(context.Background(), "nonexistent")

	require.Error(t, err)
	require.Contains(t, err.Error(), "download archive")
}

func TestGovernanceClient_EmptyResult(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Items []AuditLog `json:"items"`
		}{
			Items: []AuditLog{},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewGovernanceClient(NewClient(server.URL, "tenant-123", 5*time.Second))
	result, err := client.ListAuditLogs(context.Background(), nil)

	require.NoError(t, err)
	assert.Empty(t, result)
}
