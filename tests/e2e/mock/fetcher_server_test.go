//go:build e2e

package mock

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMockFetcherServer_ListConnections_RequiresAndFiltersOrgID(t *testing.T) {
	t.Parallel()

	server := NewMockFetcherServer()
	server.AddConnection(MockConnection{OrgID: "tenant-a", ID: "conn-a", ConfigName: "alpha", DatabaseType: "POSTGRESQL", Status: "AVAILABLE"})
	server.AddConnection(MockConnection{OrgID: "tenant-b", ID: "conn-b", ConfigName: "beta", DatabaseType: "MYSQL", Status: "AVAILABLE"})

	baseURL, err := server.StartOnPort(0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
	})

	resp, err := http.Get(baseURL + "/api/v1/connections") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	filteredResp, err := http.Get(baseURL + "/api/v1/connections?orgId=tenant-a") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		filteredResp.Body.Close()
	})
	require.Equal(t, http.StatusOK, filteredResp.StatusCode)

	var payload struct {
		Connections []struct {
			ID string `json:"id"`
		} `json:"connections"`
	}
	require.NoError(t, json.NewDecoder(filteredResp.Body).Decode(&payload))
	require.Len(t, payload.Connections, 1)
	require.Equal(t, "conn-a", payload.Connections[0].ID)
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

	resp, err := http.Get(baseURL + "/api/v1/connections/conn-1/schema") //nolint:noctx // test server request
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
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
	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/connections/conn-1/test", http.NoBody) //nolint:noctx // test server request
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
