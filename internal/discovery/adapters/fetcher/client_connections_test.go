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

func TestListConnections_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/management/connections", r.URL.Path)
		assert.Equal(t, "midaz", r.Header.Get("X-Product-Name"))

		resp := fetcherConnectionListResponse{
			Items: []fetcherConnectionResponse{
				{
					ID:           "conn-1",
					ConfigName:   "prod-db",
					Type:         "POSTGRESQL",
					Host:         "db.example.com",
					Port:         5432,
					DatabaseName: "production",
					ProductName:  "PostgreSQL 16",
					CreatedAt:    "2026-01-15T10:00:00Z",
					UpdatedAt:    "2026-01-16T12:00:00Z",
				},
				{
					ID:           "conn-2",
					ConfigName:   "staging-db",
					Type:         "MYSQL",
					Host:         "staging.example.com",
					Port:         3306,
					DatabaseName: "staging",
					ProductName:  "MySQL 8",
				},
			},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "midaz")

	require.NoError(t, err)
	require.Len(t, conns, 2)
	assert.Equal(t, "conn-1", conns[0].ID)
	assert.Equal(t, "POSTGRESQL", conns[0].DatabaseType)
	assert.Equal(t, 5432, conns[0].Port)
	assert.False(t, conns[0].CreatedAt.IsZero())
	assert.Equal(t, "conn-2", conns[1].ID)
	assert.Equal(t, "MYSQL", conns[1].DatabaseType)
}

func TestListConnections_IgnoresSSLPayloadShapeVariations(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Return raw JSON to exercise compatibility with both legacy boolean and
		// newer object-shaped ssl payloads.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"items": [
				{
					"id": "conn-ssl",
					"configName": "prod-pg",
					"type": "POSTGRESQL",
					"host": "db.example.com",
					"port": 5432,
					"databaseName": "production",
					"userName": "admin",
					"productName": "PostgreSQL 16",
					"ssl": {"mode": "require"}
				},
				{
					"id": "conn-nossl",
					"configName": "dev-pg",
					"type": "POSTGRESQL",
					"host": "localhost",
					"port": 5432,
					"databaseName": "dev",
					"userName": "dev",
					"productName": "PostgreSQL 16",
					"ssl": false
				}
			],
			"page": 1,
			"limit": 10,
			"total": 2
		}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	require.Len(t, conns, 2)
	assert.Equal(t, "conn-ssl", conns[0].ID)
	assert.Equal(t, "conn-nossl", conns[1].ID)
}

func TestListConnections_EmptyList(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherConnectionListResponse{Items: []fetcherConnectionResponse{}}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	assert.Empty(t, conns)
}

func TestListConnections_NoProductName(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/management/connections", r.URL.Path)
		assert.Empty(t, r.Header.Get("X-Product-Name"))

		resp := fetcherConnectionListResponse{Items: []fetcherConnectionResponse{}}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	_, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
}

func TestListConnections_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
}

func TestListConnections_BadJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{invalid json}")) //nolint:errcheck // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.Contains(t, err.Error(), "decode connections response")
}

func TestListConnections_NullPayloadRejected(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("null"))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "null/empty payload")
}

func TestListConnections_ErrorBodyParsing_StructuredError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"code":"INVALID_PATH_PARAMETER","message":"connection id is invalid"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "INVALID_PATH_PARAMETER")
	assert.NotContains(t, err.Error(), "connection id is invalid")
}

func TestListConnections_ErrorBodyParsing_NonJSONBody(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Internal Server Error"))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "status 500")
	assert.NotContains(t, err.Error(), "[")
}
