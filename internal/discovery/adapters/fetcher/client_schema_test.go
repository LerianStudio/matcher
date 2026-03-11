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

func TestGetSchema_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/connections/conn-abc/schema", r.URL.Path)
		resp := fetcherSchemaResponse{ConnectionID: "conn-abc", Tables: []fetcherTableResponse{{TableName: "transactions", Columns: []fetcherColumnResponse{{Name: "id", Type: "uuid", Nullable: false}, {Name: "amount", Type: "decimal", Nullable: false}, {Name: "note", Type: "text", Nullable: true}}}}}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "conn-abc")
	require.NoError(t, err)
	assert.Equal(t, "conn-abc", schema.ConnectionID)
	assert.False(t, schema.DiscoveredAt.IsZero())
	require.Len(t, schema.Tables, 1)
	assert.Equal(t, "transactions", schema.Tables[0].TableName)
	require.Len(t, schema.Tables[0].Columns, 3)
	assert.Equal(t, "id", schema.Tables[0].Columns[0].Name)
	assert.False(t, schema.Tables[0].Columns[0].Nullable)
	assert.True(t, schema.Tables[0].Columns[2].Nullable)
}

func TestGetSchema_NotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "nonexistent")
	require.Error(t, err)
	assert.Nil(t, schema)
	assert.ErrorIs(t, err, ErrFetcherNotFound)
}

func TestGetSchema_EmptyTables(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherSchemaResponse{ConnectionID: "conn-empty", Tables: []fetcherTableResponse{}}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "conn-empty")
	require.NoError(t, err)
	assert.Empty(t, schema.Tables)
}

func TestGetSchema_NullPayloadRejected(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("null"))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "conn-abc")
	require.Error(t, err)
	assert.Nil(t, schema)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestGetSchema_MismatchedConnectionID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := fetcherSchemaResponse{ConnectionID: "conn-other"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "conn-abc")
	require.Error(t, err)
	assert.Nil(t, schema)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "connection id mismatch")
}
