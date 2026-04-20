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

func TestClientManagement_GetSchema_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/management/connections/conn-1/schema", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(fetcherSchemaResponse{ //nolint:errcheck,errchkjson // test helper
			ID:           "conn-1",
			ConfigName:   "prod-db",
			DatabaseName: "ledger",
			Type:         "POSTGRESQL",
			Tables: []fetcherTableResponse{{
				Name:   "transactions",
				Fields: []string{"id", "amount"},
			}},
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	schema, err := client.GetSchema(context.Background(), "conn-1")

	require.NoError(t, err)
	require.NotNil(t, schema)
	assert.Equal(t, "conn-1", schema.ID)
	assert.Equal(t, "prod-db", schema.ConfigName)
	require.Len(t, schema.Tables, 1)
	assert.Equal(t, "transactions", schema.Tables[0].Name)
}

func TestClientManagement_TestConnection_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/management/connections/conn-1/test", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(fetcherTestResponse{ //nolint:errcheck,errchkjson // test helper
			Status:    "success",
			Message:   "ok",
			LatencyMs: 42,
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	result, err := client.TestConnection(context.Background(), "conn-1")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "success", result.Status)
	assert.Equal(t, "ok", result.Message)
	assert.Equal(t, int64(42), result.LatencyMs)
}
