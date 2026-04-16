//go:build unit

package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListConnections_Pagination_CollectsAllPages(t *testing.T) {
	t.Parallel()

	const totalConns = 150

	var requestCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		assert.Equal(t, "test-product", r.Header.Get("X-Product-Name"))
		requestCount.Add(1)

		var items []fetcherConnectionResponse

		switch page {
		case "1", "":
			for i := 0; i < listConnectionsPageSize; i++ {
				items = append(items, fetcherConnectionResponse{
					ID:   fmt.Sprintf("conn-%d", i),
					Type: "POSTGRESQL",
				})
			}
		case "2":
			for i := listConnectionsPageSize; i < totalConns; i++ {
				items = append(items, fetcherConnectionResponse{
					ID:   fmt.Sprintf("conn-%d", i),
					Type: "POSTGRESQL",
				})
			}
		default:
			t.Errorf("unexpected page requested: %s", page)
		}

		resp := fetcherConnectionListResponse{
			Items: items,
			Page:  1,
			Limit: listConnectionsPageSize,
			Total: totalConns,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "test-product")

	require.NoError(t, err)
	require.Len(t, conns, totalConns)
	assert.Equal(t, "conn-0", conns[0].ID)
	assert.Equal(t, "conn-149", conns[totalConns-1].ID)
	assert.Equal(t, int32(2), requestCount.Load())
}

func TestListConnections_Pagination_IgnoresUnderreportedTotal(t *testing.T) {
	t.Parallel()

	const actualTotal = 150

	var requestCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		requestCount.Add(1)

		var items []fetcherConnectionResponse
		reportedTotal := listConnectionsPageSize

		switch page {
		case "1", "":
			for i := 0; i < listConnectionsPageSize; i++ {
				items = append(items, fetcherConnectionResponse{
					ID:   fmt.Sprintf("conn-%d", i),
					Type: "POSTGRESQL",
				})
			}
		case "2":
			reportedTotal = actualTotal
			for i := listConnectionsPageSize; i < actualTotal; i++ {
				items = append(items, fetcherConnectionResponse{
					ID:   fmt.Sprintf("conn-%d", i),
					Type: "POSTGRESQL",
				})
			}
		default:
			t.Fatalf("unexpected page requested: %s", page)
		}

		resp := fetcherConnectionListResponse{
			Items: items,
			Page:  1,
			Limit: listConnectionsPageSize,
			Total: reportedTotal,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	require.Len(t, conns, actualTotal)
	assert.Equal(t, int32(2), requestCount.Load())
}

func TestListConnections_Pagination_ThreePages(t *testing.T) {
	t.Parallel()

	const totalConns = 250

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")

		var start int
		var end int

		switch page {
		case "1", "":
			start, end = 0, listConnectionsPageSize
		case "2":
			start, end = listConnectionsPageSize, 2*listConnectionsPageSize
		case "3":
			start, end = 2*listConnectionsPageSize, totalConns
		}

		items := make([]fetcherConnectionResponse, 0, end-start)
		for i := start; i < end; i++ {
			items = append(items, fetcherConnectionResponse{
				ID:   fmt.Sprintf("conn-%d", i),
				Type: "MYSQL",
			})
		}

		resp := fetcherConnectionListResponse{
			Items: items,
			Limit: listConnectionsPageSize,
			Total: totalConns,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	assert.Len(t, conns, totalConns)
}

func TestListConnections_Pagination_SinglePage_NoExtraRequests(t *testing.T) {
	t.Parallel()

	var requestCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)

		resp := fetcherConnectionListResponse{
			Items: []fetcherConnectionResponse{
				{ID: "conn-1", Type: "POSTGRESQL"},
				{ID: "conn-2", Type: "POSTGRESQL"},
			},
			Page:  1,
			Limit: listConnectionsPageSize,
			Total: 2,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.NoError(t, err)
	assert.Len(t, conns, 2)
	assert.Equal(t, int32(1), requestCount.Load(), "should only make one request for small result sets")
}

func TestListConnections_Pagination_PageError_ReturnsError(t *testing.T) {
	t.Parallel()

	var requestCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count := requestCount.Add(1)
		if count == 1 {
			resp := fetcherConnectionListResponse{
				Items: make([]fetcherConnectionResponse, listConnectionsPageSize),
				Limit: listConnectionsPageSize,
				Total: 200,
			}
			for i := range resp.Items {
				resp.Items[i] = fetcherConnectionResponse{ID: fmt.Sprintf("conn-%d", i), Type: "POSTGRESQL"}
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper

			return
		}

		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.Contains(t, err.Error(), "list connections page 2")
}

func TestListConnections_Pagination_RepeatedFullPage_ReturnsOverflow(t *testing.T) {
	t.Parallel()

	var requestCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)

		items := make([]fetcherConnectionResponse, 0, listConnectionsPageSize)
		for i := 0; i < listConnectionsPageSize; i++ {
			items = append(items, fetcherConnectionResponse{
				ID:   fmt.Sprintf("conn-%d", i),
				Type: "POSTGRESQL",
			})
		}

		resp := fetcherConnectionListResponse{
			Items: items,
			Page:  1,
			Limit: listConnectionsPageSize,
			Total: listConnectionsPageSize * 2,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp) //nolint:errcheck,errchkjson // test helper

		_ = r
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL)
	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.ErrorIs(t, err, ErrFetcherPaginationOverflow)
	assert.Contains(t, err.Error(), "page 2 repeated")
	assert.Equal(t, int32(2), requestCount.Load())
}

func TestListConnections_Pagination_OverflowSafetyLimit(t *testing.T) {
	t.Parallel()

	var requestCount atomic.Int32

	client := &HTTPFetcherClient{
		httpClient: &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			page, err := strconv.Atoi(req.URL.Query().Get("page"))
			require.NoError(t, err)
			requestCount.Add(1)

			items := make([]fetcherConnectionResponse, 0, listConnectionsPageSize)
			for i := 0; i < listConnectionsPageSize; i++ {
				items = append(items, fetcherConnectionResponse{
					ID:   fmt.Sprintf("conn-%d-%d", page, i),
					Type: "POSTGRESQL",
				})
			}

			payload, marshalErr := json.Marshal(fetcherConnectionListResponse{
				Items: items,
				Page:  page,
				Limit: listConnectionsPageSize,
				Total: listConnectionsPageSize * (maxPaginationPages + 1),
			})
			require.NoError(t, marshalErr)

			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(payload)),
				Header:     make(http.Header),
			}, nil
		})},
		baseURL: "http://fetcher.internal",
		cfg: HTTPClientConfig{
			RequestTimeout:  time.Second,
			AllowPrivateIPs: true,
		},
	}

	conns, err := client.ListConnections(context.Background(), "")

	require.Error(t, err)
	assert.Nil(t, conns)
	assert.ErrorIs(t, err, ErrFetcherPaginationOverflow)
	assert.Equal(t, int32(maxPaginationPages), requestCount.Load())
}
