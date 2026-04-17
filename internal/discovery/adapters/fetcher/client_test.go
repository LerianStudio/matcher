//go:build unit

package fetcher

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// newTestClient creates an HTTPFetcherClient pointing at the given test server.
func newTestClient(t *testing.T, serverURL string) *HTTPFetcherClient {
	t.Helper()

	cfg := DefaultConfig()
	cfg.BaseURL = serverURL
	cfg.AllowPrivateIPs = true
	cfg.MaxRetries = 0 // no retries by default to keep tests fast

	client, err := NewHTTPFetcherClient(cfg)
	require.NoError(t, err)

	return client
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestNewHTTPFetcherClient_ValidConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	client, err := NewHTTPFetcherClient(cfg)

	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewHTTPFetcherClient_InvalidConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = ""

	client, err := NewHTTPFetcherClient(cfg)

	require.Error(t, err)
	assert.Nil(t, client)
	assert.ErrorIs(t, err, ErrEmptyURL)
}

func TestNewHTTPFetcherClient_TrimsTrailingSlash(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http://localhost:4006/"

	client, err := NewHTTPFetcherClient(cfg)

	require.NoError(t, err)
	assert.Equal(t, "http://localhost:4006", client.baseURL)
}

func TestHTTPFetcherClient_NilReceiverSafety(t *testing.T) {
	t.Parallel()

	var client *HTTPFetcherClient

	assert.False(t, client.IsHealthy(context.Background()))

	_, err := client.ListConnections(context.Background(), "org-1")
	require.ErrorIs(t, err, ErrFetcherClientNil)

	_, err = client.GetSchema(context.Background(), "conn-1")
	require.ErrorIs(t, err, ErrFetcherClientNil)

	_, err = client.TestConnection(context.Background(), "conn-1")
	require.ErrorIs(t, err, ErrFetcherClientNil)

	_, err = client.SubmitExtractionJob(context.Background(), sharedPorts.ExtractionJobInput{})
	require.ErrorIs(t, err, ErrFetcherClientNil)

	_, err = client.GetExtractionJobStatus(context.Background(), "job-1")
	require.ErrorIs(t, err, ErrFetcherClientNil)
}
