//go:build e2e

package journeys

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
)

// TestHealth_Endpoints verifies the health and ready endpoints are accessible.
func TestHealth_Endpoints(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, _ *e2e.Client) {
		cfg := tc.Config()
		httpClient := &http.Client{Timeout: 10 * time.Second}

		t.Run("health endpoint returns 200", func(t *testing.T) {
			resp, err := httpClient.Get(cfg.AppBaseURL + "/health")
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
		})

		t.Run("ready endpoint returns 200", func(t *testing.T) {
			resp, err := httpClient.Get(cfg.AppBaseURL + "/readyz")
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
		})

		t.Run("swagger endpoint is accessible when enabled", func(t *testing.T) {
			resp, err := httpClient.Get(cfg.AppBaseURL + "/swagger/index.html")
			require.NoError(t, err)
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				t.Skip("swagger is disabled in this environment")
			}

			require.Equal(t, http.StatusOK, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), "swagger")
		})
	})
}

// TestHealth_VersionEndpoint verifies the version endpoint returns build info.
func TestHealth_VersionEndpoint(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, _ *e2e.Client) {
		cfg := tc.Config()
		httpClient := &http.Client{Timeout: 10 * time.Second}

		resp, err := httpClient.Get(cfg.AppBaseURL + "/version")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var result map[string]interface{}
		require.NoError(t, json.Unmarshal(body, &result))
		require.Contains(t, result, "version")
		require.Contains(t, result, "requestDate")

		tc.Logf("Service version: %s", result["version"])
	})
}

// TestHealth_APIBaseRoute verifies the API base route responds.
func TestHealth_APIBaseRoute(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
		ctx := context.Background()

		// List contexts should work (may return empty list)
		contexts, err := client.Configuration.ListContexts(ctx)
		require.NoError(t, err)
		tc.Logf("Found %d existing contexts", len(contexts))
	})
}
