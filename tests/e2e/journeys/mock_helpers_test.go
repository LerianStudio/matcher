//go:build e2e

package journeys

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/LerianStudio/matcher/tests/e2e/mock"
)

// systemplaneHTTPClient is a shared HTTP client with a timeout for systemplane
// operations, preventing test hangs when the endpoint is unresponsive.
var systemplaneHTTPClient = &http.Client{Timeout: 30 * time.Second}

// mockFetcher is the package-level mock Fetcher server instance.
// It is started in TestMain and stopped after all journey tests complete.
// Journey tests that exercise the Discovery context can use getMockFetcher()
// to manipulate connections, schemas, and extraction jobs.
var mockFetcher *mock.MockFetcherServer

// getMockFetcher returns the running mock Fetcher server.
// Returns nil if the mock was not started (should not happen in normal E2E runs).
func getMockFetcher() *mock.MockFetcherServer {
	return mockFetcher
}

// fetcherConfigKeys are the systemplane keys that patchSystemplaneFetcherConfig
// modifies. The snapshot captures only keys that were actually present so the
// restore PATCH doesn't create absent keys with zero values (which would shadow
// the registry defaults — e.g. fetcher.url default is http://localhost:4006,
// not "").
var fetcherConfigKeys = []string{
	"fetcher.enabled",
	"fetcher.url",
	"fetcher.allow_private_ips",
}

// systemplaneNamespace is the namespace used by Matcher for all runtime
// config keys in the v5 systemplane admin API.
const systemplaneNamespace = "matcher"

// systemplaneListResponse matches the v5 admin GET /system/:namespace response.
type systemplaneListResponse struct {
	Namespace string `json:"namespace"`
	Entries   []struct {
		Key   string `json:"key"`
		Value any    `json:"value"`
	} `json:"entries"`
}

// systemplaneGetResponse matches the v5 admin GET /system/:namespace/:key response.
type systemplaneGetResponse struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     any    `json:"value"`
}

func doSystemplaneRequest(method, url string, body io.Reader, headers map[string]string) (*http.Response, []byte, error) {
	req, err := http.NewRequest(method, url, body) //nolint:noctx // test helper
	if err != nil {
		return nil, nil, fmt.Errorf("create systemplane request: %w", err)
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := systemplaneHTTPClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("execute systemplane request: %w", err)
	}

	bodyBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		resp.Body.Close() //nolint:errcheck // test helper
		return nil, nil, fmt.Errorf("read systemplane response body: %w", readErr)
	}

	return resp, bodyBytes, nil
}

// readSystemplaneKeyValue reads a single key from the v5 systemplane admin API.
// Returns (value, true, nil) if the key exists, (nil, false, nil) if 404.
func readSystemplaneKeyValue(appBaseURL, key string) (any, bool, error) {
	url := fmt.Sprintf("%s/system/%s/%s", appBaseURL, systemplaneNamespace, key)

	resp, err := systemplaneHTTPClient.Get(url) //nolint:noctx // test helper
	if err != nil {
		return nil, false, fmt.Errorf("get systemplane key %s: %w", key, err)
	}
	defer resp.Body.Close() //nolint:errcheck // test helper

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, false, fmt.Errorf("read systemplane key %s body: %w", key, readErr)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("systemplane key %s returned %d: %s", key, resp.StatusCode, string(body))
	}

	var entry systemplaneGetResponse
	if err := json.Unmarshal(body, &entry); err != nil {
		return nil, false, fmt.Errorf("parse systemplane key %s: %w", key, err)
	}

	return entry.Value, true, nil
}

// readFetcherSnapshot captures the current values of fetcher config keys.
// Only keys that are actually present in the systemplane are included;
// absent keys are omitted so the restore PUT won't create them.
func readFetcherSnapshot(appBaseURL string) (map[string]any, error) {
	snap := make(map[string]any, len(fetcherConfigKeys))

	for _, key := range fetcherConfigKeys {
		value, found, err := readSystemplaneKeyValue(appBaseURL, key)
		if err != nil {
			return nil, fmt.Errorf("snapshot key %s: %w", key, err)
		}

		if found {
			snap[key] = value
		}
	}

	return snap, nil
}

// putSystemplaneValues sets each key individually using the v5 admin
// PUT /system/:namespace/:key endpoint. No revision/If-Match required.
func putSystemplaneValues(appBaseURL string, values map[string]any) error {
	for key, value := range values {
		body, marshalErr := json.Marshal(map[string]any{"value": value})
		if marshalErr != nil {
			return fmt.Errorf("marshal systemplane value for %s: %w", key, marshalErr)
		}

		url := fmt.Sprintf("%s/system/%s/%s", appBaseURL, systemplaneNamespace, key)

		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body)) //nolint:noctx // test helper
		if err != nil {
			return fmt.Errorf("create put request for %s: %w", key, err)
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := systemplaneHTTPClient.Do(req)
		if err != nil {
			return fmt.Errorf("put systemplane key %s: %w", key, err)
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close() //nolint:errcheck // test helper

		// v5 returns 204 No Content on success.
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
			return fmt.Errorf("put systemplane key %s returned %d: %s", key, resp.StatusCode, string(respBody))
		}
	}

	return nil
}

// readSystemplaneSettings reads keys from the v5 systemplane namespace.
// In v5, settings are merged into config — scope and userID are no longer used
// but kept in the signature for compatibility with the skip-gated settings test.
func readSystemplaneSettings(appBaseURL, _ /* scope */, _ /* userID */ string) (*systemplaneListResponse, error) {
	url := fmt.Sprintf("%s/system/%s", appBaseURL, systemplaneNamespace)

	resp, err := systemplaneHTTPClient.Get(url) //nolint:noctx // test helper
	if err != nil {
		return nil, fmt.Errorf("get systemplane namespace: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // test helper

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read systemplane namespace body: %w", readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("systemplane namespace returned %d: %s", resp.StatusCode, string(body))
	}

	var current systemplaneListResponse
	if err := json.Unmarshal(body, &current); err != nil {
		return nil, fmt.Errorf("parse systemplane namespace: %w", err)
	}

	return &current, nil
}

// patchSystemplaneSettingValues sets each key via PUT in the v5 systemplane.
// scope and userID are no longer used in v5 but kept for signature compatibility.
func patchSystemplaneSettingValues(appBaseURL, _ /* scope */ string, values map[string]any, _ /* userID */ string) error {
	return putSystemplaneValues(appBaseURL, values)
}

// patchSystemplaneFetcherConfig updates the v5 systemplane runtime config to
// point the Fetcher client at the mock server.
func patchSystemplaneFetcherConfig(appBaseURL string, port int) (func() error, error) {
	snapshot, err := readFetcherSnapshot(appBaseURL)
	if err != nil {
		return nil, err
	}

	if err := putSystemplaneValues(appBaseURL, map[string]any{
		"fetcher.enabled":           true,
		"fetcher.url":               fmt.Sprintf("http://host.docker.internal:%d", port),
		"fetcher.allow_private_ips": true,
	}); err != nil {
		return nil, err
	}

	// Restore only keys that were originally present. Absent keys stay as-is
	// (the test value persists, but fetcher.enabled=false disables the client).
	return func() error {
		if len(snapshot) == 0 {
			return nil
		}

		return putSystemplaneValues(appBaseURL, snapshot)
	}, nil
}
