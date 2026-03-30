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

type systemplaneFetcherSnapshot struct {
	Enabled         bool
	URL             string
	AllowPrivateIPs bool
}

type systemplaneConfigResponse struct {
	Revision int `json:"revision"`
	Values   map[string]struct {
		Value any `json:"value"`
	} `json:"values"`
}

func readSystemplaneConfig(appBaseURL string) (*systemplaneConfigResponse, error) {
	resp, err := systemplaneHTTPClient.Get(appBaseURL + "/v1/system/configs") //nolint:noctx // test helper
	if err != nil {
		return nil, fmt.Errorf("get systemplane config: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // test helper

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read systemplane config body: %w", readErr)
	}

	var current systemplaneConfigResponse
	if err := json.Unmarshal(body, &current); err != nil {
		return nil, fmt.Errorf("parse systemplane config: %w", err)
	}

	return &current, nil
}

func readFetcherSnapshot(appBaseURL string) (*systemplaneFetcherSnapshot, error) {
	current, err := readSystemplaneConfig(appBaseURL)
	if err != nil {
		return nil, err
	}

	return &systemplaneFetcherSnapshot{
		Enabled:         lookupBoolConfigValue(current.Values, "fetcher.enabled"),
		URL:             lookupStringConfigValue(current.Values, "fetcher.url"),
		AllowPrivateIPs: lookupBoolConfigValue(current.Values, "fetcher.allow_private_ips"),
	}, nil
}

func lookupBoolConfigValue(values map[string]struct {
	Value any `json:"value"`
}, key string,
) bool {
	entry, ok := values[key]
	if !ok {
		return false
	}

	value, ok := entry.Value.(bool)
	if !ok {
		return false
	}

	return value
}

func lookupStringConfigValue(values map[string]struct {
	Value any `json:"value"`
}, key string,
) string {
	entry, ok := values[key]
	if !ok {
		return ""
	}

	value, ok := entry.Value.(string)
	if !ok {
		return ""
	}

	return value
}

func patchSystemplaneConfigValues(appBaseURL string, values map[string]any) error {
	current, err := readSystemplaneConfig(appBaseURL)
	if err != nil {
		return err
	}

	patch := map[string]any{"values": values}
	patchBody, marshalErr := json.Marshal(patch)
	if marshalErr != nil {
		return fmt.Errorf("marshal systemplane patch: %w", marshalErr)
	}

	req, err := http.NewRequest(http.MethodPatch, appBaseURL+"/v1/system/configs", bytes.NewReader(patchBody)) //nolint:noctx // test helper
	if err != nil {
		return fmt.Errorf("create patch request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", fmt.Sprintf("%d", current.Revision))

	patchResp, err := systemplaneHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("patch systemplane: %w", err)
	}
	defer patchResp.Body.Close() //nolint:errcheck // test helper

	if patchResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(patchResp.Body)
		return fmt.Errorf("patch systemplane returned %d: %s", patchResp.StatusCode, string(respBody))
	}

	return nil
}

// patchSystemplaneFetcherConfig updates the systemplane runtime config to point
// the Fetcher client at the mock server. The systemplane uses optimistic
// concurrency via the If-Match header (current revision).
func patchSystemplaneFetcherConfig(appBaseURL string, port int) (func() error, error) {
	snapshot, err := readFetcherSnapshot(appBaseURL)
	if err != nil {
		return nil, err
	}

	if err := patchSystemplaneConfigValues(appBaseURL, map[string]any{
		"fetcher.enabled":           true,
		"fetcher.url":               fmt.Sprintf("http://host.docker.internal:%d", port),
		"fetcher.allow_private_ips": true,
	}); err != nil {
		return nil, err
	}

	return func() error {
		return patchSystemplaneConfigValues(appBaseURL, map[string]any{
			"fetcher.enabled":           snapshot.Enabled,
			"fetcher.url":               snapshot.URL,
			"fetcher.allow_private_ips": snapshot.AllowPrivateIPs,
		})
	}, nil
}
