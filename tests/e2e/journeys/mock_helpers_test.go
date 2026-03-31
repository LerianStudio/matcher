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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("systemplane config returned %d: %s", resp.StatusCode, string(body))
	}

	var current systemplaneConfigResponse
	if err := json.Unmarshal(body, &current); err != nil {
		return nil, fmt.Errorf("parse systemplane config: %w", err)
	}

	return &current, nil
}

// readFetcherSnapshot captures the current values of fetcher config keys.
// Only keys that are actually present in the systemplane response are included;
// absent keys are omitted so the restore PATCH won't create them.
func readFetcherSnapshot(appBaseURL string) (map[string]any, error) {
	current, err := readSystemplaneConfig(appBaseURL)
	if err != nil {
		return nil, err
	}

	snap := make(map[string]any, len(fetcherConfigKeys))
	for _, key := range fetcherConfigKeys {
		if entry, ok := current.Values[key]; ok {
			snap[key] = entry.Value
		}
	}

	return snap, nil
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

	// Restore only keys that were originally present. Absent keys stay as-is
	// (the test value persists, but fetcher.enabled=false disables the client).
	return func() error {
		if len(snapshot) == 0 {
			return nil
		}
		return patchSystemplaneConfigValues(appBaseURL, snapshot)
	}, nil
}
