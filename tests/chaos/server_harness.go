//go:build chaos

package chaos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/bootstrap"
	outboxServices "github.com/LerianStudio/matcher/internal/outbox/services"
)

// ChaosServer wraps a fully bootstrapped Matcher service whose infrastructure
// flows through Toxiproxy. It provides HTTP helpers for driving the service
// during chaos scenarios.
type ChaosServer struct {
	Service    *bootstrap.Service
	App        *fiber.App
	Dispatcher *outboxServices.Dispatcher
	Harness    *ChaosHarness
}

// BootChaosServer initializes a full Matcher service connected through the
// proxied infrastructure. Call this in tests that need the full HTTP API.
//
// The service is shut down automatically via t.Cleanup.
func BootChaosServer(t *testing.T, h *ChaosHarness) *ChaosServer {
	t.Helper()

	h.SetEnvForBootstrap(t)

	svc, err := bootstrap.InitServersWithOptions(&bootstrap.Options{
		Logger: &libLog.NopLogger{},
	})
	require.NoError(t, err, "bootstrap chaos service")

	cs := &ChaosServer{
		Service: svc,
		App:     svc.GetApp(),
		Harness: h,
	}

	// Extract outbox dispatcher for controlled event dispatch.
	if runner := svc.GetOutboxRunner(); runner != nil {
		if d, ok := runner.(*outboxServices.Dispatcher); ok {
			cs.Dispatcher = d
		}
	}

	t.Cleanup(func() {
		_ = cs.App.Shutdown()
	})

	return cs
}

// DispatchOutbox triggers a single outbox dispatch cycle.
// Returns the number of events processed.
func (cs *ChaosServer) DispatchOutbox(t *testing.T) int {
	t.Helper()

	if cs.Dispatcher == nil {
		t.Log("warning: outbox dispatcher not available")
		return 0
	}

	return cs.Dispatcher.DispatchOnce(cs.Harness.Ctx())
}

// DispatchOutboxUntilEmpty drains the outbox by dispatching repeatedly
// until no more events are processed or maxIterations is reached.
func (cs *ChaosServer) DispatchOutboxUntilEmpty(t *testing.T, maxIterations int) int {
	t.Helper()

	total := 0

	for range maxIterations {
		processed := cs.DispatchOutbox(t)
		if processed == 0 {
			break
		}

		total += processed
		time.Sleep(50 * time.Millisecond)
	}

	return total
}

// --------------------------------------------------------------------------
// HTTP request helpers
// --------------------------------------------------------------------------

// DoJSON sends a JSON request and returns the response.
func (cs *ChaosServer) DoJSON(
	t *testing.T, method, path string, payload any,
) (*http.Response, []byte) {
	t.Helper()

	var body io.Reader

	if payload != nil {
		data, err := json.Marshal(payload)
		require.NoError(t, err, "marshal JSON payload")

		body = bytes.NewReader(data)
	}

	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", cs.Harness.Seed.TenantID.String())

	if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
		req.Header.Set("X-Idempotency-Key", uuid.New().String())
	}

	resp, err := cs.App.Test(req, 30000)
	require.NoError(t, err, "%s %s: request failed", method, path)

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "%s %s: read response body", method, path)
	require.NoError(t, resp.Body.Close(), "%s %s: close response body", method, path)

	resp.Body = io.NopCloser(bytes.NewReader(respBody))

	return resp, respBody
}

// DoMultipart sends a multipart file upload request.
func (cs *ChaosServer) DoMultipart(
	t *testing.T, path string,
	fieldName, fileName string, fileContent []byte,
	formFields map[string]string,
) (*http.Response, []byte) {
	t.Helper()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile(fieldName, fileName)
	require.NoError(t, err, "create form file")
	_, err = part.Write(fileContent)
	require.NoError(t, err, "write file content")

	for k, v := range formFields {
		require.NoError(t, writer.WriteField(k, v), "write field %s", k)
	}

	require.NoError(t, writer.Close(), "close multipart writer")

	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-Idempotency-Key", uuid.New().String())
	req.Header.Set("X-Tenant-ID", cs.Harness.Seed.TenantID.String())

	resp, err := cs.App.Test(req, 30000)
	require.NoError(t, err, "multipart upload to %s", path)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read multipart response body")

	return resp, respBody
}

// DoGet is a convenience wrapper for GET requests.
func (cs *ChaosServer) DoGet(t *testing.T, path string) (*http.Response, []byte) {
	t.Helper()
	return cs.DoJSON(t, http.MethodGet, path, nil)
}

// --------------------------------------------------------------------------
// Business operation helpers
// --------------------------------------------------------------------------

// CreateFieldMap creates a field map for a source.
func (cs *ChaosServer) CreateFieldMap(t *testing.T, contextID, sourceID uuid.UUID) {
	t.Helper()

	path := fmt.Sprintf("/v1/config/contexts/%s/sources/%s/field-maps", contextID, sourceID)

	resp, body := cs.DoJSON(t, http.MethodPost, path, map[string]any{
		"mapping": map[string]string{
			"date":       "date",
			"amount":     "amount",
			"currency":   "currency",
			"externalId": "external_id",
		},
	})

	require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"create field map: expected 2xx, got %d: %s", resp.StatusCode, string(body))
}

// CreateMatchRule creates a basic exact-match rule for a context.
func (cs *ChaosServer) CreateMatchRule(t *testing.T, contextID uuid.UUID) {
	t.Helper()

	path := fmt.Sprintf("/v1/config/contexts/%s/rules", contextID)

	resp, body := cs.DoJSON(t, http.MethodPost, path, map[string]any{
		"priority": 1,
		"type":     "EXACT",
		"config": map[string]any{
			"matchAmount":   true,
			"matchCurrency": true,
			"matchDate":     true,
		},
	})

	require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"create match rule: expected 2xx, got %d: %s", resp.StatusCode, string(body))
}

// UploadCSV uploads a CSV file for ingestion.
func (cs *ChaosServer) UploadCSV(
	t *testing.T, contextID, sourceID uuid.UUID, csvContent string,
) (int, []byte) {
	t.Helper()

	path := fmt.Sprintf("/v1/imports/contexts/%s/sources/%s/upload", contextID, sourceID)

	resp, body := cs.DoMultipart(t, path,
		"file", "chaos-test.csv", []byte(csvContent),
		map[string]string{"format": "csv"},
	)

	return resp.StatusCode, body
}

// TriggerMatchRun triggers a match run for a context.
func (cs *ChaosServer) TriggerMatchRun(
	t *testing.T, contextID uuid.UUID, mode string,
) (int, []byte) {
	t.Helper()

	path := fmt.Sprintf("/v1/matching/contexts/%s/run", contextID)

	resp, body := cs.DoJSON(t, http.MethodPost, path, map[string]any{
		"mode": mode,
	})

	return resp.StatusCode, body
}

// BuildCSVContent generates a simple CSV for chaos testing.
func BuildCSVContent(rows int) string {
	csv := "external_id,date,amount,currency\n"

	for i := range rows {
		csv += fmt.Sprintf("CHAOS-%05d,2025-01-15,%d.00,USD\n", i, (i+1)*100)
	}

	return csv
}
