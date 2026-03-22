// Package fetcher provides the HTTP client adapter for the Fetcher service.
package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// maxResponseBodySize limits response body reads to prevent memory exhaustion.
const maxResponseBodySize = 10 * 1024 * 1024 // 10 MB

// Compile-time interface check.
var _ sharedPorts.FetcherClient = (*HTTPFetcherClient)(nil)

// Sentinel errors.
var (
	ErrFetcherUnreachable             = sharedPorts.ErrFetcherUnavailable
	ErrFetcherBadResponse             = errors.New("unexpected response from fetcher")
	ErrFetcherNotFound                = sharedPorts.ErrFetcherResourceNotFound
	ErrFetcherClientNil               = errors.New("fetcher client is not initialized")
	ErrFetcherJobIDEmpty              = errors.New("fetcher extraction response missing job id")
	ErrFetcherResultPathRequired      = errors.New("result path required")
	ErrFetcherResultPathNotAbsolute   = errors.New("result path must be absolute")
	ErrFetcherResultPathInvalidFormat = errors.New("result path must not include URL scheme, query, or fragment")
	ErrFetcherResultPathTraversal     = errors.New("result path must not contain traversal segments")
)

// HTTPFetcherClient communicates with the Fetcher REST API over HTTP.
type HTTPFetcherClient struct {
	httpClient *http.Client
	baseURL    string
	cfg        HTTPClientConfig
}

func (client *HTTPFetcherClient) ensureReady() error {
	if client == nil || client.httpClient == nil || strings.TrimSpace(client.baseURL) == "" {
		return ErrFetcherClientNil
	}

	return nil
}

// NewHTTPFetcherClient creates a new Fetcher HTTP client.
func NewHTTPFetcherClient(cfg HTTPClientConfig) (*HTTPFetcherClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid fetcher config: %w", err)
	}

	transport := cfg.buildTransport()

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   cfg.RequestTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return &HTTPFetcherClient{
		httpClient: httpClient,
		baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
		cfg:        cfg,
	}, nil
}

// IsHealthy checks if the Fetcher service is reachable and healthy.
func (client *HTTPFetcherClient) IsHealthy(ctx context.Context) bool {
	if err := client.ensureReady(); err != nil {
		return false
	}

	healthCtx, cancel := context.WithTimeout(ctx, client.cfg.HealthTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(healthCtx, http.MethodGet, client.baseURL+"/health", http.NoBody)
	if err != nil {
		return false
	}

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return false
	}

	body, err := func() ([]byte, error) {
		defer func() {
			_ = resp.Body.Close()
		}()

		return readBoundedBody(resp.Body)
	}()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	if err != nil {
		return false
	}

	if err := rejectEmptyOrNullBody(body); err != nil {
		return false
	}

	var health fetcherHealthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		return false
	}

	return strings.EqualFold(health.Status, "ok") || strings.EqualFold(health.Status, "healthy")
}

// ListConnections retrieves all database connections managed by Fetcher.
func (client *HTTPFetcherClient) ListConnections(ctx context.Context, orgID string) ([]*sharedPorts.FetcherConnection, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/api/v1/connections"
	if orgID != "" {
		reqURL += "?orgId=" + url.QueryEscape(orgID)
	}

	body, err := client.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}

	var listResp fetcherConnectionListResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("decode connections response: %w", err)
	}

	result := make([]*sharedPorts.FetcherConnection, 0, len(listResp.Connections))
	for _, conn := range listResp.Connections {
		result = append(result, &sharedPorts.FetcherConnection{
			ID:           conn.ID,
			ConfigName:   conn.ConfigName,
			DatabaseType: conn.DatabaseType,
			Host:         conn.Host,
			Port:         conn.Port,
			DatabaseName: conn.DatabaseName,
			ProductName:  conn.ProductName,
			Status:       conn.Status,
		})
	}

	return result, nil
}

// GetSchema retrieves the schema (tables and columns) for a specific connection.
func (client *HTTPFetcherClient) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/api/v1/connections/" + url.PathEscape(connectionID) + "/schema"

	body, err := client.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}

	var schemaResp fetcherSchemaResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &schemaResp); err != nil {
		return nil, fmt.Errorf("decode schema response: %w", err)
	}

	if err := validateFetcherResourceID("connection", connectionID, schemaResp.ConnectionID); err != nil {
		return nil, err
	}

	tables := make([]sharedPorts.FetcherTableSchema, 0, len(schemaResp.Tables))
	for _, table := range schemaResp.Tables {
		cols := make([]sharedPorts.FetcherColumnInfo, 0, len(table.Columns))
		for _, col := range table.Columns {
			cols = append(cols, sharedPorts.FetcherColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		}

		tables = append(tables, sharedPorts.FetcherTableSchema{
			TableName: table.TableName,
			Columns:   cols,
		})
	}

	return &sharedPorts.FetcherSchema{
		ConnectionID: schemaResp.ConnectionID,
		Tables:       tables,
		DiscoveredAt: time.Now().UTC(),
	}, nil
}

// TestConnection tests connectivity for a specific Fetcher connection.
func (client *HTTPFetcherClient) TestConnection(ctx context.Context, connectionID string) (*sharedPorts.FetcherTestResult, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/api/v1/connections/" + url.PathEscape(connectionID) + "/test"

	body, err := client.doPost(ctx, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("test connection: %w", err)
	}

	var testResp fetcherTestResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &testResp); err != nil {
		return nil, fmt.Errorf("decode test response: %w", err)
	}

	if err := validateFetcherResourceID("connection", connectionID, testResp.ConnectionID); err != nil {
		return nil, err
	}

	return &sharedPorts.FetcherTestResult{
		ConnectionID: testResp.ConnectionID,
		Healthy:      testResp.Healthy,
		LatencyMs:    testResp.LatencyMs,
		ErrorMessage: testResp.ErrorMessage,
	}, nil
}

// SubmitExtractionJob submits an async data extraction job to Fetcher.
// Returns the Fetcher-assigned job ID.
func (client *HTTPFetcherClient) SubmitExtractionJob(ctx context.Context, input sharedPorts.ExtractionJobInput) (string, error) {
	if err := client.ensureReady(); err != nil {
		return "", err
	}

	reqBody := fetcherExtractionSubmitRequest{
		ConnectionID: input.ConnectionID,
		Tables:       make(map[string]fetcherExtractionTable),
		Filters:      input.Filters,
	}

	for name, tbl := range input.Tables {
		reqBody.Tables[name] = fetcherExtractionTable{
			Columns:   tbl.Columns,
			StartDate: tbl.StartDate,
			EndDate:   tbl.EndDate,
		}
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal extraction request: %w", err)
	}

	body, err := client.doPost(ctx, client.baseURL+"/api/v1/extractions", jsonBody)
	if err != nil {
		return "", fmt.Errorf("submit extraction: %w", err)
	}

	var resp fetcherExtractionSubmitResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return "", err
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("decode extraction response: %w", err)
	}

	if strings.TrimSpace(resp.JobID) == "" {
		return "", fmt.Errorf("%w: %w", ErrFetcherBadResponse, ErrFetcherJobIDEmpty)
	}

	return resp.JobID, nil
}

// GetExtractionJobStatus polls the status of a running extraction job.
func (client *HTTPFetcherClient) GetExtractionJobStatus(ctx context.Context, jobID string) (*sharedPorts.ExtractionJobStatus, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/api/v1/extractions/" + url.PathEscape(jobID)

	body, err := client.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("get extraction status: %w", err)
	}

	var resp fetcherExtractionStatusResponse

	if err := rejectEmptyOrNullBody(body); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode extraction status: %w", err)
	}

	if err := validateFetcherResourceID("job", jobID, resp.JobID); err != nil {
		return nil, err
	}

	normalizedStatus, err := normalizeExtractionStatus(resp)
	if err != nil {
		return nil, err
	}

	return &sharedPorts.ExtractionJobStatus{
		JobID:        resp.JobID,
		Status:       normalizedStatus,
		Progress:     resp.Progress,
		ResultPath:   resp.ResultPath,
		ErrorMessage: resp.ErrorMessage,
	}, nil
}

func validateFetcherResourceID(resource, expected, actual string) error {
	trimmedExpected := strings.TrimSpace(expected)
	trimmedActual := strings.TrimSpace(actual)

	if trimmedExpected == "" || trimmedActual == "" {
		return fmt.Errorf("%w: %s id is required", ErrFetcherBadResponse, resource)
	}

	if trimmedExpected != trimmedActual {
		return fmt.Errorf("%w: %s id mismatch (expected %q, got %q)", ErrFetcherBadResponse, resource, trimmedExpected, trimmedActual)
	}

	return nil
}

func normalizeExtractionStatus(resp fetcherExtractionStatusResponse) (string, error) {
	normalizedStatus := strings.ToUpper(strings.TrimSpace(resp.Status))
	if normalizedStatus == "CANCELED" {
		normalizedStatus = "CANCELLED"
	}

	switch normalizedStatus {
	case "PENDING", "SUBMITTED", "RUNNING", "EXTRACTING":
		return normalizedStatus, nil
	case "COMPLETE":
		if strings.TrimSpace(resp.ResultPath) == "" {
			return "", fmt.Errorf("%w: complete extraction missing result path", ErrFetcherBadResponse)
		}

		if err := validateFetcherResultPath(resp.ResultPath); err != nil {
			return "", fmt.Errorf("%w: %w", ErrFetcherBadResponse, err)
		}

		return normalizedStatus, nil
	case "FAILED":
		if strings.TrimSpace(resp.ErrorMessage) == "" {
			return "", fmt.Errorf("%w: failed extraction missing error message", ErrFetcherBadResponse)
		}

		return normalizedStatus, nil
	case "CANCELLED":
		return normalizedStatus, nil
	default:
		return "", fmt.Errorf("%w: unknown extraction status %q", ErrFetcherBadResponse, resp.Status)
	}
}

func validateFetcherResultPath(resultPath string) error {
	trimmed := strings.TrimSpace(resultPath)

	if trimmed == "" {
		return ErrFetcherResultPathRequired
	}

	if !strings.HasPrefix(trimmed, "/") {
		return ErrFetcherResultPathNotAbsolute
	}

	if strings.Contains(trimmed, "://") || strings.ContainsAny(trimmed, "?#") {
		return ErrFetcherResultPathInvalidFormat
	}

	cleaned := path.Clean(trimmed)
	if cleaned != trimmed || strings.Contains(trimmed, "..") {
		return ErrFetcherResultPathTraversal
	}

	return nil
}

// doGet performs a GET request with retry logic.
func (client *HTTPFetcherClient) doGet(ctx context.Context, requestURL string) ([]byte, error) {
	return client.doRequest(ctx, http.MethodGet, requestURL, nil, true)
}

// doPost performs a POST request with retry logic.
func (client *HTTPFetcherClient) doPost(ctx context.Context, requestURL string, body []byte) ([]byte, error) {
	return client.doRequest(ctx, http.MethodPost, requestURL, body, false)
}

func readBoundedBody(body io.Reader) ([]byte, error) {
	limitedReader := io.LimitReader(body, int64(maxResponseBodySize)+1)

	respBody, readErr := io.ReadAll(limitedReader)
	if readErr != nil {
		return nil, fmt.Errorf("read response body: %w", readErr)
	}

	if int64(len(respBody)) > int64(maxResponseBodySize) {
		return nil, fmt.Errorf("%w: response body exceeds %d bytes", ErrFetcherBadResponse, maxResponseBodySize)
	}

	return respBody, nil
}

func rejectEmptyOrNullBody(body []byte) error {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || trimmed == "null" {
		return fmt.Errorf("%w: null/empty payload", ErrFetcherBadResponse)
	}

	return nil
}

// maxBackoffDelay caps the exponential backoff to prevent indefinite waits
// when MaxRetries is set to a high value.
const maxBackoffDelay = 30 * time.Second

// maxRetryShift caps the bit-shift exponent to prevent integer overflow
// in the exponential backoff calculation (1 << shift).
const maxRetryShift = 30

// doRequest performs an HTTP request with retry and exponential backoff.
//
//nolint:gocognit,gocyclo,cyclop // retry loop with error classification is inherently branchy; extraction done via classifyResponse.
func (client *HTTPFetcherClient) doRequest(ctx context.Context, method, requestURL string, body []byte, retryable bool) ([]byte, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	trackingLogger, tracer, trackingHeaderID, trackingMetricsFactory := libCommons.NewTrackingFromContext(ctx)
	_ = trackingLogger
	_ = trackingHeaderID
	_ = trackingMetricsFactory

	ctx, span := tracer.Start(ctx, "fetcher.http.request")
	defer span.End()

	span.SetAttributes(
		attribute.String("http.method", method),
		attribute.String("http.url", requestURL),
		attribute.Bool("fetcher.retryable", retryable),
	)

	var lastErr error

	attempts := 1
	if retryable {
		attempts += client.cfg.MaxRetries
	}

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			shift := attempt - 1
			if shift > maxRetryShift {
				shift = maxRetryShift // cap shift to prevent integer overflow
			}

			delay := client.cfg.RetryBaseDelay * time.Duration(1<<uint(shift))
			if delay > maxBackoffDelay {
				delay = maxBackoffDelay
			}

			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("request canceled: %w", ctx.Err())
			case <-time.After(delay):
			}
		}

		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}

		req, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := client.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%w: %v", ErrFetcherUnreachable, err) //nolint:errorlint // wrapping sentinel with context detail
			libOpentelemetry.HandleSpanError(span, "fetcher http request failed", lastErr)

			if !retryable {
				return nil, lastErr
			}

			continue
		}

		respBody, bodyErr := func() ([]byte, error) {
			defer func() {
				_ = resp.Body.Close()
			}()

			return readBoundedBody(resp.Body)
		}()
		if bodyErr != nil {
			lastErr = bodyErr
			if !retryable {
				return nil, lastErr
			}

			continue
		}

		result, statusErr := classifyResponse(resp.StatusCode, respBody)
		if statusErr == nil {
			span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

			return result, nil
		}

		libOpentelemetry.HandleSpanError(span, "fetcher classify response", statusErr)

		if resp.StatusCode >= http.StatusInternalServerError && retryable {
			lastErr = statusErr

			continue // retry on 5xx
		}

		return nil, statusErr
	}

	if retryable {
		return nil, fmt.Errorf("exhausted retries: %w", lastErr)
	}

	return nil, lastErr
}

// classifyResponse maps HTTP status codes to domain errors or returns the body on success.
func classifyResponse(statusCode int, body []byte) ([]byte, error) {
	if statusCode == http.StatusNotFound {
		return nil, ErrFetcherNotFound
	}

	if statusCode >= http.StatusMultipleChoices && statusCode < http.StatusBadRequest {
		return nil, fmt.Errorf("%w: redirects are not allowed (status %d)", ErrFetcherBadResponse, statusCode)
	}

	if statusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("%w: status %d", ErrFetcherBadResponse, statusCode)
	}

	return body, nil
}
