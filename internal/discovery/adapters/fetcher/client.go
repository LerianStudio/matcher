// Package fetcher provides the HTTP client adapter for the Fetcher service.
package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"

	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
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
	ErrFetcherCircuitOpen             = errors.New("fetcher service circuit breaker is open")
	ErrFetcherServerError             = errors.New("fetcher returned server error")
	ErrFetcherNilCredentials          = errors.New("M2M provider returned nil credentials without error")
)

// fetcherCircuitBreakerName is the service name used for the fetcher circuit breaker.
// All requests to the fetcher share a single breaker because the fetcher service is
// a single upstream identified by its base URL.
const fetcherCircuitBreakerName = "fetcher"

// HTTPFetcherClient communicates with the Fetcher REST API over HTTP.
type HTTPFetcherClient struct {
	httpClient     *http.Client
	baseURL        string
	cfg            HTTPClientConfig
	breaker        circuitbreaker.Manager
	m2mProvider    sharedPorts.M2MProvider       // nil in single-tenant mode
	tokenExchanger discoveryPorts.TokenExchanger // nil in single-tenant mode
}

// SetM2MProvider sets the M2M credential provider for multi-tenant authentication.
// This is safe to call after construction to wire in the provider from bootstrap.
func (client *HTTPFetcherClient) SetM2MProvider(p sharedPorts.M2MProvider) {
	if client != nil {
		client.m2mProvider = p
	}
}

// SetTokenExchanger sets the OAuth2 token exchanger for Bearer authentication.
// This is safe to call after construction to wire in the exchanger from bootstrap.
func (client *HTTPFetcherClient) SetTokenExchanger(te discoveryPorts.TokenExchanger) {
	if client != nil {
		client.tokenExchanger = te
	}
}

func (client *HTTPFetcherClient) ensureReady() error {
	if client == nil || client.httpClient == nil || strings.TrimSpace(client.baseURL) == "" {
		return ErrFetcherClientNil
	}

	return nil
}

// ClientOption configures optional HTTPFetcherClient behavior.
type ClientOption func(*HTTPFetcherClient)

// WithM2MProvider sets the M2M credential provider for multi-tenant authentication.
// When set, the client injects per-tenant BasicAuth credentials into outbound requests.
// When nil (single-tenant mode), no authentication is injected.
func WithM2MProvider(p sharedPorts.M2MProvider) ClientOption {
	return func(c *HTTPFetcherClient) {
		if p != nil {
			c.m2mProvider = p
		}
	}
}

// WithTokenExchanger sets the OAuth2 token exchanger for Bearer authentication.
// When set alongside an M2MProvider, the client exchanges M2M credentials for
// Bearer tokens instead of using BasicAuth.
func WithTokenExchanger(te discoveryPorts.TokenExchanger) ClientOption {
	return func(c *HTTPFetcherClient) {
		if te != nil {
			c.tokenExchanger = te
		}
	}
}

// NewHTTPFetcherClient creates a new Fetcher HTTP client.
// The optional circuitbreaker.Manager protects outbound calls; when nil the client
// operates without circuit-breaker protection (backward-compatible).
func NewHTTPFetcherClient(cfg HTTPClientConfig, breaker ...circuitbreaker.Manager) (*HTTPFetcherClient, error) {
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

	client := &HTTPFetcherClient{
		httpClient: httpClient,
		baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
		cfg:        cfg,
	}

	if len(breaker) > 0 && breaker[0] != nil {
		// Typed-nil guard: the variadic parameter breaker[0] can be a typed-nil interface
		// value (e.g., (*concreteManager)(nil) assigned to circuitbreaker.Manager) that
		// passes the `!= nil` check above. reflect.ValueOf().IsNil() catches this case.
		// A cleaner alternative would be a functional option WithCircuitBreaker(cb) that
		// avoids the variadic-nil corner case; see https://go.dev/doc/faq#nil_error for
		// background on typed nils.
		if rv := reflect.ValueOf(breaker[0]); rv.IsValid() && !rv.IsNil() {
			client.breaker = breaker[0]

			if _, err := client.breaker.GetOrCreate(fetcherCircuitBreakerName, circuitbreaker.HTTPServiceConfig()); err != nil {
				return nil, fmt.Errorf("register fetcher circuit breaker: %w", err)
			}
		}
	}

	return client, nil
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

	resp, err := client.httpClient.Do(req) // #nosec G704 -- URL comes from validated fetcher config, not user input
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
// productName is sent as X-Product-Name header to scope the listing.
func (client *HTTPFetcherClient) ListConnections(ctx context.Context, productName string) ([]*sharedPorts.FetcherConnection, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/v1/management/connections"

	var extraHeaders map[string]string
	if productName != "" {
		extraHeaders = map[string]string{"X-Product-Name": productName}
	}

	body, err := client.doGetWithHeaders(ctx, reqURL, extraHeaders)
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

	result := make([]*sharedPorts.FetcherConnection, 0, len(listResp.Items))
	for _, conn := range listResp.Items {
		result = append(result, &sharedPorts.FetcherConnection{
			ID:           conn.ID,
			ConfigName:   conn.ConfigName,
			DatabaseType: conn.Type,
			Host:         conn.Host,
			Port:         conn.Port,
			Schema:       conn.Schema,
			DatabaseName: conn.DatabaseName,
			UserName:     conn.UserName,
			ProductName:  conn.ProductName,
			Metadata:     conn.Metadata,
			CreatedAt:    parseOptionalRFC3339(conn.CreatedAt),
			UpdatedAt:    parseOptionalRFC3339(conn.UpdatedAt),
		})
	}

	return result, nil
}

// GetSchema retrieves the schema (tables and columns) for a specific connection.
func (client *HTTPFetcherClient) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/v1/management/connections/" + url.PathEscape(connectionID) + "/schema"

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

	if err := validateFetcherResourceID("connection", connectionID, schemaResp.ID); err != nil {
		return nil, err
	}

	tables := make([]sharedPorts.FetcherTableSchema, 0, len(schemaResp.Tables))
	for _, table := range schemaResp.Tables {
		tables = append(tables, sharedPorts.FetcherTableSchema{
			Name:   table.Name,
			Fields: table.Fields,
		})
	}

	return &sharedPorts.FetcherSchema{
		ID:           schemaResp.ID,
		ConfigName:   schemaResp.ConfigName,
		DatabaseName: schemaResp.DatabaseName,
		Type:         schemaResp.Type,
		Tables:       tables,
		DiscoveredAt: time.Now().UTC(),
	}, nil
}

// TestConnection tests connectivity for a specific Fetcher connection.
func (client *HTTPFetcherClient) TestConnection(ctx context.Context, connectionID string) (*sharedPorts.FetcherTestResult, error) {
	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	reqURL := client.baseURL + "/v1/management/connections/" + url.PathEscape(connectionID) + "/test"

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

	return &sharedPorts.FetcherTestResult{
		Status:    testResp.Status,
		Message:   testResp.Message,
		LatencyMs: testResp.LatencyMs,
	}, nil
}

// SubmitExtractionJob submits an async data extraction job to Fetcher.
// Returns the Fetcher-assigned job ID.
func (client *HTTPFetcherClient) SubmitExtractionJob(ctx context.Context, input sharedPorts.ExtractionJobInput) (string, error) {
	if err := client.ensureReady(); err != nil {
		return "", err
	}

	reqBody := fetcherExtractionSubmitRequest{
		DataRequest: fetcherDataRequest{
			MappedFields: input.MappedFields,
			Filters:      input.Filters,
		},
		Metadata: input.Metadata,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal extraction request: %w", err)
	}

	body, err := client.doPost(ctx, client.baseURL+"/v1/fetcher", jsonBody)
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

	reqURL := client.baseURL + "/v1/fetcher/" + url.PathEscape(jobID)

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

	if err := validateFetcherResourceID("job", jobID, resp.ID); err != nil {
		return nil, err
	}

	normalizedStatus, err := normalizeExtractionStatus(resp)
	if err != nil {
		return nil, err
	}

	return &sharedPorts.ExtractionJobStatus{
		ID:          resp.ID,
		Status:      normalizedStatus,
		ResultPath:  resp.ResultPath,
		ResultHmac:  resp.ResultHmac,
		RequestHash: resp.RequestHash,
		Metadata:    resp.Metadata,
		CreatedAt:   parseOptionalRFC3339(resp.CreatedAt),
		CompletedAt: parseOptionalRFC3339Ptr(resp.CompletedAt),
	}, nil
}

// parseOptionalRFC3339 parses an RFC3339 timestamp string, returning zero time on failure.
func parseOptionalRFC3339(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}

	return t
}

// parseOptionalRFC3339Ptr parses an RFC3339 timestamp string, returning nil on empty/failure.
func parseOptionalRFC3339Ptr(raw string) *time.Time {
	if raw == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil
	}

	return &t
}
