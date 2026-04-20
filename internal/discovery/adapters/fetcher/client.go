// Package fetcher provides the HTTP client adapter for the Fetcher service.
package fetcher

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"

	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// maxResponseBodySize limits response body reads to prevent memory exhaustion.
const maxResponseBodySize = 10 * 1024 * 1024 // 10 MB

// listConnectionsPageSize is the number of connections requested per page.
// Fetcher allows up to 1000; we use 100 to balance between fewer round-trips
// and reasonable per-response sizes for typical deployments.
const listConnectionsPageSize = 100

// maxPaginationPages is a defensive upper bound on the number of pages we'll
// fetch before aborting. At 100 items per page this covers up to 100k connections,
// far beyond any realistic deployment. Prevents infinite loops if Fetcher returns
// inconsistent total values.
const maxPaginationPages = 1000

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
	ErrFetcherPaginationOverflow      = errors.New("fetcher connections pagination exceeded safety limit")
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
// A nil provider is ignored (matching WithM2MProvider's nil-guard behavior) so a
// misconfigured caller cannot silently disable auth on an already-wired client.
func (client *HTTPFetcherClient) SetM2MProvider(p sharedPorts.M2MProvider) {
	if client != nil && p != nil {
		client.m2mProvider = p
	}
}

// SetTokenExchanger sets the OAuth2 token exchanger for Bearer authentication.
// This is safe to call after construction to wire in the exchanger from bootstrap.
// A nil exchanger is ignored (matching WithTokenExchanger's nil-guard behavior).
func (client *HTTPFetcherClient) SetTokenExchanger(te discoveryPorts.TokenExchanger) {
	if client != nil && te != nil {
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
