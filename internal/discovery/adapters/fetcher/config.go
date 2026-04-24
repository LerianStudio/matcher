// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package fetcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Sentinel errors for config validation.
var (
	ErrEmptyURL         = errors.New("fetcher URL is required")
	ErrInvalidURL       = errors.New("fetcher URL is invalid")
	ErrInvalidScheme    = errors.New("fetcher URL must use http or https scheme")
	ErrMissingHost      = errors.New("fetcher URL host is required")
	ErrPrivateIPBlocked = errors.New("connections to private IPs are not allowed")
	ErrNoIPsFound       = errors.New("no IPs found for host")
)

// Default client configuration constants.
const (
	defaultHealthTimeout   = 5 * time.Second
	defaultRequestTimeout  = 30 * time.Second
	defaultMaxRetries      = 3
	defaultRetryBaseDelay  = 500 * time.Millisecond
	defaultMaxIdleConns    = 10
	defaultIdleConnTimeout = 90 * time.Second
)

// HTTPClientConfig configures the Fetcher HTTP client.
type HTTPClientConfig struct {
	// BaseURL is the Fetcher Manager API base URL (e.g., http://localhost:4006).
	BaseURL string

	// HealthTimeout is the timeout for health check requests.
	HealthTimeout time.Duration

	// RequestTimeout is the timeout for general API requests.
	RequestTimeout time.Duration

	// MaxRetries is the number of retry attempts for transient failures.
	MaxRetries int

	// RetryBaseDelay is the base delay for exponential backoff.
	RetryBaseDelay time.Duration

	// AllowPrivateIPs allows connections to private IP ranges.
	// Defaults to false; internal deployments must opt in explicitly.
	AllowPrivateIPs bool
}

// DefaultConfig returns a config with sensible defaults for local development.
func DefaultConfig() HTTPClientConfig {
	return HTTPClientConfig{
		BaseURL:         "http://localhost:4006",
		HealthTimeout:   defaultHealthTimeout,
		RequestTimeout:  defaultRequestTimeout,
		MaxRetries:      defaultMaxRetries,
		RetryBaseDelay:  defaultRetryBaseDelay,
		AllowPrivateIPs: false,
	}
}

// Validate checks config fields.
func (cfg HTTPClientConfig) Validate() error {
	if cfg.BaseURL == "" {
		return ErrEmptyURL
	}

	parsed, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidURL, err.Error())
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%w: got %s", ErrInvalidScheme, parsed.Scheme)
	}

	if strings.TrimSpace(parsed.Hostname()) == "" {
		return ErrMissingHost
	}

	if parsed.User != nil {
		return fmt.Errorf("%w: credentials in URL are not allowed", ErrInvalidURL)
	}

	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%w: query strings and fragments are not allowed", ErrInvalidURL)
	}

	if cfg.HealthTimeout <= 0 {
		return fmt.Errorf("%w: health timeout must be positive", ErrInvalidURL)
	}

	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("%w: request timeout must be positive", ErrInvalidURL)
	}

	if cfg.MaxRetries < 0 {
		return fmt.Errorf("%w: max retries must be non-negative", ErrInvalidURL)
	}

	if cfg.RetryBaseDelay < 0 {
		return fmt.Errorf("%w: retry base delay must be non-negative", ErrInvalidURL)
	}

	return nil
}

// BuildArtifactTransport creates an http.Transport with SSRF protection for
// artifact download clients. It reuses the same DialContext guard as the main
// fetcher HTTP client but tunes connection-pooling for bursty concurrent
// artifact downloads under T-003 bridge worker load:
//   - MaxIdleConnsPerHost is raised to 10 so a burst of concurrent workers
//     hitting Fetcher does not starve the default-2 idle pool.
//   - TLSHandshakeTimeout is explicitly bounded so stuck TLS handshakes fail
//     inside the per-request deadline instead of the client-level timeout.
//   - ResponseHeaderTimeout bounds the wait for the first byte from Fetcher
//     separately from the total-request timeout, so slow server-side handles
//     surface as a distinct failure class.
//
// Exported (not method-private) because the bootstrap artifact HTTP client in
// init_fetcher_bridge.go must share this guard without importing internal
// method scope.
func BuildArtifactTransport(cfg HTTPClientConfig) *http.Transport {
	t := cfg.buildTransport()
	t.MaxIdleConnsPerHost = artifactMaxIdleConnsPerHost
	t.TLSHandshakeTimeout = artifactTLSHandshakeTimeout
	t.ResponseHeaderTimeout = artifactResponseHeaderTimeout

	return t
}

const (
	artifactMaxIdleConnsPerHost   = 10
	artifactTLSHandshakeTimeout   = 10 * time.Second
	artifactResponseHeaderTimeout = 30 * time.Second
)

// buildTransport creates an http.Transport with SSRF protection.
func (cfg HTTPClientConfig) buildTransport() *http.Transport {
	dialer := &net.Dialer{Timeout: cfg.RequestTimeout}

	return &http.Transport{
		MaxIdleConns:       defaultMaxIdleConns,
		IdleConnTimeout:    defaultIdleConnTimeout,
		DisableCompression: false,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, fmt.Errorf("invalid address: %w", err)
			}

			if cfg.AllowPrivateIPs {
				return dialer.DialContext(ctx, network, addr)
			}

			ips, resolveErr := net.DefaultResolver.LookupIPAddr(ctx, host)
			if resolveErr != nil {
				return nil, fmt.Errorf("resolve host: %w", resolveErr)
			}

			if len(ips) == 0 {
				return nil, fmt.Errorf("resolve host %s: %w", host, ErrNoIPsFound)
			}

			for _, ipAddr := range ips {
				ip := ipAddr.IP
				if isBlockedSSRFTarget(ip) {
					return nil, fmt.Errorf("%w: %s", ErrPrivateIPBlocked, ip.String())
				}
			}

			lastDialErr := ErrNoIPsFound

			for _, ipAddr := range ips {
				targetAddr := net.JoinHostPort(ipAddr.IP.String(), port)

				conn, dialErr := dialer.DialContext(ctx, network, targetAddr)
				if dialErr == nil {
					return conn, nil
				}

				lastDialErr = dialErr
			}

			return nil, fmt.Errorf("dial resolved host: %w", lastDialErr)
		},
	}
}

// cgnatIPv4Net is the RFC 6598 shared address space (100.64.0.0/10).
// The stdlib's ip.IsPrivate() covers RFC 1918 but not CGNAT, so we
// maintain an explicit net for it. Parsed once at package load; the
// literal is a compile-time constant so parsing cannot fail at runtime.
var cgnatIPv4Net = mustParseCIDR("100.64.0.0/10")

// mustParseCIDR parses a hardcoded CIDR and panics on failure. Only
// used at package init with compile-time-constant inputs, so the panic
// branch is unreachable for any value the function actually ships with.
// Matches the init-time parse pattern used in
// internal/exception/adapters/http/connectors/config.go.
func mustParseCIDR(cidr string) *net.IPNet {
	_, n, err := net.ParseCIDR(cidr)
	if err != nil {
		panic("fetcher: failed to parse hardcoded CIDR " + cidr + ": " + err.Error()) //nolint:forbidigo // init-time only; hardcoded CIDRs cannot fail
	}

	return n
}

// isBlockedSSRFTarget reports whether an IP address should be rejected
// by the SSRF guard. Covers:
//   - RFC 1918 private (10/8, 172.16/12, 192.168/16) and ULA fc00::/7 via IsPrivate
//   - Loopback (127/8, ::1/128)
//   - Link-local unicast (169.254/16, fe80::/10) and link-local multicast
//   - Unspecified (0.0.0.0, ::) — some kernels treat as localhost
//   - Multicast (224/4, ff00::/8) — never a valid external-service target
//   - CGNAT shared space (100.64/10) — routable to internal metadata in
//     several cloud providers but NOT IsPrivate per stdlib
//
// RFC 5737 TEST-NET ranges are intentionally NOT blocked here so
// integration tests that resolve them in controlled environments still
// work.
func isBlockedSSRFTarget(ip net.IP) bool {
	if ip.IsPrivate() ||
		ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast() {
		return true
	}

	if v4 := ip.To4(); v4 != nil && cgnatIPv4Net.Contains(v4) {
		return true
	}

	return false
}
