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
	ErrNoDialableIPs    = errors.New("no dialable IPs for host")
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
	// Defaults to true since Fetcher is an internal service.
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
		AllowPrivateIPs: true,
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

	return nil
}

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
				if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
					return nil, fmt.Errorf("%w: %s", ErrPrivateIPBlocked, ip.String())
				}
			}

			var lastDialErr error

			for _, ipAddr := range ips {
				targetAddr := net.JoinHostPort(ipAddr.IP.String(), port)

				conn, dialErr := dialer.DialContext(ctx, network, targetAddr)
				if dialErr == nil {
					return conn, nil
				}

				lastDialErr = dialErr
			}

			if lastDialErr != nil {
				return nil, fmt.Errorf("dial resolved host: %w", lastDialErr)
			}

			return nil, fmt.Errorf("dial resolved host %s: %w", host, ErrNoDialableIPs)
		},
	}
}
