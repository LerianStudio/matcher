package connectors

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

// privateIPNets contains pre-parsed CIDR ranges for private IP detection.
// Parsed at init time for performance.
var privateIPNets []*net.IPNet

func init() {
	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"169.254.0.0/16",
		"127.0.0.0/8",
		"::1/128",
		"fe80::/10",
		"fc00::/7",
	}

	privateIPNets = make([]*net.IPNet, 0, len(privateRanges))

	for _, cidr := range privateRanges {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			panic("connectors: failed to parse hardcoded CIDR " + cidr + ": " + err.Error()) //nolint:forbidigo // init-time only; hardcoded CIDRs cannot fail
		}

		privateIPNets = append(privateIPNets, ipNet)
	}
}

// Default configuration values.
//
// The global default timeout is now configurable via WEBHOOK_TIMEOUT_SEC env var.
//
// DESIGN NOTE: Per-webhook timeout configuration.
// To support per-webhook timeout overrides via the admin API, the following changes
// would be needed:
//
//  1. Add a `TimeoutSec *int` field to the webhook entity (nullable, so it falls back
//     to the global default when not set).
//  2. Create an admin API endpoint (e.g., PATCH /api/v1/admin/webhooks/{id}/settings)
//     that allows updating per-webhook settings including timeout.
//  3. Update the dispatcher to consult the per-webhook TimeoutSec when building the
//     HTTP client, falling back to ConnectorConfig.BaseConnectorConfig.TimeoutOrDefault()
//     when the webhook does not specify a custom timeout.
//  4. Add validation: per-webhook timeout must be between 1 and 300 seconds (the global
//     max enforced by Config.WebhookTimeout()).
const (
	DefaultTimeout      = 30 * time.Second
	DefaultMaxRetries   = 3
	DefaultRetryBackoff = 1 * time.Second

	dnsLookupTimeout = 5 * time.Second
)

// Configuration errors.
var (
	ErrInvalidConnectorConfig = errors.New("invalid connector configuration")
	ErrMissingBaseURL         = errors.New("base URL is required")
	ErrInvalidBaseURL         = errors.New("base URL is invalid")
	ErrMissingAuthToken       = errors.New("auth token is required")
	ErrPrivateIPNotAllowed    = errors.New("private IP addresses are not allowed")
	ErrDNSLookupFailed        = errors.New("dns lookup failed")

	// ErrWebhookMissingSharedSecret indicates that a webhook connector has no shared
	// secret configured. Without a shared secret, webhook payloads are unsigned and
	// the receiving endpoint cannot verify authenticity, making it vulnerable to
	// spoofing attacks. Callers may treat this as a warning or an error depending on
	// their security policy.
	ErrWebhookMissingSharedSecret = errors.New("webhook shared secret is not configured: payloads will be unsigned")
)

// ConnectorConfig holds all external connector configurations.
type ConnectorConfig struct {
	Jira       *JiraConnectorConfig
	Webhook    *WebhookConnectorConfig
	ServiceNow *ServiceNowConnectorConfig

	// AllowPrivateIPs disables the runtime SSRF protection that blocks connections
	// to private/loopback IP addresses. This should ONLY be set in development or
	// test environments. Production deployments MUST leave this as false.
	AllowPrivateIPs bool
}

// BaseConnectorConfig contains shared retry and timeout settings for all connectors.
type BaseConnectorConfig struct {
	Timeout      *time.Duration
	MaxRetries   *int
	RetryBackoff *time.Duration
}

// TimeoutOrDefault returns the configured timeout or the default value.
func (b *BaseConnectorConfig) TimeoutOrDefault() time.Duration {
	if b == nil || b.Timeout == nil {
		return DefaultTimeout
	}

	return *b.Timeout
}

// MaxRetriesOrDefault returns the configured max retries or the default value.
func (b *BaseConnectorConfig) MaxRetriesOrDefault() int {
	if b == nil || b.MaxRetries == nil {
		return DefaultMaxRetries
	}

	return *b.MaxRetries
}

// RetryBackoffOrDefault returns the configured retry backoff or the default value.
func (b *BaseConnectorConfig) RetryBackoffOrDefault() time.Duration {
	if b == nil || b.RetryBackoff == nil {
		return DefaultRetryBackoff
	}

	return *b.RetryBackoff
}

// JiraConnectorConfig contains JIRA connection settings.
type JiraConnectorConfig struct {
	BaseConnectorConfig
	BaseURL string
	// AuthToken is the API token for Jira authentication.
	// SECURITY: This value should be sourced from environment variables or secret
	// management (e.g., Vault), never from configuration files or version control.
	AuthToken  string `json:"-"`
	ProjectKey string
	IssueType  string
}

// Normalize applies safe transformations to configuration values.
// Call before Validate() to ensure normalized state.
func (cfg *JiraConnectorConfig) Normalize() {
	if cfg == nil {
		return
	}

	cfg.AuthToken = strings.TrimPrefix(strings.TrimSpace(cfg.AuthToken), "Bearer ")
}

// Validate checks if the JIRA connector configuration is valid.
// This method is side-effect free and does not mutate the receiver.
// Callers should call Normalize() first if token normalization is desired.
func (cfg *JiraConnectorConfig) Validate() error {
	if cfg == nil {
		return ErrInvalidConnectorConfig
	}

	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		return ErrMissingBaseURL
	}

	if err := validateURL(baseURL); err != nil {
		return err
	}

	token := strings.TrimSpace(cfg.AuthToken)
	if token == "" {
		return ErrMissingAuthToken
	}

	if strings.TrimSpace(cfg.ProjectKey) == "" {
		return ErrMissingJiraProjectKey
	}

	if strings.TrimSpace(cfg.IssueType) == "" {
		return ErrMissingJiraIssueType
	}

	return nil
}

// validateURL validates that the URL is well-formed with scheme and host.
func validateURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ErrInvalidBaseURL
	}

	if parsed.Scheme == "" || parsed.Host == "" {
		return ErrInvalidBaseURL
	}

	host := parsed.Hostname()

	switch parsed.Scheme {
	case "https":
		if err := validateNotPrivateIP(host); err != nil {
			return err
		}

		return nil
	case "http":
		if host == "localhost" || host == "127.0.0.1" || host == "::1" {
			return nil
		}

		return ErrInvalidBaseURL
	default:
		return ErrInvalidBaseURL
	}
}

func validateNotPrivateIP(hostname string) error {
	ip := net.ParseIP(hostname)
	if ip != nil {
		if isPrivateIP(ip) {
			return ErrPrivateIPNotAllowed
		}

		return nil
	}

	return validateHostnameNotPrivate(hostname)
}

// validateHostnameNotPrivate resolves the hostname and checks that none of the resolved
// IP addresses are private. Returns an error if the DNS lookup fails or any resolved
// address is private.
//
// TOCTOU note: This validation runs at configuration time and provides an early-fail
// check. Runtime protection against DNS rebinding attacks is enforced by the
// net.Dialer ControlContext hook installed in NewHTTPConnector, which validates the
// resolved IP at actual connection time.
func validateHostnameNotPrivate(hostname string) error {
	resolver := &net.Resolver{}
	ctx, cancel := context.WithTimeout(context.Background(), dnsLookupTimeout)

	defer cancel()

	addrs, err := resolver.LookupIPAddr(ctx, hostname)
	if err != nil {
		return fmt.Errorf("resolve host for validation: %w", ErrDNSLookupFailed)
	}

	for _, addr := range addrs {
		if isPrivateIP(addr.IP) {
			return ErrPrivateIPNotAllowed
		}
	}

	return nil
}

func isPrivateIP(ip net.IP) bool {
	for _, ipNet := range privateIPNets {
		if ipNet.Contains(ip) {
			return true
		}
	}

	return false
}

// WebhookConnectorConfig contains webhook connection settings.
type WebhookConnectorConfig struct {
	BaseConnectorConfig
	URL          string
	SharedSecret string

	// RequireSignedPayloads enforces that SharedSecret is set. When true,
	// Validate() returns ErrWebhookMissingSharedSecret if SharedSecret is empty.
	// Production deployments of a financial system should set this to true to
	// prevent unsigned webhook payloads (CWE-345).
	RequireSignedPayloads bool
}

// Validate checks if the webhook connector configuration is valid.
//
// When RequireSignedPayloads is true, SharedSecret must be non-empty; otherwise
// ErrWebhookMissingSharedSecret is returned. When RequireSignedPayloads is false
// (default), an empty SharedSecret is permitted for backward compatibility --
// the warning log in dispatchToWebhook already flags unsigned payloads at runtime.
func (cfg *WebhookConnectorConfig) Validate() error {
	if cfg == nil {
		return ErrInvalidConnectorConfig
	}

	webhookURL := strings.TrimSpace(cfg.URL)
	if webhookURL == "" {
		return ErrMissingBaseURL
	}

	if err := validateURL(webhookURL); err != nil {
		return err
	}

	if cfg.RequireSignedPayloads && strings.TrimSpace(cfg.SharedSecret) == "" {
		return ErrWebhookMissingSharedSecret
	}

	return nil
}

// ServiceNowConnectorConfig contains ServiceNow connection settings (stubbed).
type ServiceNowConnectorConfig struct {
	BaseConnectorConfig
	BaseURL   string
	AuthToken string `json:"-"`
}

// Validate checks if the ServiceNow connector configuration is valid.
func (cfg *ServiceNowConnectorConfig) Validate() error {
	if cfg == nil {
		return ErrInvalidConnectorConfig
	}

	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		return ErrMissingBaseURL
	}

	if err := validateURL(baseURL); err != nil {
		return err
	}

	if strings.TrimSpace(cfg.AuthToken) == "" {
		return ErrMissingAuthToken
	}

	return nil
}
