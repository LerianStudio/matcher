//go:build unit

package connectors

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateURL_AllowsHTTPS(t *testing.T) {
	t.Parallel()

	err := validateURL("https://example.com/webhook")
	require.NoError(t, err)
}

func TestValidateURL_AllowsLocalhostHTTP(t *testing.T) {
	t.Parallel()

	err := validateURL("http://localhost:8080/callback")
	require.NoError(t, err)
}

func TestValidateURL_RejectsHTTPNonLocalhost(t *testing.T) {
	t.Parallel()

	err := validateURL("http://example.com/callback")
	require.ErrorIs(t, err, ErrInvalidBaseURL)
}

func TestValidateURL_RejectsEmptyURL(t *testing.T) {
	t.Parallel()

	err := validateURL("")
	require.ErrorIs(t, err, ErrInvalidBaseURL)
}

func TestValidateURL_RejectsWhitespaceURL(t *testing.T) {
	t.Parallel()

	err := validateURL("   ")
	require.Error(t, err)
}

func TestValidateURL_RejectsMalformedURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
	}{
		{name: "plain string", url: "not-a-valid-url"},
		{name: "missing scheme", url: "://missing-scheme"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateURL(tt.url)
			require.ErrorIs(t, err, ErrInvalidBaseURL)
		})
	}
}

func TestBaseConnectorConfig_TimeoutOrDefault(t *testing.T) {
	t.Parallel()

	t.Run("returns default for nil receiver", func(t *testing.T) {
		t.Parallel()

		var cfg *BaseConnectorConfig
		require.Equal(t, DefaultTimeout, cfg.TimeoutOrDefault())
	})

	t.Run("returns default when timeout is nil", func(t *testing.T) {
		t.Parallel()

		cfg := &BaseConnectorConfig{}
		require.Equal(t, DefaultTimeout, cfg.TimeoutOrDefault())
	})

	t.Run("returns custom timeout when set", func(t *testing.T) {
		t.Parallel()

		custom := 12 * time.Second
		cfg := &BaseConnectorConfig{Timeout: &custom}
		require.Equal(t, custom, cfg.TimeoutOrDefault())
	})
}

func TestBaseConnectorConfig_MaxRetriesOrDefault(t *testing.T) {
	t.Parallel()

	t.Run("returns default for nil receiver", func(t *testing.T) {
		t.Parallel()

		var cfg *BaseConnectorConfig
		require.Equal(t, DefaultMaxRetries, cfg.MaxRetriesOrDefault())
	})

	t.Run("returns default when max retries is nil", func(t *testing.T) {
		t.Parallel()

		cfg := &BaseConnectorConfig{}
		require.Equal(t, DefaultMaxRetries, cfg.MaxRetriesOrDefault())
	})

	t.Run("returns custom max retries when set", func(t *testing.T) {
		t.Parallel()

		custom := 5
		cfg := &BaseConnectorConfig{MaxRetries: &custom}
		require.Equal(t, custom, cfg.MaxRetriesOrDefault())
	})
}

func TestBaseConnectorConfig_RetryBackoffOrDefault(t *testing.T) {
	t.Parallel()

	t.Run("returns default for nil receiver", func(t *testing.T) {
		t.Parallel()

		var cfg *BaseConnectorConfig
		require.Equal(t, DefaultRetryBackoff, cfg.RetryBackoffOrDefault())
	})

	t.Run("returns default when retry backoff is nil", func(t *testing.T) {
		t.Parallel()

		cfg := &BaseConnectorConfig{}
		require.Equal(t, DefaultRetryBackoff, cfg.RetryBackoffOrDefault())
	})

	t.Run("returns custom retry backoff when set", func(t *testing.T) {
		t.Parallel()

		custom := 5 * time.Second
		cfg := &BaseConnectorConfig{RetryBackoff: &custom}
		require.Equal(t, custom, cfg.RetryBackoffOrDefault())
	})
}

func TestJiraConnectorConfig_PromotedDefaults(t *testing.T) {
	t.Parallel()

	cfg := &JiraConnectorConfig{}
	require.Equal(t, DefaultTimeout, cfg.TimeoutOrDefault())
	require.Equal(t, DefaultMaxRetries, cfg.MaxRetriesOrDefault())
	require.Equal(t, DefaultRetryBackoff, cfg.RetryBackoffOrDefault())

	custom := 12 * time.Second
	customCfg := &JiraConnectorConfig{
		BaseConnectorConfig: BaseConnectorConfig{Timeout: &custom},
	}
	require.Equal(t, custom, customCfg.TimeoutOrDefault())
}

func TestJiraConnectorConfig_Normalize(t *testing.T) {
	t.Parallel()

	t.Run("nil receiver is safe", func(t *testing.T) {
		t.Parallel()

		var cfg *JiraConnectorConfig
		cfg.Normalize() // must not panic
	})

	t.Run("strips Bearer prefix", func(t *testing.T) {
		t.Parallel()

		cfg := &JiraConnectorConfig{AuthToken: "Bearer my-token"}
		cfg.Normalize()
		require.Equal(t, "my-token", cfg.AuthToken)
	})

	t.Run("strips whitespace and Bearer prefix", func(t *testing.T) {
		t.Parallel()

		cfg := &JiraConnectorConfig{AuthToken: "  Bearer my-token  "}
		cfg.Normalize()
		require.Equal(t, "my-token", cfg.AuthToken)
	})

	t.Run("trims whitespace only when no Bearer prefix", func(t *testing.T) {
		t.Parallel()

		cfg := &JiraConnectorConfig{AuthToken: "  plain-token  "}
		cfg.Normalize()
		require.Equal(t, "plain-token", cfg.AuthToken)
	})

	t.Run("leaves clean token unchanged", func(t *testing.T) {
		t.Parallel()

		cfg := &JiraConnectorConfig{AuthToken: "clean-token"}
		cfg.Normalize()
		require.Equal(t, "clean-token", cfg.AuthToken)
	})
}

func TestJiraConnectorConfig_Validate_DoesNotMutate(t *testing.T) {
	t.Parallel()

	cfg := &JiraConnectorConfig{
		BaseURL:    "http://localhost:8080",
		AuthToken:  "Bearer my-token",
		ProjectKey: "PROJ",
		IssueType:  "Bug",
	}

	err := cfg.Validate()
	require.NoError(t, err)

	// Validate must NOT strip the "Bearer " prefix — that is Normalize's job.
	require.Equal(t, "Bearer my-token", cfg.AuthToken)
}

func TestWebhookConnectorConfig_PromotedDefaults(t *testing.T) {
	t.Parallel()

	cfg := &WebhookConnectorConfig{}
	require.Equal(t, DefaultTimeout, cfg.TimeoutOrDefault())
	require.Equal(t, DefaultMaxRetries, cfg.MaxRetriesOrDefault())
	require.Equal(t, DefaultRetryBackoff, cfg.RetryBackoffOrDefault())

	custom := 15 * time.Second
	customCfg := &WebhookConnectorConfig{
		BaseConnectorConfig: BaseConnectorConfig{Timeout: &custom},
	}
	require.Equal(t, custom, customCfg.TimeoutOrDefault())
}

func TestValidateURL_PrivateIPBlocking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantErr error
	}{
		{
			name:    "public HTTPS allowed",
			url:     "https://api.jira.com/webhook",
			wantErr: nil,
		},
		{
			name:    "10.x.x.x private range blocked",
			url:     "https://10.0.0.1/internal",
			wantErr: ErrPrivateIPNotAllowed,
		},
		{
			name:    "10.255.255.255 blocked",
			url:     "https://10.255.255.255/api",
			wantErr: ErrPrivateIPNotAllowed,
		},
		{
			name:    "172.16.x.x private range blocked",
			url:     "https://172.16.0.1/internal",
			wantErr: ErrPrivateIPNotAllowed,
		},
		{
			name:    "172.31.255.255 blocked",
			url:     "https://172.31.255.255/api",
			wantErr: ErrPrivateIPNotAllowed,
		},
		{
			name:    "192.168.x.x private range blocked",
			url:     "https://192.168.1.100/webhook",
			wantErr: ErrPrivateIPNotAllowed,
		},
		{
			name:    "169.254.x.x link-local blocked",
			url:     "https://169.254.169.254/latest/meta-data",
			wantErr: ErrPrivateIPNotAllowed,
		},
		{
			name:    "127.0.0.1 loopback blocked via HTTPS",
			url:     "https://127.0.0.1/local",
			wantErr: ErrPrivateIPNotAllowed,
		},
		{
			name:    "localhost HTTP allowed for dev",
			url:     "http://localhost:8080/callback",
			wantErr: nil,
		},
		{
			name:    "127.0.0.1 HTTP allowed for dev",
			url:     "http://127.0.0.1:3000/api",
			wantErr: nil,
		},
		{
			name:    "public domain not blocked",
			url:     "https://webhook.site/abc123",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateURL(tt.url)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsPrivateIP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		ip     string
		isPriv bool
	}{
		// IPv4 private ranges
		{"10.0.0.0", "10.0.0.0", true},
		{"10.0.0.1", "10.0.0.1", true},
		{"10.255.255.255", "10.255.255.255", true},
		{"172.16.0.0", "172.16.0.0", true},
		{"172.16.0.1", "172.16.0.1", true},
		{"172.31.255.255", "172.31.255.255", true},
		{"172.15.0.0 not private", "172.15.0.0", false},
		{"172.32.0.0 not private", "172.32.0.0", false},
		{"192.168.0.0", "192.168.0.0", true},
		{"192.168.1.1", "192.168.1.1", true},
		{"192.168.255.255", "192.168.255.255", true},
		{"169.254.0.0 link-local", "169.254.0.0", true},
		{"169.254.169.254 AWS metadata", "169.254.169.254", true},
		{"127.0.0.1 loopback", "127.0.0.1", true},
		{"127.0.0.2 loopback range", "127.0.0.2", true},
		{"8.8.8.8 public", "8.8.8.8", false},
		{"1.1.1.1 public", "1.1.1.1", false},
		{"203.0.113.1 test range", "203.0.113.1", false},
		// IPv6 private ranges
		{"::1 loopback", "::1", true},
		{"fe80::1 link-local", "fe80::1", true},
		{"fe80::abcd:1234 link-local", "fe80::abcd:1234", true},
		{"fc00::1 unique local", "fc00::1", true},
		{"fd00::1 unique local", "fd00::1", true},
		{"2001:db8::1 documentation/example IPv6 (RFC 3849)", "2001:db8::1", false},
		{"2607:f8b0:4004:800::200e public IPv6", "2607:f8b0:4004:800::200e", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ip := parseIP(tt.ip)
			require.NotNil(t, ip, "failed to parse IP: %s", tt.ip)
			require.Equal(t, tt.isPriv, isPrivateIP(ip))
		})
	}
}

func TestValidateHostnameNotPrivate_DNSFailure_NoHostnameLeak(t *testing.T) {
	t.Parallel()

	// Use a hostname that will certainly fail DNS resolution.
	bogusHost := "this-host-does-not-exist-12345.invalid"

	err := validateHostnameNotPrivate(bogusHost)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDNSLookupFailed)
	require.NotContains(t, err.Error(), bogusHost,
		"error message must not leak the internal hostname")
}

func parseIP(s string) net.IP {
	return net.ParseIP(s)
}

func TestNewHTTPConnector_DialerRejectsPrivateIPs(t *testing.T) {
	t.Parallel()

	// Start a local test server — its address is 127.0.0.1 (loopback/private).
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	cfg := ConnectorConfig{
		Webhook: &WebhookConnectorConfig{
			URL: server.URL, // http://127.0.0.1:<port>
		},
	}

	connector, err := NewHTTPConnector(cfg)
	require.NoError(t, err)

	// Build a direct HTTP request to the test server (which resolves to 127.0.0.1).
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		server.URL,
		nil,
	)
	require.NoError(t, err)

	// The ControlContext hook on the dialer should reject the private IP at
	// connection time, preventing the request from completing.
	_, err = connector.client.Do(req) //nolint:bodyclose // error expected; no body
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPrivateIPNotAllowed)
}

func TestErrWebhookMissingSharedSecret_Defined(t *testing.T) {
	t.Parallel()

	// Verify the sentinel error is defined and has a meaningful message.
	require.NotNil(t, ErrWebhookMissingSharedSecret)
	require.Contains(t, ErrWebhookMissingSharedSecret.Error(), "unsigned")
}

func TestWebhookConnectorConfig_RequireSignedPayloads(t *testing.T) {
	t.Parallel()

	t.Run("returns error when required and secret is empty", func(t *testing.T) {
		t.Parallel()

		cfg := &WebhookConnectorConfig{
			URL:                   "http://localhost:8080/callback",
			RequireSignedPayloads: true,
			SharedSecret:          "",
		}
		err := cfg.Validate()
		require.ErrorIs(t, err, ErrWebhookMissingSharedSecret)
	})

	t.Run("returns error when required and secret is whitespace", func(t *testing.T) {
		t.Parallel()

		cfg := &WebhookConnectorConfig{
			URL:                   "http://localhost:8080/callback",
			RequireSignedPayloads: true,
			SharedSecret:          "   ",
		}
		err := cfg.Validate()
		require.ErrorIs(t, err, ErrWebhookMissingSharedSecret)
	})

	t.Run("passes when required and secret is set", func(t *testing.T) {
		t.Parallel()

		cfg := &WebhookConnectorConfig{
			URL:                   "http://localhost:8080/callback",
			RequireSignedPayloads: true,
			SharedSecret:          "my-webhook-secret",
		}
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("passes when not required and secret is empty", func(t *testing.T) {
		t.Parallel()

		cfg := &WebhookConnectorConfig{
			URL:                   "http://localhost:8080/callback",
			RequireSignedPayloads: false,
			SharedSecret:          "",
		}
		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestNewHTTPConnector_TransportHasCustomDialer(t *testing.T) {
	t.Parallel()

	connector, err := NewHTTPConnector(ConnectorConfig{})
	require.NoError(t, err)

	// Verify the connector's HTTP client has a custom transport with DialContext set.
	transport, ok := connector.client.Transport.(*http.Transport)
	require.True(t, ok, "expected *http.Transport")
	require.NotNil(t, transport.DialContext, "expected DialContext to be set for SSRF protection")
}
