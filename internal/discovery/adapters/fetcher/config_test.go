//go:build unit

package fetcher

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()

	assert.Equal(t, "http://localhost:4006", cfg.BaseURL)
	assert.Equal(t, 5*time.Second, cfg.HealthTimeout)
	assert.Equal(t, 30*time.Second, cfg.RequestTimeout)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, 500*time.Millisecond, cfg.RetryBaseDelay)
	assert.False(t, cfg.AllowPrivateIPs)
}

func TestHTTPClientConfig_Validate_ValidConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	err := cfg.Validate()

	assert.NoError(t, err)
}

func TestHTTPClientConfig_Validate_ValidHTTPS(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "https://fetcher.example.com"

	err := cfg.Validate()

	assert.NoError(t, err)
}

func TestHTTPClientConfig_Validate_EmptyURL(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = ""

	err := cfg.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyURL)
}

func TestHTTPClientConfig_Validate_InvalidURL(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "://missing-scheme"

	err := cfg.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidURL)
}

func TestHTTPClientConfig_Validate_InvalidScheme_FTP(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "ftp://fetcher.example.com"

	err := cfg.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidScheme)
}

func TestHTTPClientConfig_Validate_InvalidScheme_Empty(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "fetcher.example.com"

	err := cfg.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidScheme)
}

func TestHTTPClientConfig_Validate_TrailingSlash(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http://localhost:4006/"

	err := cfg.Validate()

	assert.NoError(t, err)
}

func TestHTTPClientConfig_Validate_WithPath(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http://localhost:4006/some/path"

	err := cfg.Validate()

	assert.NoError(t, err)
}

func TestHTTPClientConfig_Validate_RejectsMissingHost(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http:///missing-host"

	err := cfg.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMissingHost)
}

func TestHTTPClientConfig_Validate_RejectsCredentials(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http://user:pass@localhost:4006"

	err := cfg.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidURL)
}

func TestHTTPClientConfig_Validate_RejectsQueryAndFragment(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.BaseURL = "http://localhost:4006?debug=true#frag"

	err := cfg.Validate()

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidURL)
}

func TestHTTPClientConfig_Validate_RejectsNegativeRetries(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.MaxRetries = -1

	err := cfg.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "max retries")
}

func TestHTTPClientConfig_Validate_RejectsNonPositiveTimeouts(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.HealthTimeout = 0
	cfg.RequestTimeout = -1 * time.Second

	err := cfg.Validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "health timeout")
}

func TestHTTPClientConfig_BuildTransport_BlocksPrivateIPWhenDisabled(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	transport := cfg.buildTransport()

	conn, err := transport.DialContext(t.Context(), "tcp", "127.0.0.1:80")
	if conn != nil {
		_ = conn.Close()
	}

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPrivateIPBlocked)
}

func TestHTTPClientConfig_BuildTransport_ResolveFailure(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	transport := cfg.buildTransport()

	conn, err := transport.DialContext(t.Context(), "tcp", "host.invalid:80")
	if conn != nil {
		_ = conn.Close()
	}

	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve host")
}

// TestIsBlockedSSRFTarget_DeniedRanges documents every IP class that
// isBlockedSSRFTarget rejects. Covers the stdlib boolean helpers
// (Private/Loopback/LinkLocal*) plus the explicit additions for
// Unspecified, Multicast, and CGNAT (100.64/10) that are NOT
// IsPrivate per stdlib.
func TestIsBlockedSSRFTarget_DeniedRanges(t *testing.T) {
	t.Parallel()

	denied := []struct {
		name string
		ip   string
	}{
		// Covered by stdlib helpers.
		{"rfc1918 10/8", "10.0.0.1"},
		{"rfc1918 172.16/12", "172.16.0.1"},
		{"rfc1918 192.168/16", "192.168.1.1"},
		{"ipv6 ula fc00::/7", "fc00::1"},
		{"ipv4 loopback", "127.0.0.1"},
		{"ipv6 loopback", "::1"},
		{"ipv4 link-local", "169.254.1.1"},
		{"ipv6 link-local unicast", "fe80::1"},
		// Explicit additions — must not be in the stdlib covered set above.
		{"ipv4 unspecified", "0.0.0.0"},
		{"ipv6 unspecified", "::"},
		{"ipv4 multicast 224/4 (boundary)", "224.0.0.1"},
		{"ipv4 multicast mid-range", "239.255.255.255"},
		{"ipv6 multicast", "ff02::1"},
		{"cgnat 100.64/10 low", "100.64.0.1"},
		{"cgnat 100.64/10 high", "100.127.255.255"},
	}

	for _, tc := range denied {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ip := net.ParseIP(tc.ip)
			require.NotNil(t, ip, "test data: parse %q", tc.ip)
			assert.Truef(t, isBlockedSSRFTarget(ip), "%s (%s) must be blocked", tc.name, tc.ip)
		})
	}
}

// TestIsBlockedSSRFTarget_AllowedRanges documents that the guard does
// NOT reject public internet addresses. Includes RFC 5737 TEST-NET
// ranges which are intentionally allowed so integration tests that
// resolve them in controlled environments keep working.
func TestIsBlockedSSRFTarget_AllowedRanges(t *testing.T) {
	t.Parallel()

	allowed := []struct {
		name string
		ip   string
	}{
		{"public ipv4", "8.8.8.8"},
		{"public ipv4 cloudflare", "1.1.1.1"},
		{"public ipv6 google dns", "2001:4860:4860::8888"},
		// Routable non-CGNAT addresses bordering the CGNAT range.
		{"just below cgnat (100.63.255.254)", "100.63.255.254"},
		{"just above cgnat (100.128.0.1)", "100.128.0.1"},
		// RFC 5737 TEST-NET ranges are intentionally allowed.
		{"rfc5737 test-net-1", "192.0.2.1"},
		{"rfc5737 test-net-2", "198.51.100.1"},
		{"rfc5737 test-net-3", "203.0.113.1"},
	}

	for _, tc := range allowed {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ip := net.ParseIP(tc.ip)
			require.NotNil(t, ip, "test data: parse %q", tc.ip)
			assert.Falsef(t, isBlockedSSRFTarget(ip), "%s (%s) must NOT be blocked", tc.name, tc.ip)
		})
	}
}
