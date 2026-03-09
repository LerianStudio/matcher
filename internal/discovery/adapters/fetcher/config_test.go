//go:build unit

package fetcher

import (
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
	assert.True(t, cfg.AllowPrivateIPs)
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
