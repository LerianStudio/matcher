//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
)

// --- validateAbsoluteHTTPURL ---

func TestValidateAbsoluteHTTPURL_ValidHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "http_localhost", value: "http://localhost:8080"},
		{name: "https_host", value: "https://api.example.com"},
		{name: "http_with_path", value: "http://example.com/api/v1"},
		{name: "https_with_port", value: "https://example.com:443/path"},
		{name: "http_with_trailing_space", value: " http://example.com "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateAbsoluteHTTPURL(tt.value)

			assert.NoError(t, err)
		})
	}
}

func TestValidateAbsoluteHTTPURL_InvalidURLs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "relative_path", value: "/api/v1"},
		{name: "no_host", value: "http://"},
		{name: "ftp_scheme", value: "ftp://example.com"},
		{name: "ws_scheme", value: "ws://example.com"},
		{name: "empty_string", value: ""},
		{name: "not_a_string", value: 42},
		{name: "just_host_no_scheme", value: "example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateAbsoluteHTTPURL(tt.value)

			require.Error(t, err)
		})
	}
}

func TestValidateAbsoluteHTTPURL_NonStringReturnsSpecificError(t *testing.T) {
	t.Parallel()

	err := validateAbsoluteHTTPURL(42)

	require.Error(t, err)
	assert.ErrorIs(t, err, errFetcherURLMustBeString)
}

func TestValidateAbsoluteHTTPURL_RelativeReturnsAbsoluteError(t *testing.T) {
	t.Parallel()

	err := validateAbsoluteHTTPURL("/relative/path")

	require.Error(t, err)
	assert.ErrorIs(t, err, errFetcherURLMustBeAbsolute)
}

func TestValidateAbsoluteHTTPURL_NonHTTPSchemeReturnsSchemeError(t *testing.T) {
	t.Parallel()

	err := validateAbsoluteHTTPURL("ftp://example.com/file")

	require.Error(t, err)
	assert.ErrorIs(t, err, errFetcherURLMustUseHTTPScheme)
}

// --- validateHTTPSEndpoint ---

func TestValidateHTTPSEndpoint_ValidHTTPS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "https_host", value: "https://example.com"},
		{name: "https_with_port", value: "https://example.com:443"},
		{name: "https_with_path", value: "https://example.com/api/v1"},
		{name: "empty_allowed", value: ""},
		{name: "whitespace_allowed", value: "  "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateHTTPSEndpoint(tt.value)

			assert.NoError(t, err)
		})
	}
}

func TestValidateHTTPSEndpoint_InvalidEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "http_not_https", value: "http://example.com"},
		{name: "ftp_scheme", value: "ftp://example.com"},
		{name: "no_scheme", value: "example.com"},
		{name: "relative_path", value: "/api/v1"},
		{name: "not_a_string", value: 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateHTTPSEndpoint(tt.value)

			require.Error(t, err)
		})
	}
}

func TestValidateHTTPSEndpoint_NonStringReturnsErrValueInvalid(t *testing.T) {
	t.Parallel()

	err := validateHTTPSEndpoint(42)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestValidateHTTPSEndpoint_HTTPSchemeReturnsErrValueInvalid(t *testing.T) {
	t.Parallel()

	err := validateHTTPSEndpoint("http://example.com")

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

// --- toInt ---

func TestToInt_ValidConversions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    any
		expected int64
		ok       bool
	}{
		{name: "int", value: 42, expected: 42, ok: true},
		{name: "int_zero", value: 0, expected: 0, ok: true},
		{name: "int_negative", value: -5, expected: -5, ok: true},
		{name: "int64", value: int64(100), expected: 100, ok: true},
		{name: "float64_whole", value: float64(42), expected: 42, ok: true},
		{name: "float64_negative_whole", value: float64(-10), expected: -10, ok: true},
		{name: "float64_zero", value: float64(0), expected: 0, ok: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, ok := toInt(tt.value)

			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToInt_InvalidConversions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "float64_fractional", value: 3.14},
		{name: "string", value: "42"},
		{name: "bool", value: true},
		{name: "nil", value: nil},
		{name: "float64_small_fraction", value: 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, ok := toInt(tt.value)

			assert.False(t, ok)
		})
	}
}

// --- Cross-validator consistency ---

func TestValidatePositiveInt_RejectsNonIntTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "string", value: "10"},
		{name: "bool", value: true},
		{name: "nil", value: nil},
		{name: "float64_fraction", value: 1.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validatePositiveInt(tt.value)

			require.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrValueInvalid)
		})
	}
}

func TestValidateNonNegativeInt_RejectsNonIntTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "string", value: "0"},
		{name: "bool", value: false},
		{name: "nil", value: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateNonNegativeInt(tt.value)

			require.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrValueInvalid)
		})
	}
}

func TestValidateLogLevel_CaseInsensitive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "lower_debug", value: "debug"},
		{name: "upper_DEBUG", value: "DEBUG"},
		{name: "mixed_Info", value: "Info"},
		{name: "upper_WARN", value: "WARN"},
		{name: "mixed_Error", value: "Error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateLogLevel(tt.value)

			assert.NoError(t, err)
		})
	}
}

func TestValidateSSLMode_ExactValues(t *testing.T) {
	t.Parallel()

	validModes := []string{"disable", "require", "verify-ca", "verify-full"}

	for _, mode := range validModes {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			err := validateSSLMode(mode)

			assert.NoError(t, err)
		})
	}
}

func TestValidateNonEmptyString_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	// Strings that are only whitespace should be rejected.
	err := validateNonEmptyString("   \t\n  ")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)

	// Non-whitespace with surrounding spaces should pass.
	err = validateNonEmptyString("  valid  ")
	assert.NoError(t, err)
}
