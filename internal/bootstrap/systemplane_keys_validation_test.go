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
		{name: "http_host", value: "http://example.com"},
		{name: "http_with_port", value: "http://example.com:9000"},
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

func TestValidateHTTPSEndpoint_HTTPAllowed(t *testing.T) {
	t.Parallel()

	err := validateHTTPSEndpoint("http://example.com")

	assert.NoError(t, err)
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

func TestValidateDedupeTTLSeconds(t *testing.T) {
	t.Parallel()

	t.Run("rejects sub minute values", func(t *testing.T) {
		t.Parallel()

		err := validateDedupeTTLSeconds(59)

		require.Error(t, err)
		assert.ErrorIs(t, err, domain.ErrValueInvalid)
	})

	t.Run("accepts one minute and above", func(t *testing.T) {
		t.Parallel()

		assert.NoError(t, validateDedupeTTLSeconds(60))
		assert.NoError(t, validateDedupeTTLSeconds(120))
	})
}

func TestValidateRateLimitRequestsPerWindow(t *testing.T) {
	t.Parallel()

	assert.NoError(t, validateRateLimitRequestsPerWindow(maxRateLimitRequestsPerWindow))

	err := validateRateLimitRequestsPerWindow(maxRateLimitRequestsPerWindow + 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestValidateRateLimitWindowSeconds(t *testing.T) {
	t.Parallel()

	assert.NoError(t, validateRateLimitWindowSeconds(maxRateLimitWindowSeconds))

	err := validateRateLimitWindowSeconds(maxRateLimitWindowSeconds + 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
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

// --- Fetcher bridge bounded validators (Fix 9) ---

// TestValidateFetcherMaxExtractionBytes_RejectsBelowFloor exercises the
// minimum bound: a 1 KiB ceiling defeats the DoS guard's purpose because
// most legitimate Fetcher payloads exceed it. The validator must reject
// configurations that would silently make the cap useless.
func TestValidateFetcherMaxExtractionBytes_RejectsBelowFloor(t *testing.T) {
	t.Parallel()

	err := validateFetcherMaxExtractionBytes(int64(1024))
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

// TestValidateFetcherMaxExtractionBytes_RejectsAboveCeiling guards against
// operators accidentally disabling the DoS guard with MaxInt64-style values.
func TestValidateFetcherMaxExtractionBytes_RejectsAboveCeiling(t *testing.T) {
	t.Parallel()

	// 32 GiB is over the 16 GiB ceiling.
	err := validateFetcherMaxExtractionBytes(int64(32 * (1 << 30)))
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

// TestValidateFetcherMaxExtractionBytes_AcceptsDefault sanity-checks that
// the production default (2 GiB) sits comfortably inside the bounded range.
func TestValidateFetcherMaxExtractionBytes_AcceptsDefault(t *testing.T) {
	t.Parallel()

	err := validateFetcherMaxExtractionBytes(int64(2 << 30))
	assert.NoError(t, err)
}

// TestValidateBridgeIntervalSec_RejectsTooFast guards against operators
// configuring a 1-second poll that would hammer the extraction_requests
// table on every cycle.
func TestValidateBridgeIntervalSec_RejectsTooFast(t *testing.T) {
	t.Parallel()

	err := validateBridgeIntervalSec(1)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

// TestValidateBridgeIntervalSec_RejectsTooSlow guards against operators
// disabling the bridge worker by configuring a multi-day poll interval.
func TestValidateBridgeIntervalSec_RejectsTooSlow(t *testing.T) {
	t.Parallel()

	err := validateBridgeIntervalSec(7200)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

// TestValidateBridgeIntervalSec_AcceptsDefault sanity-checks the prod default.
func TestValidateBridgeIntervalSec_AcceptsDefault(t *testing.T) {
	t.Parallel()

	err := validateBridgeIntervalSec(30)
	assert.NoError(t, err)
}

// TestValidateBridgeBatchSize_RejectsZero guards against silent worker
// disablement (a batch size of 0 means "process nothing").
func TestValidateBridgeBatchSize_RejectsZero(t *testing.T) {
	t.Parallel()

	err := validateBridgeBatchSize(0)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

// TestValidateBridgeBatchSize_RejectsAboveCeiling guards against per-cycle
// latency blowups under backlog conditions.
func TestValidateBridgeBatchSize_RejectsAboveCeiling(t *testing.T) {
	t.Parallel()

	err := validateBridgeBatchSize(20000)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrValueInvalid)
}

// TestValidateBridgeBatchSize_AcceptsDefault sanity-checks the prod default.
func TestValidateBridgeBatchSize_AcceptsDefault(t *testing.T) {
	t.Parallel()

	err := validateBridgeBatchSize(50)
	assert.NoError(t, err)
}
