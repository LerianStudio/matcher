// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for TLS detection helpers.
//
// Contract:
//   - Helpers must parse conn strings/URLs using net/url or key=value parsing.
//   - MUST NOT use strings.Contains() against the raw string (anti-pattern #4 in the
//     dev-readyz skill): substring matches leak on URL-encoded params and on
//     tokens that happen to contain "tls=" or "sslmode=" inside path segments.
//   - Empty conn string is not a failure; it returns (false, nil) — meaning
//     "not configured" — except for S3, where AWS's implicit default is HTTPS
//     and so an empty endpoint must return (true, nil).
//   - Malformed URLs return (false, err). Caller logs; TLS posture is not guessed.

func TestDetectPostgresTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsn     string
		wantTLS bool
		wantErr bool
	}{
		{
			name:    "empty_dsn_returns_false_no_error",
			dsn:     "",
			wantTLS: false,
			wantErr: false,
		},
		{
			name:    "sslmode_require_is_tls",
			dsn:     "host=localhost port=5432 user=u password=p dbname=db sslmode=require",
			wantTLS: true,
			wantErr: false,
		},
		{
			name:    "sslmode_verify_full_is_tls",
			dsn:     "host=localhost port=5432 user=u password=p dbname=db sslmode=verify-full",
			wantTLS: true,
			wantErr: false,
		},
		{
			name:    "sslmode_disable_is_not_tls",
			dsn:     "host=localhost port=5432 user=u password=p dbname=db sslmode=disable",
			wantTLS: false,
			wantErr: false,
		},
		{
			name:    "missing_sslmode_defaults_to_not_tls",
			dsn:     "host=localhost port=5432 user=u password=p dbname=db",
			wantTLS: false,
			wantErr: false,
		},
		{
			name:    "url_form_sslmode_require_is_tls",
			dsn:     "postgres://u:p@localhost:5432/db?sslmode=require",
			wantTLS: true,
			wantErr: false,
		},
		{
			name:    "url_form_sslmode_disable_is_not_tls",
			dsn:     "postgres://u:p@localhost:5432/db?sslmode=disable",
			wantTLS: false,
			wantErr: false,
		},
		{
			name: "substring_ambiguous_path_not_misread_as_tls",
			// password "sslmode=require" is a substring trap; real sslmode is disable.
			dsn:     "host=localhost port=5432 user=u password=sslmode=require dbname=db sslmode=disable",
			wantTLS: false,
			wantErr: false,
		},
		{
			name:    "whitespace_around_sslmode_value_handled",
			dsn:     "host=localhost  sslmode=require  dbname=db",
			wantTLS: true,
			wantErr: false,
		},
		{
			name:    "malformed_url_returns_error",
			dsn:     "postgres://u:p@localhost:5432/db?sslmode=%ZZ",
			wantTLS: false,
			wantErr: true,
		},
		{
			name:    "url_missing_host_returns_error",
			dsn:     "postgres://",
			wantTLS: false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := detectPostgresTLS(tt.dsn)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantTLS, got)
		})
	}
}

func TestDetectRedisTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantTLS bool
		wantErr bool
	}{
		{name: "empty_returns_false_no_error", url: "", wantTLS: false, wantErr: false},
		{name: "rediss_scheme_is_tls", url: "rediss://localhost:6379/0", wantTLS: true, wantErr: false},
		{name: "redis_scheme_is_not_tls", url: "redis://localhost:6379/0", wantTLS: false, wantErr: false},
		{name: "scheme_is_case_insensitive", url: "REDISS://localhost:6379/0", wantTLS: true, wantErr: false},
		{name: "malformed_url_returns_error", url: "rediss://%ZZlocalhost", wantTLS: false, wantErr: true},
		{name: "missing_host_returns_error", url: "rediss://", wantTLS: false, wantErr: true},
		{
			name: "substring_trap_not_misread",
			// path segment contains "rediss" but scheme is "redis" (not TLS)
			url: "redis://localhost:6379/rediss-key", wantTLS: false, wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := detectRedisTLS(tt.url)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantTLS, got)
		})
	}
}

func TestDetectAMQPTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantTLS bool
		wantErr bool
	}{
		{name: "empty_returns_false_no_error", url: "", wantTLS: false, wantErr: false},
		{name: "amqps_scheme_is_tls", url: "amqps://user:pass@host:5671/%2F", wantTLS: true, wantErr: false},
		{name: "amqp_scheme_is_not_tls", url: "amqp://user:pass@host:5672/%2F", wantTLS: false, wantErr: false},
		{name: "scheme_is_case_insensitive", url: "AMQPS://user:pass@host:5671/%2F", wantTLS: true, wantErr: false},
		{name: "malformed_url_returns_error", url: "amqps://%ZZ", wantTLS: false, wantErr: true},
		{name: "missing_host_returns_error", url: "amqps://", wantTLS: false, wantErr: true},
		{
			name: "vhost_named_amqps_not_misread_as_tls",
			// scheme is amqp; vhost path segment happens to be "amqps"
			url: "amqp://user:pass@host:5672/amqps-vhost", wantTLS: false, wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := detectAMQPTLS(tt.url)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantTLS, got)
		})
	}
}

func TestDetectS3TLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		wantTLS  bool
		wantErr  bool
	}{
		{
			name:     "empty_endpoint_defaults_to_tls_AWS_default_https",
			endpoint: "",
			wantTLS:  true,
			wantErr:  false,
		},
		{
			name:     "https_endpoint_is_tls",
			endpoint: "https://s3.us-east-1.amazonaws.com",
			wantTLS:  true,
			wantErr:  false,
		},
		{
			name:     "http_endpoint_is_not_tls",
			endpoint: "http://localhost:8333",
			wantTLS:  false,
			wantErr:  false,
		},
		{
			name:     "scheme_is_case_insensitive",
			endpoint: "HTTPS://s3.example.com",
			wantTLS:  true,
			wantErr:  false,
		},
		{
			name:     "malformed_url_returns_error",
			endpoint: "https://%ZZ",
			wantTLS:  false,
			wantErr:  true,
		},
		{
			name:     "missing_host_returns_error",
			endpoint: "https://",
			wantTLS:  false,
			wantErr:  true,
		},
		{
			name: "substring_trap_not_misread",
			// path contains "https" but scheme is "http"
			endpoint: "http://localhost:8333/https-bucket",
			wantTLS:  false,
			wantErr:  false,
		},
		{
			name: "bare_host_port_rejected",
			// Matcher convention is scheme-prefixed URLs ("http://" or
			// "https://"). A bare host:port is malformed input — url.Parse
			// treats "localhost:9000" as scheme="localhost" path="9000" with
			// no host, so detectS3TLS returns errS3EndpointNoHost. Test pins
			// the behaviour so a future refactor does not accidentally loosen
			// the contract.
			endpoint: "localhost:9000",
			wantTLS:  false,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := detectS3TLS(tt.endpoint)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantTLS, got)
		})
	}
}
