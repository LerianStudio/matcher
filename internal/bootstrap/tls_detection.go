// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// TLS detection helpers for /readyz check result enrichment and for SaaS
// TLS enforcement at bootstrap. Each helper returns (tls bool, err error):
//
//   - tls=true  — the dep's configured posture is TLS-on
//   - tls=false — posture is TLS-off, or the conn string is empty/unconfigured
//     (except for S3, where an empty endpoint means "AWS default", which is HTTPS)
//   - err != nil — the conn string is malformed; caller decides severity
//
// Detection must parse structure (URL scheme, key=value sslmode), never use
// substring matching against the raw string. The dev-readyz skill bans
// strings.Contains() TLS detection because it fails on URL-encoded parameters
// and matches tokens inside paths or passwords.

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var (
	errRedisURLMissingHost    = errors.New("redis url missing host")
	errAMQPURLMissingHost     = errors.New("amqp url missing host")
	errS3EndpointNoHost       = errors.New("s3 endpoint missing host")
	errPostgresURLMissingHost = errors.New("postgres url missing host")
)

// detectPostgresTLS inspects a libpq-style keyword/value DSN ("host=... sslmode=require ...")
// or a URL-style DSN ("postgres://user:pass@host/db?sslmode=require").
// Returns true when sslmode is set to any value other than "disable".
// Empty DSN returns (false, nil): "not configured" is not a failure.
func detectPostgresTLS(dsn string) (bool, error) {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return false, nil
	}

	// URL form: postgres://... / postgresql://...
	if strings.HasPrefix(strings.ToLower(trimmed), "postgres://") ||
		strings.HasPrefix(strings.ToLower(trimmed), "postgresql://") {
		parsedURL, err := url.Parse(trimmed)
		if err != nil {
			return false, fmt.Errorf("parse postgres url: %w", err)
		}

		// Symmetric with detectRedisTLS / detectAMQPTLS: a missing host is a
		// malformed URL, not a silent "TLS-off".
		if strings.TrimSpace(parsedURL.Host) == "" {
			return false, errPostgresURLMissingHost
		}

		q, err := url.ParseQuery(parsedURL.RawQuery)
		if err != nil {
			return false, fmt.Errorf("parse postgres query: %w", err)
		}

		return sslmodeIsTLS(q.Get("sslmode")), nil
	}

	// Keyword/value DSN. Parse by splitting on whitespace, then key=value.
	// We must extract the declared sslmode value without being fooled by
	// substrings in other fields (e.g., a password containing "sslmode=").
	return sslmodeIsTLS(extractKeywordValue(trimmed, "sslmode")), nil
}

// detectRedisTLS reports whether a Redis URL uses the rediss:// TLS scheme.
func detectRedisTLS(rawURL string) (bool, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return false, nil
	}

	parsedURL, err := url.Parse(trimmed)
	if err != nil {
		return false, fmt.Errorf("parse redis url: %w", err)
	}

	if strings.TrimSpace(parsedURL.Host) == "" {
		return false, errRedisURLMissingHost
	}

	return strings.EqualFold(parsedURL.Scheme, "rediss"), nil
}

// detectAMQPTLS reports whether a RabbitMQ URL uses the amqps:// TLS scheme.
func detectAMQPTLS(rawURL string) (bool, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return false, nil
	}

	parsedURL, err := url.Parse(trimmed)
	if err != nil {
		return false, fmt.Errorf("parse amqp url: %w", err)
	}

	if strings.TrimSpace(parsedURL.Host) == "" {
		return false, errAMQPURLMissingHost
	}

	return strings.EqualFold(parsedURL.Scheme, "amqps"), nil
}

// detectS3TLS reports whether an S3 endpoint uses https://.
// An empty endpoint is the AWS default — HTTPS — and returns (true, nil).
func detectS3TLS(endpoint string) (bool, error) {
	trimmed := strings.TrimSpace(endpoint)
	if trimmed == "" {
		return true, nil
	}

	parsedURL, err := url.Parse(trimmed)
	if err != nil {
		return false, fmt.Errorf("parse s3 endpoint: %w", err)
	}

	if strings.TrimSpace(parsedURL.Host) == "" {
		return false, errS3EndpointNoHost
	}

	return strings.EqualFold(parsedURL.Scheme, "https"), nil
}

// sslmodeDisable is the libpq "no TLS" sentinel. Every other libpq sslmode
// value (allow, prefer, require, verify-ca, verify-full) involves TLS.
const sslmodeDisable = "disable"

// sslmodeIsTLS maps a libpq sslmode value to TLS posture.
// Empty and "disable" are non-TLS; every other recognised value is TLS.
func sslmodeIsTLS(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", sslmodeDisable:
		return false
	default:
		return true
	}
}

// extractKeywordValue parses a libpq keyword/value DSN and returns the value
// for the given keyword. Empty string means "not present".
//
// We tokenise on whitespace and then split each token on the first '='. This
// avoids substring matching inside password values and other fields: only the
// leading "key=" of each token is considered, so a password such as
// "sslmode=require" is never misread as the sslmode setting.
func extractKeywordValue(dsn, key string) string {
	fields := strings.Fields(dsn)
	for _, field := range fields {
		eq := strings.IndexByte(field, '=')
		if eq <= 0 {
			continue
		}

		k := strings.TrimSpace(field[:eq])
		v := strings.TrimSpace(field[eq+1:])

		if strings.EqualFold(k, key) {
			return v
		}
	}

	return ""
}
