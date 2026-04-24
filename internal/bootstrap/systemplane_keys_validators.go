// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"fmt"
	"net/url"
)

var (
	errFetcherURLMustBeString      = errors.New("fetcher url must be a string")
	errFetcherURLMustBeAbsolute    = errors.New("fetcher url must be an absolute URL")
	errFetcherURLMustUseHTTPScheme = errors.New("fetcher url must use http or https")
	errValueNotPositive            = errors.New("value must be positive")
	errValueNotNumeric             = errors.New("value must be numeric")
	errValueNotString              = errors.New("value must be a string")
	errCORSWildcardInProd          = errors.New("CORS_ALLOWED_ORIGINS must be restricted in production (exact \"*\" not allowed)")
	errBodyLimitExceedsCeiling     = errors.New("server.body_limit_bytes must not exceed the application ceiling")
)

// appBodyLimitCeilingBytes mirrors fiber_server.go's appBodyLimitCeilingBytes
// constant. Duplicated here (rather than imported) to keep the systemplane
// validator decoupled from fiber_server.go at package init time.
const keyBodyLimitCeilingBytes = 128 * 1024 * 1024

// validatePositiveInt validates that the value is a positive integer.
func validatePositiveInt(value any) error {
	switch typed := value.(type) {
	case int:
		if typed <= 0 {
			return fmt.Errorf("%w, got %d", errValueNotPositive, typed)
		}

		return nil
	case float64:
		if typed <= 0 {
			return fmt.Errorf("%w, got %v", errValueNotPositive, typed)
		}

		return nil
	default:
		return fmt.Errorf("%w, got %T", errValueNotNumeric, value)
	}
}

// corsProductionValidator returns a validator that rejects wildcard CORS
// origins when envName is production. Used as a runtime guard on the
// `cors.allowed_origins` systemplane key so an admin PUT cannot widen the
// CORS policy past what validateProductionConfig enforced at startup.
//
// The envName is captured at registration time (from the bootstrap Config
// snapshot). This matches the semantics of the startup validator:
// ENV_NAME is bootstrap-only, so freezing it here does not race with
// runtime edits.
func corsProductionValidator(envName string) func(any) error {
	if !IsProductionEnvironment(envName) {
		return nil // no-op outside production; keep validator list lean
	}

	return func(value any) error {
		str, ok := value.(string)
		if !ok {
			return errValueNotString
		}

		if corsContainsWildcard(str) {
			return fmt.Errorf("%w: got %q", errCORSWildcardInProd, str)
		}

		return nil
	}
}

// validateBodyLimitBytes enforces the server.body_limit_bytes invariants:
// must be a positive integer AND must not exceed appBodyLimitCeilingBytes
// (128 MiB). An admin PUT beyond the ceiling would silently fail at
// request time (Fiber caps at the ceiling regardless), so we reject
// early with a clear error.
func validateBodyLimitBytes(value any) error {
	if err := validatePositiveInt(value); err != nil {
		return err
	}

	var bytes int

	switch typed := value.(type) {
	case int:
		bytes = typed
	case float64:
		bytes = int(typed)
	default:
		return fmt.Errorf("%w, got %T", errValueNotNumeric, value)
	}

	if bytes > keyBodyLimitCeilingBytes {
		return fmt.Errorf("%w: got %d, ceiling %d", errBodyLimitExceedsCeiling, bytes, keyBodyLimitCeilingBytes)
	}

	return nil
}

// validateFetcherURL validates that the value is a well-formed HTTP(S) URL.
func validateFetcherURL(value any) error {
	str, ok := value.(string)
	if !ok {
		return errFetcherURLMustBeString
	}

	if str == "" {
		// Empty URL is permitted because the Fetcher integration is gated by
		// the separate `fetcher.enabled` key (default false). When
		// `fetcher.enabled=true`, an empty URL will fail fast at Fetcher client
		// construction via dynamic_fetcher_client.go. See also init.go gating
		// at cfg.Fetcher.Enabled check sites.
		return nil
	}

	parsedURL, err := url.Parse(str)
	if err != nil {
		return fmt.Errorf("fetcher url parse: %w", err)
	}

	if !parsedURL.IsAbs() {
		return errFetcherURLMustBeAbsolute
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return errFetcherURLMustUseHTTPScheme
	}

	return nil
}
