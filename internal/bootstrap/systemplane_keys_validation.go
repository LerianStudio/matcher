// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
)

const minDedupeTTLSeconds int64 = 60

func validateAbsoluteHTTPURL(value any) error {
	rawValue, ok := value.(string)
	if !ok {
		return errFetcherURLMustBeString
	}

	parsed, err := url.Parse(strings.TrimSpace(rawValue))
	if err != nil {
		return fmt.Errorf("fetcher url must be a valid URL: %w", err)
	}

	if parsed == nil || !parsed.IsAbs() || parsed.Host == "" {
		return errFetcherURLMustBeAbsolute
	}

	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return errFetcherURLMustUseHTTPScheme
	}

	return nil
}

// validateHTTPSEndpoint validates non-empty values as absolute HTTP(S) URLs.
// Empty values pass validation to allow unconfigured (disabled) endpoints.
func validateHTTPSEndpoint(value any) error {
	rawValue, ok := value.(string)
	if !ok {
		return fmt.Errorf("endpoint must be a string: %w", domain.ErrValueInvalid)
	}

	trimmed := strings.TrimSpace(rawValue)
	if trimmed == "" {
		return nil
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("endpoint must be a valid URL: %w", err)
	}

	if parsed == nil || !parsed.IsAbs() || parsed.Host == "" {
		return fmt.Errorf("endpoint must be an absolute URL with scheme and host: %w", domain.ErrValueInvalid)
	}

	if !strings.EqualFold(parsed.Scheme, "https") && !strings.EqualFold(parsed.Scheme, "http") {
		return fmt.Errorf("endpoint must use http or https scheme, got %q: %w", parsed.Scheme, domain.ErrValueInvalid)
	}

	return nil
}

// Validators for systemplane key registration.

// validatePositiveInt rejects zero and negative integers.
func validatePositiveInt(value any) error {
	intVal, ok := toInt(value)
	if !ok {
		return fmt.Errorf("expected integer value: %w", domain.ErrValueInvalid)
	}

	if intVal <= 0 {
		return fmt.Errorf("value must be a positive integer, got %d: %w", intVal, domain.ErrValueInvalid)
	}

	return nil
}

func validateBoundedPositiveInt(value any, maxValue int64, label string) error {
	intVal, ok := toInt(value)
	if !ok {
		return fmt.Errorf("expected integer value: %w", domain.ErrValueInvalid)
	}

	if intVal <= 0 {
		return fmt.Errorf("value must be a positive integer, got %d: %w", intVal, domain.ErrValueInvalid)
	}

	if intVal > maxValue {
		return fmt.Errorf("%s must not exceed %d, got %d: %w", label, maxValue, intVal, domain.ErrValueInvalid)
	}

	return nil
}

func validateWebhookTimeoutSeconds(value any) error {
	return validateBoundedPositiveInt(value, maxWebhookTimeoutSec, "webhook timeout seconds")
}

func validateRateLimitRequestsPerWindow(value any) error {
	return validateBoundedPositiveInt(value, maxRateLimitRequestsPerWindow, "rate limit requests per window")
}

func validateRateLimitWindowSeconds(value any) error {
	return validateBoundedPositiveInt(value, maxRateLimitWindowSeconds, "rate limit window seconds")
}

func validatePresignExpirySeconds(value any) error {
	return validateBoundedPositiveInt(value, maxPresignExpirySec, "presign expiry seconds")
}

// Bounds for the Fetcher bridge knobs (Fix 9).
//
// max_extraction_bytes: floor 1 MiB keeps the DoS guard meaningful (anything
// smaller would block legitimate Fetcher payloads). Ceiling 16 GiB is the
// largest realistic working-set for a matcher pod under typical k8s memory
// limits — anything bigger almost certainly indicates misconfiguration.
//
// bridge_interval_sec: floor 5 s keeps the worker from hammering the
// extraction_requests table on every degenerate cycle. Ceiling 1 h
// preserves backstop responsiveness for newly-completed extractions.
//
// bridge_batch_size: floor 1 (zero would disable the worker silently).
// Ceiling 10 000 keeps per-cycle worst-case latency bounded; operators
// needing more drain capacity should shorten the interval instead.
const (
	minFetcherMaxExtractionBytes int64 = 1 << 20        // 1 MiB
	maxFetcherMaxExtractionBytes int64 = 16 * (1 << 30) // 16 GiB
	minBridgeIntervalSec         int64 = 5
	maxBridgeIntervalSec         int64 = 3600
	minBridgeBatchSize           int64 = 1
	maxBridgeBatchSize           int64 = 10000
	// Stale threshold bounds for bridge readiness dashboard. Floor of 60s
	// keeps the partition meaningful (sub-minute thresholds would lump
	// every retry into "stale" before the worker can finish). Ceiling of
	// one day matches typical operational SLOs without inviting "set and
	// forget" misuse.
	minBridgeStaleThresholdSec int64 = 60
	maxBridgeStaleThresholdSec int64 = 86400
	// T-005 bridge retry bound. Floor keeps the worker meaningful under
	// misconfiguration; ceiling prevents operators from disabling the
	// terminal-escalation path with absurd values.
	//
	// Polish Fix 2: the InitialBackoff/MaxBackoff bounds were deleted along
	// with the dead exponential-backoff helpers (the worker enforces backoff
	// passively via updated_at reordering, not via in-process timers).
	minBridgeRetryMaxAttempts int64 = 1
	maxBridgeRetryMaxAttempts int64 = 100
)

// validateBoundedRangeInt rejects values outside [minValue, maxValue].
// Mirrors validateBoundedPositiveInt but with a configurable lower bound
// for cases where a true positive integer is too permissive (e.g. a
// 1-second poll interval would melt the database).
func validateBoundedRangeInt(value any, minValue, maxValue int64, label string) error {
	intVal, ok := toInt(value)
	if !ok {
		return fmt.Errorf("expected integer value: %w", domain.ErrValueInvalid)
	}

	if intVal < minValue {
		return fmt.Errorf("%s must be at least %d, got %d: %w", label, minValue, intVal, domain.ErrValueInvalid)
	}

	if intVal > maxValue {
		return fmt.Errorf("%s must not exceed %d, got %d: %w", label, maxValue, intVal, domain.ErrValueInvalid)
	}

	return nil
}

func validateFetcherMaxExtractionBytes(value any) error {
	return validateBoundedRangeInt(
		value,
		minFetcherMaxExtractionBytes,
		maxFetcherMaxExtractionBytes,
		"fetcher max extraction bytes",
	)
}

func validateBridgeIntervalSec(value any) error {
	return validateBoundedRangeInt(
		value,
		minBridgeIntervalSec,
		maxBridgeIntervalSec,
		"fetcher bridge interval seconds",
	)
}

func validateBridgeBatchSize(value any) error {
	return validateBoundedRangeInt(
		value,
		minBridgeBatchSize,
		maxBridgeBatchSize,
		"fetcher bridge batch size",
	)
}

func validateBridgeStaleThresholdSec(value any) error {
	return validateBoundedRangeInt(
		value,
		minBridgeStaleThresholdSec,
		maxBridgeStaleThresholdSec,
		"fetcher bridge stale threshold seconds",
	)
}

func validateBridgeRetryMaxAttempts(value any) error {
	return validateBoundedRangeInt(
		value,
		minBridgeRetryMaxAttempts,
		maxBridgeRetryMaxAttempts,
		"fetcher bridge retry max attempts",
	)
}

func validateDedupeTTLSeconds(value any) error {
	intVal, ok := toInt(value)
	if !ok {
		return fmt.Errorf("expected integer value: %w", domain.ErrValueInvalid)
	}

	if intVal < minDedupeTTLSeconds {
		return fmt.Errorf("dedupe ttl seconds must be at least %d, got %d: %w", minDedupeTTLSeconds, intVal, domain.ErrValueInvalid)
	}

	return nil
}

// validateNonNegativeInt rejects negative integers but allows zero.
func validateNonNegativeInt(value any) error {
	intVal, ok := toInt(value)
	if !ok {
		return fmt.Errorf("expected integer value: %w", domain.ErrValueInvalid)
	}

	if intVal < 0 {
		return fmt.Errorf("value must be a non-negative integer, got %d: %w", intVal, domain.ErrValueInvalid)
	}

	return nil
}

// validateLogLevel accepts only the standard structured log levels.
func validateLogLevel(value any) error {
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value: %w", domain.ErrValueInvalid)
	}

	switch strings.ToLower(strVal) {
	case "debug", "info", "warn", "error":
		return nil
	default:
		return fmt.Errorf("invalid log level %q, must be one of: debug, info, warn, error: %w", strVal, domain.ErrValueInvalid)
	}
}

// validateSSLMode accepts only valid PostgreSQL SSL modes.
func validateSSLMode(value any) error {
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value: %w", domain.ErrValueInvalid)
	}

	switch strVal {
	case "disable", "require", "verify-ca", "verify-full":
		return nil
	default:
		return fmt.Errorf("invalid SSL mode %q, must be one of: disable, require, verify-ca, verify-full: %w", strVal, domain.ErrValueInvalid)
	}
}

// validateOptionalSSLMode allows empty string (unset replica) or a valid SSL mode.
func validateOptionalSSLMode(value any) error {
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value: %w", domain.ErrValueInvalid)
	}

	if strVal == "" {
		return nil
	}

	return validateSSLMode(value)
}

// validateNonEmptyString rejects empty strings.
func validateNonEmptyString(value any) error {
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value: %w", domain.ErrValueInvalid)
	}

	if strings.TrimSpace(strVal) == "" {
		return fmt.Errorf("value must not be empty: %w", domain.ErrValueInvalid)
	}

	return nil
}

// toInt converts value to int64 for validation, handling int, int64, and
// whole-number float64 (which is how JSON numbers arrive).
func toInt(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		if typed == float64(int64(typed)) {
			return int64(typed), true
		}

		return 0, false
	default:
		return 0, false
	}
}
